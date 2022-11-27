package main

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/XSAM/otelsql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	scrapePeriod = 5 * time.Second
)

var traceparentRegex = regexp.MustCompile(`traceparent='([^\s]+)'`)

var tracer oteltrace.Tracer
var db *sqlx.DB

func main() {
	ctx := context.Background()

	tp := trace.NewTracerProvider(
		trace.WithBatcher(newGRPCExporter(ctx)),
		trace.WithResource(newResource()),
	)
	defer tp.Shutdown(ctx)
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer("mysql")
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tracedDB, err := otelsql.Open("mysql", "root:@tcp(localhost:3306)/performance_schema?parseTime=true&loc=UTC",
		otelsql.WithAttributes(
			semconv.DBSystemMySQL,
		),
		// NB the SQL commenter injects trace propagation headers into SQL statements as comments
		// this format is technically up in the air as per https://github.com/open-telemetry/opentelemetry-specification/issues/2279
		otelsql.WithSQLCommenter(true),
	)
	defer func() {
		err = tracedDB.Close()
		if err != nil {
			panic(err)
		}
	}()
	if err != nil {
		panic(err)
	}
	db = sqlx.NewDb(tracedDB, "mysql")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	ticker := time.NewTicker(scrapePeriod)
	var cursor uint64
	for range ticker.C {
		fmt.Printf("scraping events ending after %d\n", cursor)
		cursor = recordSpans(cursor)
	}
}

func recordSpans(cursor uint64) (newCursor uint64) {
	ctx := context.Background()
	ctx, recordSpansSpan := tracer.Start(ctx, "record_spans")
	defer recordSpansSpan.End()
	recordSpansSpan.AddEvent("issuing query", oteltrace.WithAttributes(attribute.Int64("cursor", int64(cursor))))

	timeOrigin, err := getTimeOrigin(ctx)
	if err != nil {
		panic(err)
	}

	// extract events from the performance schema
	events, err := extractEvents(ctx, cursor)
	if err != nil {
		panic(err)
	} else if len(events) == 0 {
		// no events in previous period, shortcircuit
		return
	} else {
		newCursor = uint64(events[0].StatementEvent.TimerEnd)
	}

	// transform events into tree structure
	statementsById := map[uniqueEventId]StatementEvent{}
	stagesById := map[uniqueEventId]StageEvent{}
	waitsById := map[uniqueEventId]WaitEvent{}
	transactionsByStatementId := map[uniqueEventId]TransactionEvent{}
	stagesByStatementId := map[uniqueEventId][]StageEvent{}
	waitsByStageId := map[uniqueEventId][]WaitEvent{}
	for _, event := range events {
		statementId := event.StatementEvent.uniqueId()
		stageId := event.StageEvent.uniqueId()
		waitId := event.WaitEvent.uniqueId()
		if _, ok := statementsById[statementId]; event.StatementEvent.EventId.Valid && !ok {
			statementsById[statementId] = event.StatementEvent
		}
		if _, ok := transactionsByStatementId[statementId]; event.TransactionEvent.EventId.Valid && !ok {
			transactionsByStatementId[statementId] = event.TransactionEvent
		}
		if _, ok := stagesById[stageId]; event.StageEvent.EventId.Valid && !ok {
			stagesById[stageId] = event.StageEvent
			stagesByStatementId[statementId] = append(stagesByStatementId[statementId], event.StageEvent)
		}
		if _, ok := waitsById[waitId]; event.WaitEvent.EventId.Valid && !ok {
			waitsById[waitId] = event.WaitEvent
			waitsByStageId[stageId] = append(waitsByStageId[stageId], event.WaitEvent)
		}
	}

	// load the events as spans
	for statementId, statement := range statementsById {
		if statement.SqlText.Valid {
			traceparentMatches := traceparentRegex.FindStringSubmatch(statement.SqlText.String)
			if len(traceparentMatches) == 2 {
				propagator := propagation.TraceContext{}
				carrier := propagation.MapCarrier{}
				carrier.Set("traceparent", traceparentMatches[1])
				ctx = propagator.Extract(ctx, carrier)
			} else {
				ctx = context.Background()
			}

			var txnSpan oteltrace.Span
			if transaction, ok := transactionsByStatementId[statementId]; ok {
				txnSpan = transaction.toSpan(ctx, timeOrigin)
			}
			statementSpan := statement.toSpan(ctx, timeOrigin, txnSpan)
			stages := stagesByStatementId[statementId]
			for _, stage := range stages {
				stageSpan := stage.toSpan(ctx, timeOrigin, statementSpan)
				waits := waitsByStageId[stage.uniqueId()]
				for _, wait := range waits {
					// todo link to the span holding the lock??
					wait.toSpan(ctx, timeOrigin, stageSpan)
				}
			}
		}
	}

	return newCursor
}

func getTimeOrigin(ctx context.Context) (time.Time, error) {
	ctx, sp := tracer.Start(ctx, "get_time_origin")
	defer sp.End()
	timeOrigins := []TimeOrigin{}
	err := db.SelectContext(
		ctx,
		&timeOrigins,
		"SELECT "+
			"FROM_UNIXTIME((((UNIX_TIMESTAMP(CURTIME()) * 1000000) + MICROSECOND(CURTIME(6))) - (TIMER_START+TIMER_WAIT)/1000000)/1000000) as STARTED "+
			"FROM performance_schema.events_statements_current WHERE END_EVENT_ID IS NULL "+
			"LIMIT 1",
	)
	if err != nil {
		sp.RecordError(err)
		return time.Time{}, err
	} else if len(timeOrigins) != 1 {
		err = fmt.Errorf("unexpected timeorigin %v", timeOrigins)
		sp.RecordError(err)
		return time.Time{}, err
	}

	timeOrigin := timeOrigins[0].Started
	sp.AddEvent("got time origin", oteltrace.WithAttributes(attribute.String("timeOrigin", timeOrigin.String())))
	return timeOrigin, nil
}

func extractEvents(ctx context.Context, cursor uint64) ([]Event, error) {
	ctx, sp := tracer.Start(ctx, "extract_events")
	defer sp.End()

	events := []Event{}
	err := db.SelectContext(
		ctx,
		&events,
		fmt.Sprintf(
			"SELECT %s,%s,%s,%s FROM events_statements_history_long se "+
				"LEFT JOIN events_transactions_history_long te ON se.NESTING_EVENT_ID=te.EVENT_ID AND se.THREAD_ID=te.THREAD_ID "+
				"LEFT JOIN events_stages_history_long stge ON stge.NESTING_EVENT_ID=se.EVENT_ID AND stge.THREAD_ID=se.THREAD_ID "+
				"LEFT JOIN events_waits_history_long we ON we.NESTING_EVENT_ID=stge.EVENT_ID AND we.THREAD_ID=stge.THREAD_ID "+
				fmt.Sprintf("WHERE se.TIMER_START > %d ", cursor)+
				"ORDER BY se.TIMER_END DESC",
			Columns(TransactionEvent{}),
			Columns(StatementEvent{}),
			Columns(StageEvent{}),
			Columns(WaitEvent{}),
		),
	)
	if err != nil {
		sp.RecordError(err)
		return nil, err
	}
	sp.AddEvent("scraped_events", oteltrace.WithAttributes(attribute.Int("num_events", len(events))))
	return events, nil
}
