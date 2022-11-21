package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var tracer oteltrace.Tracer

func newGRPCExporter(ctx context.Context) (*otlptrace.Exporter, error) {
	var headers = map[string]string{
		"lightstep-access-token": "EcBjBBDRmEx/0rSEEjwaDmu17kRjCNYDoErvoqsLSd+/ZpOE0tWr6cv/wVW5+WShxA+cg/f1T6t2gsnhjqFStbuiwhnfFf0LirNMRyr9",
	}

	client := otlptracegrpc.NewClient(
		otlptracegrpc.WithHeaders(headers),
		otlptracegrpc.WithEndpoint("ingest.lightstep.com:443"),
	)
	return otlptrace.New(ctx, client)
}

func newExporter(w io.Writer) (trace.SpanExporter, error) {
	return stdouttrace.New(
		stdouttrace.WithWriter(w),
		// Use human-readable output.
		stdouttrace.WithPrettyPrint(),
	)
}

func newResource() *resource.Resource {
	return resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("mysql_span_exporter_test"),
		semconv.ServiceVersionKey.String("0.0.1"),
		attribute.String("environment", "dev"),
	)
}

var traceparentRegex = regexp.MustCompile(`traceparent: ([^\s]+)`)

type TransactionEvent struct {
	ThreadId   sql.NullInt64  `db:"te.THREAD_ID"`
	EventId    sql.NullInt64  `db:"te.EVENT_ID"`
	EndEventId sql.NullInt64  `db:"te.END_EVENT_ID"`
	EventName  sql.NullString `db:"te.EVENT_NAME"`
	TimerStart sql.NullInt64  `db:"te.TIMER_START"`
	TimerEnd   sql.NullInt64  `db:"te.TIMER_END"`
	TimerWait  sql.NullInt64  `db:"te.TIMER_WAIT"`
	State      sql.NullString `db:"te.STATE"`
	GTID       sql.NullString `db:"te.GTID"`
}

type StatementEvent struct {
	ThreadId             sql.NullInt64  `db:"se.THREAD_ID"`
	EventId              sql.NullInt64  `db:"se.EVENT_ID"`
	EndEventId           sql.NullInt64  `db:"se.END_EVENT_ID"`
	EventName            sql.NullString `db:"se.EVENT_NAME"`
	TimerStart           sql.NullInt64  `db:"se.TIMER_START"`
	TimerEnd             sql.NullInt64  `db:"se.TIMER_END"`
	TimerWait            sql.NullInt64  `db:"se.TIMER_WAIT"`
	LockTime             sql.NullInt64  `db:"se.LOCK_TIME"`
	SqlText              sql.NullString `db:"se.SQL_TEXT"`
	MySQLErrNo           sql.NullInt64  `db:"se.MYSQL_ERRNO"`
	ReturnedSQLState     sql.NullString `db:"se.RETURNED_SQLSTATE"`
	MessageText          sql.NullString `db:"se.MESSAGE_TEXT"`
	Errors               bool           `db:"se.ERRORS"`
	Warnings             sql.NullInt64  `db:"se.WARNINGS"`
	RowsAffected         sql.NullInt64  `db:"se.ROWS_AFFECTED"`
	RowsSent             sql.NullInt64  `db:"se.ROWS_SENT"`
	RowsExamined         sql.NullInt64  `db:"se.ROWS_EXAMINED"`
	CreatedTmpDiskTables sql.NullInt64  `db:"se.CREATED_TMP_DISK_TABLES"`
	CreatedTmpTables     sql.NullInt64  `db:"se.CREATED_TMP_TABLES"`
	SelectFullJoin       sql.NullInt64  `db:"se.SELECT_FULL_JOIN"`
	SelectFullRangeJoin  sql.NullInt64  `db:"se.SELECT_FULL_RANGE_JOIN"`
	SelectRange          sql.NullInt64  `db:"se.SELECT_RANGE"`
	SelectRangeCheck     sql.NullInt64  `db:"se.SELECT_RANGE_CHECK"`
	SelectScan           sql.NullInt64  `db:"se.SELECT_SCAN"`
	SortMergePasses      sql.NullInt64  `db:"se.SORT_MERGE_PASSES"`
	SortRange            sql.NullInt64  `db:"se.SORT_RANGE"`
	SortRows             sql.NullInt64  `db:"se.SORT_ROWS"`
	SortScan             sql.NullInt64  `db:"se.SORT_SCAN"`
	NoIndexUsed          bool           `db:"se.NO_INDEX_USED"`
	NoGoodIndexUsed      bool           `db:"se.NO_GOOD_INDEX_USED"`
	NestingEventID       sql.NullInt64  `db:"se.NESTING_EVENT_ID"`
	NestingEventType     sql.NullString `db:"se.NESTING_EVENT_TYPE"`
	NestingEventLevel    sql.NullString `db:"se.NESTING_EVENT_LEVEL"`
}

type StageEvent struct {
	ThreadId      sql.NullInt64  `db:"stge.THREAD_ID"`
	EventId       sql.NullInt64  `db:"stge.EVENT_ID"`
	EndEventId    sql.NullInt64  `db:"stge.END_EVENT_ID"`
	EventName     sql.NullString `db:"stge.EVENT_NAME"`
	TimerStart    sql.NullInt64  `db:"stge.TIMER_START"`
	TimerEnd      sql.NullInt64  `db:"stge.TIMER_END"`
	TimerWait     sql.NullInt64  `db:"stge.TIMER_WAIT"`
	WorkCompleted sql.NullInt64  `db:"stge.WORK_COMPLETED"`
	WorkEstimated sql.NullInt64  `db:"stge.WORK_ESTIMATED"`
}

type WaitEvent struct {
	ThreadId            sql.NullInt64  `db:"we.THREAD_ID"`
	EventId             sql.NullInt64  `db:"we.EVENT_ID"`
	EndEventId          sql.NullInt64  `db:"we.END_EVENT_ID"`
	EventName           sql.NullString `db:"we.EVENT_NAME"`
	TimerStart          sql.NullInt64  `db:"we.TIMER_START"`
	TimerEnd            sql.NullInt64  `db:"we.TIMER_END"`
	TimerWait           sql.NullInt64  `db:"we.TIMER_WAIT"`
	Spins               sql.NullInt64  `db:"we.SPINS"`
	ObjectSchema        sql.NullString `db:"we.OBJECT_SCHEMA"`
	ObjectName          sql.NullString `db:"we.OBJECT_NAME"`
	ObjectType          sql.NullString `db:"we.OBJECT_TYPE"`
	ObjectInstanceBegin sql.NullString `db:"we.OBJECT_INSTANCE_BEGIN"`
	IndexName           sql.NullString `db:"we.INDEX_NAME"`
	Operation           sql.NullString `db:"we.OPERATION"`
	NumberOfBytes       sql.NullInt64  `db:"we.NUMBER_OF_BYTES"`
}

type Event struct {
	TransactionEvent
	StatementEvent
	StageEvent
	WaitEvent
}

// it's frankly bananas that i have to write this
func Columns(i interface{}) string {
	rt := reflect.TypeOf(i)
	if rt.Kind() == reflect.Array || rt.Kind() == reflect.Slice || rt.Kind() == reflect.Map || rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return ""
	}
	columns := make([]string, 0)
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		v := strings.Split(f.Tag.Get("db"), ",")[0] // use split to ignore tag "options" like omitempty, etc
		columns = append(columns, v+" as '"+v+"'")
	}
	return strings.Join(columns, ",")
}

type TimeOrigin struct {
	Started time.Time `db:"STARTED"`
}

func main() {
	ctx := context.Background()

	/* tracing setup start */
	f, err := os.Create("traces.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	exp, err := newExporter(f)
	if err != nil {
		panic(err)
	}

	grpcexp, err := newGRPCExporter(ctx)
	if err != nil {
		panic(err)
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithBatcher(grpcexp),
		trace.WithResource(newResource()),
	)
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			panic(err)
		}
	}()
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer("mysql_exporter_test")
	/* tracing setup end */

	db, err := sqlx.Connect("mysql", "root:@tcp(127.0.0.1:3306)/performance_schema?parseTime=true&loc=UTC")
	if err != nil {
		panic(err)
	}
	query()
	defer db.Close()

	timeOrigin := []TimeOrigin{}
	err = db.SelectContext(
		ctx,
		&timeOrigin,
		"select "+
			"FROM_UNIXTIME((((UNIX_TIMESTAMP(CURTIME()) * 1000000) + MICROSECOND(CURTIME(6))) - (TIMER_START+TIMER_WAIT)/1000000)/1000000) as STARTED "+
			"FROM performance_schema.events_statements_current WHERE END_EVENT_ID IS NULL",
	)
	if err != nil {
		panic(err)
	} else if len(timeOrigin) != 1 {
		panic(fmt.Errorf("unexpected timeorigin %v", timeOrigin))
	}

	events := []Event{}
	err = db.SelectContext(
		ctx,
		&events,
		// todo where timestamp > previous poll
		fmt.Sprintf(
			"SELECT %s,%s,%s,%s FROM events_statements_history_long se "+
				"LEFT JOIN events_transactions_history_long te ON se.NESTING_EVENT_ID=te.EVENT_ID "+
				"LEFT JOIN events_stages_history_long stge ON stge.NESTING_EVENT_ID=se.EVENT_ID "+
				"LEFT JOIN events_waits_history_long we ON we.NESTING_EVENT_ID=stge.EVENT_ID",
			Columns(TransactionEvent{}),
			Columns(StatementEvent{}),
			Columns(StageEvent{}),
			Columns(WaitEvent{}),
		),
	)
	if err != nil {
		panic(err)
	}

	statementsById := map[int64]StatementEvent{}
	stagesById := map[int64]StageEvent{}
	waitsById := map[int64]WaitEvent{}
	stagesByStatementId := map[int64][]StageEvent{}
	waitsByStageId := map[int64][]WaitEvent{}
	for _, event := range events {
		statementId := event.StatementEvent.EventId.Int64
		stageId := event.StageEvent.EventId.Int64
		waitId := event.WaitEvent.EventId.Int64
		if _, ok := statementsById[statementId]; statementId > 0 && !ok {
			statementsById[statementId] = event.StatementEvent
		}
		if _, ok := stagesById[stageId]; stageId > 0 && !ok {
			stagesById[stageId] = event.StageEvent
			stagesByStatementId[statementId] = append(stagesByStatementId[statementId], event.StageEvent)
		}
		if _, ok := waitsById[waitId]; waitId > 0 && !ok {
			waitsById[waitId] = event.WaitEvent
			waitsByStageId[stageId] = append(waitsByStageId[stageId], event.WaitEvent)
		}
	}

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

			// statementEvent.TimerStart is in picoseconds since the start of the server
			// fixme or does the timer start after the server? how do i figure out when the timer started???
			startTime := timeOrigin[0].Started.Add(time.Duration(statement.TimerStart.Int64) * time.Nanosecond / 1000)
			statementCtx, statementSpan := tracer.Start(ctx, statement.EventName.String, oteltrace.WithTimestamp(startTime))
			statementSpan.SetAttributes(
				// todo add more attributes from struct
				attribute.String("sql_text", statement.SqlText.String),
				attribute.Int("rows_sent", int(statement.RowsSent.Int64)),
				attribute.Int("rows_examined", int(statement.RowsExamined.Int64)),
				attribute.Int("rows_affected", int(statement.RowsAffected.Int64)),
			)

			stages := stagesByStatementId[statementId]
			for _, stage := range stages {
				startTime := timeOrigin[0].Started.Add(time.Duration(stage.TimerStart.Int64) * time.Nanosecond / 1000)
				stageCtx, stageSpan := tracer.Start(statementCtx, stage.EventName.String, oteltrace.WithTimestamp(startTime))
				stageSpan.SetAttributes(
					attribute.Int64("work_completed", stage.WorkCompleted.Int64),
					attribute.Int64("work_estimated", stage.WorkEstimated.Int64),
				)

				waits := waitsByStageId[stage.EventId.Int64]
				for _, wait := range waits {
					startTime := timeOrigin[0].Started.Add(time.Duration(wait.TimerStart.Int64) * time.Nanosecond / 1000)
					_, waitSpan := tracer.Start(stageCtx, wait.EventName.String, oteltrace.WithTimestamp(startTime))
					waitSpan.SetAttributes(
						attribute.Int64("spins", wait.Spins.Int64),
						attribute.String("object_schema", wait.ObjectSchema.String),
						attribute.String("object_name", wait.ObjectName.String),
						attribute.String("object_type", wait.ObjectType.String),
						attribute.String("object_instance_begin", wait.ObjectInstanceBegin.String),
					)
					endTime := timeOrigin[0].Started.Add(time.Duration(wait.TimerEnd.Int64) * time.Nanosecond / 1000)
					waitSpan.End(oteltrace.WithTimestamp(endTime))

				}

				endTime := timeOrigin[0].Started.Add(time.Duration(stage.TimerEnd.Int64) * time.Nanosecond / 1000)
				stageSpan.End(oteltrace.WithTimestamp(endTime))
			}

			endTime := timeOrigin[0].Started.Add(time.Duration(statement.TimerEnd.Int64) * time.Nanosecond / 1000)
			statementSpan.End(oteltrace.WithTimestamp(endTime))
		}
	}
}
