package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"regexp"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// prepend traceparent info as a comment in SQL_TEXT
// scrape this table
// generate spans for each event

func newExporter(w io.Writer) (trace.SpanExporter, error) {
	return stdouttrace.New(
		stdouttrace.WithWriter(w),
		// Use human-readable output.
		stdouttrace.WithPrettyPrint(),
	)
}

func newResource() *resource.Resource {
	r, _ := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("fib"),
			semconv.ServiceVersionKey.String("v0.1.0"),
			attribute.String("environment", "demo"),
		),
	)
	return r
}

var traceparentRegex = regexp.MustCompile(`traceparent: ([^\s]+)`)

type EventStatement struct {
	ThreadId   int            `db:"THREAD_ID"`
	EventId    int            `db:"EVENT_ID"`
	EndEventId int            `db:"END_EVENT_ID"`
	EventName  string         `db:"EVENT_NAME"`
	TimerStart int            `db:"TIMER_START"`
	TimerEnd   int            `db:"TIMER_END"`
	TimerWait  int            `db:"TIMER_WAIT"`
	LockTime   int            `db:"LOCK_TIME"`
	SqlText    sql.NullString `db:"SQL_TEXT"`
}

type TimeOrigin struct {
	Started time.Time `db:"STARTED"`
}

// todo make sure the performance events are being collected at the DB level
// https://dev.mysql.com/doc/refman/8.0/en/performance-schema-statement-tables.html#performance-schema-statement-tables-configuration

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

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(newResource()),
	)
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			panic(err)
		}
	}()
	otel.SetTracerProvider(tp)
	/* tracing setup end */

	db, err := sqlx.Connect("mysql", "root:@tcp(127.0.0.1:3306)/performance_schema?parseTime=true")
	if err != nil {
		panic(err)
	}
	query()
	defer db.Close()

	timeOrigin := []TimeOrigin{}
	err = db.SelectContext(ctx, &timeOrigin, "SELECT NOW() - INTERVAL variable_value MICROSECOND STARTED FROM performance_schema.global_status WHERE variable_name='Uptime'")
	if err != nil {
		panic(err)
	} else if len(timeOrigin) != 1 {
		panic(fmt.Errorf("unexpected timeorigin %v", timeOrigin))
	}

	eventStatements := []EventStatement{}
	err = db.SelectContext(
		ctx,
		&eventStatements,
		// todo where timestamp > previous poll
		"SELECT THREAD_ID,EVENT_ID,END_EVENT_ID,EVENT_NAME,TIMER_START,TIMER_END,TIMER_WAIT,LOCK_TIME,SQL_TEXT FROM events_statements_history_long",
	)
	if err != nil {
		panic(err)
	}

	for _, eventStatement := range eventStatements {
		if eventStatement.SqlText.Valid {
			traceparentMatches := traceparentRegex.FindStringSubmatch(eventStatement.SqlText.String)
			if len(traceparentMatches) == 2 {
				propagator := propagation.TraceContext{}
				carrier := propagation.MapCarrier{}
				carrier.Set("traceparent", traceparentMatches[1])
				ctx = propagator.Extract(ctx, carrier)

				// eventStatement.TimerStart is in picoseconds since the start of the server
				// fixme or does the timer start after the server? how do i figure out when the timer started???
				startTime := timeOrigin[0].Started.Add(time.Duration(eventStatement.TimerStart) * time.Nanosecond / 1000)
				_, span := otel.Tracer("test_tracer").Start(ctx, eventStatement.EventName, oteltrace.WithTimestamp(startTime))
				span.SetAttributes(
					attribute.String("sql_text", eventStatement.SqlText.String),
				)
				endTime := timeOrigin[0].Started.Add(time.Duration(eventStatement.TimerEnd) * time.Nanosecond / 1000)
				span.End(oteltrace.WithTimestamp(endTime))
			}
		}
	}
}
