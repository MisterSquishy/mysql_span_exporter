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

type BaseEvent struct {
	ThreadId   int    `db:"THREAD_ID"`
	EventId    int    `db:"EVENT_ID"`
	EndEventId int    `db:"END_EVENT_ID"`
	EventName  string `db:"EVENT_NAME"`
	TimerStart int    `db:"TIMER_START"`
	TimerEnd   int    `db:"TIMER_END"`
	TimerWait  int    `db:"TIMER_WAIT"`
}

type TransactionEvent struct {
	BaseEvent
	State string `db:"STATE"`
	GTID  string `db:"GTID"`
}

type StatementEvent struct {
	BaseEvent
	LockTime             int            `db:"LOCK_TIME"`
	SqlText              sql.NullString `db:"SQL_TEXT"`
	MySQLErrNo           int            `db:"MYSQL_ERRNO"`
	ReturnedSQLState     sql.NullString `db:"RETURNED_SQLSTATE"`
	MessageText          sql.NullString `db:"MESSAGE_TEXT"`
	Errors               bool           `db:"ERRORS"`
	Warnings             int            `db:"WARNINGS"`
	RowsAffected         int            `db:"ROWS_AFFECTED"`
	RowsSent             int            `db:"ROWS_SENT"`
	RowsExamined         int            `db:"ROWS_EXAMINED"`
	CreatedTmpDiskTables int            `db:"CREATED_TMP_DISK_TABLES"`
	CreatedTmpTables     int            `db:"CREATED_TMP_TABLES"`
	SelectFullJoin       int            `db:"SELECT_FULL_JOIN"`
	SelectFullRangeJoin  int            `db:"SELECT_FULL_RANGE_JOIN"`
	SelectRange          int            `db:"SELECT_RANGE"`
	SelectRangeCheck     int            `db:"SELECT_RANGE_CHECK"`
	SelectScan           int            `db:"SELECT_SCAN"`
	SortMergePasses      int            `db:"SORT_MERGE_PASSES"`
	SortRange            int            `db:"SORT_RANGE"`
	SortRows             int            `db:"SORT_ROWS"`
	SortScan             int            `db:"SORT_SCAN"`
	NoIndexUsed          bool           `db:"NO_INDEX_USED"`
	NoGoodIndexUsed      bool           `db:"NO_GOOD_INDEX_USED"`
	NestingEventID       sql.NullInt64  `db:"NESTING_EVENT_ID"`
	NestingEventType     sql.NullString `db:"NESTING_EVENT_TYPE"`
	NestingEventLevel    sql.NullString `db:"NESTING_EVENT_LEVEL"`
}

type StageEvent struct {
	BaseEvent
	WorkCompleted int `db:"WORK_COMPLETED"`
	WorkEstimated int `db:" WORK_ESTIMATED"`
}

type WaitEvent struct {
	BaseEvent
	Spins               int            `db:"SPINS"`
	ObjectSchema        sql.NullString `db:"OBJECT_SCHEMA"`
	ObjectName          sql.NullString `db:"OBJECT_NAME"`
	ObjectType          sql.NullString `db:"OBJECT_TYPE"`
	ObjectInstanceBegin sql.NullString `db:"OBJECT_INSTANCE_BEGIN"`
	IndexName           sql.NullString `db:"INDEX_NAME"`
	Operation           sql.NullString `db:"OPERATION"`
	NumberOfBytes       int            `db:"NUMBER_OF_BYTES"`
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
	appendField := func(f reflect.StructField) {
		if f.Type != reflect.TypeOf(BaseEvent{}) {
			v := strings.Split(f.Tag.Get("db"), ",")[0] // use split to ignore tag "options" like omitempty, etc
			columns = append(columns, v)
		}
	}
	bt := reflect.TypeOf(BaseEvent{})
	for i := 0; i < bt.NumField(); i++ {
		appendField(bt.Field(i))
	}
	for i := 0; i < rt.NumField(); i++ {
		appendField(rt.Field(i))
	}
	return strings.Join(columns, ",")
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

	statementEvents := []StatementEvent{}
	err = db.SelectContext(
		ctx,
		&statementEvents,
		// todo where timestamp > previous poll
		// todo join up to other "nested" tables TransactionEvent => StatementEvent => StageEvent => WaitEvent
		fmt.Sprintf("SELECT %s FROM events_statements_history_long", Columns(StatementEvent{})),
	)
	if err != nil {
		panic(err)
	}

	for _, statementEvent := range statementEvents {
		if statementEvent.SqlText.Valid {
			traceparentMatches := traceparentRegex.FindStringSubmatch(statementEvent.SqlText.String)
			if len(traceparentMatches) == 2 {
				propagator := propagation.TraceContext{}
				carrier := propagation.MapCarrier{}
				carrier.Set("traceparent", traceparentMatches[1])
				ctx = propagator.Extract(ctx, carrier)

				// statementEvent.TimerStart is in picoseconds since the start of the server
				// fixme or does the timer start after the server? how do i figure out when the timer started???
				startTime := timeOrigin[0].Started.Add(time.Duration(statementEvent.TimerStart) * time.Nanosecond / 1000)
				_, span := otel.Tracer("test_tracer").Start(ctx, statementEvent.EventName, oteltrace.WithTimestamp(startTime))
				span.SetAttributes(
					// todo add all attributes from struct
					attribute.String("sql_text", statementEvent.SqlText.String),
					attribute.Int("rows_sent", statementEvent.RowsSent),
					attribute.Int("rows_examined", statementEvent.RowsExamined),
					attribute.Int("rows_affected", statementEvent.RowsAffected),
				)
				endTime := timeOrigin[0].Started.Add(time.Duration(statementEvent.TimerEnd) * time.Nanosecond / 1000)
				span.End(oteltrace.WithTimestamp(endTime))
			}
		}
	}
}
