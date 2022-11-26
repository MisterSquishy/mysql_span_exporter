package main

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type TransactionEvent struct {
	ThreadId   sql.NullInt64  `db:"te.THREAD_ID"`
	EventId    sql.NullInt64  `db:"te.EVENT_ID"`
	EndEventId sql.NullInt64  `db:"te.END_EVENT_ID"`
	EventName  sql.NullString `db:"te.EVENT_NAME"`
	TimerStart NullUInt64     `db:"te.TIMER_START"`
	TimerEnd   NullUInt64     `db:"te.TIMER_END"`
	TimerWait  NullUInt64     `db:"te.TIMER_WAIT"`
	State      sql.NullString `db:"te.STATE"`
	GTID       sql.NullString `db:"te.GTID"`
}

type StatementEvent struct {
	ThreadId             sql.NullInt64  `db:"se.THREAD_ID"`
	EventId              sql.NullInt64  `db:"se.EVENT_ID"`
	EndEventId           sql.NullInt64  `db:"se.END_EVENT_ID"`
	EventName            sql.NullString `db:"se.EVENT_NAME"`
	TimerStart           NullUInt64     `db:"se.TIMER_START"`
	TimerEnd             NullUInt64     `db:"se.TIMER_END"`
	TimerWait            NullUInt64     `db:"se.TIMER_WAIT"`
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
	TimerStart    NullUInt64     `db:"stge.TIMER_START"`
	TimerEnd      NullUInt64     `db:"stge.TIMER_END"`
	TimerWait     NullUInt64     `db:"stge.TIMER_WAIT"`
	WorkCompleted sql.NullInt64  `db:"stge.WORK_COMPLETED"`
	WorkEstimated sql.NullInt64  `db:"stge.WORK_ESTIMATED"`
}

type WaitEvent struct {
	ThreadId            sql.NullInt64  `db:"we.THREAD_ID"`
	EventId             sql.NullInt64  `db:"we.EVENT_ID"`
	EndEventId          sql.NullInt64  `db:"we.END_EVENT_ID"`
	EventName           sql.NullString `db:"we.EVENT_NAME"`
	TimerStart          NullUInt64     `db:"we.TIMER_START"`
	TimerEnd            NullUInt64     `db:"we.TIMER_END"`
	TimerWait           NullUInt64     `db:"we.TIMER_WAIT"`
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

// unique IDs are formed from event ID and thread ID
type uniqueEventId string

func (s StatementEvent) uniqueId() uniqueEventId {
	return uniqueEventId(fmt.Sprintf("%d-%d", s.EventId.Int64, s.ThreadId.Int64))
}
func (s StageEvent) uniqueId() uniqueEventId {
	return uniqueEventId(fmt.Sprintf("%d-%d", s.EventId.Int64, s.ThreadId.Int64))
}
func (w WaitEvent) uniqueId() uniqueEventId {
	return uniqueEventId(fmt.Sprintf("%d-%d", w.EventId.Int64, w.ThreadId.Int64))
}

func (t TransactionEvent) toSpan(ctx context.Context, timeOrigin time.Time) oteltrace.Span {
	// TimerStart is in picoseconds since the start of the server
	startTime := timeOrigin.Add(time.Duration(t.TimerStart) * time.Nanosecond / 1000)
	_, transactionSpan := tracer.Start(ctx, t.EventName.String, oteltrace.WithTimestamp(startTime))
	transactionSpan.SetAttributes(
		// todo add more attributes from struct
		attribute.Int64("event_id", t.EventId.Int64),
		attribute.Int64("thread_id", t.ThreadId.Int64),
		attribute.String("state", t.State.String),
		attribute.String("GTID", t.GTID.String),
	)
	endTime := timeOrigin.Add(time.Duration(t.TimerEnd) * time.Nanosecond / 1000)
	transactionSpan.End(oteltrace.WithTimestamp(endTime))
	return transactionSpan
}
func (s StatementEvent) toSpan(ctx context.Context, timeOrigin time.Time, txnSpan oteltrace.Span) oteltrace.Span {
	if txnSpan != nil {
		ctx = oteltrace.ContextWithSpanContext(ctx, txnSpan.SpanContext())
	}
	// statementEvent.TimerStart is in picoseconds since the start of the server
	startTime := timeOrigin.Add(time.Duration(s.TimerStart) * time.Nanosecond / 1000)
	_, statementSpan := tracer.Start(ctx, s.EventName.String, oteltrace.WithTimestamp(startTime))
	statementSpan.SetAttributes(
		// todo add more attributes from struct
		attribute.Int64("event_id", s.EventId.Int64),
		attribute.Int64("thread_id", s.ThreadId.Int64),
		attribute.String("sql_text", s.SqlText.String),
		attribute.Int("rows_sent", int(s.RowsSent.Int64)),
		attribute.Int("rows_examined", int(s.RowsExamined.Int64)),
		attribute.Int("rows_affected", int(s.RowsAffected.Int64)),
	)
	endTime := timeOrigin.Add(time.Duration(s.TimerEnd) * time.Nanosecond / 1000)
	statementSpan.End(oteltrace.WithTimestamp(endTime))
	return statementSpan
}
func (s StageEvent) toSpan(ctx context.Context, timeOrigin time.Time, statementSpan oteltrace.Span) oteltrace.Span {
	if statementSpan != nil {
		ctx = oteltrace.ContextWithSpanContext(ctx, statementSpan.SpanContext())
	}

	startTime := timeOrigin.Add(time.Duration(s.TimerStart) * time.Nanosecond / 1000)
	_, stageSpan := tracer.Start(ctx, s.EventName.String, oteltrace.WithTimestamp(startTime))
	stageSpan.SetAttributes(
		attribute.Int64("event_id", s.EventId.Int64),
		attribute.Int64("thread_id", s.ThreadId.Int64),
		attribute.Int64("work_completed", s.WorkCompleted.Int64),
		attribute.Int64("work_estimated", s.WorkEstimated.Int64),
	)
	endTime := timeOrigin.Add(time.Duration(s.TimerEnd) * time.Nanosecond / 1000)
	stageSpan.End(oteltrace.WithTimestamp(endTime))
	return stageSpan
}
func (w WaitEvent) toSpan(ctx context.Context, timeOrigin time.Time, stageSpan oteltrace.Span) oteltrace.Span {
	if stageSpan != nil {
		ctx = oteltrace.ContextWithSpanContext(ctx, stageSpan.SpanContext())
	}

	startTime := timeOrigin.Add(time.Duration(w.TimerStart) * time.Nanosecond / 1000)
	_, waitSpan := tracer.Start(ctx, w.EventName.String, oteltrace.WithTimestamp(startTime))
	waitSpan.SetAttributes(
		attribute.Int64("event_id", w.EventId.Int64),
		attribute.Int64("thread_id", w.ThreadId.Int64),
		attribute.Int64("spins", w.Spins.Int64),
		attribute.String("operation", w.Operation.String),
		attribute.String("object_schema", w.ObjectSchema.String),
		attribute.String("object_name", w.ObjectName.String),
		attribute.String("object_type", w.ObjectType.String),
		attribute.String("object_instance_begin", w.ObjectInstanceBegin.String),
	)
	endTime := timeOrigin.Add(time.Duration(w.TimerEnd) * time.Nanosecond / 1000)
	waitSpan.End(oteltrace.WithTimestamp(endTime))
	return waitSpan
}

type TimeOrigin struct {
	Started time.Time `db:"STARTED"`
}

// this foolishness is a minimal implementation of sql.NullUInt64, which doesnt exist for some reason
// NB the timer types might eventually overflow UInt64 as well... 🤔
type NullUInt64 uint64

func (n *NullUInt64) Scan(value any) error {
	if value == nil {
		*n = 0
		return nil
	}
	switch typedVal := value.(type) {
	case []byte:
		parsed, err := strconv.ParseUint(string(typedVal), 10, 64)
		if err != nil {
			return err
		}
		*n = NullUInt64(parsed)
	default:
		return fmt.Errorf("invalid type for NullUInt64: %V", value)
	}

	return nil
}

// this is a bummer... convert struct to comma-separated list of column names/aliases for use in SELECT statements
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
