package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/XSAM/otelsql"
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
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
)

var db *sqlx.DB

type User struct {
	User string `db:"User"`
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
		semconv.ServiceNameKey.String("demo_app"),
		semconv.ServiceVersionKey.String("0.0.1"),
		attribute.String("environment", "dev"),
	)
}

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
	tracer := tp.Tracer("demo_app")
	// currently the exporter depends on this propagator; could extend to allow others
	otel.SetTextMapPropagator(propagation.TraceContext{})
	/* tracing setup end */

	tracedDB, err := otelsql.Open("mysql", "root:@tcp(localhost:3306)/mysql",
		otelsql.WithAttributes(
			semconv.DBSystemMySQL,
		),
		// NB the SQL commenter injects trace propagation headers into SQL statements as comments
		// this format is technically up in the air as per https://github.com/open-telemetry/opentelemetry-specification/issues/2279
		otelsql.WithSQLCommenter(true),
	)
	defer tracedDB.Close()
	if err != nil {
		panic(err)
	}
	err = otelsql.RegisterDBStatsMetrics(tracedDB, otelsql.WithAttributes(
		semconv.DBSystemMySQL,
	))
	if err != nil {
		panic(err)
	}
	db = sqlx.NewDb(tracedDB, "mysql")

	http.HandleFunc("/user", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(ctx, "query")
		defer span.End()
		span.AddEvent("ima do a query now")

		var unused []User
		err = db.SelectContext(
			ctx,
			&unused,
			"SELECT User FROM user FOR UPDATE",
		)
		if err != nil {
			span.RecordError(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		span.AddEvent("i did it")
		w.Write([]byte(fmt.Sprintf("created a new query span with id %s\n", span.SpanContext().SpanID())))
	})

	http.HandleFunc("/lock_users", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(ctx, "insert")
		defer span.End()

		tx, err := db.Begin()
		if err != nil {
			span.RecordError(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer tx.Commit()

		_, err = tx.ExecContext(
			ctx,
			"SELECT User FROM user FOR UPDATE",
		)
		if err != nil {
			span.RecordError(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// take a nap
		span.AddEvent("im sleepy")
		time.Sleep(10 * time.Second)
		span.AddEvent("ok releasing lock")

		w.Write([]byte(fmt.Sprintf("created a new query span with id %s\n", span.SpanContext().SpanID())))
	})

	fmt.Println("starting server on :8000")
	err = http.ListenAndServe(":8000", nil)
	if err != nil {
		fmt.Println(err)
	}
}
