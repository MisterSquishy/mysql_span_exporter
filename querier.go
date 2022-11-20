package main

import (
	"context"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/propagation"
)

type User struct {
	User string `db:"User"`
}

func query() {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "query")
	defer span.End()
	span.AddEvent("ima connect now")
	db, err := sqlx.Connect("mysql", "root:@tcp(localhost:3306)/mysql")
	defer db.Close()
	if err != nil {
		span.RecordError(err)
		panic(err)
	}
	span.AddEvent("ima do a query now")

	propagator := propagation.TraceContext{}
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	defer db.Close()
	var unused []User
	err = db.Select(
		&unused,
		fmt.Sprintf("SELECT User FROM user /* traceparent: %s */", carrier.Get("traceparent")),
	)
	if err != nil {
		span.RecordError(err)
		panic(err)
	}
	span.AddEvent("i did it")
	fmt.Printf("created a new query span with id %s\n", span.SpanContext().SpanID())
}
