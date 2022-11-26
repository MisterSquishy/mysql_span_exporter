package main

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func newGRPCExporter(ctx context.Context) *otlptrace.Exporter {
	var headers = map[string]string{
		"lightstep-access-token": "EcBjBBDRmEx/0rSEEjwaDmu17kRjCNYDoErvoqsLSd+/ZpOE0tWr6cv/wVW5+WShxA+cg/f1T6t2gsnhjqFStbuiwhnfFf0LirNMRyr9",
	}

	client := otlptracegrpc.NewClient(
		otlptracegrpc.WithHeaders(headers),
		otlptracegrpc.WithEndpoint("ingest.lightstep.com:443"),
	)
	exp, err := otlptrace.New(ctx, client)
	if err != nil {
		panic(err)
	}
	return exp
}

func newResource() *resource.Resource {
	return resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("mysql"),
		semconv.ServiceVersionKey.String("0.0.1"),
		attribute.String("environment", "dev"),
	)
}
