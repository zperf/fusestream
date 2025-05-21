package slowio

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/fanyang89/slowio"

var tracer trace.Tracer

func SetupOTelSDK(exporter sdktrace.SpanExporter) (shutdown func(context.Context) error) {
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second)))
	shutdown = tracerProvider.Shutdown
	otel.SetTracerProvider(tracerProvider)
	tracer = tracerProvider.Tracer(tracerName)
	return
}
