package slowio

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func SetupOTelSDK(exporter sdktrace.SpanExporter) (shutdown func(context.Context) error) {
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second)))
	shutdown = tracerProvider.Shutdown
	otel.SetTracerProvider(tracerProvider)
	return
}
