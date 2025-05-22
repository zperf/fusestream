package slowio

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/fanyang89/slowio"

var tracer trace.Tracer

func SetupOTelSDK(exporter sdktrace.SpanExporter) (shutdown func(context.Context) error) {
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNamespaceKey.String("zbs"),
			semconv.ServiceNameKey.String("slowio"),
		),
	)
	if err != nil {
		log.Panic().Err(err).Msg("failed to create resource")
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(time.Minute),
			sdktrace.WithMaxExportBatchSize(10000),
			sdktrace.WithMaxQueueSize(100000),
		))
	shutdown = tracerProvider.Shutdown
	otel.SetTracerProvider(tracerProvider)
	tracer = tracerProvider.Tracer(tracerName)
	return
}
