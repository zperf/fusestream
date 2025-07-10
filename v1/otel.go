package fusestream

import (
	"context"
	"time"

	"github.com/negrel/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/fanyang89/slowio"

var tracer trace.Tracer

func SetupOTelSDK(exporter sdktrace.SpanExporter) {
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNamespaceKey.String("zbs"),
			semconv.ServiceNameKey.String("slowio"),
		),
	)
	assert.NoError(err)

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res), sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second)))
	otel.SetTracerProvider(tracerProvider)
	tracer = tracerProvider.Tracer(tracerName)
	return
}

type IORecord struct {
	Name        string
	StartTimeNs int64
	ElapsedNs   int64
	Offset      int64
	Length      int32
	Path        string
}

func NewIORecord(span sdktrace.ReadOnlySpan) IORecord {
	r := IORecord{
		Name:        span.Name(),
		StartTimeNs: span.StartTime().UnixNano(),
		ElapsedNs:   span.EndTime().Sub(span.StartTime()).Nanoseconds(),
	}
	r.FromAttributes(span.Attributes())
	return r
}

func (r *IORecord) FromAttributes(attrs []attribute.KeyValue) {
	n := 0
	for _, attr := range attrs {
		switch attr.Key {
		case "offset":
			r.Offset = attr.Value.AsInt64()
			n |= 0b1
		case "length":
			r.Length = int32(attr.Value.AsInt64())
			n |= 0b10
		case "path":
			r.Path = attr.Value.AsString()
			n |= 0b100
		}
		if n == 0b111 {
			break
		}
	}
}
