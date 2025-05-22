package slowio

import (
	"context"
	"io"
	"sync"

	"github.com/negrel/assert"
	"github.com/parquet-go/parquet-go"
	"github.com/rs/zerolog/log"
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

	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(exporter))
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
}

type ParquetSpanExporter struct {
	outputFile io.WriteCloser
	c          chan *IORecord
	wg         sync.WaitGroup
}

func NewParquetSpanExporter(outputFile io.WriteCloser) *ParquetSpanExporter {
	e := &ParquetSpanExporter{
		outputFile: outputFile,
		c:          make(chan *IORecord, 100000),
	}
	e.wg.Add(1)
	go e.ioWorker(parquet.NewGenericWriter[IORecord](outputFile))
	return e
}

func (e *ParquetSpanExporter) ioWorker(w *parquet.GenericWriter[IORecord]) {
	log.Info().Msg("Parquet span exporter I/O worker started")
	defer func() {
		log.Info().Msg("Parquet span exporter I/O worker exiting")
		e.wg.Done()
	}()

	var err error

outerLoop:
	for {
		buf := make([]IORecord, 0)

		select {
		case r := <-e.c:
			if r == nil {
				break outerLoop
			} else {
				buf = append(buf, *r)
			}
		default:
		}

		_, err = w.Write(buf)
		assert.NoError(err)
	}

	err = w.Close()
	if err != nil {
		log.Error().Err(err).Msg("Close parquet writer failed")
	}

	err = e.outputFile.Close()
	if err != nil {
		log.Error().Err(err).Msg("Close output file failed")
	}
}

func getOffsetLength(attrs []attribute.KeyValue) (offset int64, rc int32) {
	offset = -1
	rc = -1

	for _, attr := range attrs {
		if attr.Key == "offset" {
			offset = attr.Value.AsInt64()
		} else if attr.Key == "rc" {
			rc = int32(attr.Value.AsInt64())
		}
		if rc >= 0 && offset >= 0 {
			break
		}
	}

	return
}

func (e *ParquetSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		for _, span := range spans {
			name := span.Name()
			r := &IORecord{
				Name:        name,
				StartTimeNs: span.StartTime().UnixNano(),
				ElapsedNs:   span.EndTime().Sub(span.StartTime()).Nanoseconds(),
			}
			if name == "fuse.Read" || name == "fuse.Write" {
				r.Offset, r.Length = getOffsetLength(span.Attributes())
			}
			e.c <- r
		}

	}
}

func (e *ParquetSpanExporter) Shutdown(ctx context.Context) error {
	_ = ctx
	e.c <- nil
	e.wg.Wait()
	return nil
}
