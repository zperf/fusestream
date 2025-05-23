package slowio

import (
	"context"
	"io"
	"sync"

	"github.com/negrel/assert"
	"github.com/parquet-go/parquet-go"
	"github.com/rs/zerolog/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type ParquetSpanExporter struct {
	outputFile io.WriteCloser
	c          chan *IORecord
	wg         sync.WaitGroup
}

func NewParquetSpanExporter(outputFile io.WriteCloser) *ParquetSpanExporter {
	e := &ParquetSpanExporter{
		outputFile: outputFile,
		c:          make(chan *IORecord, 10000),
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
	for r := range e.c {
		if r == nil {
			break
		}
		_, err = w.Write([]IORecord{*r})
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

func (e *ParquetSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		for _, span := range spans {
			r := NewIORecord(span)
			if r.Name == "fuse.Read" || r.Name == "fuse.Write" {
				r.FromAttributes(span.Attributes())
			}
			e.c <- r
		}

	}
}

func (e *ParquetSpanExporter) Shutdown(ctx context.Context) error {
	_ = ctx
	log.Info().Msg("Shutting down, wait for I/O worker")
	e.c <- nil
	e.wg.Wait()
	close(e.c)
	return nil
}
