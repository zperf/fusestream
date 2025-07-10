package fusestream

import (
	"context"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"go.opentelemetry.io/otel/sdk/trace"
)

type SpanExporter struct {
	fw source.ParquetFile
	pw *writer.ParquetWriter
}

func NewSpanExporter(path string) (*SpanExporter, error) {
	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		return nil, err
	}

	const parallelNum = 4
	pw, err := writer.NewParquetWriter(fw, &IORecord{}, parallelNum)
	if err != nil {
		return nil, err
	}

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.PageSize = 8 * 1024
	pw.CompressionType = parquet.CompressionCodec_LZ4
	return &SpanExporter{fw: fw, pw: pw}, nil
}

func (e *SpanExporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	_ = ctx
	for _, span := range spans {
		record := NewIORecord(span)
		err := e.pw.Write(&record)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *SpanExporter) Shutdown(ctx context.Context) error {
	_ = ctx
	err := e.pw.WriteStop()
	if err != nil {
		return err
	}
	return e.fw.Close()
}
