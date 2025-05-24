package slowio

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/marcboeker/go-duckdb"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type DuckdbSpanExporter struct {
	db        *sqlx.DB
	tableName string
	appender  *duckdb.Appender
}

const tableName = "slowio_records"

func createTable(db *sqlx.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS slowio_records (
"name" VARCHAR, "start_time_ns" BIGINT, "elapsed_ns" BIGINT,
"offset" BIGINT, "length" INTEGER, "path" VARCHAR)`)
	return err
}

func NewDuckdbSpanExporter(dsn string) (*DuckdbSpanExporter, error) {
	exp := &DuckdbSpanExporter{
		tableName: tableName,
	}

	c, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		return nil, err
	}

	db := sqlx.NewDb(sql.OpenDB(c), "duckdb")
	exp.db = db

	err = createTable(db)
	if err != nil {
		return nil, err
	}

	conn, err := c.Connect(context.Background())
	if err != nil {
		return nil, err
	}

	appender, err := duckdb.NewAppenderFromConn(conn, "", tableName)
	if err != nil {
		return nil, err
	}
	exp.appender = appender

	return exp, nil
}

func (e *DuckdbSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	for _, span := range spans {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		r := NewIORecord(span)
		if r.Name == "fuse.Read" || r.Name == "fuse.Write" {
			r.FromAttributes(span.Attributes())
		}

		err := e.appender.AppendRow(
			r.Name, r.StartTimeNs, r.ElapsedNs,
			r.Offset, r.Length, r.Path)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *DuckdbSpanExporter) Shutdown(ctx context.Context) error {
	_ = ctx

	err := e.appender.Flush()
	if err != nil {
		return err
	}

	err = e.appender.Close()
	if err != nil {
		return err
	}

	return e.db.Close()
}
