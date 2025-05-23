package slowio

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/marcboeker/go-duckdb"
	"github.com/rs/zerolog/log"
	"github.com/smallnest/chanx"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type DuckdbSpanExporter struct {
	db        *sqlx.DB
	tableName string

	c       *chanx.UnboundedChan[*IORecord]
	cCtx    context.Context
	cCancel context.CancelFunc

	wg sync.WaitGroup
}

func NewDuckdbSpanExporter(dsn string, tableName string) (*DuckdbSpanExporter, error) {
	cCtx, cCancel := context.WithCancel(context.Background())
	exp := &DuckdbSpanExporter{
		tableName: tableName,
		c:         chanx.NewUnboundedChan[*IORecord](cCtx, 1024),
		cCtx:      cCtx,
		cCancel:   cCancel,
	}

	c, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		return nil, err
	}

	db := sqlx.NewDb(sql.OpenDB(c), "duckdb")
	exp.db = db

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	createTable := `CREATE TABLE IF NOT EXISTS ` + tableName + ` (
name VARCHAR, start_time_ns BIGINT, elapsed_ns BIGINT,
offset BIGINT, length INTEGER, path VARCHAR)`

	_, err = db.Exec(createTable)
	if err != nil {
		return nil, err
	}

	exp.wg.Add(1)
	go exp.ioWorker()

	return exp, nil
}

func (e *DuckdbSpanExporter) ioWorker() {
	log.Info().Msg("Parquet span exporter I/O worker started")
	defer func() {
		log.Info().Msg("Parquet span exporter I/O worker exiting")
		e.wg.Done()
	}()

	conn, err := e.db.Conn(context.Background())
	if err != nil {
		log.Panic().Err(err).Msg("Get database connection failed")
	}

	err = conn.Raw(func(driverConn any) error {
		appender, err := duckdb.NewAppenderFromConn(driverConn.(driver.Conn), "", e.tableName)
		if err != nil {
			return err
		}
		defer func() {
			_ = appender.Flush()
			_ = appender.Close()
		}()

		for r := range e.c.Out {
			if r == nil {
				break
			}

			err = appender.AppendRow(
				r.Name, r.StartTimeNs, r.ElapsedNs,
				r.Offset, r.Length, r.Path)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		log.Error().Err(err).Msg("Write to database failed")
	}
}

func (e *DuckdbSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
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
			e.c.In <- r
		}
	}
}

func (e *DuckdbSpanExporter) Shutdown(ctx context.Context) error {
	_ = ctx

	log.Info().Msg("Shutting down, wait for I/O worker")
	e.c.In <- nil
	e.wg.Wait()
	e.cCancel()

	return e.db.Close()
}
