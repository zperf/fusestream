package slowio

import (
	"context"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
)

func TestParquetSpanExporter(t *testing.T) {
	suite.Run(t, new(ParquetSpanExporterTestSuite))
}

type ParquetSpanExporterTestSuite struct {
	suite.Suite
}

func (s *ParquetSpanExporterTestSuite) TestWrite() {
	file, err := os.CreateTemp("", "slowio-*.parquet")
	s.Require().NoError(err)
	log.Info().Str("output", file.Name()).Msg("Export spans to temporary file")

	exporter := NewParquetSpanExporter(file)

	exporter.c <- &IORecord{"fuse.Read", 1, 2, 100, 100}
	exporter.c <- &IORecord{"fuse.Write", 2, 3, 200, 100}
	exporter.c <- &IORecord{"fuse.Read", 3, 4, 300, 100}

	_ = exporter.Shutdown(context.Background())
}

func TestMain(m *testing.M) {
	InitLogging(zerolog.InfoLevel)
	os.Exit(m.Run())
}
