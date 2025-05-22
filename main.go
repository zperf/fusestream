package main

import (
	"context"
	"errors"
	"os"
	"os/signal"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/fanyang89/slowio/cmd"
	"github.com/fanyang89/slowio/v1"
)

func main() {
	slowio.InitLogging(zerolog.InfoLevel)

	err := godotenv.Load()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatal().Err(err).Msg("Load env file failed")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	exportPath := os.Getenv("SLOWIO_EXPORT_PATH")
	if exportPath == "" {
		log.Fatal().Msg("Environment variable SLOWIO_EXPORT_PATH not set")
	}

	exportFile, err := os.OpenFile(exportPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal().Err(err).Msg("Open export file failed")
	}

	exporter := slowio.NewParquetSpanExporter(exportFile)
	defer func() {
		if err := exporter.Shutdown(context.Background()); err != nil {
			log.Error().Err(err).Msg("Shutdown exporter failed")
		}
	}()

	slowio.SetupOTelSDK(exporter)

	err = cmd.RootCommand.Run(ctx, os.Args)
	if err != nil {
		log.Fatal().Err(err).Msg("Unexpected error")
	}
}
