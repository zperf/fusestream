package main

import (
	"context"
	"errors"
	"os"
	"os/signal"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"

	"github.com/fanyang89/slowio/cmd"
	"github.com/fanyang89/slowio/v1"
)

func main() {
	cmd.InitLogging(zerolog.InfoLevel)

	err := godotenv.Load()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Warn().Msg(".env file not found")
		} else {
			log.Fatal().Err(err).Msg("Load env file failed")
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Create OTLP trace exporter failed")
	}

	shutdown := slowio.SetupOTelSDK(exporter)
	defer func() { _ = shutdown(context.Background()) }()

	err = cmd.RootCommand.Run(ctx, os.Args)
	if err != nil {
		log.Fatal().Err(err).Msg("Unexpected error")
	}
}
