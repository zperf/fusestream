package main

import (
	"context"
	"errors"
	"net/http"
	_ "net/http/pprof"
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

	debug := os.Getenv("SLOWIO_DEBUG")
	if debug != "" {
		go func() {
			log.Info().Str("listen", debug).Msg("Debug HTTP server listening")
			err := http.ListenAndServe(debug, nil)
			if err != nil {
				log.Fatal().Err(err).Str("listen", debug).Msg("http listen failed")
			}
		}()
	}

	exportPath := os.Getenv("SLOWIO_EXPORT_PATH")
	if exportPath == "" {
		log.Warn().Msg("Environment variable SLOWIO_EXPORT_PATH not set")
	} else {
		exporter, err := slowio.NewDuckdbSpanExporter(exportPath)
		if err != nil {
			log.Fatal().Err(err).Str("exportPath", exportPath).Msg("Create duckdb exporter failed")
		}

		defer func() {
			if err := exporter.Shutdown(context.Background()); err != nil {
				log.Error().Err(err).Msg("Shutdown exporter failed")
			}
		}()

		slowio.SetupOTelSDK(exporter)
	}

	err = cmd.RootCommand.Run(ctx, os.Args)
	if err != nil {
		log.Fatal().Err(err).Msg("Unexpected error")
	}
}
