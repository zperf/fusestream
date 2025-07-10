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

	"github.com/zperf/fusestream/cmd"
	"github.com/zperf/fusestream/v1"
)

func main() {
	slowio.InitLogging(zerolog.InfoLevel)

	err := godotenv.Load()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatal().Err(err).Msg("Load env file failed")
	}

	debugListen := os.Getenv("SLOWIO_DEBUG")
	if debugListen != "" {
		slowio.InitLogging(zerolog.TraceLevel)
		go func() {
			log.Info().Str("listen", debugListen).Msg("Debug HTTP server listening")
			if err := http.ListenAndServe(debugListen, nil); err != nil {
				log.Fatal().Err(err).Msg("Listen failed")
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	err = cmd.RootCommand.Run(ctx, os.Args)
	if err != nil {
		log.Fatal().Err(err).Msg("Unexpected error")
	}
}
