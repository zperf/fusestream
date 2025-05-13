package main

import (
	"context"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/fanyang89/slowio/cmd"
)

func main() {
	cmd.InitLogging(zerolog.InfoLevel)

	err := cmd.RootCommand.Run(context.TODO(), os.Args)
	if err != nil {
		log.Fatal().Err(err).Msg("Unexpected error")
	}
}
