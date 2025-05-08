package cmd

import (
	"context"
	"fmt"
	"github.com/urfave/cli/v3"
	"regexp"
)

var regexCommand = &cli.Command{
	Name:    "regex",
	Usage:   "Test if a regex can match the input string",
	Aliases: []string{"re"},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "input",
			Aliases:  []string{"i"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "re",
			Aliases:  []string{"g"},
			Required: true,
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		r, err := regexp.Compile(command.String("re"))
		if err != nil {
			return err
		}

		if r.Match([]byte(command.String("input"))) {
			fmt.Println("Matched")
		} else {
			fmt.Println("Can't match")
		}

		return nil
	},
}
