package cmd

import (
	"context"
	"fmt"
	"regexp"

	"github.com/d5/tengo/v2"
	"github.com/urfave/cli/v3"
)

var toolCommand = &cli.Command{
	Name: "tool",
	Commands: []*cli.Command{
		regexCommand,
		blkPreCondTestCommand,
	},
}

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

var blkPreCondTestCommand = &cli.Command{
	Name: "test-blk-pre-cond",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "expression",
			Aliases:  []string{"e", "exp"},
			Required: true,
		},
		&cli.Int64Flag{
			Name:     "offset",
			Aliases:  []string{"off", "o"},
			Required: true,
		},
		&cli.IntFlag{
			Name:     "length",
			Aliases:  []string{"len", "l"},
			Required: true,
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		expression := command.String("expression")
		offset := command.Int64("offset")
		length := command.Int("length")

		res, err := tengo.Eval(ctx, expression, map[string]interface{}{
			"offset": offset,
			"length": length,
		})
		if err != nil {
			return err
		}

		fmt.Printf("Script executed successfully, result=%v\n", res)
		return nil
	},
}
