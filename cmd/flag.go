package cmd

import (
	"github.com/urfave/cli/v3"
)

var flagAddress = &cli.StringFlag{
	Name:    "address",
	Aliases: []string{"a"},
	Usage:   "The server address connect to",
	Value:   "127.0.0.1:4321",
}

var flagNetwork = &cli.StringFlag{
	Name:  "network",
	Value: "tcp",
}

var flagVerbose = &cli.BoolFlag{
	Name:    "verbose",
	Aliases: []string{"v"},
	Usage:   "Enable verbose logging",
	Value:   false,
}

var flagPathRegex = &cli.StringFlag{
	Name:     "path-regex",
	Aliases:  []string{"g"},
	Usage:    "The path regex to match",
	Required: true,
}

var flagPossibility = &cli.Float32Flag{
	Name:     "possibility",
	Aliases:  []string{"p"},
	Usage:    "The possibility of fault triggering",
	Required: true,
}

var flagFuseOp = &cli.GenericFlag{
	Name:     "op",
	Usage:    "The operation type",
	Value:    NewFuseOpCliEnum(),
	Required: true,
}

var flagNbdOp = &cli.GenericFlag{
	Name:     "op",
	Usage:    "The operation type",
	Value:    NewNbdOpCliEnum(),
	Required: true,
}

var flagPreCond = &cli.StringFlag{
	Name:    "pre-cond",
	Aliases: []string{"pred"},
}

var flagDelay = &cli.DurationFlag{
	Name:     "delay",
	Aliases:  []string{"d", "lat"},
	Required: true,
}

var flagReturnValue = &cli.Int64Flag{
	Name:     "return-value",
	Aliases:  []string{"rc", "ec"},
	Required: true,
}
