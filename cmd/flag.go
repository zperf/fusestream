package cmd

import "github.com/urfave/cli/v3"

var flagAddress = &cli.StringFlag{
	Name:    "address",
	Aliases: []string{"a"},
	Usage:   "The RPC server address connect to",
	Value:   "127.0.0.1:4321",
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

var flagOp = &cli.GenericFlag{
	Name:     "op",
	Usage:    "The operation type",
	Value:    &OpCodeEnumValue{},
	Required: true,
}
