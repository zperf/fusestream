package cmd

import (
	"github.com/urfave/cli/v3"
)

var RootCommand = &cli.Command{
	Name:  "fusestream",
	Usage: "A simple FUSE tool for file system fault injection tests",
	Commands: []*cli.Command{
		faultCommand,
		toolCommand,
		statCommand,
	},
}
