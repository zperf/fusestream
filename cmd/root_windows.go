package cmd

import (
	"github.com/urfave/cli/v3"
)

var RootCommand = &cli.Command{
	Name:  "slowio",
	Usage: "A simple FUSE/NBD tool for file system and block device fault injection tests",
	Commands: []*cli.Command{
		fuseCommand,
		faultCommand,
		toolCommand,
		statCommand,
	},
}
