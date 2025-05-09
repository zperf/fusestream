package cmd

import (
	"context"
	"net"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v3"
	"github.com/winfsp/cgofuse/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fanyang89/slowfs/pb"
	"github.com/fanyang89/slowfs/slowfs"
)

var mountCommand = &cli.Command{
	Name:  "mount",
	Usage: "Mount the filesystem",
	Flags: []cli.Flag{
		flagVerbose,
		&cli.StringFlag{
			Name:    "listen",
			Aliases: []string{"l"},
			Usage:   "The RPC server listen address",
			Value:   "127.0.0.1:4321",
		},
		&cli.StringFlag{
			Name:     "base-dir",
			Aliases:  []string{"b"},
			Usage:    "Data base directory",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "mountpoint",
			Aliases:  []string{"m"},
			Usage:    "Mount point",
			Required: true,
		},
		&cli.BoolFlag{
			Name:  "use-ino",
			Usage: "Use own inode values [FUSE3 only]",
			Value: true,
		},
		&cli.StringFlag{
			Name:  "record",
			Usage: "Filesystem operations record path",
		},
		&cli.StringSliceFlag{
			Name:  "mount-options",
			Usage: "FUSE mount options",
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		verbose := command.Bool("verbose")
		if verbose {
			InitLogging(zerolog.TraceLevel)
		}
		syscall.Umask(0)

		faults := slowfs.NewFaultManager()
		server := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
		pb.RegisterSlowFsServer(server, &slowfs.Rpc{Faults: faults})
		fs := slowfs.New(
			command.String("base-dir"), faults,
			command.String("record"), verbose,
		)

		// start RPC server
		listener, err := net.Listen("tcp", command.String("listen"))
		if err != nil {
			return err
		}
		go func() {
			err := server.Serve(listener)
			if err != nil {
				log.Fatal().Err(err).Msg("failed to serve gRPC server")
			}
		}()

		// mount FUSE
		host := fuse.NewFileSystemHost(fs)
		host.SetUseIno(command.Bool("use-ino"))
		host.Mount(command.String("mountpoint"), command.StringSlice("mount-options"))

		return nil
	},
}
