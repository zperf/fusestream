//go:build linux || windows

package cmd

import (
	"context"
	"fmt"
	"net"
	"runtime"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v3"
	"github.com/winfsp/cgofuse/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fanyang89/slowio/pb"
	"github.com/fanyang89/slowio/v1"
)

var fuseCommand = &cli.Command{
	Name:  "fuse",
	Usage: "FUSE commands",
	Commands: []*cli.Command{
		fuseMountCommand,
		injectFuseDelayCommand,
		injectFuseReturnValueCommand,
	},
}

var fuseMountCommand = &cli.Command{
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
			// Windows uses FUSE not FUSE3
			Value: runtime.GOOS != "windows",
		},
		&cli.StringSliceFlag{
			Name:  "mount-options",
			Usage: "FUSE mount options",
		},
		&cli.BoolFlag{
			Name:  "without-faults",
			Usage: "FUSE mount without faults",
			Value: false,
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		verbose := command.Bool("verbose")
		if verbose {
			slowio.InitLogging(zerolog.TraceLevel)
		}
		syscallUmask()

		faults := slowio.NewFaultManager()
		server := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
		pb.RegisterSlowIOServer(server, &slowio.Rpc{Faults: faults})

		var fs fuse.FileSystemInterface
		baseDir := command.String("base-dir")
		if command.Bool("without-faults") {
			fs = slowio.NewRawFS(baseDir)
		} else {
			fs = slowio.NewSlowFS(baseDir, faults)
		}

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

var injectFuseDelayCommand = &cli.Command{
	Name:  "inject-latency",
	Usage: "Inject delay to the filesystem",
	Flags: []cli.Flag{
		flagAddress,
		flagPathRegex,
		flagPossibility,
		flagFuseOp,
		flagDelay,
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		address := command.String("address")
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()

		client := pb.NewSlowIOClient(conn)

		fault := &pb.FuseFault{
			PathRe: command.String("path-regex"),
			Op:     command.Value("op").(pb.FuseOp),
			Delay: &pb.FuseFault_DelayFault{
				DelayFault: &pb.DelayFault{
					Possibility: command.Float32("possibility"),
					DelayMs:     command.Duration("delay").Milliseconds(),
				},
			},
		}

		rsp, err := client.InjectFuseFault(ctx, &pb.InjectFuseFaultRequest{Fault: fault})
		if err != nil {
			return err
		}

		fmt.Printf("Fault injected, id: %d\n", rsp.GetId())
		return nil
	},
}

var injectFuseReturnValueCommand = &cli.Command{
	Name:  "inject-return-value",
	Usage: "Inject return-value fault to the filesystem",
	Flags: []cli.Flag{
		flagAddress,
		flagPathRegex,
		flagPossibility,
		flagFuseOp,
		flagReturnValue,
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		address := command.String("address")
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()

		client := pb.NewSlowIOClient(conn)
		rsp, err := client.InjectFuseFault(ctx, &pb.InjectFuseFaultRequest{
			Fault: &pb.FuseFault{
				PathRe: command.String("path-regex"),
				Op:     command.Value("op").(pb.FuseOp),
				ReturnValue: &pb.FuseFault_ReturnValueFault{
					ReturnValueFault: &pb.ReturnValueFault{
						Possibility: command.Float32("possibility"),
						ReturnValue: command.Int64("return-value"),
					},
				},
			},
		})
		if err != nil {
			return err
		}

		fmt.Printf("Fault injected, id: %d\n", rsp.GetId())
		return nil
	},
}
