//go:build linux

package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/pojntfx/go-nbd/pkg/client"
	"github.com/pojntfx/go-nbd/pkg/server"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zperf/fusestream/pb"
	"github.com/zperf/fusestream/v1"
)

var nbdCommand = &cli.Command{
	Name:  "nbd",
	Usage: "NBD commands",
	Commands: []*cli.Command{
		nbdServeCommand,
		nbdConnectCommand,
		injectNbdDelayCommand,
		injectNbdReturnValueCommand,
		injectNbdErrorCommand,
	},
}

var nbdServeCommand = &cli.Command{
	Name:  "serve",
	Usage: "Start the NBD server",
	Flags: []cli.Flag{
		flagNetwork,
		&cli.StringFlag{
			Name:    "listen",
			Aliases: []string{"l"},
			Usage:   "The server listen address",
			Value:   "127.0.0.1:4321",
		},
		&cli.StringFlag{
			Name:    "rpc-listen",
			Aliases: []string{"rl"},
			Usage:   "The RPC server listen address",
			Value:   "127.0.0.1:4321",
		},
		&cli.StringFlag{
			Name:     "backend-file",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "export",
			Required: true,
		},
		&cli.BoolFlag{
			Name:    "read-only",
			Aliases: []string{"ro"},
			Value:   false,
		},
		&cli.Uint32Flag{
			Name:  "min-block-size",
			Value: client.MinimumBlockSize,
		},
		&cli.Uint32Flag{
			Name:  "preferred-block-size",
			Value: client.MaximumBlockSize,
		},
		&cli.Uint32Flag{
			Name:  "max-block-size",
			Value: client.MaximumBlockSize,
		},
		&cli.BoolFlag{
			Name:  "multi-conn",
			Value: true,
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		backendFilePath := command.String("backend-file")
		readOnly := command.Bool("read-only")

		var err error
		var fh *os.File
		if readOnly {
			fh, err = os.OpenFile(backendFilePath, os.O_RDONLY, 0644)
		} else {
			fh, err = os.OpenFile(backendFilePath, os.O_RDWR, 0644)
		}
		if err != nil {
			return err
		}
		defer func() { _ = fh.Close() }()

		faults := fusestream.NewFaultManager()
		rpcServer := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
			   pb.RegisterFuseStreamServer(rpcServer, &fusestream.Rpc{Faults: faults})
		fileBackend := fusestream.NewFileBackend(fh, faults)

		options := &server.Options{
			ReadOnly:           readOnly,
			MinimumBlockSize:   command.Uint32("min-block-size"),
			PreferredBlockSize: command.Uint32("preferred-block-size"),
			MaximumBlockSize:   command.Uint32("max-block-size"),
			SupportsMultiConn:  command.Bool("multi-conn"),
		}
		exports := []*server.Export{
			{
				Name:    command.String("export"),
				Backend: fileBackend,
			},
		}

		listener, err := net.Listen(command.String("network"), command.String("listen"))
		if err != nil {
			return err
		}

		rpcListener, err := net.Listen("tcp", command.String("rpc-listen"))
		if err != nil {
			return err
		}
		go func() {
			err := rpcServer.Serve(rpcListener)
			if err != nil {
				log.Fatal().Err(err).Msg("gRPC server exited with error")
			}
		}()

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigs
			log.Info().Msg("Closing NBD server")
			_ = listener.Close()
		}()

		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					break
				}
				log.Error().Err(err).Msg("Accept failed")
				continue
			}

			go func(conn net.Conn) {
				err := server.Handle(conn, exports, options)
				if err != nil {
					log.Error().Err(err).Msg("Handle failed")
				}
			}(conn)
		}

		return nil
	},
}

var nbdConnectCommand = &cli.Command{
	Name:  "connect",
	Usage: "Connect to an NBD server",
	Flags: []cli.Flag{
		flagAddress,
		flagNetwork,
		&cli.StringFlag{
			Name:     "export",
			Required: true,
		},
		&cli.Uint32Flag{
			Name: "block-size",
		},
		&cli.StringFlag{
			Name:  "path",
			Value: "/dev/nbd0",
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		conn, err := net.Dial(command.String("network"), command.String("address"))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()

		fh, err := os.Open(command.String("path"))
		if err != nil {
			return err
		}
		defer func() { _ = fh.Close() }()

		return client.Connect(conn, fh, &client.Options{
			ExportName: command.String("export"),
			BlockSize:  command.Uint32("block-size"),
		})
	},
}

var injectNbdDelayCommand = &cli.Command{
	Name:  "inject-delay",
	Usage: "Inject delay for NBD",
	Flags: []cli.Flag{
		flagAddress,
		flagPossibility,
		flagNbdOp,
		flagPreCond,
		flagDelay,
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		address := command.String("address")
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
			   client := pb.NewFuseStreamClient(conn)

		fault := &pb.NbdFault{
			Op: command.Value("op").(pb.NbdOp),
			Delay: &pb.NbdFault_DelayFault{
				DelayFault: &pb.DelayFault{
					Possibility: command.Float32("possibility"),
					DelayMs:     command.Duration("delay").Milliseconds(),
				},
			},
		}

		preCond := command.String("pre-cond")
		if preCond != "" {
			fault.PreCond = &pb.NbdFault_Expression{
				Expression: preCond,
			}
		}

		rsp, err := client.InjectNbdFault(ctx, &pb.InjectNbdFaultRequest{Fault: fault})
		if err != nil {
			return err
		}

		fmt.Printf("Fault injected, id: %d\n", rsp.GetId())
		return nil
	},
}

var injectNbdErrorCommand = &cli.Command{
	Name:  "inject-error",
	Usage: "Inject error for block device",
	Flags: []cli.Flag{
		flagAddress,
		flagPossibility,
		flagNbdOp,
		flagPreCond,
		&cli.StringFlag{
			Name:     "error",
			Required: true,
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		address := command.String("address")
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
			   client := pb.NewFuseStreamClient(conn)

		fault := &pb.NbdFault{
			Op: command.Value("op").(pb.NbdOp),
			Err: &pb.NbdFault_ErrorFault{
				ErrorFault: &pb.ErrorFault{
					Possibility: command.Float32("possibility"),
					Err:         command.String("error"),
				},
			},
		}

		preCond := command.String("pre-cond")
		if preCond != "" {
			fault.PreCond = &pb.NbdFault_Expression{
				Expression: preCond,
			}
		}

		rsp, err := client.InjectNbdFault(ctx, &pb.InjectNbdFaultRequest{Fault: fault})
		if err != nil {
			return err
		}

		fmt.Printf("Fault injected, id: %d\n", rsp.GetId())
		return nil
	},
}

var injectNbdReturnValueCommand = &cli.Command{
	Name:  "inject-return-value",
	Usage: "Inject return value for block device",
	Flags: []cli.Flag{
		flagAddress,
		flagPossibility,
		flagReturnValue,
		flagNbdOp,
		flagPreCond,
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		address := command.String("address")
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
			   client := pb.NewFuseStreamClient(conn)

		fault := &pb.NbdFault{
			Op: command.Value("op").(pb.NbdOp),
			ReturnValue: &pb.NbdFault_ReturnValueFault{
				ReturnValueFault: &pb.ReturnValueFault{
					Possibility: command.Float32("possibility"),
					ReturnValue: command.Int64("return-value"),
				},
			},
		}

		preCond := command.String("pre-cond")
		if preCond != "" {
			fault.PreCond = &pb.NbdFault_Expression{
				Expression: preCond,
			}
		}

		rsp, err := client.InjectNbdFault(ctx, &pb.InjectNbdFaultRequest{Fault: fault})
		if err != nil {
			return err
		}

		fmt.Printf("Fault injected, id: %d\n", rsp.GetId())
		return nil
	},
}
