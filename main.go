package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/rodaine/table"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v3"
	"github.com/winfsp/cgofuse/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fanyang89/slowfs/pb"
	"github.com/fanyang89/slowfs/slowfs"
)

type OpCodeEnumValue struct {
	selected pb.OpCode
}

func (e *OpCodeEnumValue) Get() any {
	return e.selected
}

func (e *OpCodeEnumValue) Set(value string) error {
	op, ok := pb.OpCode_value[value]
	if !ok {
		keys := make([]string, 0, len(pb.OpCode_value))
		for k := range pb.OpCode_value {
			keys = append(keys, k)
		}
		return fmt.Errorf("invalid opcode: %s. Allowed values are %s", value, strings.Join(keys, ", "))
	}
	e.selected = pb.OpCode(op)
	return nil
}

func (e *OpCodeEnumValue) String() string {
	return e.selected.String()
}

func initLogging(level zerolog.Level) {
	writer := zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339Nano,
		PartsOrder: []string{
			zerolog.TimestampFieldName,
			zerolog.LevelFieldName,
			zerolog.CallerFieldName,
			zerolog.MessageFieldName,
		},
		FieldsExclude: []string{
			zerolog.ErrorStackFieldName,
		},
		FormatExtra: func(m map[string]interface{}, buffer *bytes.Buffer) error {
			s, ok := m["stack"]
			if ok {
				_, err := buffer.WriteString(s.(string))
				return err
			}
			return nil
		},
	}

	zerolog.ErrorStackMarshaler = func(err error) interface{} {
		return fmt.Sprintf("\n%+v", err)
	}

	log.Logger = zerolog.New(writer).
		Level(level).With().Timestamp().Caller().Stack().
		Logger()
}

var mountCommand = &cli.Command{
	Name:  "mount",
	Usage: "Mount the filesystem",
	Flags: []cli.Flag{
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
		&cli.StringFlag{
			Name:    "listen",
			Aliases: []string{"l"},
			Usage:   "RPC server listen address",
			Value:   "127.0.0.1:4321",
		},
		&cli.BoolFlag{
			Name:  "use-ino",
			Usage: "Use own inode values [FUSE3 only]",
			Value: true,
		},
		&cli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"v"},
			Usage:   "Enable verbose loggings",
			Value:   false,
		},
		&cli.StringFlag{
			Name:  "record",
			Usage: "Filesystem operations record path",
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		if command.Bool("verbose") {
			initLogging(zerolog.TraceLevel)
		}
		syscall.Umask(0)

		faults := slowfs.NewFaultManager()
		server := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
		pb.RegisterSlowFsServer(server, &slowfs.Rpc{Faults: faults})
		fs := slowfs.New(
			command.String("base-dir"),
			faults,
			command.String("record"),
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
		host.Mount(command.String("mountpoint"), []string{})

		return nil
	},
}

var faultCommand = &cli.Command{
	Name:  "fault",
	Usage: "Fault injection commands",
	Commands: []*cli.Command{
		injectLatencyCommand,
		injectErrorCommand,
		listFaultCommand,
		clearFaultCommand,
	},
}

var clearFaultCommand = &cli.Command{
	Name: "clear",
	Action: func(ctx context.Context, command *cli.Command) error {
		address := command.String("address")
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()

		client := pb.NewSlowFsClient(conn)
		_, err = client.DeleteFault(ctx, &pb.DeleteFaultRequest{
			All: true,
		})
		return err
	},
}

var listFaultCommand = &cli.Command{
	Name: "list",
	Action: func(ctx context.Context, command *cli.Command) error {
		address := command.String("address")
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()

		client := pb.NewSlowFsClient(conn)
		rsp, err := client.ListFaults(ctx, &pb.Void{})
		if err != nil {
			return err
		}

		tbl := table.New("ID", "Path", "Op", "Fault")
		tbl.WithHeaderFormatter(color.New(color.FgGreen, color.Underline).SprintfFunc()).
			WithFirstColumnFormatter(color.New(color.FgYellow).SprintfFunc())

		for _, f := range rsp.GetFaults() {
			var fault string
			switch m := f.GetFault().(type) {
			case *pb.FaultVariant_InjectLatencyRequest:
				req := m.InjectLatencyRequest
				fault = fmt.Sprintf("lat/%vms/p=%.2f", req.LatencyMs, req.Possibility)
			case *pb.FaultVariant_InjectErrorRequest:
				req := m.InjectErrorRequest
				fault = fmt.Sprintf("err/rc=%v/p=%.2f", req.ErrorCode, req.Possibility)
			}
			tbl.AddRow(f.Id, f.Path, f.Op.String(), fault)
		}

		tbl.Print()
		return nil
	},
}

var injectLatencyCommand = &cli.Command{
	Name: "inject-latency",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "address",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "path-regex",
			Required: true,
		},
		&cli.DurationFlag{
			Name:     "latency",
			Required: true,
		},
		&cli.Float32Flag{
			Name:     "possibility",
			Required: true,
		},
		&cli.GenericFlag{
			Name:     "op",
			Value:    &OpCodeEnumValue{},
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

		client := pb.NewSlowFsClient(conn)
		rsp, err := client.InjectLatency(ctx, &pb.InjectLatencyRequest{
			PathRe:      command.String("path-regex"),
			Op:          command.Value("op").(pb.OpCode),
			LatencyMs:   command.Duration("latency").Milliseconds(),
			Possibility: command.Float32("possibility"),
		})
		if err != nil {
			return err
		}

		fmt.Printf("Fault injected, id: %d\n", rsp.GetId())
		return nil
	},
}

var injectErrorCommand = &cli.Command{
	Name: "inject-error",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "address",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "path-regex",
			Required: true,
		},
		&cli.Int32Flag{
			Name:     "error-code",
			Required: true,
		},
		&cli.Float32Flag{
			Name:     "possibility",
			Required: true,
		},
		&cli.GenericFlag{
			Name:     "op",
			Value:    &OpCodeEnumValue{},
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

		client := pb.NewSlowFsClient(conn)
		rsp, err := client.InjectError(ctx, &pb.InjectErrorRequest{
			PathRe:      command.String("path-regex"),
			Op:          command.Value("op").(pb.OpCode),
			ErrorCode:   command.Int32("error-code"),
			Possibility: command.Float32("possibility"),
		})
		if err != nil {
			return err
		}

		fmt.Printf("Fault injected, id: %d\n", rsp.GetId())
		return nil
	},
}

var rootCommand = &cli.Command{
	Name: "slowfs",
	Commands: []*cli.Command{
		mountCommand,
		faultCommand,
	},
}

func main() {
	initLogging(zerolog.InfoLevel)

	err := rootCommand.Run(context.TODO(), os.Args)
	if err != nil {
		log.Fatal().Err(err).Msg("Unexpected error")
	}
}
