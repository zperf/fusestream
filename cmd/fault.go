package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/fatih/color"
	"github.com/rodaine/table"
	"github.com/urfave/cli/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fanyang89/slowfs/pb"
)

var faultCommand = &cli.Command{
	Name:  "fault",
	Usage: "Fault injection commands",
	Commands: []*cli.Command{
		injectLatencyCommand,
		injectErrorCommand,
		listFaultCommand,
		clearFaultCommand,
		removeFaultCommand,
	},
}

var injectLatencyCommand = &cli.Command{
	Name:  "inject-latency",
	Usage: "Inject latency to the filesystem",
	Flags: []cli.Flag{
		flagAddress,
		flagPathRegex,
		flagPossibility,
		flagOp,
		&cli.DurationFlag{
			Name:     "latency",
			Aliases:  []string{"l", "lat"},
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

func removeFaults(ctx context.Context, address string, request *pb.DeleteFaultRequest) error {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewSlowFsClient(conn)
	_, err = client.DeleteFault(ctx, request)
	return err
}

var clearFaultCommand = &cli.Command{
	Name:  "clear",
	Usage: "Clear all faults",
	Flags: []cli.Flag{
		flagAddress,
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		req := &pb.DeleteFaultRequest{All: true}
		return removeFaults(ctx, command.String("address"), req)
	},
}

var removeFaultCommand = &cli.Command{
	Name:  "remove",
	Usage: "Remove faults",
	Flags: []cli.Flag{
		flagAddress,
		&cli.StringFlag{
			Name:    "path-regex",
			Aliases: []string{"g"},
		},
		&cli.Int32SliceFlag{
			Name: "ids",
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		ids := command.Int32Slice("ids")
		pathRegex := command.String("path-regex")
		if len(ids) == 0 && pathRegex == "" {
			return errors.New("must specify at least one id or path-regex")
		}

		var req *pb.DeleteFaultRequest
		if len(ids) > 0 {
			req = &pb.DeleteFaultRequest{Id: ids}
		} else {
			req = &pb.DeleteFaultRequest{PathRe: pathRegex}
		}

		return removeFaults(ctx, command.String("address"), req)
	},
}

var listFaultCommand = &cli.Command{
	Name:  "list",
	Usage: "List faults",
	Flags: []cli.Flag{
		flagAddress,
	},
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
			var op string
			switch m := f.GetFault().(type) {
			case *pb.FaultVariant_InjectLatencyRequest:
				req := m.InjectLatencyRequest
				fault = fmt.Sprintf("lat/%vms/p=%.2f", req.LatencyMs, req.Possibility)
				op = req.Op.String()
			case *pb.FaultVariant_InjectErrorRequest:
				req := m.InjectErrorRequest
				fault = fmt.Sprintf("err/rc=%v/p=%.2f", req.ErrorCode, req.Possibility)
				op = req.Op.String()
			}
			tbl.AddRow(f.Id, f.Path, op, fault)
		}

		tbl.Print()
		return nil
	},
}

var injectErrorCommand = &cli.Command{
	Name:  "inject-error",
	Usage: "Inject error-code to the filesystem",
	Flags: []cli.Flag{
		flagAddress,
		flagPathRegex,
		flagPossibility,
		flagOp,
		&cli.Int32Flag{
			Name:     "error-code",
			Aliases:  []string{"rc", "ec"},
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
