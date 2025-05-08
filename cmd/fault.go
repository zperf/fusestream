package cmd

import (
	"context"
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
	},
}

var injectLatencyCommand = &cli.Command{
	Name: "inject-latency",
	Flags: []cli.Flag{
		flagAddress,
		flagPathRegex,
		flagPossibility,
		flagOp,
		&cli.DurationFlag{
			Name:     "latency",
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

var clearFaultCommand = &cli.Command{
	Name: "clear",
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
		_, err = client.DeleteFault(ctx, &pb.DeleteFaultRequest{
			All: true,
		})
		return err
	},
}

var listFaultCommand = &cli.Command{
	Name: "list",
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

var injectErrorCommand = &cli.Command{
	Name: "inject-error",
	Flags: []cli.Flag{
		flagAddress,
		flagPathRegex,
		flagPossibility,
		flagOp,
		&cli.Int32Flag{
			Name:     "error-code",
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
