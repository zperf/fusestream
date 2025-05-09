package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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
		faultFsCommand,
		faultBlkCommand,
		listFaultCommand,
		clearFaultCommand,
		removeFaultCommand,
	},
}

var faultFsCommand = &cli.Command{
	Name:  "fs",
	Usage: "Fault injection commands for filesystem",
	Commands: []*cli.Command{
		injectFsDelayCommand,
		injectFsReturnValueCommand,
	},
}

var faultBlkCommand = &cli.Command{
	Name:  "blk",
	Usage: "Fault injection commands for block device",
	Commands: []*cli.Command{
		injectBlkDelayCommand,
		injectBlkReturnValueCommand,
		injectBlkErrorCommand,
	},
}

var injectBlkDelayCommand = &cli.Command{
	Name:  "inject-delay",
	Usage: "inject delay for block device",
	Flags: []cli.Flag{
		flagAddress,
		flagPossibility,
		flagBlkOp,
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
		client := pb.NewSlowFsClient(conn)

		fault := &pb.BlkFault{
			Op: command.Value("op").(pb.BlkOp),
			Delay: &pb.BlkFault_DelayFault{
				DelayFault: &pb.DelayFault{
					Possibility: command.Float32("possibility"),
					DelayMs:     command.Duration("delay").Milliseconds(),
				},
			},
		}

		preCond := command.String("pre-cond")
		if preCond != "" {
			fault.PreCond = &pb.BlkFault_Expression{
				Expression: preCond,
			}
		}

		rsp, err := client.InjectBlkFault(ctx, &pb.InjectBlkFaultRequest{Fault: fault})
		if err != nil {
			return err
		}

		fmt.Printf("Fault injected, id: %d\n", rsp.GetId())
		return nil
	},
}

var injectBlkErrorCommand = &cli.Command{
	Name:  "inject-error",
	Usage: "inject error for block device",
	Flags: []cli.Flag{
		flagAddress,
		flagPossibility,
		flagBlkOp,
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
		client := pb.NewSlowFsClient(conn)

		fault := &pb.BlkFault{
			Op: command.Value("op").(pb.BlkOp),
			Err: &pb.BlkFault_ErrorFault{
				ErrorFault: &pb.ErrorFault{
					Possibility: command.Float32("possibility"),
					Err:         command.String("error"),
				},
			},
		}

		preCond := command.String("pre-cond")
		if preCond != "" {
			fault.PreCond = &pb.BlkFault_Expression{
				Expression: preCond,
			}
		}

		rsp, err := client.InjectBlkFault(ctx, &pb.InjectBlkFaultRequest{Fault: fault})
		if err != nil {
			return err
		}

		fmt.Printf("Fault injected, id: %d\n", rsp.GetId())
		return nil
	},
}

var injectBlkReturnValueCommand = &cli.Command{
	Name:  "inject-return-value",
	Usage: "inject return value for block device",
	Flags: []cli.Flag{
		flagAddress,
		flagPossibility,
		flagReturnValue,
		flagBlkOp,
		flagPreCond,
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		address := command.String("address")
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		client := pb.NewSlowFsClient(conn)

		fault := &pb.BlkFault{
			Op: command.Value("op").(pb.BlkOp),
			ReturnValue: &pb.BlkFault_ReturnValueFault{
				ReturnValueFault: &pb.ReturnValueFault{
					Possibility: command.Float32("possibility"),
					ReturnValue: command.Int64("return-value"),
				},
			},
		}

		preCond := command.String("pre-cond")
		if preCond != "" {
			fault.PreCond = &pb.BlkFault_Expression{
				Expression: preCond,
			}
		}

		rsp, err := client.InjectBlkFault(ctx, &pb.InjectBlkFaultRequest{Fault: fault})
		if err != nil {
			return err
		}

		fmt.Printf("Fault injected, id: %d\n", rsp.GetId())
		return nil
	},
}

var injectFsDelayCommand = &cli.Command{
	Name:  "inject-latency",
	Usage: "Inject delay to the filesystem",
	Flags: []cli.Flag{
		flagAddress,
		flagPathRegex,
		flagPossibility,
		flagFsOp,
		flagDelay,
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		address := command.String("address")
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()

		client := pb.NewSlowFsClient(conn)

		fault := &pb.FsFault{
			PathRe: command.String("path-regex"),
			Op:     command.Value("op").(pb.FsOp),
			Delay: &pb.FsFault_DelayFault{
				DelayFault: &pb.DelayFault{
					Possibility: command.Float32("possibility"),
					DelayMs:     command.Duration("delay").Milliseconds(),
				},
			},
		}

		rsp, err := client.InjectFsFault(ctx, &pb.InjectFsFaultRequest{Fault: fault})
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

		tbl := table.New("ID", "Type", "Path", "Op", "Fault")
		tbl.WithHeaderFormatter(color.New(color.FgGreen, color.Underline).SprintfFunc()).
			WithFirstColumnFormatter(color.New(color.FgYellow).SprintfFunc())

		for _, f := range rsp.FsFaults {
			faults := make([]string, 0)

			switch m := f.Delay.(type) {
			case *pb.FsFault_DelayFault:
				faults = append(faults, fmt.Sprintf("delay{p=%.2f,v=%v}",
					m.DelayFault.Possibility,
					time.Duration(m.DelayFault.DelayMs)*time.Millisecond))
			}

			switch m := f.ReturnValue.(type) {
			case *pb.FsFault_ReturnValueFault:
				faults = append(faults, fmt.Sprintf("rc{p=%.2f,v=%v}",
					m.ReturnValueFault.Possibility,
					m.ReturnValueFault.ReturnValue))
			}

			tbl.AddRow(f.Id, "fs", f.PathRe, f.Op.String(), strings.Join(faults, "/"))
		}

		for _, f := range rsp.BlkFaults {
			faults := make([]string, 0)

			switch m := f.Delay.(type) {
			case *pb.BlkFault_DelayFault:
				faults = append(faults, fmt.Sprintf("delay{p=%.2f,v=%v}",
					m.DelayFault.Possibility,
					time.Duration(m.DelayFault.DelayMs)*time.Millisecond))
			}

			switch m := f.ReturnValue.(type) {
			case *pb.BlkFault_ReturnValueFault:
				faults = append(faults, fmt.Sprintf("rc{p=%.2f,v=%v}",
					m.ReturnValueFault.Possibility,
					m.ReturnValueFault.ReturnValue))
			}

			switch m := f.Err.(type) {
			case *pb.BlkFault_ErrorFault:
				faults = append(faults, fmt.Sprintf("err{p=%.2f,v=%v}",
					m.ErrorFault.Possibility,
					m.ErrorFault.Err))
			}

			tbl.AddRow(f.Id, "blk", "/", f.Op.String(), strings.Join(faults, "/"))
		}

		tbl.Print()
		return nil
	},
}

var injectFsReturnValueCommand = &cli.Command{
	Name:  "inject-return-value",
	Usage: "Inject return-value fault to the filesystem",
	Flags: []cli.Flag{
		flagAddress,
		flagPathRegex,
		flagPossibility,
		flagFsOp,
		flagReturnValue,
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		address := command.String("address")
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()

		client := pb.NewSlowFsClient(conn)
		rsp, err := client.InjectFsFault(ctx, &pb.InjectFsFaultRequest{
			Fault: &pb.FsFault{
				PathRe: command.String("path-regex"),
				Op:     command.Value("op").(pb.FsOp),
				ReturnValue: &pb.FsFault_ReturnValueFault{
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
