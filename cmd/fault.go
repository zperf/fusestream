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

	"github.com/zperf/fusestream/pb"
)

var faultCommand = &cli.Command{
	Name:  "fault",
	Usage: "Fault commands",
	Commands: []*cli.Command{
		listFaultCommand,
		removeFaultCommand,
	},
}

func removeFaults(ctx context.Context, address string, request *pb.DeleteFaultRequest) error {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewSlowIOClient(conn)
	_, err = client.DeleteFault(ctx, request)
	return err
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
		&cli.BoolFlag{
			Name: "all",
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		ids := command.Int32Slice("ids")
		pathRegex := command.String("path-regex")
		all := command.Bool("all")

		if len(ids) == 0 && pathRegex == "" && !all {
			return errors.New("must specify at least one fault to remove")
		}

		var req *pb.DeleteFaultRequest
		if all {
			req = &pb.DeleteFaultRequest{All: true}
		} else if len(ids) > 0 {
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

		client := pb.NewSlowIOClient(conn)
		rsp, err := client.ListFaults(ctx, &pb.Void{})
		if err != nil {
			return err
		}

		tbl := table.New("ID", "Type", "Path", "Op", "Fault")
		tbl.WithHeaderFormatter(color.New(color.FgGreen, color.Underline).SprintfFunc()).
			WithFirstColumnFormatter(color.New(color.FgYellow).SprintfFunc())

		for _, f := range rsp.FuseFaults {
			faults := make([]string, 0)

			switch m := f.Delay.(type) {
			case *pb.FuseFault_DelayFault:
				faults = append(faults, fmt.Sprintf("delay{p=%.2f,v=%v}",
					m.DelayFault.Possibility,
					time.Duration(m.DelayFault.DelayMs)*time.Millisecond))
			}

			switch m := f.ReturnValue.(type) {
			case *pb.FuseFault_ReturnValueFault:
				faults = append(faults, fmt.Sprintf("rc{p=%.2f,v=%v}",
					m.ReturnValueFault.Possibility,
					m.ReturnValueFault.ReturnValue))
			}

			tbl.AddRow(f.Id, "fs", f.PathRe, f.Op.String(), strings.Join(faults, "/"))
		}

		for _, f := range rsp.NbdFaults {
			faults := make([]string, 0)

			switch m := f.Delay.(type) {
			case *pb.NbdFault_DelayFault:
				faults = append(faults, fmt.Sprintf("delay{p=%.2f,v=%v}",
					m.DelayFault.Possibility,
					time.Duration(m.DelayFault.DelayMs)*time.Millisecond))
			}

			switch m := f.ReturnValue.(type) {
			case *pb.NbdFault_ReturnValueFault:
				faults = append(faults, fmt.Sprintf("rc{p=%.2f,v=%v}",
					m.ReturnValueFault.Possibility,
					m.ReturnValueFault.ReturnValue))
			}

			switch m := f.Err.(type) {
			case *pb.NbdFault_ErrorFault:
				faults = append(faults, fmt.Sprintf("err{p=%.2f,v=%v}",
					m.ErrorFault.Possibility,
					m.ErrorFault.Err))
			}

			tbl.AddRow(f.Id, "nbd", "/", f.Op.String(), strings.Join(faults, "/"))
		}

		tbl.Print()
		return nil
	},
}
