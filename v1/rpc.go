package fusestream

import (
	"context"
	"errors"
	"time"

	"github.com/d5/tengo/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zperf/fusestream/pb"
)

type Rpc struct {
	pb.UnimplementedFuseStreamServer
	Faults *FaultManager
}

func (r *Rpc) InjectNbdFault(ctx context.Context, req *pb.InjectNbdFaultRequest) (*pb.InjectNbdFaultResponse, error) {
	fault := &NbdFault{
		Op: req.Fault.Op,
	}

	switch m := req.Fault.PreCond.(type) {
	case *pb.NbdFault_Expression:
		_, err := tengo.Eval(ctx, m.Expression, map[string]interface{}{"offset": 0, "length": 0})
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid pre-cond script, err: %v", err)
		}

		s := m.Expression
		fault.preCond = &s
	}

	switch m := req.Fault.Delay.(type) {
	case *pb.NbdFault_DelayFault:
		fault.DelayPossibility = m.DelayFault.Possibility
		d := time.Duration(m.DelayFault.DelayMs) * time.Millisecond
		fault.Delay = &d
	}

	switch m := req.Fault.Err.(type) {
	case *pb.NbdFault_ErrorFault:
		fault.ErrPossibility = m.ErrorFault.Possibility
		err := errors.New(m.ErrorFault.Err)
		fault.Err = &err
	}

	switch m := req.Fault.ReturnValue.(type) {
	case *pb.NbdFault_ReturnValueFault:
		fault.ReturnValuePossibility = m.ReturnValueFault.Possibility
		rc := m.ReturnValueFault.ReturnValue
		fault.ReturnValue = &rc
	}

	id := r.Faults.NbdInject(fault)
	return &pb.InjectNbdFaultResponse{Id: id}, nil
}

func (r *Rpc) InjectFuseFault(_ context.Context, req *pb.InjectFuseFaultRequest) (*pb.InjectFuseFaultResponse, error) {
	fault := &FuseFault{
		PathRe: req.Fault.PathRe,
		Op:     req.Fault.Op,
	}

	switch m := req.Fault.ReturnValue.(type) {
	case *pb.FuseFault_ReturnValueFault:
		fault.ReturnValuePossibility = m.ReturnValueFault.Possibility
		c := int32(m.ReturnValueFault.ReturnValue) // FUSE only use int for return code
		fault.ReturnValue = &c
	}

	switch m := req.Fault.Delay.(type) {
	case *pb.FuseFault_DelayFault:
		fault.DelayPossibility = m.DelayFault.Possibility
		d := time.Duration(m.DelayFault.DelayMs) * time.Millisecond
		fault.Delay = &d
	}

	id := r.Faults.FuseInject(fault)
	return &pb.InjectFuseFaultResponse{Id: id}, nil
}

func (r *Rpc) DeleteFault(_ context.Context, req *pb.DeleteFaultRequest) (*pb.DeleteFaultResponse, error) {
	rsp := &pb.DeleteFaultResponse{}
	if req.All {
		rsp.DeletedIds = r.Faults.DeleteAll()
	} else if ids := req.GetId(); ids != nil {
		rsp.DeletedIds = r.Faults.DeleteByID(ids)
	} else if pathRe := req.GetPathRe(); pathRe != "" {
		rsp.DeletedIds = r.Faults.DeleteByPathRegex(pathRe)
	}
	return rsp, nil
}

func (r *Rpc) ListFaults(_ context.Context, _ *pb.Void) (*pb.ListFaultsResponse, error) {
	f, b := r.Faults.ListFaults()

	FuseFaults := make([]*pb.FuseFault, 0)
	NbdFaults := make([]*pb.NbdFault, 0)

	for _, fault := range f {
		fuseFault := &pb.FuseFault{
			Id:     fault.ID,
			PathRe: fault.PathRe,
			Op:     fault.Op,
		}

		if fault.Delay != nil {
			fuseFault.Delay = &pb.FuseFault_DelayFault{
				DelayFault: &pb.DelayFault{
					Possibility: fault.DelayPossibility,
					DelayMs:     fault.Delay.Milliseconds(),
				},
			}
		}

		if fault.ReturnValue != nil {
			fuseFault.ReturnValue = &pb.FuseFault_ReturnValueFault{
				ReturnValueFault: &pb.ReturnValueFault{
					Possibility: fault.ReturnValuePossibility,
					ReturnValue: int64(*fault.ReturnValue),
				},
			}
		}

		FuseFaults = append(FuseFaults, fuseFault)
	}

	for _, fault := range b {
		nbdFault := &pb.NbdFault{
			Id: fault.ID,
			Op: fault.Op,
		}

		if fault.Delay != nil {
			nbdFault.Delay = &pb.NbdFault_DelayFault{
				DelayFault: &pb.DelayFault{
					Possibility: fault.DelayPossibility,
					DelayMs:     fault.Delay.Milliseconds(),
				},
			}
		}

		if fault.ReturnValue != nil {
			nbdFault.ReturnValue = &pb.NbdFault_ReturnValueFault{
				ReturnValueFault: &pb.ReturnValueFault{
					Possibility: fault.ReturnValuePossibility,
					ReturnValue: *fault.ReturnValue,
				},
			}
		}

		if fault.Err != nil {
			nbdFault.Err = &pb.NbdFault_ErrorFault{
				ErrorFault: &pb.ErrorFault{
					Possibility: fault.ErrPossibility,
					Err:         (*fault.Err).Error(),
				},
			}
		}

		NbdFaults = append(NbdFaults, nbdFault)
	}

	return &pb.ListFaultsResponse{FuseFaults: FuseFaults, NbdFaults: NbdFaults}, nil
}
