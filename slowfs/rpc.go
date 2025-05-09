package slowfs

import (
	"context"
	"time"

	"github.com/fanyang89/slowfs/pb"
)

type Rpc struct {
	pb.UnimplementedSlowFsServer
	Faults *FaultManager
}

func (r *Rpc) InjectFsFault(_ context.Context, req *pb.InjectFsFaultRequest) (*pb.InjectFsFaultResponse, error) {
	fault := &FsFault{
		PathRe: req.Fault.PathRe,
		Op:     req.Fault.Op,
	}

	switch m := req.Fault.ReturnValue.(type) {
	case *pb.FsFault_ReturnValueFault:
		fault.ReturnValuePossibility = m.ReturnValueFault.Possibility
		c := int32(m.ReturnValueFault.ReturnValue) // FUSE only use int for return code
		fault.ReturnValue = &c
	}

	switch m := req.Fault.Delay.(type) {
	case *pb.FsFault_DelayFault:
		fault.DelayPossibility = m.DelayFault.Possibility
		d := time.Duration(m.DelayFault.DelayMs) * time.Millisecond
		fault.Delay = &d
	}

	id := r.Faults.FsInject(req.Fault.PathRe, fault)
	return &pb.InjectFsFaultResponse{Id: id}, nil
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

	fsFaults := make([]*pb.FsFault, 0)
	blkFaults := make([]*pb.BlkFault, 0)

	for _, fault := range f {
		fsFault := &pb.FsFault{
			Id:     fault.ID,
			PathRe: fault.PathRe,
			Op:     fault.Op,
		}

		if fault.Delay != nil {
			fsFault.Delay = &pb.FsFault_DelayFault{
				DelayFault: &pb.DelayFault{
					Possibility: fault.DelayPossibility,
					DelayMs:     fault.Delay.Milliseconds(),
				},
			}
		}

		if fault.ReturnValue != nil {
			fsFault.ReturnValue = &pb.FsFault_ReturnValueFault{
				ReturnValueFault: &pb.ReturnValueFault{
					Possibility: fault.ReturnValuePossibility,
					ReturnValue: int64(*fault.ReturnValue),
				},
			}
		}

		fsFaults = append(fsFaults, fsFault)
	}

	for _, fault := range b {
		blkFault := &pb.BlkFault{
			Id: fault.ID,
			Op: fault.Op,
		}

		if fault.Delay != nil {
			blkFault.Delay = &pb.BlkFault_DelayFault{
				DelayFault: &pb.DelayFault{
					Possibility: fault.DelayPossibility,
					DelayMs:     fault.Delay.Milliseconds(),
				},
			}
		}

		if fault.ReturnValue != nil {
			blkFault.ReturnValue = &pb.BlkFault_ReturnValueFault{
				ReturnValueFault: &pb.ReturnValueFault{
					Possibility: fault.ReturnValuePossibility,
					ReturnValue: *fault.ReturnValue,
				},
			}
		}

		if fault.Err != nil {
			blkFault.Err = &pb.BlkFault_ErrorFault{
				ErrorFault: &pb.ErrorFault{
					Possibility: fault.ErrPossibility,
					Err:         (*fault.Err).Error(),
				},
			}
		}

		blkFaults = append(blkFaults, blkFault)
	}

	return &pb.ListFaultsResponse{FsFaults: fsFaults, BlkFaults: blkFaults}, nil
}
