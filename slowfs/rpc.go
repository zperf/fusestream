package slowfs

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/fanyang89/slowfs/pb"
)

type Rpc struct {
	pb.UnimplementedSlowFsServer
	Faults *FaultManager
}

func (r *Rpc) InjectError(_ context.Context, req *pb.InjectErrorRequest) (*pb.InjectErrorResponse, error) {
	id, err := r.Faults.InjectError(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.InjectErrorResponse{Id: id}, nil
}

func (r *Rpc) InjectLatency(_ context.Context, req *pb.InjectLatencyRequest) (*pb.InjectLatencyResponse, error) {
	id, err := r.Faults.InjectLatency(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.InjectLatencyResponse{Id: id}, nil
}

func (r *Rpc) DeleteFault(_ context.Context, req *pb.DeleteFaultRequest) (*pb.DeleteFaultResponse, error) {
	rsp := &pb.DeleteFaultResponse{}
	if req.All {
		rsp.Deleted = r.Faults.DeleteAll()
	} else if ids := req.GetId(); ids != nil {
		rsp.Deleted = r.Faults.DeleteByID(ids)
	} else if pathRe := req.GetPathRe(); pathRe != "" {
		rsp.Deleted = r.Faults.DeleteByPathRegex(pathRe)
	}
	return rsp, nil
}

func (r *Rpc) ListFaults(_ context.Context, _ *pb.Void) (*pb.ListFaultsResponse, error) {
	faults := r.Faults.ListFaults()
	return &pb.ListFaultsResponse{
		Faults: faults,
	}, nil
}
