package slowfs

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/fanyang89/slowfs/pb"
)

type RPC struct {
	pb.UnimplementedSlowFsServer
	Faults *FaultManager
}

func (r *RPC) InjectError(_ context.Context, req *pb.InjectErrorRequest) (*pb.InjectErrorResponse, error) {
	id, err := r.Faults.InjectError(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.InjectErrorResponse{Id: id}, nil
}

func (r *RPC) InjectLatency(_ context.Context, req *pb.InjectLatencyRequest) (*pb.InjectLatencyResponse, error) {
	id, err := r.Faults.InjectLatency(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.InjectLatencyResponse{Id: id}, nil
}

func (r *RPC) DeleteFault(_ context.Context, req *pb.DeleteFaultRequest) (*pb.DeleteFaultResponse, error) {
	rsp := &pb.DeleteFaultResponse{}
	if req.All {
		rsp.Deleted = r.Faults.DeleteAll()
	} else if ids := req.GetId(); ids != nil {
		rsp.Deleted = r.Faults.DeleteByID(ids)
	} else if path := req.GetPath(); path != "" {
		rsp.Deleted = r.Faults.DeleteByPath(path)
	}
	return rsp, nil
}

func (r *RPC) ListFaults(_ context.Context, _ *pb.Void) (*pb.ListFaultsResponse, error) {
	return &pb.ListFaultsResponse{
		Faults: r.Faults.ListFaults(),
	}, nil
}
