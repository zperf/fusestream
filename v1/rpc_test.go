package fusestream

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zperf/fusestream/pb"
)

func TestRpc(t *testing.T) {
	suite.Run(t, new(RpcTestSuite))
}

type RpcTestSuite struct {
	suite.Suite
}

func (s *RpcTestSuite) TestItWorks() {
	faults := NewFaultManager()
	server := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	pb.RegisterSlowIOServer(server, &Rpc{Faults: faults})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	address := listener.Addr().String()

	go func() {
		s.Require().NoError(server.Serve(listener))
	}()

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	client := pb.NewSlowIOClient(conn)

	_, err = client.ListFaults(context.TODO(), &pb.Void{})
	s.NoError(err)
	s.NoError(conn.Close())
}
