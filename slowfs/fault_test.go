package slowfs

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/fanyang89/slowfs/pb"
)

func TestFaultManager(t *testing.T) {
	suite.Run(t, new(FaultManagerTestSuite))
}

type FaultManagerTestSuite struct {
	suite.Suite
}

func (s *FaultManagerTestSuite) TestFaultManager() {
	f := NewFaultManager()
	s.Len(f.ListFaults(), 0)

	request1 := &pb.InjectLatencyRequest{
		PathRe:      "test_file.*",
		Op:          pb.OpCode_READ,
		LatencyMs:   100,
		Possibility: 0.5,
	}
	id, err := f.InjectLatency(request1)
	s.NoError(err)
	s.Equal(int32(0), id)

	faults := f.ListFaults()
	s.Len(faults, 1)

	req, ok := faults[0].GetFault().(*pb.FaultVariant_InjectLatencyRequest)
	s.True(ok)
	s.True(proto.Equal(request1, req.InjectLatencyRequest))

	deleted := f.DeleteAll()
	s.Len(deleted, 1)

	s.Len(f.ListFaults(), 0)
}
