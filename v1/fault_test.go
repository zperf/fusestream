package fusestream

import (
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/suite"

	"github.com/zperf/fusestream/pb"
)

func TestFaultManager(t *testing.T) {
	suite.Run(t, new(FaultManagerTestSuite))
}

type FaultManagerTestSuite struct {
	suite.Suite
}

func (s *FaultManagerTestSuite) TestFaultManager() {
	f := NewFaultManager()
	fuseFaults, nbdFaults := f.ListFaults()
	s.Len(fuseFaults, 0)
	s.Len(nbdFaults, 0)

	d := 100 * time.Millisecond
	id := f.FuseInject(&FuseFault{
		PathRe:           "test_file.*",
		Op:               pb.FuseOp_FUSE_READ,
		Delay:            &d,
		DelayPossibility: 0.5,
	})
	s.Equal(int32(0), id)

	fuseFaults, _ = f.ListFaults()
	s.Len(fuseFaults, 1)

	fault := fuseFaults[0]
	s.NotNil(fault.Delay)
	s.Equal("test_file.*", fault.PathRe)
	s.Equal(pb.FuseOp_FUSE_READ, fault.Op)
	s.Equal(d, *fault.Delay)
	s.Equal(float32(0.5), fault.DelayPossibility)

	deleted := f.DeleteAll()
	s.Len(deleted, 1)

	fuseFaults, _ = f.ListFaults()
	s.Len(fuseFaults, 0)
}

func TestMain(m *testing.M) {
	InitLogging(zerolog.InfoLevel)
	os.Exit(m.Run())
}
