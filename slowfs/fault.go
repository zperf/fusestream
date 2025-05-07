package slowfs

import (
	"math/rand/v2"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/negrel/assert"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/fanyang89/slowfs/pb"
)

type FaultManager struct {
	mutex        sync.RWMutex
	faultByRegex map[*regexp.Regexp][]*pb.FaultVariant // guarded by mutex
	rawString2Re map[string]*regexp.Regexp             // guarded by mutex
	nextID       int32
}

func NewFaultManager() *FaultManager {
	return &FaultManager{
		faultByRegex: make(map[*regexp.Regexp][]*pb.FaultVariant),
		rawString2Re: make(map[string]*regexp.Regexp),
	}
}

func setFaultByList(fa *Fault, fvs []*pb.FaultVariant, op pb.OpCode) {
	for _, fv := range fvs {
		if fv.Op != op {
			continue
		}

		if e := fv.GetInjectErrorRequest(); e != nil {
			if rand.Float32() <= e.Possibility {
				fa.ReturnCode = &[]int{int(e.ErrorCode)}[0]
			}
		} else if l := fv.GetInjectLatencyRequest(); l != nil {
			if rand.Float32() <= l.Possibility {
				fa.Delay = &[]time.Duration{time.Duration(l.LatencyMs) * time.Millisecond}[0]
			}
		}
	}
}

func (f *FaultManager) getNextID() int32 {
	return atomic.AddInt32(&f.nextID, 1) - 1
}

func (f *FaultManager) Query(path string, op pb.OpCode) FaultExecute {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	for r, fvs := range f.faultByRegex {
		if r.Match([]byte(path)) {
			var fa Fault
			setFaultByList(&fa, fvs, op)
			if fa.HasValue() {
				fa.WriteTrace(log.Trace().Str("path", path).Str("op", op.String())).Msg("Fault injected")
				return &fa
			}
		}
	}

	return zeroFault
}

func (f *FaultManager) inject(fv *pb.FaultVariant) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	var err error
	var ok bool
	var regex *regexp.Regexp

	var re string
	switch m := fv.GetFault().(type) {
	case *pb.FaultVariant_InjectLatencyRequest:
		re = m.InjectLatencyRequest.PathRe
	case *pb.FaultVariant_InjectErrorRequest:
		re = m.InjectErrorRequest.PathRe
	}

	regex, ok = f.rawString2Re[re]
	if !ok {
		regex, err = regexp.Compile(re)
		if err != nil {
			return err
		}

		f.rawString2Re[re] = regex
		f.faultByRegex[regex] = make([]*pb.FaultVariant, 0)
	}

	fvs, ok := f.faultByRegex[regex]
	assert.True(ok)

	f.faultByRegex[regex] = append(fvs, fv)
	return nil
}

func (f *FaultManager) InjectLatency(request *pb.InjectLatencyRequest) (int32, error) {
	id := f.getNextID()
	err := f.inject(&pb.FaultVariant{
		Id:    id,
		Fault: &pb.FaultVariant_InjectLatencyRequest{InjectLatencyRequest: request},
	})
	return id, err
}

func (f *FaultManager) InjectError(request *pb.InjectErrorRequest) (int32, error) {
	id := f.getNextID()
	err := f.inject(&pb.FaultVariant{
		Id:    id,
		Fault: &pb.FaultVariant_InjectErrorRequest{InjectErrorRequest: request},
	})
	return id, err
}

func (f *FaultManager) ListFaults() []*pb.FaultVariant {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	m := make([]*pb.FaultVariant, 0)

	for path, re := range f.rawString2Re {
		fvs, ok := f.faultByRegex[re]
		if !ok {
			continue
		}

		for _, fv := range fvs {
			p := proto.CloneOf(fv)
			p.Path = path
			m = append(m, p)
		}
	}

	return m
}

func (f *FaultManager) DeleteAll() []int32 {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	m := make([]int32, 0)
	for _, fvs := range f.faultByRegex {
		for _, fv := range fvs {
			m = append(m, fv.Id)
		}
	}

	f.faultByRegex = make(map[*regexp.Regexp][]*pb.FaultVariant)
	f.rawString2Re = make(map[string]*regexp.Regexp)

	return m
}

func (f *FaultManager) DeleteByPath(path string) []int32 {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	re, ok := f.rawString2Re[path]
	if !ok {
		return make([]int32, 0)
	}

	m := make([]int32, 0)
	for _, fv := range f.faultByRegex[re] {
		m = append(m, fv.Id)
	}

	delete(f.rawString2Re, path)
	delete(f.faultByRegex, re)
	return m
}

func (f *FaultManager) DeleteByID(ids []int32) []int32 {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	idm := make(map[int32]struct{})
	for _, id := range ids {
		idm[id] = struct{}{}
	}

	nm := make(map[*regexp.Regexp][]*pb.FaultVariant)

	for re, fvs := range f.faultByRegex {
		n := make([]*pb.FaultVariant, 0)
		for _, fv := range fvs {
			if _, ok := idm[fv.Id]; !ok {
				n = append(n, fv)
			}
		}
		if len(n) > 0 {
			nm[re] = n
		}
	}

	f.faultByRegex = nm
	return ids
}

type FaultExecute interface {
	Execute(rc int) int
}

type ZeroFault struct{}

func (z ZeroFault) Execute(rc int) int {
	return rc
}

var zeroFault FaultExecute = &ZeroFault{}

type Fault struct {
	ReturnCode *int
	Delay      *time.Duration
}

func (f *Fault) HasValue() bool {
	return f.ReturnCode != nil || f.Delay != nil
}

func (f *Fault) Execute(rc int) int {
	if f.Delay != nil {
		time.Sleep(*f.Delay)
	}
	if f.ReturnCode != nil {
		return *f.ReturnCode
	}
	return rc
}

func (f *Fault) WriteTrace(e *zerolog.Event) *zerolog.Event {
	if f.Delay != nil {
		e = e.Dur("latency", *f.Delay)
	}
	if f.ReturnCode != nil {
		e = e.Int("rc", *f.ReturnCode)
	}
	return e
}
