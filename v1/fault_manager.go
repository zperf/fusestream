package slowio

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/d5/tengo/v2"
	"github.com/rs/zerolog/log"

	"github.com/fanyang89/slowio/pb"
)

type FuseFaultKey struct {
	Path string
	Op   pb.FuseOp
}

type FuseFault struct {
	ID     int32
	PathRe string
	Op     pb.FuseOp

	ReturnValue            *int32
	ReturnValuePossibility float32

	Delay            *time.Duration
	DelayPossibility float32
}

func (f *FuseFault) Clone() *FuseFault {
	v := &FuseFault{
		ID:                     f.ID,
		PathRe:                 f.PathRe,
		Op:                     f.Op,
		ReturnValuePossibility: f.ReturnValuePossibility,
		DelayPossibility:       f.DelayPossibility,
	}

	if f.ReturnValue != nil {
		rc := *f.ReturnValue
		v.ReturnValue = &rc
	}

	if f.Delay != nil {
		d := *f.Delay
		v.Delay = &d
	}

	return v
}

type NbdFault struct {
	ID int32
	Op pb.NbdOp

	preCond *string

	ReturnValue            *int64
	ReturnValuePossibility float32

	Err            *error
	ErrPossibility float32

	Delay            *time.Duration
	DelayPossibility float32
}

func (f *NbdFault) Clone() *NbdFault {

	v := &NbdFault{
		ID:                     f.ID,
		Op:                     f.Op,
		ReturnValuePossibility: f.ReturnValuePossibility,
		ErrPossibility:         f.ErrPossibility,
		DelayPossibility:       f.DelayPossibility,
	}

	if f.ReturnValue != nil {
		rc := *f.ReturnValue
		v.ReturnValue = &rc
	}

	if f.Delay != nil {
		d := *f.Delay
		v.Delay = &d
	}

	if f.Err != nil {
		d := *f.Err
		v.Err = &d
	}

	return v
}

type FaultManager struct {
	regexCache *RegexCache
	nextID     int32

	mutex        sync.RWMutex
	fuseFaultMap map[FuseFaultKey]*FuseFault // guarded by mutex
	nbdFaultMap  map[pb.NbdOp]*NbdFault      // guarded by mutex

}

func NewFaultManager() *FaultManager {
	return &FaultManager{
		regexCache:   NewRegexCache(),
		fuseFaultMap: make(map[FuseFaultKey]*FuseFault),
		nbdFaultMap:  make(map[pb.NbdOp]*NbdFault),
	}
}

func (f *FaultManager) getNextID() int32 {
	return atomic.AddInt32(&f.nextID, 1) - 1
}

func (f *FaultManager) GetFuseFault(path string, op pb.FuseOp) FaultExecute {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	for key, fuseFault := range f.fuseFaultMap {
		if key.Op != op {
			continue
		}

		re, err := f.regexCache.Compile(key.Path)
		if err != nil {
			log.Warn().Err(err).Str("regex", key.Path).Msg("Invalid regex")
			continue
		}

		if re.Match([]byte(path)) {
			var fault Fault
			fault.FromFuse(fuseFault)
			if fault.HasValue() {
				e := log.Trace().Str("path", path).Str("op", op.String())
				e = fault.AppendTrace(e)
				e.Msg("Fault injected")
				return &fault
			}
		}
	}

	return zeroFault
}

func (f *FaultManager) FuseInject(s *FuseFault) int32 {
	f.mutex.Lock()
	id := f.getNextID()
	s.ID = id
	f.fuseFaultMap[FuseFaultKey{s.PathRe, s.Op}] = s
	f.mutex.Unlock()
	return id
}

func (f *FaultManager) NbdInject(s *NbdFault) int32 {
	f.mutex.Lock()
	id := f.getNextID()
	s.ID = id
	f.nbdFaultMap[s.Op] = s
	f.mutex.Unlock()
	return id
}

func (f *FaultManager) ListFaults() ([]*FuseFault, []*NbdFault) {
	f.mutex.RLock()
	m := make([]*FuseFault, 0)
	for _, fault := range f.fuseFaultMap {
		m = append(m, fault.Clone())
	}

	b := make([]*NbdFault, 0)
	for _, fault := range f.nbdFaultMap {
		b = append(b, fault.Clone())
	}

	f.mutex.RUnlock()
	return m, b
}

func (f *FaultManager) DeleteAll() []int32 {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	m := f.fuseFaultMap
	f.fuseFaultMap = make(map[FuseFaultKey]*FuseFault)

	deletedIDs := make([]int32, 0)
	for _, fault := range m {
		deletedIDs = append(deletedIDs, fault.ID)
	}
	return deletedIDs
}

func (f *FaultManager) DeleteByPathRegex(pathRe string) []int32 {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	deletedIDs := make([]int32, 0)
	toDelete := make(map[FuseFaultKey]struct{})

	for key, fault := range f.fuseFaultMap {
		if key.Path != pathRe {
			continue
		}
		toDelete[key] = struct{}{}
		deletedIDs = append(deletedIDs, fault.ID)
	}

	for key := range toDelete {
		delete(f.fuseFaultMap, key)
	}

	return deletedIDs
}

func (f *FaultManager) DeleteByID(ids []int32) []int32 {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	idm := make(map[int32]struct{})
	for _, id := range ids {
		idm[id] = struct{}{}
	}

	toDelete := make(map[FuseFaultKey]struct{})

	for key, fault := range f.fuseFaultMap {
		_, ok := idm[fault.ID]
		if ok {
			toDelete[key] = struct{}{}
		}
	}

	for key := range toDelete {
		delete(f.fuseFaultMap, key)
	}

	return ids
}

func (f *FaultManager) GetNbdFault(op pb.NbdOp, offset int64, len int) FaultExecute {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	nbdFault, ok := f.nbdFaultMap[op]
	if !ok {
		return zeroFault
	}

	sc := nbdFault.preCond
	if sc != nil {
		preCondObject, err := tengo.Eval(context.Background(), *sc, map[string]interface{}{
			"offset": offset,
			"length": len,
		})
		var preCond bool
		preCond, ok = preCondObject.(bool)
		if err != nil || !ok {
			log.Warn().Err(err).Int64("offset", offset).Int("len", len).
				Interface("preCondObject", preCondObject).
				Msg("Execute pre-condition script failed")
			return zeroFault
		}

		if !preCond {
			return zeroFault
		}
	}

	fault := &Fault{}
	fault.FromNbd(nbdFault)
	if fault.HasValue() {
		e := log.Trace().Str("op", op.String()).Int64("offset", offset).Int("len", len)
		e = fault.AppendTrace(e)
		e.Msg("Fault injected")
		return fault
	}

	return zeroFault
}
