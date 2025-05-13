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
	rc := *f.ReturnValue
	d := *f.Delay
	return &FuseFault{
		ID:                     f.ID,
		PathRe:                 f.PathRe,
		Op:                     f.Op,
		ReturnValue:            &rc,
		ReturnValuePossibility: f.ReturnValuePossibility,
		Delay:                  &d,
		DelayPossibility:       f.DelayPossibility,
	}
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
	rc := *f.ReturnValue
	err := *f.Err
	d := *f.Delay
	return &NbdFault{
		ID:                     f.ID,
		Op:                     f.Op,
		ReturnValue:            &rc,
		ReturnValuePossibility: f.ReturnValuePossibility,
		Err:                    &err,
		ErrPossibility:         f.ErrPossibility,
		Delay:                  &d,
		DelayPossibility:       f.DelayPossibility,
	}
}

type FaultManager struct {
	regexCache *RegexCache
	nextID     int32

	mutex       sync.RWMutex
	fsFaultMap  map[FuseFaultKey]*FuseFault // guarded by mutex
	blkFaultMap map[pb.NbdOp]*NbdFault      // guarded by mutex

}

func NewFaultManager() *FaultManager {
	return &FaultManager{
		regexCache:  NewRegexCache(),
		fsFaultMap:  make(map[FuseFaultKey]*FuseFault),
		blkFaultMap: make(map[pb.NbdOp]*NbdFault),
	}
}

func (f *FaultManager) getNextID() int32 {
	return atomic.AddInt32(&f.nextID, 1) - 1
}

func (f *FaultManager) GetFsFault(path string, op pb.FuseOp) FaultExecute {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	for key, fsFault := range f.fsFaultMap {
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
			fault.FromFs(fsFault)
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

func (f *FaultManager) FsInject(path string, s *FuseFault) int32 {
	f.mutex.Lock()
	id := f.getNextID()
	s.ID = id
	f.fsFaultMap[FuseFaultKey{path, s.Op}] = s
	f.mutex.Unlock()
	return id
}

func (f *FaultManager) BlkInject(s *NbdFault) int32 {
	f.mutex.Lock()
	id := f.getNextID()
	s.ID = id
	f.blkFaultMap[s.Op] = s
	f.mutex.Unlock()
	return id
}

func (f *FaultManager) ListFaults() ([]*FuseFault, []*NbdFault) {
	f.mutex.RLock()
	m := make([]*FuseFault, 0)
	for _, fault := range f.fsFaultMap {
		m = append(m, fault.Clone())
	}

	b := make([]*NbdFault, 0)
	for _, fault := range f.blkFaultMap {
		b = append(b, fault.Clone())
	}

	f.mutex.RUnlock()
	return m, b
}

func (f *FaultManager) DeleteAll() []int32 {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	m := f.fsFaultMap
	f.fsFaultMap = make(map[FuseFaultKey]*FuseFault)

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

	for key, fault := range f.fsFaultMap {
		if key.Path != pathRe {
			continue
		}
		toDelete[key] = struct{}{}
		deletedIDs = append(deletedIDs, fault.ID)
	}

	for key := range toDelete {
		delete(f.fsFaultMap, key)
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

	for key, fault := range f.fsFaultMap {
		_, ok := idm[fault.ID]
		if ok {
			toDelete[key] = struct{}{}
		}
	}

	for key := range toDelete {
		delete(f.fsFaultMap, key)
	}

	return ids
}

func (f *FaultManager) GetBlkFault(op pb.NbdOp, offset int64, len int) FaultExecute {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	blkFault, ok := f.blkFaultMap[op]
	if !ok {
		return zeroFault
	}

	sc := blkFault.preCond
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
	fault.FromBlk(blkFault)
	if fault.HasValue() {
		e := log.Trace().Str("op", op.String()).Int64("offset", offset).Int("len", len)
		e = fault.AppendTrace(e)
		e.Msg("Fault injected")
		return fault
	}

	return zeroFault
}
