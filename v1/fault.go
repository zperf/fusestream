package fusestream

import (
	"math/rand"
	"time"

	"github.com/rs/zerolog"
)

type FaultExecute interface {
	Delay()
	MayReplaceErrorCode(rc int64) int64
	MayReplaceError(err error) error
}

type ZeroFault struct{}

func (z ZeroFault) MayReplaceErrorCode(rc int64) int64 {
	return rc
}

func (z ZeroFault) MayReplaceError(err error) error {
	return err
}

func (z ZeroFault) Delay() {}

var zeroFault FaultExecute = &ZeroFault{}

type Fault struct {
	ReturnCode    *int64
	Err           *error
	DelayDuration *time.Duration
}

func (f *Fault) HasValue() bool {
	return f.ReturnCode != nil || f.DelayDuration != nil || f.Err != nil
}

func (f *Fault) Delay() {
	if f.DelayDuration != nil {
		time.Sleep(*f.DelayDuration)
	}
}

func (f *Fault) MayReplaceErrorCode(rc int64) int64 {
	if f.ReturnCode != nil {
		return *f.ReturnCode
	}
	return rc
}

func (f *Fault) MayReplaceError(err error) error {
	if f.Err != nil {
		return *f.Err
	}
	return err
}

func (f *Fault) AppendTrace(e *zerolog.Event) *zerolog.Event {
	if f.DelayDuration != nil {
		e = e.Dur("latency", *f.DelayDuration)
	}
	if f.ReturnCode != nil {
		e = e.Int64("rc", *f.ReturnCode)
	}
	if f.Err != nil {
		e = e.Err(*f.Err)
	}
	return e
}

func (f *Fault) FromFuse(s *FuseFault) {
	if s.Delay != nil && rand.Float32() <= s.DelayPossibility {
		d := *s.Delay
		f.DelayDuration = &d
	}

	if s.ReturnValue != nil && rand.Float32() <= s.ReturnValuePossibility {
		ec := int64(*s.ReturnValue)
		f.ReturnCode = &ec
	}
}

func (f *Fault) FromNbd(s *NbdFault) {
	if s.Delay != nil && rand.Float32() <= s.DelayPossibility {
		d := *s.Delay
		f.DelayDuration = &d
	}

	if s.ReturnValue != nil && rand.Float32() <= s.ReturnValuePossibility {
		a := *s.ReturnValue
		f.ReturnCode = &a
	}

	if s.Err != nil && rand.Float32() <= s.ErrPossibility {
		err := *s.Err
		f.Err = &err
	}
}
