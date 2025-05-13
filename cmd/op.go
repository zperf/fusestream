package cmd

import (
	"flag"
	"fmt"
	"strings"

	"github.com/fanyang89/slowfs/pb"
)

func getKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

type OpCliEnum[T fmt.Stringer] struct {
	selected T
	m        map[string]int32
	cast     func(int32) T
}

func (e *OpCliEnum[T]) Get() any {
	return e.selected
}

func (e *OpCliEnum[T]) String() string {
	return e.selected.String()
}

func (e *OpCliEnum[T]) Set(value string) error {
	op, ok := e.m[value]
	if !ok {
		return fmt.Errorf("invalid opcode: %s. Allowed values are %s", value,
			strings.Join(getKeys(e.m), ", "))
	}
	e.selected = e.cast(op)
	return nil
}

func NewFuseOpCliEnum() flag.Getter {
	return &OpCliEnum[pb.FuseOp]{
		m:    pb.FuseOp_value,
		cast: func(a int32) pb.FuseOp { return pb.FuseOp(a) },
	}
}

func NewNbdOpCliEnum() flag.Getter {
	return &OpCliEnum[pb.NbdOp]{
		m:    pb.NbdOp_value,
		cast: func(a int32) pb.NbdOp { return pb.NbdOp(a) },
	}
}
