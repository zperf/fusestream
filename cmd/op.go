package cmd

import (
	"fmt"
	"strings"

	"github.com/fanyang89/slowfs/pb"
)

type OpCodeEnumValue struct {
	selected pb.FsOp
}

func (e *OpCodeEnumValue) Get() any {
	return e.selected
}

func (e *OpCodeEnumValue) Set(value string) error {
	op, ok := pb.FsOp_value[value]
	if !ok {
		keys := make([]string, 0, len(pb.FsOp_value))
		for k := range pb.FsOp_value {
			keys = append(keys, k)
		}
		return fmt.Errorf("invalid opcode: %s. Allowed values are %s", value, strings.Join(keys, ", "))
	}
	e.selected = pb.FsOp(op)
	return nil
}

func (e *OpCodeEnumValue) String() string {
	return e.selected.String()
}
