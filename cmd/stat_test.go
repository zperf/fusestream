package cmd

import "testing"

func TestStat(t *testing.T) {
	rnd := int64(0)
	seq := int64(0)
	cnt := int64(0)

	ioState := ioOperation{}

	for _, v := range []struct {
		Name   string
		Offset int64
		Length int32
	}{
		{"fuse.Read", 0, 131072},        // rnd
		{"fuse.Read", 4931584, 131072},  // rnd
		{"fuse.Read", 5062656, 65536},   // seq
		{"fuse.Read", 10006528, 131072}, // rnd
		{"fuse.Read", 10137600, 65536},  // seq
	} {
		name := v.Name
		offset := v.Offset
		length := v.Length

		if ioState.Empty() {
			ioState = ioOperation{name, offset + int64(length)}
		} else {
			if ioState.Advance(name, offset, int64(length)) {
				seq++
			} else {
				rnd++
			}
			cnt++
		}
	}

	if cnt != seq+rnd {
		t.Fail()
	}

	if seq != int64(2) {
		t.Fail()
	}

	if rnd != int64(2) {
		t.Fail()
	}
}
