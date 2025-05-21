package slowio

import (
	"context"
	"os"

	"go.opentelemetry.io/otel/attribute"

	"github.com/fanyang89/slowio/pb"
)

type FileBackend struct {
	file   *os.File
	faults *FaultManager
}

func NewFileBackend(file *os.File, faults *FaultManager) *FileBackend {
	return &FileBackend{file: file, faults: faults}
}

func (f *FileBackend) ReadAt(p []byte, off int64) (n int, err error) {
	_, span := tracer.Start(context.TODO(), "nbd.ReadAt")
	defer span.End()

	n, err = f.file.ReadAt(p, off)

	fault := f.faults.GetNbdFault(pb.NbdOp_NBD_READAT, off, len(p))
	fault.Delay()
	n = int(fault.MayReplaceErrorCode(int64(n)))
	err = fault.MayReplaceError(err)

	span.SetAttributes(attribute.Int64("offset", off), attribute.Int("n", n))
	if err != nil {
		span.SetAttributes(attribute.String("err", err.Error()))
	} else {
		span.SetAttributes(attribute.String("err", ""))
	}
	return
}

func (f *FileBackend) WriteAt(p []byte, off int64) (n int, err error) {
	_, span := tracer.Start(context.TODO(), "nbd.WriteAt")
	defer span.End()

	n, err = f.file.WriteAt(p, off)

	fault := f.faults.GetNbdFault(pb.NbdOp_NBD_WRITEAT, off, len(p))
	fault.Delay()
	n = int(fault.MayReplaceErrorCode(int64(n)))
	err = fault.MayReplaceError(err)
	span.SetAttributes(attribute.Int64("offset", off), attribute.Int("n", n))
	if err != nil {
		span.SetAttributes(attribute.String("err", err.Error()))
	} else {
		span.SetAttributes(attribute.String("err", ""))
	}
	return
}

func (f *FileBackend) Size() (size int64, err error) {
	_, span := tracer.Start(context.TODO(), "nbd.Size")
	defer span.End()

	stat, err := f.file.Stat()
	if err != nil {
		size = -1
	} else {
		size = stat.Size()
	}

	fault := f.faults.GetNbdFault(pb.NbdOp_NBD_SIZE, 0, 0)
	fault.Delay()
	size = fault.MayReplaceErrorCode(size)
	err = fault.MayReplaceError(err)

	span.SetAttributes(attribute.Int64("size", size))
	if err != nil {
		span.SetAttributes(attribute.String("err", err.Error()))
	} else {
		span.SetAttributes(attribute.String("err", ""))
	}
	return
}

func (f *FileBackend) Sync() (err error) {
	_, span := tracer.Start(context.TODO(), "nbd.Sync")
	defer span.End()

	err = f.file.Sync()

	fault := f.faults.GetNbdFault(pb.NbdOp_NBD_SYNC, 0, 0)
	fault.Delay()
	err = fault.MayReplaceError(err)

	if err != nil {
		span.SetAttributes(attribute.String("err", err.Error()))
	} else {
		span.SetAttributes(attribute.String("err", ""))
	}
	return err
}
