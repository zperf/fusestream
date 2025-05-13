package slowio

import (
	"os"

	"github.com/fanyang89/slowfs/pb"
)

type FileBackend struct {
	file   *os.File
	faults *FaultManager
}

func NewFileBackend(file *os.File, faults *FaultManager) *FileBackend {
	return &FileBackend{file: file, faults: faults}
}

func (f *FileBackend) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = f.file.ReadAt(p, off)

	fault := f.faults.GetBlkFault(pb.NbdOp_NBD_READAT, off, len(p))
	fault.Delay()
	n = int(fault.MayReplaceErrorCode(int64(n)))
	err = fault.MayReplaceError(err)
	return
}

func (f *FileBackend) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = f.file.WriteAt(p, off)

	fault := f.faults.GetBlkFault(pb.NbdOp_NBD_WRITEAT, off, len(p))
	fault.Delay()
	n = int(fault.MayReplaceErrorCode(int64(n)))
	err = fault.MayReplaceError(err)
	return
}

func (f *FileBackend) Size() (size int64, err error) {
	stat, err := f.file.Stat()
	if err != nil {
		size = -1
	} else {
		size = stat.Size()
	}

	fault := f.faults.GetBlkFault(pb.NbdOp_NBD_SIZE, 0, 0)
	fault.Delay()
	size = fault.MayReplaceErrorCode(size)
	err = fault.MayReplaceError(err)
	return
}

func (f *FileBackend) Sync() (err error) {
	err = f.file.Sync()

	fault := f.faults.GetBlkFault(pb.NbdOp_NBD_SYNC, 0, 0)
	fault.Delay()
	err = fault.MayReplaceError(err)

	return err
}
