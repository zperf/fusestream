//go:build darwin || linux

package slowio

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/winfsp/cgofuse/fuse"
	"go.opentelemetry.io/otel/attribute"

	"github.com/fanyang89/slowio/pb"
)

type SlowFS struct {
	fuse.FileSystemBase

	BaseDir string
	Faults  *FaultManager
}

func New(baseDir string, faults *FaultManager) *SlowFS {
	return &SlowFS{
		BaseDir: baseDir,
		Faults:  faults,
	}
}

func (f *SlowFS) Init() {
	e := syscall.Chdir(f.BaseDir)
	if e == nil {
		log.Trace().Str("base-dir", f.BaseDir).Msg("Change dir")
		f.BaseDir = "./"
	}
}

func (f *SlowFS) Statfs(path string, stat *fuse.Statfs_t) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Statfs")
	defer span.End()

	path = filepath.Join(f.BaseDir, path)
	stgo := syscall.Statfs_t{}
	errc = errno(syscall.Statfs(path, &stgo))
	*stat = *newFuseStatfsFromGo(&stgo)

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_STATFS)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int("rc", errc),
	)
	return
}

func (f *SlowFS) Mknod(path string, mode uint32, dev uint64) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Mknod")
	defer span.End()

	defer setUIDAndGID()()
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Mknod(path, mode, int(dev)))

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_MKNOD)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int64("mode", int64(mode)),
		attribute.String("dev", fmt.Sprintf("%d", dev)),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Mkdir(path string, mode uint32) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Mkdir")
	defer span.End()

	defer setUIDAndGID()()
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Mkdir(path, mode))

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_MKDIR)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int64("mode", int64(mode)),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Unlink(path string) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Unlink")
	defer span.End()

	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Unlink(path))

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_UNLINK)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Rmdir(path string) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Rmdir")
	defer span.End()

	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Rmdir(path))

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_RMDIR)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Link(oldpath string, newpath string) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Link")
	defer span.End()

	defer setUIDAndGID()()
	oldpath = filepath.Join(f.BaseDir, oldpath)
	newpath = filepath.Join(f.BaseDir, newpath)
	errc = errno(syscall.Link(oldpath, newpath))

	fault := f.Faults.GetFuseFault(oldpath, pb.FuseOp_FUSE_LINK)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("oldpath", oldpath),
		attribute.String("newpath", newpath),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Symlink(target string, newpath string) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Symlink")
	defer span.End()

	defer setUIDAndGID()()
	newpath = filepath.Join(f.BaseDir, newpath)
	errc = errno(syscall.Symlink(target, newpath))

	fault := f.Faults.GetFuseFault(target, pb.FuseOp_FUSE_SYMLINK)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("target", target),
		attribute.String("newpath", newpath),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Readlink(path string) (errc int, target string) {
	_, span := tracer.Start(context.TODO(), "fuse.Readlink")
	defer span.End()

	path = filepath.Join(f.BaseDir, path)
	buff := [1024]byte{}
	n, e := syscall.Readlink(path, buff[:])
	if e != nil {
		errc = errno(e)
		target = ""
	} else {
		errc = 0
		target = string(buff[:n])
	}

	fault := f.Faults.GetFuseFault(target, pb.FuseOp_FUSE_READLINK)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int("errc", errc),
		attribute.String("target", target),
	)
	return
}

func (f *SlowFS) Rename(oldpath string, newpath string) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Rename")
	defer span.End()

	defer setUIDAndGID()()
	oldpath = filepath.Join(f.BaseDir, oldpath)
	newpath = filepath.Join(f.BaseDir, newpath)
	errc = errno(syscall.Rename(oldpath, newpath))

	fault := f.Faults.GetFuseFault(oldpath, pb.FuseOp_FUSE_RENAME)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("oldpath", oldpath),
		attribute.String("newpath", newpath),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Chmod(path string, mode uint32) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Chmod")
	defer span.End()

	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Chmod(path, mode))

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_CHMOD)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int64("mode", int64(mode)),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Chown(path string, uid uint32, gid uint32) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Chown")
	defer span.End()

	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Lchown(path, int(uid), int(gid)))

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_CHOWN)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int64("uid", int64(uid)),
		attribute.Int64("gid", int64(gid)),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Utimens(path string, tmsp1 []fuse.Timespec) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Utimens")
	defer span.End()

	path = filepath.Join(f.BaseDir, path)
	tmsp := [2]syscall.Timespec{}
	tmsp[0].Sec, tmsp[0].Nsec = tmsp1[0].Sec, tmsp1[0].Nsec
	tmsp[1].Sec, tmsp[1].Nsec = tmsp1[1].Sec, tmsp1[1].Nsec
	errc = errno(syscall.UtimesNano(path, tmsp[:]))

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_UTIMENS)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int64("atime_sec", tmsp1[0].Sec),
		attribute.Int64("atime_ns", tmsp1[0].Nsec),
		attribute.Int64("mtime_sec", tmsp1[1].Sec),
		attribute.Int64("mtime_ns", tmsp1[1].Nsec),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Create(path string, flags int, mode uint32) (errc int, fh uint64) {
	_, span := tracer.Start(context.TODO(), "fuse.Create")
	defer span.End()

	defer setUIDAndGID()()
	errc, fh = f.open(path, flags, mode)

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_CREATE)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int("flags", flags),
		attribute.Int64("mode", int64(mode)),
		attribute.Int("errc", errc),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
	)
	return
}

func (f *SlowFS) Open(path string, flags int) (errc int, fh uint64) {
	_, span := tracer.Start(context.TODO(), "fuse.Open")
	defer span.End()

	mode := uint32(0)
	errc, fh = f.open(path, flags, mode)

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_OPEN)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int("flags", flags),
		attribute.Int("errc", errc),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
	)
	return
}

func (f *SlowFS) open(path string, flags int, mode uint32) (errc int, fh uint64) {
	fd, e := syscall.Open(filepath.Join(f.BaseDir, path), flags, mode)
	if e != nil {
		errc = errno(e)
		fh = ^uint64(0)
	} else {
		errc = 0
		fh = uint64(fd)
	}
	return
}

func (f *SlowFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Getattr")
	defer span.End()

	stgo := syscall.Stat_t{}
	if fh == ^uint64(0) {
		path = filepath.Join(f.BaseDir, path)
		errc = errno(syscall.Lstat(path, &stgo))
	} else {
		errc = errno(syscall.Fstat(int(fh), &stgo))
	}
	*stat = *newFuseStatFromGo(&stgo)

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_GETATTR)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Truncate(path string, size int64, fh uint64) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Truncate")
	defer span.End()

	if fh == ^uint64(0) {
		path = filepath.Join(f.BaseDir, path)
		errc = errno(syscall.Truncate(path, size))
	} else {
		errc = errno(syscall.Ftruncate(int(fh), size))
	}

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_TRUNCATE)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int64("size", size),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Read(path string, buff []byte, ofst int64, fh uint64) (rc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Read")
	defer span.End()

	var err error
	var n int
	n, err = syscall.Pread(int(fh), buff, ofst)
	if err != nil {
		rc = errno(err)
	} else {
		rc = n
	}

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_READ)
	fault.Delay()
	rc = int(fault.MayReplaceErrorCode(int64(rc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int64("offset", ofst),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
		attribute.Int("rc", rc),
	)
	return
}

func (f *SlowFS) Write(path string, buff []byte, ofst int64, fh uint64) (rc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Write")
	defer span.End()

	var err error
	var n int

	n, err = syscall.Pwrite(int(fh), buff, ofst)
	if err != nil {
		rc = errno(err)
	} else {
		rc = n
	}

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_WRITE)
	fault.Delay()
	rc = int(fault.MayReplaceErrorCode(int64(rc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int64("offset", ofst),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
		attribute.Int("rc", rc),
	)
	return
}

func (f *SlowFS) Release(path string, fh uint64) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Release")
	defer span.End()

	errc = errno(syscall.Close(int(fh)))

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_RELEASE)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Fsync(path string, datasync bool, fh uint64) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Fsync")
	defer span.End()

	if datasync {
		errc = errno(syscall.Fdatasync(int(fh)))
	} else {
		errc = errno(syscall.Fsync(int(fh)))
	}

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_FSYNC)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Bool("data_sync", datasync),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Opendir(path string) (errc int, fh uint64) {
	_, span := tracer.Start(context.TODO(), "fuse.Opendir")
	defer span.End()

	path = filepath.Join(f.BaseDir, path)
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		errc = errno(err)
		fh = ^uint64(0)
	} else {
		errc = 0
		fh = uint64(fd)
	}

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_OPENDIR)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
		attribute.Int("errc", errc),
	)
	return
}

type fillFn = func(name string, stat *fuse.Stat_t, ofst int64) bool

func (f *SlowFS) Readdir(path string, fill fillFn, ofst int64, fh uint64) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Readdir")
	defer span.End()

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_READDIR)
	fault.Delay()

	file, err := os.Open(filepath.Join(f.BaseDir, path))
	if err != nil {
		errc = errno(err)
		errc = int(fault.MayReplaceErrorCode(int64(errc)))
		return
	}
	defer func() { _ = file.Close() }()

	nams, err := file.Readdirnames(0)
	if err != nil {
		errc = errno(err)
	} else {
		nams = append([]string{".", ".."}, nams...)
		for _, name := range nams {
			if !fill(name, nil, 0) {
				break
			}
		}
		errc = 0
	}

	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int64("offset", ofst),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Releasedir(path string, fh uint64) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Releasedir")
	defer span.End()

	errc = errno(syscall.Close(int(fh)))

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_READDIR)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
		attribute.Int("errc", errc),
	)
	return
}

func errno(err error) int {
	if err != nil {
		return -int(err.(syscall.Errno))
	} else {
		return 0
	}
}
