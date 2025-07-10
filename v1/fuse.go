//go:build linux || windows

package slowio

import (
	"context"
	"fmt"
	"path/filepath"
	"syscall"

	"github.com/winfsp/cgofuse/fuse"
	"go.opentelemetry.io/otel/attribute"

	"github.com/fanyang89/slowio/pb"
)

type SlowFS struct {
	RawFS
	Faults *FaultManager
}

func NewSlowFS(baseDir string, faults *FaultManager) *SlowFS {
	return &SlowFS{
		RawFS: RawFS{
			BaseDir:          baseDir,
			DisableReadAhead: true,
		},
		Faults: faults,
	}
}

func (f *SlowFS) Init() {
	f.RawFS.Init()
}

func (f *SlowFS) Statfs(path string, stat *fuse.Statfs_t) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Statfs")
	defer span.End()

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_STATFS)
	fault.Delay()

	errc = f.RawFS.Statfs(path, stat)
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int("errc", errc),
	)
	return
}

func (f *SlowFS) Mknod(path string, mode uint32, dev uint64) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Mknod")
	defer span.End()

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_MKNOD)
	fault.Delay()

	errc = f.RawFS.Mknod(path, mode, dev)
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_MKDIR)
	fault.Delay()

	errc = f.RawFS.Mkdir(path, mode)
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_UNLINK)
	fault.Delay()

	errc = f.RawFS.Unlink(path)
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_RMDIR)
	fault.Delay()

	errc = f.RawFS.Rmdir(path)
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

	fault := f.Faults.GetFuseFault(oldpath, pb.FuseOp_FUSE_LINK)
	fault.Delay()

	errc = f.RawFS.Link(oldpath, newpath)
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

	fault := f.Faults.GetFuseFault(target, pb.FuseOp_FUSE_SYMLINK)
	fault.Delay()

	errc = f.RawFS.Symlink(target, newpath)
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

	fault := f.Faults.GetFuseFault(target, pb.FuseOp_FUSE_READLINK)
	fault.Delay()

	errc, target = f.RawFS.Readlink(path)
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

	fault := f.Faults.GetFuseFault(oldpath, pb.FuseOp_FUSE_RENAME)
	fault.Delay()

	errc = f.RawFS.Rename(oldpath, newpath)
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_CHMOD)
	fault.Delay()

	errc = f.RawFS.Chmod(path, mode)
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_CHOWN)
	fault.Delay()

	errc = f.RawFS.Chown(path, uid, gid)
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_UTIMENS)
	fault.Delay()

	errc = f.RawFS.Utimens(path, tmsp1)
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_CREATE)
	fault.Delay()

	errc, fh = f.RawFS.Create(path, flags, mode)
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_OPEN)
	fault.Delay()

	errc, fh = f.RawFS.Open(path, flags)
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int("flags", flags),
		attribute.Int("errc", errc),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_GETATTR)
	fault.Delay()

	errc = f.RawFS.Getattr(path, stat, fh)
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_TRUNCATE)
	fault.Delay()

	errc = f.RawFS.Truncate(path, size, fh)
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_READ)
	fault.Delay()

	rc = f.RawFS.Read(path, buff, ofst, fh)
	rc = int(fault.MayReplaceErrorCode(int64(rc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int64("offset", ofst),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
		attribute.Int("length", rc),
	)
	return
}

func (f *SlowFS) Write(path string, buff []byte, ofst int64, fh uint64) (rc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Write")
	defer span.End()

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_WRITE)
	fault.Delay()

	rc = f.RawFS.Write(path, buff, ofst, fh)
	rc = int(fault.MayReplaceErrorCode(int64(rc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.Int64("offset", ofst),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
		attribute.Int("length", rc),
	)
	return
}

func (f *SlowFS) Release(path string, fh uint64) (errc int) {
	_, span := tracer.Start(context.TODO(), "fuse.Release")
	defer span.End()

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_RELEASE)
	fault.Delay()

	errc = f.RawFS.Release(path, fh)
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_FSYNC)
	fault.Delay()

	errc = f.RawFS.Fsync(path, datasync, fh)
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_OPENDIR)
	fault.Delay()

	errc, fh = f.RawFS.Opendir(path)
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

	errc = f.RawFS.Readdir(path, fill, ofst, fh)
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

	fault := f.Faults.GetFuseFault(path, pb.FuseOp_FUSE_READDIR)
	fault.Delay()

	errc = f.RawFS.Releasedir(path, fh)
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	span.SetAttributes(
		attribute.String("path", path),
		attribute.String("fh", fmt.Sprintf("%d", fh)),
		attribute.Int("errc", errc),
	)
	return
}
