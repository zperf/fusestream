//go:build darwin || linux

package slowfs

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/winfsp/cgofuse/fuse"

	"github.com/fanyang89/slowfs/pb"
)

type SlowFS struct {
	fuse.FileSystemBase

	BaseDir string
	Faults  *FaultManager
	recordC chan string
	verbose bool
}

func New(baseDir string, faults *FaultManager, record string, verbose bool) *SlowFS {
	f := &SlowFS{
		BaseDir: baseDir,
		Faults:  faults,
		verbose: verbose,
	}

	if record != "" {
		f.recordC = make(chan string, 1024)
		go recordWriter(record, f.recordC)
	}

	return f
}

func recordWriter(record string, recordC chan string) {
	log.Trace().Str("record", record).Msg("Record writer started")
	defer log.Trace().Str("record", record).Msg("Record writer exiting")

	fh, err := os.OpenFile(record, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal().Err(err).Msg("Open record file failed")
	}
	w := bufio.NewWriter(fh)
	defer func() { _ = w.Flush(); _ = fh.Close() }()

	for r := range recordC {
		_, err = fmt.Fprintln(w, r)
		if err != nil {
			log.Fatal().Err(err).Msg("Write record file failed")
		}
	}
}

func (f *SlowFS) Init() {
	e := syscall.Chdir(f.BaseDir)
	if e == nil {
		log.Trace().Str("base-dir", f.BaseDir).Msg("Change dir")
		f.BaseDir = "./"
	}
}

func (f *SlowFS) Close() {
	// FIXME: call close()
	if f.recordC != nil {
		close(f.recordC)
	}
}

func (f *SlowFS) maybeRecord(v any) {
	if f.verbose {
		log.Trace().Interface("operation", v).Msg("")
	}
	if f.recordC != nil {
		f.recordC <- mustJsonMarshal(v)
	}
}

func (f *SlowFS) Statfs(path string, stat *fuse.Statfs_t) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	stgo := syscall.Statfs_t{}
	errc = errno(syscallStatfs(path, &stgo))
	copyFuseStatfsFromGoStatfs(stat, &stgo)

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_STATFS)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord((&StatfsRecord{
		Record: Record{
			Path:      path,
			ErrorCode: errc,
			Op:        pb.FsOp_FS_STATFS.String(),
		}}).From(stat))
	return
}

func (f *SlowFS) Mknod(path string, mode uint32, dev uint64) (errc int) {
	defer setuidgid()()
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Mknod(path, mode, int(dev)))

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_MKNOD)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&MknodRecord{
		Record: Record{
			Path:      path,
			ErrorCode: errc,
			Op:        pb.FsOp_FS_MKNOD.String(),
		},
		Mode: mode,
		Dev:  dev})
	return
}

func (f *SlowFS) Mkdir(path string, mode uint32) (errc int) {
	defer setuidgid()()
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Mkdir(path, mode))

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_MKDIR)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&MkdirRecord{
		Record: Record{path, errc, pb.FsOp_FS_MKDIR.String(), 0},
		Mode:   mode})
	return
}

func (f *SlowFS) Unlink(path string) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Unlink(path))

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_UNLINK)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(NewRecord(path, errc, pb.FsOp_FS_UNLINK))
	return
}

func (f *SlowFS) Rmdir(path string) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Rmdir(path))

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_RMDIR)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(NewRecord(path, errc, pb.FsOp_FS_RMDIR))
	return
}

func (f *SlowFS) Link(oldpath string, newpath string) (errc int) {
	defer setuidgid()()
	oldpath = filepath.Join(f.BaseDir, oldpath)
	newpath = filepath.Join(f.BaseDir, newpath)
	errc = errno(syscall.Link(oldpath, newpath))

	fault := f.Faults.GetFsFault(oldpath, pb.FsOp_FS_LINK)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&LinkRecord{
		Record:  Record{oldpath, errc, pb.FsOp_FS_LINK.String(), 0},
		NewPath: newpath})
	return
}

func (f *SlowFS) Symlink(target string, newpath string) (errc int) {
	defer setuidgid()()
	newpath = filepath.Join(f.BaseDir, newpath)
	errc = errno(syscall.Symlink(target, newpath))

	fault := f.Faults.GetFsFault(target, pb.FsOp_FS_SYMLINK)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&SymlinkRecord{
		Record:  Record{target, errc, pb.FsOp_FS_SYMLINK.String(), 0},
		NewPath: newpath})
	return
}

func (f *SlowFS) Readlink(path string) (errc int, target string) {
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

	fault := f.Faults.GetFsFault(target, pb.FsOp_FS_READLINK)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&ReadlinkRecord{
		Record: Record{path, errc, pb.FsOp_FS_READLINK.String(), 0},
		Target: target})
	return
}

func (f *SlowFS) Rename(oldpath string, newpath string) (errc int) {
	defer setuidgid()()
	oldpath = filepath.Join(f.BaseDir, oldpath)
	newpath = filepath.Join(f.BaseDir, newpath)
	errc = errno(syscall.Rename(oldpath, newpath))

	fault := f.Faults.GetFsFault(oldpath, pb.FsOp_FS_RENAME)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&RenameRecord{
		Record:  Record{oldpath, errc, pb.FsOp_FS_RENAME.String(), 0},
		NewPath: newpath})
	return
}

func (f *SlowFS) Chmod(path string, mode uint32) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Chmod(path, mode))

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_CHMOD)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&ChmodRecord{
		Record: Record{path, errc, pb.FsOp_FS_CHMOD.String(), 0},
		Mode:   mode,
	})
	return
}

func (f *SlowFS) Chown(path string, uid uint32, gid uint32) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Lchown(path, int(uid), int(gid)))

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_CHOWN)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&ChownRecord{
		Record: Record{path, errc, pb.FsOp_FS_CHOWN.String(), 0},
		UID:    uid,
		GID:    gid,
	})
	return
}

func (f *SlowFS) Utimens(path string, tmsp1 []fuse.Timespec) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	tmsp := [2]syscall.Timespec{}
	tmsp[0].Sec, tmsp[0].Nsec = tmsp1[0].Sec, tmsp1[0].Nsec
	tmsp[1].Sec, tmsp[1].Nsec = tmsp1[1].Sec, tmsp1[1].Nsec
	errc = errno(syscall.UtimesNano(path, tmsp[:]))

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_UTIMENS)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord((&UtimensRecord{
		Record: Record{path, errc, pb.FsOp_FS_UTIMENS.String(), 0},
	}).From(tmsp1))
	return
}

func (f *SlowFS) Create(path string, flags int, mode uint32) (errc int, fh uint64) {
	defer setuidgid()()
	errc, fh = f.open(path, flags, mode)

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_CREATE)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&OpenCreateRecord{
		Record: Record{path, errc, pb.FsOp_FS_CREATE.String(), fh},
		Flags:  flags,
		Mode:   mode,
	})
	return
}

func (f *SlowFS) Open(path string, flags int) (errc int, fh uint64) {
	mode := uint32(0)
	errc, fh = f.open(path, flags, mode)

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_OPEN)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&OpenCreateRecord{
		Record: Record{path, errc, pb.FsOp_FS_OPEN.String(), fh},
		Flags:  flags,
		Mode:   mode,
	})
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
	stgo := syscall.Stat_t{}
	if ^uint64(0) == fh {
		path = filepath.Join(f.BaseDir, path)
		errc = errno(syscall.Lstat(path, &stgo))
	} else {
		errc = errno(syscall.Fstat(int(fh), &stgo))
	}
	copyFusestatFromGostat(stat, &stgo)

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_GETATTR)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	var s Stat
	s.From(stat)
	f.maybeRecord(&GetattrRecord{
		Record: Record{path, errc, pb.FsOp_FS_GETATTR.String(), fh},
		Stat:   s,
	})
	return
}

func (f *SlowFS) Truncate(path string, size int64, fh uint64) (errc int) {
	if fh == ^uint64(0) {
		path = filepath.Join(f.BaseDir, path)
		errc = errno(syscall.Truncate(path, size))
	} else {
		errc = errno(syscall.Ftruncate(int(fh), size))
	}

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_TRUNCATE)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&TruncateRecord{
		Record: Record{path, errc, pb.FsOp_FS_TRUNCATE.String(), fh},
		Size:   size,
	})
	return
}

func (f *SlowFS) Read(path string, buff []byte, ofst int64, fh uint64) (rc int) {
	var err error
	var n int
	n, err = syscall.Pread(int(fh), buff, ofst)
	if err != nil {
		rc = errno(err)
	} else {
		rc = n
	}

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_READ)
	fault.Delay()
	rc = int(fault.MayReplaceErrorCode(int64(rc)))

	f.maybeRecord(&ReadWriteRecord{
		Record: Record{path, rc, pb.FsOp_FS_READ.String(), fh},
		Offset: ofst,
	})
	return
}

func (f *SlowFS) Write(path string, buff []byte, ofst int64, fh uint64) (rc int) {
	var err error
	var n int

	n, err = syscall.Pwrite(int(fh), buff, ofst)
	if err != nil {
		rc = errno(err)
	} else {
		rc = n
	}

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_WRITE)
	fault.Delay()
	rc = int(fault.MayReplaceErrorCode(int64(rc)))

	f.maybeRecord(&ReadWriteRecord{
		Record: Record{path, rc, pb.FsOp_FS_WRITE.String(), fh},
		Offset: ofst,
	})
	return
}

func (f *SlowFS) Release(path string, fh uint64) (errc int) {
	errc = errno(syscall.Close(int(fh)))

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_RELEASE)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&Record{path, errc, pb.FsOp_FS_RELEASE.String(), fh})
	return
}

func (f *SlowFS) Fsync(path string, datasync bool, fh uint64) (errc int) {
	errc = errno(fsync(datasync, int(fh)))

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_FSYNC)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&FsyncRecord{
		Record:     Record{path, errc, pb.FsOp_FS_FSYNC.String(), fh},
		IsDataSync: datasync,
	})
	return
}

func (f *SlowFS) Opendir(path string) (errc int, fh uint64) {
	path = filepath.Join(f.BaseDir, path)
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		errc = errno(err)
		fh = ^uint64(0)
	} else {
		errc = 0
		fh = uint64(fd)
	}

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_OPENDIR)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&Record{path, errc, pb.FsOp_FS_OPENDIR.String(), fh})
	return
}

type fillFn = func(name string, stat *fuse.Stat_t, ofst int64) bool

func (f *SlowFS) Readdir(path string, fill fillFn, ofst int64, fh uint64) (errc int) {
	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_READDIR)
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
	f.maybeRecord(&ReaddirRecord{
		Record: Record{path, errc, pb.FsOp_FS_READDIR.String(), fh},
		Offset: ofst,
	})
	return
}

func (f *SlowFS) Releasedir(path string, fh uint64) (errc int) {
	errc = errno(syscall.Close(int(fh)))

	fault := f.Faults.GetFsFault(path, pb.FsOp_FS_READDIR)
	fault.Delay()
	errc = int(fault.MayReplaceErrorCode(int64(errc)))

	f.maybeRecord(&Record{path, errc, pb.FsOp_FS_RELEASEDIR.String(), fh})
	return
}

func errno(err error) int {
	if err != nil {
		return -int(err.(syscall.Errno))
	} else {
		return 0
	}
}
