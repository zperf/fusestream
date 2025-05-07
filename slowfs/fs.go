//go:build darwin || linux

package slowfs

import (
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
}

func (f *SlowFS) Init() {
	e := syscall.Chdir(f.BaseDir)
	if e == nil {
		log.Trace().Str("base-dir", f.BaseDir).Msg("Change dir")
		f.BaseDir = "./"
	}
}

func (f *SlowFS) Statfs(path string, stat *fuse.Statfs_t) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	stgo := syscall.Statfs_t{}
	errc = errno(syscallStatfs(path, &stgo))
	copyFuseStatfsFromGoStatfs(stat, &stgo)
	errc = f.Faults.Query(path, pb.OpCode_STATFS).Execute(errc)
	return
}

func (f *SlowFS) Mknod(path string, mode uint32, dev uint64) (errc int) {
	defer setuidgid()()
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Mknod(path, mode, int(dev)))
	errc = f.Faults.Query(path, pb.OpCode_MKNOD).Execute(errc)
	return
}

func (f *SlowFS) Mkdir(path string, mode uint32) (errc int) {
	defer setuidgid()()
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Mkdir(path, mode))
	errc = f.Faults.Query(path, pb.OpCode_MKDIR).Execute(errc)
	return
}

func (f *SlowFS) Unlink(path string) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Unlink(path))
	errc = f.Faults.Query(path, pb.OpCode_UNLINK).Execute(errc)
	return
}

func (f *SlowFS) Rmdir(path string) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Rmdir(path))
	errc = f.Faults.Query(path, pb.OpCode_RMDIR).Execute(errc)
	return
}

func (f *SlowFS) Link(oldpath string, newpath string) (errc int) {
	defer setuidgid()()
	oldpath = filepath.Join(f.BaseDir, oldpath)
	newpath = filepath.Join(f.BaseDir, newpath)
	errc = errno(syscall.Link(oldpath, newpath))
	errc = f.Faults.Query(oldpath, pb.OpCode_LINK).Execute(errc)
	return
}

func (f *SlowFS) Symlink(target string, newpath string) (errc int) {
	defer setuidgid()()
	newpath = filepath.Join(f.BaseDir, newpath)
	errc = errno(syscall.Symlink(target, newpath))
	errc = f.Faults.Query(target, pb.OpCode_SYMLINK).Execute(errc)
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
	errc = f.Faults.Query(target, pb.OpCode_READLINK).Execute(errc)
	return
}

func (f *SlowFS) Rename(oldpath string, newpath string) (errc int) {
	defer setuidgid()()
	oldpath = filepath.Join(f.BaseDir, oldpath)
	newpath = filepath.Join(f.BaseDir, newpath)
	errc = errno(syscall.Rename(oldpath, newpath))
	errc = f.Faults.Query(oldpath, pb.OpCode_RENAME).Execute(errc)
	return
}

func (f *SlowFS) Chmod(path string, mode uint32) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Chmod(path, mode))
	errc = f.Faults.Query(path, pb.OpCode_CHMOD).Execute(errc)
	return
}

func (f *SlowFS) Chown(path string, uid uint32, gid uint32) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Lchown(path, int(uid), int(gid)))
	errc = f.Faults.Query(path, pb.OpCode_CHOWN).Execute(errc)
	return
}

func (f *SlowFS) Utimens(path string, tmsp1 []fuse.Timespec) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	tmsp := [2]syscall.Timespec{}
	tmsp[0].Sec, tmsp[0].Nsec = tmsp1[0].Sec, tmsp1[0].Nsec
	tmsp[1].Sec, tmsp[1].Nsec = tmsp1[1].Sec, tmsp1[1].Nsec
	errc = errno(syscall.UtimesNano(path, tmsp[:]))
	errc = f.Faults.Query(path, pb.OpCode_UTIMENS).Execute(errc)
	return
}

func (f *SlowFS) Create(path string, flags int, mode uint32) (errc int, fh uint64) {
	defer setuidgid()()
	return f.open(path, flags, mode)
}

func (f *SlowFS) Open(path string, flags int) (errc int, fh uint64) {
	return f.open(path, flags, 0)
}

func (f *SlowFS) open(path string, flags int, mode uint32) (errc int, fh uint64) {
	path = filepath.Join(f.BaseDir, path)
	fd, e := syscall.Open(path, flags, mode)
	if e != nil {
		errc = errno(e)
		fh = ^uint64(0)
	} else {
		errc = 0
		fh = uint64(fd)
	}
	if mode == 0 {
		errc = f.Faults.Query(path, pb.OpCode_OPEN).Execute(errc)
	} else {
		errc = f.Faults.Query(path, pb.OpCode_CREATE).Execute(errc)
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
	errc = f.Faults.Query(path, pb.OpCode_GETATTR).Execute(errc)
	copyFusestatFromGostat(stat, &stgo)
	return
}

func (f *SlowFS) Truncate(path string, size int64, fh uint64) (errc int) {
	if fh == ^uint64(0) {
		path = filepath.Join(f.BaseDir, path)
		errc = errno(syscall.Truncate(path, size))
	} else {
		errc = errno(syscall.Ftruncate(int(fh), size))
	}
	errc = f.Faults.Query(path, pb.OpCode_TRUNCATE).Execute(errc)
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
	rc = f.Faults.Query(path, pb.OpCode_READ).Execute(rc)
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
	rc = f.Faults.Query(path, pb.OpCode_WRITE).Execute(rc)
	return
}

func (f *SlowFS) Release(path string, fh uint64) (errc int) {
	errc = errno(syscall.Close(int(fh)))
	errc = f.Faults.Query(path, pb.OpCode_RELEASE).Execute(errc)
	return
}

func (f *SlowFS) Fsync(path string, datasync bool, fh uint64) (errc int) {
	errc = errno(fsync(datasync, int(fh)))
	errc = f.Faults.Query(path, pb.OpCode_FSYNC).Execute(errc)
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
	errc = f.Faults.Query(path, pb.OpCode_OPENDIR).Execute(errc)
	return
}

type fillFn = func(name string, stat *fuse.Stat_t, ofst int64) bool

func (f *SlowFS) Readdir(path string, fill fillFn, ofst int64, fh uint64) (errc int) {
	file, err := os.Open(filepath.Join(f.BaseDir, path))
	if err != nil {
		errc = errno(err)
		errc = f.Faults.Query(path, pb.OpCode_READDIR).Execute(errc)
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
	errc = f.Faults.Query(path, pb.OpCode_READDIR).Execute(errc)
	return
}

func (f *SlowFS) Releasedir(path string, fh uint64) (errc int) {
	errc = errno(syscall.Close(int(fh)))
	errc = f.Faults.Query(path, pb.OpCode_RELEASEDIR).Execute(errc)
	return
}

func errno(err error) int {
	if err != nil {
		return -int(err.(syscall.Errno))
	} else {
		return 0
	}
}
