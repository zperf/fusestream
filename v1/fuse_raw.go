package slowio

import (
	"os"
	"path/filepath"
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/winfsp/cgofuse/fuse"
)

type RawFS struct {
	fuse.FileSystemBase
	BaseDir          string
	DisableReadAhead bool
}

func NewRawFS(baseDir string) *RawFS {
	return &RawFS{
		BaseDir:          baseDir,
		DisableReadAhead: true,
	}
}

func (f *RawFS) Init() {
	e := syscall.Chdir(f.BaseDir)
	if e == nil {
		log.Trace().Str("base-dir", f.BaseDir).Msg("Change dir")
		f.BaseDir = "./"
	}
}

func (f *RawFS) Statfs(path string, stat *fuse.Statfs_t) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	stgo := syscall.Statfs_t{}
	errc = errno(syscall.Statfs(path, &stgo))
	*stat = *newFuseStatfsFromGo(&stgo)
	return
}

func (f *RawFS) Mknod(path string, mode uint32, dev uint64) (errc int) {
	defer setUIDAndGID()()
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Mknod(path, mode, int(dev)))
	return
}

func (f *RawFS) Mkdir(path string, mode uint32) (errc int) {
	defer setUIDAndGID()()
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Mkdir(path, mode))
	return
}

func (f *RawFS) Unlink(path string) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Unlink(path))
	return
}

func (f *RawFS) Rmdir(path string) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Rmdir(path))
	return
}

func (f *RawFS) Link(oldpath string, newpath string) (errc int) {
	defer setUIDAndGID()()
	oldpath = filepath.Join(f.BaseDir, oldpath)
	newpath = filepath.Join(f.BaseDir, newpath)
	errc = errno(syscall.Link(oldpath, newpath))
	return
}

func (f *RawFS) Symlink(target string, newpath string) (errc int) {
	defer setUIDAndGID()()
	newpath = filepath.Join(f.BaseDir, newpath)
	errc = errno(syscall.Symlink(target, newpath))
	return
}

func (f *RawFS) Readlink(path string) (errc int, target string) {
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
	return
}

func (f *RawFS) Rename(oldpath string, newpath string) (errc int) {
	defer setUIDAndGID()()
	oldpath = filepath.Join(f.BaseDir, oldpath)
	newpath = filepath.Join(f.BaseDir, newpath)
	errc = errno(syscall.Rename(oldpath, newpath))
	return
}

func (f *RawFS) Chmod(path string, mode uint32) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Chmod(path, mode))
	return
}

func (f *RawFS) Chown(path string, uid uint32, gid uint32) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	errc = errno(syscall.Lchown(path, int(uid), int(gid)))
	return
}

func (f *RawFS) Utimens(path string, tmsp1 []fuse.Timespec) (errc int) {
	path = filepath.Join(f.BaseDir, path)
	tmsp := [2]syscall.Timespec{}
	tmsp[0].Sec, tmsp[0].Nsec = tmsp1[0].Sec, tmsp1[0].Nsec
	tmsp[1].Sec, tmsp[1].Nsec = tmsp1[1].Sec, tmsp1[1].Nsec
	errc = errno(syscall.UtimesNano(path, tmsp[:]))
	return
}

func (f *RawFS) Create(path string, flags int, mode uint32) (errc int, fh uint64) {
	defer setUIDAndGID()()
	errc, fh = f.open(path, flags, mode)
	return
}

func (f *RawFS) Open(path string, flags int) (errc int, fh uint64) {
	errc, fh = f.open(path, flags, 0)
	return
}

func (f *RawFS) open(path string, flags int, mode uint32) (errc int, fh uint64) {
	fd, e := syscall.Open(filepath.Join(f.BaseDir, path), flags, mode)
	if e != nil {
		errc = errno(e)
		fh = ^uint64(0)
	} else {
		errc = 0
		fh = uint64(fd)
	}
	if f.DisableReadAhead {
		_ = fadviseRandom(fd)
	}
	return
}

func (f *RawFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	stgo := syscall.Stat_t{}
	if fh == ^uint64(0) {
		path = filepath.Join(f.BaseDir, path)
		errc = errno(syscall.Lstat(path, &stgo))
	} else {
		errc = errno(syscall.Fstat(int(fh), &stgo))
	}
	*stat = *newFuseStatFromGo(&stgo)
	return
}

func (f *RawFS) Truncate(path string, size int64, fh uint64) (errc int) {
	if fh == ^uint64(0) {
		path = filepath.Join(f.BaseDir, path)
		errc = errno(syscall.Truncate(path, size))
	} else {
		errc = errno(syscall.Ftruncate(int(fh), size))
	}
	return
}

func (f *RawFS) Read(path string, buff []byte, ofst int64, fh uint64) (rc int) {
	_ = path
	n, err := syscall.Pread(int(fh), buff, ofst)
	if err != nil {
		rc = errno(err)
	} else {
		rc = n
	}
	return
}

func (f *RawFS) Write(path string, buff []byte, ofst int64, fh uint64) (rc int) {
	_ = path
	n, err := syscall.Pwrite(int(fh), buff, ofst)
	if err != nil {
		rc = errno(err)
	} else {
		rc = n
	}
	return
}

func (f *RawFS) Release(path string, fh uint64) (errc int) {
	_ = path
	errc = errno(syscall.Close(int(fh)))
	return
}

func (f *RawFS) Fsync(path string, datasync bool, fh uint64) (errc int) {
	_ = path
	if datasync {
		errc = errno(syscall.Fdatasync(int(fh)))
	} else {
		errc = errno(syscall.Fsync(int(fh)))
	}
	return
}

func (f *RawFS) Opendir(path string) (errc int, fh uint64) {
	path = filepath.Join(f.BaseDir, path)
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		errc = errno(err)
		fh = ^uint64(0)
	} else {
		errc = 0
		fh = uint64(fd)
	}
	return
}

func (f *RawFS) Readdir(path string, fill fillFn, ofst int64, fh uint64) (errc int) {
	file, err := os.Open(filepath.Join(f.BaseDir, path))
	if err != nil {
		errc = errno(err)
		return
	}
	defer func() { _ = file.Close() }()

	names, err := file.Readdirnames(0)
	if err != nil {
		errc = errno(err)
	} else {
		names = append([]string{".", ".."}, names...)
		for _, name := range names {
			if !fill(name, nil, 0) {
				break
			}
		}
		errc = 0
	}
	return
}

func (f *RawFS) Releasedir(path string, fh uint64) (errc int) {
	_ = path
	errc = errno(syscall.Close(int(fh)))
	return
}
