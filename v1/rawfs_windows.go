package slowio

import (
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/winfsp/cgofuse/fuse"
	"golang.org/x/sys/windows"
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

//func (f *RawFS) Statfs(path string, stat *fuse.Statfs_t) (errc int) {
//	path = filepath.Join(f.BaseDir, path)
//	windows.GetFileInformationByHandle()
//	stgo := syscall.Statfs_t{}
//	errc = errno(syscall.Statfs(path, &stgo))
//	*stat = *newFuseStatfsFromGo(&stgo)
//	return
//}

func (f *RawFS) Mkdir(path string, mode uint32) (errc int) {
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
	oldpath = filepath.Join(f.BaseDir, oldpath)
	newpath = filepath.Join(f.BaseDir, newpath)
	errc = errno(syscall.Link(oldpath, newpath))
	return
}

func (f *RawFS) Symlink(target string, newpath string) (errc int) {
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
	errc, fh = f.open(path, flags, mode)
	return
}

func (f *RawFS) Open(path string, flags int) (errc int, fh uint64) {
	errc, fh = f.open(path, flags, 0)
	return
}

func (f *RawFS) open(path string, flags int, mode uint32) (errc int, fh uint64) {
	pPath, err := windows.UTF16PtrFromString(path)
	if err != nil {
		errc = errno(err)
		fh = ^uint64(0)
		return
	}

	var handle windows.Handle
	handle, err = windows.CreateFile(
		pPath,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		uint32(windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OVERLAPPED|windows.FILE_FLAG_RANDOM_ACCESS),
		0,
	)

	if err != nil {
		errc = errno(err)
		fh = ^uint64(0)
		return
	}

	fh = uint64(handle)
	return
}

// FileStat 结构体用于存储文件属性信息
type FileStat struct {
	Ino     uint64    // 文件 inode
	Nlink   uint32    // 硬链接数
	Mode    uint32    // 文件模式（权限和类型）
	Size    int64     // 文件大小
	Blocks  int64     // 分配的块数
	Blksize int64     // 块大小
	Atime   time.Time // 访问时间
	Mtime   time.Time // 修改时间
	Ctime   time.Time // 创建时间
	Uid     uint32    // 用户ID
	Gid     uint32    // 组ID
}

func getFileStat(handle windows.Handle) (*FileStat, error) {
	var fileInfo windows.ByHandleFileInformation
	err := windows.GetFileInformationByHandle(handle, &fileInfo)
	if err != nil {
		return nil, err
	}

	fileSize := int64(fileInfo.FileSizeHigh)<<32 + int64(fileInfo.FileSizeLow)
	atime := fileTimeToTime(fileInfo.LastAccessTime)
	mtime := fileTimeToTime(fileInfo.LastWriteTime)
	ctime := fileTimeToTime(fileInfo.CreationTime)

	var mode uint32
	if fileInfo.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		// is a directory
		mode = syscall.S_IFDIR | 0555
	} else {
		// is a file
		mode = syscall.S_IFREG | 0444
	}

	// 计算 inode (Windows 使用 FileIndex 作为类似 inode 的标识)
	ino := (uint64(fileInfo.FileIndexHigh) << 32) | uint64(fileInfo.FileIndexLow)

	return &FileStat{
		Ino:     ino,
		Nlink:   fileInfo.NumberOfLinks,
		Mode:    mode,
		Size:    fileSize,
		Blocks:  (fileSize + 511) / 512,
		Blksize: 4096,
		Atime:   atime,
		Mtime:   mtime,
		Ctime:   ctime,
	}, nil
}

func fileTimeToTime(ft windows.Filetime) time.Time {
	// 100-nanosecond intervals since January 1, 1601
	nsec := int64(ft.HighDateTime)<<32 + int64(ft.LowDateTime)
	// convert to Unix epoch (seconds since 1970-01-01)
	unix := (nsec - 116444736000000000) / 10000000
	return time.Unix(unix, 0)
}

func (f *RawFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	fileStat, err := getFileStat(windows.Handle(fh))
	if err != nil {
		errc = errno(err)
		return
	}

	stat = &fuse.Stat_t{
		Ino:      fileStat.Ino,
		Mode:     fileStat.Mode,
		Nlink:    fileStat.Nlink,
		Uid:      fileStat.Uid,
		Gid:      fileStat.Gid,
		Size:     fileStat.Size,
		Atim:     fuse.Timespec{Sec: fileStat.Atime.Unix()},
		Mtim:     fuse.Timespec{Sec: fileStat.Mtime.Unix()},
		Ctim:     fuse.Timespec{Sec: fileStat.Ctime.Unix()},
		Blksize:  fileStat.Blksize,
		Blocks:   fileStat.Blocks,
		Birthtim: fuse.Timespec{},
	}
	return
}

func (f *RawFS) Truncate(path string, size int64, fh uint64) (errc int) {
	errc = errno(syscall.Ftruncate(syscall.Handle(fh), size))
	return
}

func (f *RawFS) Read(path string, buff []byte, offset int64, fh uint64) (rc int) {
	_ = path
	n := uint32(0)
	err := windows.ReadFile(windows.Handle(fh), buff, &n, &windows.Overlapped{
		Offset:     uint32(offset),
		OffsetHigh: uint32(offset >> 32),
	})
	if err != nil {
		rc = errno(err)
	} else {
		rc = int(n)
	}
	return
}

func (f *RawFS) Write(path string, buff []byte, offset int64, fh uint64) (rc int) {
	_ = path
	n := uint32(0)
	err := windows.WriteFile(windows.Handle(fh), buff, &n, &windows.Overlapped{
		Offset:     uint32(offset),
		OffsetHigh: uint32(offset >> 32),
	})
	if err != nil {
		rc = errno(err)
	} else {
		rc = int(n)
	}
	return
}

func (f *RawFS) Release(path string, fh uint64) (errc int) {
	_ = path
	errc = errno(syscall.Close(syscall.Handle(fh)))
	return
}

func (f *RawFS) Fsync(path string, datasync bool, fh uint64) (errc int) {
	_ = path

	if datasync {
		// https://learn.microsoft.com/zh-cn/windows/win32/api/memoryapi/nf-memoryapi-flushviewoffile
		err := windows.FlushViewOfFile(uintptr(fh), 0)
		if err != nil {
			errc = errno(err)
			return
		}
		err = windows.FlushFileBuffers(windows.Handle(fh))
		errc = errno(err)
		return
	}

	errc = errno(syscall.Fsync(syscall.Handle(fh)))
	return
}

func (f *RawFS) Opendir(path string) (errc int, fh uint64) {
	path = filepath.Join(f.BaseDir, path)
	fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
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
	errc = errno(syscall.Close(syscall.Handle(fh)))
	return
}
