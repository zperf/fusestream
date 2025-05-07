//go:build darwin

package slowfs

import (
	"syscall"

	"github.com/winfsp/cgofuse/fuse"
)

func setuidgid() func() {
	euid := syscall.Geteuid()
	if 0 == euid {
		uid, gid, _ := fuse.Getcontext()
		egid := syscall.Getegid()
		syscall.Setegid(int(gid))
		syscall.Seteuid(int(uid))
		return func() {
			syscall.Seteuid(euid)
			syscall.Setegid(egid)
		}
	}
	return func() {}
}

func copyFuseStatfsFromGoStatfs(dst *fuse.Statfs_t, src *syscall.Statfs_t) {
	*dst = fuse.Statfs_t{}
	dst.Bsize = uint64(src.Bsize)
	dst.Frsize = 1
	dst.Blocks = src.Blocks
	dst.Bfree = src.Bfree
	dst.Bavail = src.Bavail
	dst.Files = src.Files
	dst.Ffree = src.Ffree
	dst.Favail = src.Ffree
	dst.Namemax = 255 //uint64(src.Namelen)
}

func copyFusestatFromGostat(dst *fuse.Stat_t, src *syscall.Stat_t) {
	*dst = fuse.Stat_t{}
	dst.Dev = uint64(src.Dev)
	dst.Ino = src.Ino
	dst.Mode = uint32(src.Mode)
	dst.Nlink = uint32(src.Nlink)
	dst.Uid = src.Uid
	dst.Gid = src.Gid
	dst.Rdev = uint64(src.Rdev)
	dst.Size = src.Size
	dst.Atim.Sec, dst.Atim.Nsec = src.Atimespec.Sec, src.Atimespec.Nsec
	dst.Mtim.Sec, dst.Mtim.Nsec = src.Mtimespec.Sec, src.Mtimespec.Nsec
	dst.Ctim.Sec, dst.Ctim.Nsec = src.Ctimespec.Sec, src.Ctimespec.Nsec
	dst.Blksize = int64(src.Blksize)
	dst.Blocks = src.Blocks
	dst.Birthtim.Sec, dst.Birthtim.Nsec = src.Birthtimespec.Sec, src.Birthtimespec.Nsec
}

func syscallStatfs(path string, stat *syscall.Statfs_t) error {
	return syscall.Statfs(path, stat)
}

func fsync(datasync bool, fh int) error {
	return syscall.Fsync(fh)
}
