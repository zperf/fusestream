//go:build linux

package slowio

import (
	"syscall"

	"github.com/winfsp/cgofuse/fuse"
	"golang.org/x/sys/unix"
)

type fillFn = func(name string, stat *fuse.Stat_t, ofst int64) bool

func errno(err error) int {
	if err != nil {
		return -int(err.(syscall.Errno))
	} else {
		return 0
	}
}

func fadviseRandom(fd int) error {
	return unix.Fadvise(fd, 0, 0, unix.FADV_RANDOM)
}

func fadviseSequential(fd int) error {
	return unix.Fadvise(fd, 0, 0, unix.FADV_SEQUENTIAL)
}

func setUIDAndGID() func() {
	euid := syscall.Geteuid()
	if euid == 0 {
		uid, gid, _ := fuse.Getcontext()
		egid := syscall.Getegid()
		_ = syscall.Setregid(-1, int(gid))
		_ = syscall.Setreuid(-1, int(uid))
		return func() {
			_ = syscall.Setreuid(-1, euid)
			_ = syscall.Setregid(-1, egid)
		}
	}
	return func() {}
}

func newFuseStatfsFromGo(src *syscall.Statfs_t) *fuse.Statfs_t {
	dst := &fuse.Statfs_t{}
	dst.Bsize = uint64(src.Bsize)
	dst.Frsize = 1
	dst.Blocks = src.Blocks
	dst.Bfree = src.Bfree
	dst.Bavail = src.Bavail
	dst.Files = src.Files
	dst.Ffree = src.Ffree
	dst.Favail = src.Ffree
	dst.Namemax = uint64(src.Namelen)
	return dst
}

func newFuseStatFromGo(src *syscall.Stat_t) *fuse.Stat_t {
	dst := &fuse.Stat_t{}
	dst.Dev = src.Dev
	dst.Ino = src.Ino
	dst.Mode = src.Mode
	dst.Nlink = uint32(src.Nlink)
	dst.Uid = src.Uid
	dst.Gid = src.Gid
	dst.Rdev = src.Rdev
	dst.Size = src.Size
	dst.Atim.Sec, dst.Atim.Nsec = src.Atim.Sec, src.Atim.Nsec
	dst.Mtim.Sec, dst.Mtim.Nsec = src.Mtim.Sec, src.Mtim.Nsec
	dst.Ctim.Sec, dst.Ctim.Nsec = src.Ctim.Sec, src.Ctim.Nsec
	//goland:noinspection GoRedundantConversion
	dst.Blksize = int64(src.Blksize)
	dst.Blocks = src.Blocks
	return dst
}
