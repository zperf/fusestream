package slowio

import (
	"github.com/winfsp/cgofuse/fuse"

	"github.com/fanyang89/slowfs/pb"
)

type Record struct {
	Path      string `json:"path,omitzero"`
	ErrorCode int    `json:"error_code"`
	Op        string `json:"op"`
	Fh        uint64 `json:"fh,omitzero"`
}

func NewRecord(path string, ec int, op pb.FuseOp) *Record {
	return &Record{
		Path:      path,
		ErrorCode: ec,
		Op:        op.String(),
	}
}

type StatfsRecord struct {
	Record
	Bsize   uint64 `json:"bsize,omitzero"`
	Frsize  uint64 `json:"frsize,omitzero"`
	Blocks  uint64 `json:"blocks,omitzero"`
	Bfree   uint64 `json:"bfree,omitzero"`
	Bavail  uint64 `json:"bavail,omitzero"`
	Files   uint64 `json:"files,omitzero"`
	Ffree   uint64 `json:"ffree,omitzero"`
	Favail  uint64 `json:"favail,omitzero"`
	Fsid    uint64 `json:"fsid,omitzero"`
	Flag    uint64 `json:"flag,omitzero"`
	Namemax uint64 `json:"namemax,omitzero"`
}

func (r *StatfsRecord) From(s *fuse.Statfs_t) *StatfsRecord {
	r.Bsize = s.Bsize
	r.Frsize = s.Frsize
	r.Blocks = s.Blocks
	r.Bfree = s.Bfree
	r.Bavail = s.Bavail
	r.Files = s.Files
	r.Ffree = s.Ffree
	r.Favail = s.Favail
	r.Fsid = s.Fsid
	r.Flag = s.Flag
	r.Namemax = s.Namemax
	return r
}

type MknodRecord struct {
	Record
	Mode uint32 `json:"mode,omitzero"`
	Dev  uint64 `json:"dev,omitzero"`
}

type MkdirRecord struct {
	Record
	Mode uint32 `json:"mode,omitzero"`
}

type LinkRecord struct {
	Record
	NewPath string `json:"new_path,omitzero"`
}

type SymlinkRecord struct {
	Record
	NewPath string `json:"new_path,omitzero"`
}

type ReadlinkRecord struct {
	Record
	Target string `json:"target,omitzero"`
}

type RenameRecord struct {
	Record
	NewPath string `json:"new_path,omitzero"`
}

type ChmodRecord struct {
	Record
	Mode uint32 `json:"mode,omitzero"`
}

type ChownRecord struct {
	Record
	UID uint32 `json:"uid,omitzero"`
	GID uint32 `json:"gid,omitzero"`
}

type TimeSpec struct {
	Sec  int64 `json:"sec"`
	Nsec int64 `json:"nsec"`
}

func NewTimeSpecFromFuse(ts fuse.Timespec) TimeSpec {
	return TimeSpec{
		Sec:  ts.Sec,
		Nsec: ts.Nsec,
	}
}

type UtimensRecord struct {
	Record
	Tmpsp1 []TimeSpec `json:"tmpsp1"`
}

func (r *UtimensRecord) From(tmpsp1 []fuse.Timespec) *UtimensRecord {
	r.Tmpsp1 = make([]TimeSpec, 0, len(tmpsp1))
	for _, t := range tmpsp1 {
		r.Tmpsp1 = append(r.Tmpsp1, TimeSpec{t.Sec, t.Nsec})
	}
	return r
}

type OpenCreateRecord struct {
	Record
	Flags int    `json:"flags,omitzero"`
	Mode  uint32 `json:"mode,omitzero"`
}

type Stat struct {
	Dev      uint64   `json:"dev,omitzero"`
	Ino      uint64   `json:"ino,omitzero"`
	Mode     uint32   `json:"mode,omitzero"`
	Nlink    uint32   `json:"nlink,omitzero"`
	Uid      uint32   `json:"uid,omitzero"`
	Gid      uint32   `json:"gid,omitzero"`
	Rdev     uint64   `json:"rdev,omitzero"`
	Size     int64    `json:"size,omitzero"`
	Atim     TimeSpec `json:"atim,omitzero"`
	Mtim     TimeSpec `json:"mtim,omitzero"`
	Ctim     TimeSpec `json:"ctim,omitzero"`
	Blksize  int64    `json:"blksize,omitzero"`
	Blocks   int64    `json:"blocks,omitzero"`
	Birthtim TimeSpec `json:"birthtim,omitzero"`
	Flags    uint32   `json:"flags,omitzero"`
}

func (st *Stat) From(s *fuse.Stat_t) *Stat {
	st.Dev = s.Dev
	st.Ino = s.Ino
	st.Mode = s.Mode
	st.Nlink = s.Nlink
	st.Uid = s.Uid
	st.Gid = s.Gid
	st.Rdev = s.Rdev
	st.Size = s.Size
	st.Atim = NewTimeSpecFromFuse(s.Atim)
	st.Mtim = NewTimeSpecFromFuse(s.Mtim)
	st.Ctim = NewTimeSpecFromFuse(s.Ctim)
	st.Blksize = s.Blksize
	st.Blocks = s.Blocks
	st.Birthtim = NewTimeSpecFromFuse(s.Birthtim)
	st.Flags = s.Flags
	return st
}

type GetattrRecord struct {
	Record
	Stat
}

type TruncateRecord struct {
	Record
	Size int64 `json:"size"`
}

type ReadWriteRecord struct {
	Record
	Offset int64 `json:"offset"`
}

type FsyncRecord struct {
	Record
	IsDataSync bool `json:"is_data_sync"`
}

type ReaddirRecord struct {
	Record
	Offset int64 `json:"offset"`
}
