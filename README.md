# FuseStream

A simple tool for file system fault injection tests, inspired by [chaos-mesh/toda](https://github.com/chaos-mesh/toda).

## Compile

### Windows

```bash
$env:CPATH="C:\Program Files (x86)\WinFsp\inc\fuse"
go build main.go
```

### Linux

```bash
go build main.go
```

## Usage

### Environment

```bash
# Start the pprof HTTP server (optional)
export FUSESTREAM_DEBUG="127.0.0.1:6000"

# Export OpenTelemetry spans to parquet
export FUSESTREAM_EXPORT_PATH="/tmp/slowio.parquet"
```

### FUSE

```bash
# mount the file system
slowio fuse mount -b --base-dir /tmp/slowio --mountpoint /mnt/slowio

# inject fault
slowio fuse inject-latency -g 'test-file.*' -p 1 --op CREATE -l 1000ms

# list injected faults
slowio fault list

# time touch /mnt/slowio/test-file14
0.00s user 0.00s system 0% cpu 1.002 total
```

## OpCodes

### FUSE

- UNKNOWN
- STATFS
- MKNOD
- MKDIR
- UNLINK
- RMDIR
- LINK
- SYMLINK
- READLINK
- RENAME
- CHMOD
- CHOWN
- UTIMENS
- CREATE
- OPEN
- GETATTR
- TRUNCATE
- READ
- WRITE
- RELEASE
- FSYNC
- OPENDIR
- READDIR
- RELEASEDIR

## Licence

MIT
