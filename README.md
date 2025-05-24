# SlowIO

A simple tool for file system and block device fault injection tests,
inspired by [chaos-mesh/toda](https://github.com/chaos-mesh/toda).

## Usage

### Environment

```bash
# Start the pprof HTTP server (optional)
export SLOWIO_DEBUG=127.0.0.1:6000

# Export OpenTelemetry Spans to DuckDB
export SLOWIO_EXPORT_PATH=/tmp/slowio.ddb
```

### FUSE

```bash
# mount the file system
slowio mount -b --base-dir /tmp/slowio --mountpoint /mnt/slowio

# inject fault
slowio fault inject-latency -g 'test-file.*' -p 1 --op CREATE -l 1000ms

# list injected faults
slowio fault list

# time touch /mnt/slowio/test-file14
0.00s user 0.00s system 0% cpu 1.002 total
```

### NBD

TBD

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

### NBD

- READAT
- WRITEAT
- SIZE
- SYNC

## Licence

MIT
