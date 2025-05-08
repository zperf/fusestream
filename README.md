# SlowFS

A simple FUSE tool for file system fault injection tests,
inspired by [chaos-mesh/toda](https://github.com/chaos-mesh/toda).

## Usage

```bash
# mount the file system
slowfs mount --base-dir /tmp/slowfs --mountpoint /mnt/slowfs --listen 127.0.0.1:1234

# inject faults
slowfs fault inject-latency --address 127.0.0.1:1234 --op READ --path-regex 'test-file.*' --possibility 0.5 --latency 100ms 
slowfs fault inject-error --address 127.0.0.1:1234 --op READ --path-regex 'test-file.*' --possibility 0.5 --error-code -1 

# list injected faults
slowfs fault list
```

## OpCodes

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
