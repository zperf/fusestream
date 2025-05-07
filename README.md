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

```
"UNKNOWN":    0,
"STATFS":     1,
"MKNOD":      2,
"MKDIR":      3,
"UNLINK":     4,
"RMDIR":      5,
"LINK":       6,
"SYMLINK":    7,
"READLINK":   8,
"RENAME":     9,
"CHMOD":      10,
"CHOWN":      11,
"UTIMENS":    12,
"CREATE":     13,
"OPEN":       14,
"GETATTR":    15,
"TRUNCATE":   16,
"READ":       17,
"WRITE":      18,
"RELEASE":    19,
"FSYNC":      20,
"OPENDIR":    21,
"READDIR":    22,
"RELEASEDIR": 23,
```

## Licence

MIT
