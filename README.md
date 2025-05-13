# SlowFS

A simple FUSE tool for file system fault injection tests,
inspired by [chaos-mesh/toda](https://github.com/chaos-mesh/toda).

## Usage

```bash
# mount the file system
slowio mount --base-dir /tmp/slowio --mountpoint /mnt/slowio --listen 127.0.0.1:1234

# inject faults
slowio fault inject-latency --address 127.0.0.1:1234 --op READ --path-regex 'test-file.*' --possibility 0.5 --latency 100ms 
slowio fault inject-error --address 127.0.0.1:1234 --op READ --path-regex 'test-file.*' --possibility 0.5 --error-code -1 

# list injected faults
slowio fault list
```

## Example

Mount:

```bash
slowio mount -v -b /tmp/slowio -m /mnt/slowio
```

Inject latency for creating file:

```bash
slowio fault inject-latency -g 'test-file.*' -p 1 --op CREATE -l 1000ms
```

Create file:

```bash
# time touch /mnt/slowio/test-file14
touch /mnt/slowio/test-file14  0.00s user 0.00s system 0% cpu 1.002 total
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
