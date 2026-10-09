# AFS POSIX smoke probes

`posix_smoke.py` is a short, real filesystem probe used during P2c/POSIX bring-up. It creates one exclusive random fixture directory under the supplied mount and records every step in JSON evidence.

It covers a targeted subset only:

- directory create/list/remove
- regular-file `mknod` with mode/uid/empty-file checks followed by read/write/fsync
- file mode and created uid
- hardlink nlink
- symlink/readlink
- rename replacement
- open-unlink fd lifetime
- `user.*` xattr binary and empty values plus CREATE/REPLACE/ENODATA behavior, libc length query, exact-size read and ERANGE
- sparse high-offset write, zero hole read and `st_blocks`
- shrink/grow truncate semantics
- file `fsync` and parent directory `fsync`

It does not replace pjdfstest, LTP, FSx, or differential random tests. Unsupported behavior is reported as failure. `getxattr` length query, exact-size read, empty value and ERANGE are checked through `ctypes.CDLL(None, use_errno=True).getxattr` so raw Linux errno is preserved.

Usage:

```bash
python3 build/e2e/afs/acceptance/probes/posix_smoke.py \
  --mount /path/to/mount \
  --evidence /path/to/evidence/posix-smoke-run
```

On success the fixture directory is removed. On failure it is kept and its path is written into `report.json` and `summary.txt`.

## Multi-UID smoke

`multi_uid_smoke.py` is a separate Linux-root-only probe for permission and caller identity behavior. It creates one mode-0777 random fixture under the supplied mount and runs child processes through `setpriv` using numeric uid/gid values that do not need passwd entries.

Covered checks:

- uid/gid 60001 creates, reads, writes, fsyncs and closes a file; resulting file owner is the child uid/gid
- owner uid 60001 can `chgrp` a file to supplementary group 60002
- uid 60003 with supplementary group 60002 can read/write and set/get/remove `user.*` xattr on a root-owned mode-660 file with gid 60002
- uid 60004 without the group is denied data and xattr access
- non-owner chmod/chown returns Linux permission errno

These cases are intentionally not replaced by root-only operations. They exercise both kernel `default_permissions` style enforcement and backend `CallerContext` propagation.

Usage:

```bash
sudo python3 build/e2e/afs/acceptance/probes/multi_uid_smoke.py \
  --mount /path/to/mount \
  --evidence /path/to/evidence/multi-uid-run
```

## Lock smoke

`locks_smoke.py` is a short Linux advisory-lock probe for FUN-10 / REL-11 lock bring-up. It runs real `fcntl` byte-range locks and BSD `flock` locks with distinct child processes and bounded timeouts. Every run writes `report.json` and `summary.txt` evidence.

Covered checks:

- `fcntl` same-file conflicting byte ranges fail while non-overlapping ranges succeed
- `F_GETLK` reports a conflicting lock
- shared `fcntl` locks coexist and exclusive locks conflict with them
- blocking `F_SETLKW` waits and wakes after unlock
- owner process exit releases `fcntl` locks
- POSIX close semantics: closing another fd for the same file releases process-owned record locks
- BSD `flock` exclusive/shared conflict behavior
- BSD `flock` blocking waiter wakeup
- BSD `flock` survives closing the original fd while a duplicate fd remains open, then releases after the final duplicate closes
- optional live process identity evidence via `/proc/<pid>/exe` SHA256 and start ticks without reading cmdline/environ

Usage on a normal filesystem reference:

```bash
python3 build/e2e/afs/acceptance/probes/locks_smoke.py \
  --mount /tmp/afs-lock-reference \
  --evidence /tmp/afs-lock-evidence/ext4
```

Usage on an existing AFS mount with an observed node process identity:

```bash
sudo python3 build/e2e/afs/acceptance/probes/locks_smoke.py \
  --mount /path/to/afs-mount \
  --evidence /tmp/afs-lock-evidence/owner-local \
  --process afs-node=5379
```

Cross-mount fixture mode is ready but should only be used when the caller has two mounted paths that really name the same logical file:

```bash
sudo python3 build/e2e/afs/acceptance/probes/locks_smoke.py \
  --mount /mnt/afs-a \
  --second-mount /mnt/afs-b \
  --evidence /tmp/afs-lock-evidence/cross-mount
```

The report marks `cross_path_probe=true` when primary and secondary paths differ. That result proves only the observed behavior between those two paths; it does not by itself prove cross-node coverage.

On success the fixture is removed. On failure it is kept unless setup failed before a target file was created. `ENOSYS` or `EOPNOTSUPP` failures are treated as clear evidence that the current filesystem/mount does not support the requested lock callback path.

## Cross-Mount Consistency

`consistency_cross.py` runs ten short scenarios through two Linux workers: OwnerFs and DFS same-mount visibility, close-to-open, remote-owner writes, OwnerFs local and remote-Home overwrite rename with a surviving hardlink, DFS reopen and dirty writes from the former owner after owner handover, and an existing remote writable handle surviving handleless resize. The rename checks compare the surviving alias's inode, link count and original bytes, then the replacement's source identity and bytes. The final resize scenario reads accepted writes before close, then checks a fresh open after close. Assertions are one-shot and do not poll until stale data happens to disappear. Rename fixtures require a fresh run-scoped directory; an existing directory fails instead of deleting earlier results.

The `host` command takes JSON worker command prefixes, the OwnerFs workspace and DFS roots visible to each worker, and exact expected Node/Meta PID and executable SHA pairs. Use `--require-cross-mount` for product runs. The report must identify different Linux boot IDs, all four actual AFS FUSE mounts and the same expected processes throughout the run. `--allow-host-non-linux` permits VM orchestration only; worker file operations remain on Linux. See `host --help` for the complete arguments.

Run `python3 consistency_cross.py selftest` on Linux to verify positive reference behavior, bounded silent-worker timeout, wrong-binary rejection and rejection of unqualified mounts. Reference selftests do not qualify AFS behavior. Evidence includes exact commands, phase-specific errors, content, sizes, identities and discovered step accounting.
