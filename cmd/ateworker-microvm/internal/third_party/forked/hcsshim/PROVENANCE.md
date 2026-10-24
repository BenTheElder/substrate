# third_party/forked/hcsshim: forked hcsshim ext4 writer

A fork of the ext4 image writer from
[microsoft/hcsshim](https://github.com/microsoft/hcsshim), for `cmd/ateworker-microvm` to
build writable ext4 disk images in pure Go, without `mkfs.ext4` (e2fsprogs) in the worker
image. The upstream `LICENSE` in this directory covers everything under it.

Unlike `third_party/kata`, this lives under `forked/` because we modify the upstream
source, see "Local Modifications" below. The import and our changes are separate commits,
so the fork diff against upstream stays readable.

- **Upstream:** github.com/microsoft/hcsshim
- **Version:** tag `v0.14.1`, commit `fb5aa2e9478c8f5dcaba00601cc7c7d10e1320cd`
- **License:** MIT. `./LICENSE` is the upstream license verbatim (mirrored to
  `_LICENSES/third_party/forked/hcsshim/LICENSE` by `hack/update/licenses.sh`). Upstream
  has no per-file copyright headers, the directory `LICENSE` covers the MIT notice.

## What Was Taken

Only the self-contained ext4 subtree (upstream paths):

- `ext4/internal/format/format.go`: on-disk superblock / inode / group descriptor structs
- `ext4/internal/compactext4/compact.go`: the ext4 writer (`Writer`, `File`, options)
- `ext4/tar2ext4/tar2ext4.go`: tar stream to ext4 (`Convert`)
- upstream tests `compact_test.go`, `verify_test.go`, `verify_linux_test.go`

The layout under `ext4/` mirrors upstream so future refreshes diff cleanly.

### Why A Fork Instead Of A Module Dependency

hcsshim is a large, Windows oriented module, and `compactext4` is an upstream `internal/`
package we can't import anyhow. We need to modify it too, so we copy just the ext4
subtree, like kubernetes does for forked source under `third_party/`.

## Local Modifications

### On Import

Mechanical only:

1. Import paths repointed from `github.com/Microsoft/hcsshim/ext4/...` to this in-tree
   path.
2. Dropped upstream `ext4/tar2ext4/vhdfooter.go`, `ext4/dmverity/`, and the VHD footer /
   dm-verity options and helpers in `tar2ext4.go` (`AppendVhdFooter`,
   `OnlyAppendVhdFooter`, `AppendDMVerity`, `ConvertToVhd`, `ConvertFileToVhd`,
   `ConvertAndComputeRootDigest`). `Convert` now just calls `ConvertTarToExt4`.
3. Replaced `github.com/Microsoft/hcsshim/internal/memory` with inline size constants
   (`1 << 30`, `1 << 20`) in `compactext4`.
4. Replaced `github.com/pkg/errors` with the stdlib `errors`/`fmt` in `tar2ext4.go`, and
   dropped `IsDeviceExt4` (it only existed to use `github.com/Microsoft/hcsshim/internal/log`).

### Inode Bitmap Padding

Upstream leaves the inode bitmap bits past `inodesPerGroup` unset, and `e2fsck` reports
"Padding at end of inode bitmap is not set" on every image, read-only or not (upstream's
own tests fail on this on Linux with e2fsprogs 1.47.2). We mark them used, like e2fsprogs.

## Refreshing From Upstream

1. Check out the new hcsshim tag and copy `ext4/internal/format`,
   `ext4/internal/compactext4`, and `ext4/tar2ext4/tar2ext4.go` over this subtree.
2. Re-apply the mechanical changes above (import repoint, drop VHD / dm-verity, inline the
   `memory` / `log` / `pkg/errors` deps).
3. Re-apply the functional patches above on top, each as its own commit.
4. Update the version / commit pin above and run `hack/update/licenses.sh`.
