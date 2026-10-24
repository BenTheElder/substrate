package tar2ext4

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/forked/hcsshim/ext4/internal/format"
)

// sampleTar builds a small, deterministic tar (fixed mtimes) with a dir, two files, and
// a symlink.
func sampleTar(t *testing.T) []byte {
	t.Helper()
	mtime := time.Unix(1000000000, 0).UTC()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	write := func(hdr *tar.Header, body string) {
		hdr.ModTime = mtime
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if body != "" {
			if _, err := tw.Write([]byte(body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(&tar.Header{Typeflag: tar.TypeDir, Name: "dir/", Mode: 0o755}, "")
	write(&tar.Header{Typeflag: tar.TypeReg, Name: "dir/hello", Mode: 0o644, Size: 5, Uid: 0, Gid: 0}, "hello")
	write(&tar.Header{Typeflag: tar.TypeReg, Name: "file", Mode: 0o600, Size: 3, Uid: 1000, Gid: 1000}, "abc")
	write(&tar.Header{Typeflag: tar.TypeSymlink, Name: "link", Linkname: "file", Mode: 0o777}, "")
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func convertFixed(t *testing.T, tarBytes []byte, path string, sizeBytes, inodes int64) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := Convert(bytes.NewReader(tarBytes), f, FixedSize(sizeBytes, inodes)); err != nil {
		t.Fatalf("Convert(FixedSize): %v", err)
	}
}

func TestFixedSizeSuperblock(t *testing.T) {
	const (
		sizeBytes = int64(64) << 20 // 64 MiB
		inodes    = int64(4096)
	)
	dir := t.TempDir()
	img := filepath.Join(dir, "fs.ext4")
	convertFixed(t, sampleTar(t), img, sizeBytes, inodes)

	f, err := os.Open(img)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sb, err := ReadExt4SuperBlockReadSeeker(f)
	if err != nil {
		t.Fatalf("ReadExt4SuperBlock: %v", err)
	}

	blockSize := int64(1024) * (1 << sb.LogBlockSize)
	wantBlocks := sizeBytes / blockSize

	// The filesystem spans exactly the requested size.
	if int64(sb.BlocksCountLow) != wantBlocks {
		t.Errorf("BlocksCountLow = %d (%d bytes), want %d (%d bytes)",
			sb.BlocksCountLow, int64(sb.BlocksCountLow)*blockSize, wantBlocks, sizeBytes)
	}
	// most of the disk is free
	if int64(sb.FreeBlocksCountLow) < wantBlocks*9/10 {
		t.Errorf("FreeBlocksCountLow = %d, want >= %d (~90%% of %d)",
			sb.FreeBlocksCountLow, wantBlocks*9/10, wantBlocks)
	}
	// Enough inodes were provisioned, with free ones for new files.
	if int64(sb.InodesCount) < inodes {
		t.Errorf("InodesCount = %d, want >= %d", sb.InodesCount, inodes)
	}
	if sb.FreeInodesCount == 0 {
		t.Error("FreeInodesCount = 0, want free inodes")
	}
	if sb.FeatureRoCompat&format.RoCompatReadonly != 0 {
		t.Error("FeatureRoCompat has RoCompatReadonly set; fixed-size image must be writable")
	}
}

func TestFixedSizeSparse(t *testing.T) {
	const sizeBytes = int64(128) << 20 // 128 MiB logical
	dir := t.TempDir()
	img := filepath.Join(dir, "fs.ext4")
	convertFixed(t, sampleTar(t), img, sizeBytes, 4096)

	fi, err := os.Stat(img)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != sizeBytes {
		t.Errorf("logical size = %d, want %d", fi.Size(), sizeBytes)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("no syscall.Stat_t on this platform")
	}
	allocated := int64(st.Blocks) * 512 // st_blocks is always in 512-byte units
	// A near-empty 128 MiB ext4 only materializes metadata; the free data region is a
	// hole. Allocated bytes should be a tiny fraction of the logical size.
	if allocated >= sizeBytes/4 {
		t.Errorf("allocated %d bytes for a %d-byte image; expected sparse (<%d)",
			allocated, sizeBytes, sizeBytes/4)
	}
	t.Logf("sparse OK: logical=%d allocated=%d (%.2f%%)", sizeBytes, allocated, 100*float64(allocated)/float64(sizeBytes))
}

func TestFixedSizeDeterministic(t *testing.T) {
	const sizeBytes = int64(64) << 20
	tb := sampleTar(t)
	dir := t.TempDir()
	a := filepath.Join(dir, "a.ext4")
	b := filepath.Join(dir, "b.ext4")
	convertFixed(t, tb, a, sizeBytes, 4096)
	convertFixed(t, tb, b, sizeBytes, 4096)

	ab, err := os.ReadFile(a)
	if err != nil {
		t.Fatal(err)
	}
	bb, err := os.ReadFile(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ab, bb) {
		t.Errorf("non-deterministic output: identical input produced different images (%d vs %d bytes)", len(ab), len(bb))
	}
}

// e2fsck reports "Padding at end of inode bitmap is not set" otherwise
func TestFixedSizeInodeBitmapPadding(t *testing.T) {
	const sizeBytes = int64(64) << 20
	dir := t.TempDir()
	img := filepath.Join(dir, "fs.ext4")
	convertFixed(t, sampleTar(t), img, sizeBytes, 4096)

	f, err := os.Open(img)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sb, err := ReadExt4SuperBlockReadSeeker(f)
	if err != nil {
		t.Fatal(err)
	}
	blockSize := int64(1024) * (1 << sb.LogBlockSize)
	ipg := int64(sb.InodesPerGroup)

	// First group descriptor lives at block 1; read it for the inode-bitmap block.
	if _, err := f.Seek(blockSize, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	var gd format.GroupDescriptor
	if err := binary.Read(f, binary.LittleEndian, &gd); err != nil {
		t.Fatal(err)
	}

	bitmap := make([]byte, blockSize)
	if _, err := f.ReadAt(bitmap, int64(gd.InodeBitmapLow)*blockSize); err != nil {
		t.Fatal(err)
	}
	for j := ipg; j < blockSize*8; j++ {
		if bitmap[j/8]&(1<<(uint(j)%8)) == 0 {
			t.Fatalf("inode-bitmap padding bit %d (>= inodesPerGroup %d) is not set", j, ipg)
		}
	}
}

func TestFixedSizeTooSmall(t *testing.T) {
	// 64 KiB can't hold even the metadata; Close should fail rather than silently
	// produce a broken image.
	dir := t.TempDir()
	img := filepath.Join(dir, "fs.ext4")
	f, err := os.Create(img)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := Convert(bytes.NewReader(sampleTar(t)), f, FixedSize(64<<10, 4096)); err == nil {
		t.Error("Convert(FixedSize too small): want error, got nil")
	}
}
