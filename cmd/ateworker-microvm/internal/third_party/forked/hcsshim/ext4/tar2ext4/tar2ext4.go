package tar2ext4

import (
	"archive/tar"
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/forked/hcsshim/ext4/internal/compactext4"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/forked/hcsshim/ext4/internal/format"
)

type params struct {
	convertWhiteout  bool
	convertBackslash bool
	ext4opts         []compactext4.Option
}

// Option is the type for optional parameters to Convert.
type Option func(*params)

// ConvertWhiteout instructs the converter to convert OCI-style whiteouts
// (beginning with .wh.) to overlay-style whiteouts.
func ConvertWhiteout(p *params) {
	p.convertWhiteout = true
}

// ConvertBackslash instructs the converter to replace `\` in path names with `/`.
// This is useful if the tar file was created on Windows, where `\` is the filepath separator.
func ConvertBackslash(p *params) {
	p.convertBackslash = true
}

// InlineData instructs the converter to write small files into the inode
// structures directly. This creates smaller images but currently is not
// compatible with DAX.
func InlineData(p *params) {
	p.ext4opts = append(p.ext4opts, compactext4.InlineData)
}

// MaximumDiskSize instructs the writer to limit the disk size to the specified
// value. This also reserves enough metadata space for the specified disk size.
// If not provided, then 16GB is the default.
func MaximumDiskSize(size int64) Option {
	return func(p *params) {
		p.ext4opts = append(p.ext4opts, compactext4.MaximumDiskSize(size))
	}
}

// FixedSize wraps compactext4.FixedSize, added in this fork
func FixedSize(sizeBytes int64, inodeCount int64) Option {
	return func(p *params) {
		p.ext4opts = append(p.ext4opts, compactext4.FixedSize(sizeBytes, inodeCount))
	}
}

const (
	whiteoutPrefix = ".wh."
	opaqueWhiteout = ".wh..wh..opq"
)

// ConvertTarToExt4 writes a compact ext4 file system image that contains the files in the
// input tar stream.
func ConvertTarToExt4(r io.Reader, w io.ReadWriteSeeker, options ...Option) error {
	var p params
	for _, opt := range options {
		opt(&p)
	}

	t := tar.NewReader(bufio.NewReader(r))
	fs := compactext4.NewWriter(w, p.ext4opts...)
	for {
		hdr, err := t.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		name := hdr.Name
		linkName := hdr.Linkname
		if p.convertBackslash {
			// compactext assumes all paths are `/` separated
			// unconditionally replace all instances of `/`, regardless of GOOS
			name = strings.ReplaceAll(name, `\`, "/")
			linkName = strings.ReplaceAll(linkName, `\`, "/")
		}

		if err = fs.MakeParents(name); err != nil {
			return fmt.Errorf("failed to ensure parent directories for %s: %w", name, err)
		}

		if p.convertWhiteout {
			dir, file := path.Split(name)
			if strings.HasPrefix(file, whiteoutPrefix) {
				if file == opaqueWhiteout {
					// Update the directory with the appropriate xattr.
					f, err := fs.Stat(dir)
					if err != nil {
						return fmt.Errorf("failed to stat parent directory of whiteout %s: %w", file, err)
					}
					f.Xattrs["trusted.overlay.opaque"] = []byte("y")
					err = fs.Create(dir, f)
					if err != nil {
						return fmt.Errorf("failed to create opaque dir %s: %w", file, err)
					}
				} else {
					// Create an overlay-style whiteout.
					f := &compactext4.File{
						Mode:     compactext4.S_IFCHR,
						Devmajor: 0,
						Devminor: 0,
					}
					err = fs.Create(path.Join(dir, file[len(whiteoutPrefix):]), f)
					if err != nil {
						return fmt.Errorf("failed to create whiteout file for %s: %w", file, err)
					}
				}

				continue
			}
		}

		if hdr.Typeflag == tar.TypeLink {
			err = fs.Link(linkName, name)
			if err != nil {
				return err
			}
		} else {
			f := &compactext4.File{
				Mode:     uint16(hdr.Mode),
				Atime:    hdr.AccessTime,
				Mtime:    hdr.ModTime,
				Ctime:    hdr.ChangeTime,
				Crtime:   hdr.ModTime,
				Size:     hdr.Size,
				Uid:      uint32(hdr.Uid),
				Gid:      uint32(hdr.Gid),
				Linkname: linkName,
				Devmajor: uint32(hdr.Devmajor),
				Devminor: uint32(hdr.Devminor),
				Xattrs:   make(map[string][]byte),
			}
			for key, value := range hdr.PAXRecords {
				const xattrPrefix = "SCHILY.xattr."
				if strings.HasPrefix(key, xattrPrefix) {
					f.Xattrs[key[len(xattrPrefix):]] = []byte(value)
				}
			}

			var typ uint16
			switch hdr.Typeflag {
			case tar.TypeReg:
				typ = compactext4.S_IFREG
			case tar.TypeSymlink:
				typ = compactext4.S_IFLNK
			case tar.TypeChar:
				typ = compactext4.S_IFCHR
			case tar.TypeBlock:
				typ = compactext4.S_IFBLK
			case tar.TypeDir:
				typ = compactext4.S_IFDIR
			case tar.TypeFifo:
				typ = compactext4.S_IFIFO
			}
			f.Mode &= ^compactext4.TypeMask
			f.Mode |= typ
			err = fs.Create(name, f)
			if err != nil {
				return err
			}
			_, err = io.Copy(fs, t)
			if err != nil {
				return err
			}
		}
	}
	return fs.Close()
}

// Convert wraps ConvertTarToExt4
// the upstream VHD footer and dm-verity options are dropped in this fork
func Convert(r io.Reader, w io.ReadWriteSeeker, options ...Option) error {
	return ConvertTarToExt4(r, w, options...)
}

// ReadExt4SuperBlock reads and returns ext4 super block from given device.
func ReadExt4SuperBlock(devicePath string) (*format.SuperBlock, error) {
	dev, err := os.OpenFile(devicePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer dev.Close()

	return ReadExt4SuperBlockReadSeeker(dev)
}

// ReadExt4SuperBlockReadSeeker reads and returns ext4 super block given
// an io.ReadSeeker.
//
// The layout on disk is as follows:
// | Group 0 padding     | - 1024 bytes
// | ext4 SuperBlock     | - 1 block
// | Group Descriptors   | - many blocks
// | Reserved GDT Blocks | - many blocks
// | Data Block Bitmap   | - 1 block
// | inode Bitmap        | - 1 block
// | inode Table         | - many blocks
// | Data Blocks         | - many blocks
//
// More details can be found here https://ext4.wiki.kernel.org/index.php/Ext4_Disk_Layout
//
// Our goal is to skip the Group 0 padding, read and return the ext4 SuperBlock
func ReadExt4SuperBlockReadSeeker(rsc io.ReadSeeker) (*format.SuperBlock, error) {
	// save current reader position
	currBytePos, err := rsc.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	if _, err := rsc.Seek(1024, io.SeekCurrent); err != nil {
		return nil, err
	}
	var sb format.SuperBlock
	if err := binary.Read(rsc, binary.LittleEndian, &sb); err != nil {
		return nil, err
	}

	// reset the reader to initial position
	if _, err := rsc.Seek(currBytePos, io.SeekStart); err != nil {
		return nil, err
	}

	if sb.Magic != format.SuperBlockMagic {
		return nil, errors.New("not an ext4 file system")
	}
	return &sb, nil
}

// Ext4FileSystemSize reads ext4 superblock and returns the size of the underlying
// ext4 file system and its block size.
func Ext4FileSystemSize(r io.ReadSeeker) (int64, int, error) {
	sb, err := ReadExt4SuperBlockReadSeeker(r)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read ext4 superblock: %w", err)
	}
	blockSize := 1024 * (1 << sb.LogBlockSize)
	fsSize := int64(blockSize) * int64(sb.BlocksCountLow)
	return fsSize, blockSize, nil
}
