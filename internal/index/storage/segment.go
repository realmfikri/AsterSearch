package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	postingsExt = ".postings"
	docsExt     = ".docs"
	metaExt     = ".meta"
)

// Compressor exposes hooks for optional compression/decompression.
type Compressor interface {
	Compress([]byte) ([]byte, error)
	Decompress([]byte) ([]byte, error)
}

// SegmentWriteOptions controls how immutable segment files are emitted.
type SegmentWriteOptions struct {
	Compressor Compressor
}

// SegmentReadOptions controls how immutable segment files are opened.
type SegmentReadOptions struct {
	Compressor Compressor
	UseMmap    bool
}

// SegmentFiles holds the raw bytes for each immutable segment component.
type SegmentFiles struct {
	ID       string
	Postings []byte
	Docs     []byte
	Meta     []byte
}

// WriteSegment materializes the immutable segment files to disk.
func WriteSegment(basePath string, files SegmentFiles, opts SegmentWriteOptions) error {
	if files.ID == "" {
		return fmt.Errorf("segment id is required")
	}
	if err := os.MkdirAll(basePath, 0o755); err != nil {
		return fmt.Errorf("create segments dir: %w", err)
	}

	writeBlob := func(ext string, payload []byte) error {
		data := payload
		if opts.Compressor != nil {
			compressed, err := opts.Compressor.Compress(payload)
			if err != nil {
				return fmt.Errorf("compress %s: %w", ext, err)
			}
			data = compressed
		}

		path := filepath.Join(basePath, files.ID+ext)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return fmt.Errorf("write segment %s: %w", path, err)
		}
		return nil
	}

	if err := writeBlob(postingsExt, files.Postings); err != nil {
		return err
	}
	if err := writeBlob(docsExt, files.Docs); err != nil {
		return err
	}
	return writeBlob(metaExt, files.Meta)
}

// ReadSegment loads the immutable segment files from disk.
// When UseMmap is enabled and supported, it will attempt a best-effort mmap; otherwise a standard read is used.
func ReadSegment(basePath, segmentID string, opts SegmentReadOptions) (SegmentFiles, error) {
	loadBlob := func(ext string) ([]byte, error) {
		path := filepath.Join(basePath, segmentID+ext)
		if opts.UseMmap {
			// Best-effort mmap hook; fallback to reading when mmap fails or is unsupported.
			data, err := mmapFile(path)
			if err == nil {
				return maybeDecompress(data, opts.Compressor)
			}
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read segment %s: %w", path, err)
		}
		return maybeDecompress(data, opts.Compressor)
	}

	postings, err := loadBlob(postingsExt)
	if err != nil {
		return SegmentFiles{}, err
	}

	docs, err := loadBlob(docsExt)
	if err != nil {
		return SegmentFiles{}, err
	}

	meta, err := loadBlob(metaExt)
	if err != nil {
		return SegmentFiles{}, err
	}

	return SegmentFiles{ID: segmentID, Postings: postings, Docs: docs, Meta: meta}, nil
}

func maybeDecompress(data []byte, compressor Compressor) ([]byte, error) {
	if compressor == nil {
		return data, nil
	}
	decoded, err := compressor.Decompress(data)
	if err != nil {
		return nil, fmt.Errorf("decompress: %w", err)
	}
	return decoded, nil
}

// mmapFile is separated for testability and optional platform support.
func mmapFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	if size == 0 {
		return []byte{}, nil
	}

	// Portable fallback: read fully when syscall.Mmap is unavailable in the target environment.
	buf := make([]byte, size)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
