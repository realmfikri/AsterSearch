package storage

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"
)

type gzipCompressor struct{}

func (gzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (gzipCompressor) Decompress(data []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}

func TestSegmentWriteAndReadWithCompression(t *testing.T) {
	dir := t.TempDir()
	compressor := gzipCompressor{}
	files := SegmentFiles{
		ID:       "seg-1",
		Postings: []byte("postings"),
		Docs:     []byte("docs"),
		Meta:     []byte("meta"),
	}

	if err := WriteSegment(dir, files, SegmentWriteOptions{Compressor: compressor}); err != nil {
		t.Fatalf("write segment: %v", err)
	}

	loaded, err := ReadSegment(dir, files.ID, SegmentReadOptions{Compressor: compressor, UseMmap: true})
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}

	if !bytes.Equal(loaded.Postings, files.Postings) || !bytes.Equal(loaded.Docs, files.Docs) || !bytes.Equal(loaded.Meta, files.Meta) {
		t.Fatalf("segment content mismatch: %+v", loaded)
	}
}
