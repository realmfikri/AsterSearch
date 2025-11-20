package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const walFilename = "wal.log"

// WALRecord represents a single append-only mutation event.
type WALRecord struct {
	Operation string         `json:"op"`
	Index     string         `json:"index"`
	Document  map[string]any `json:"document,omitempty"`
	SegmentID string         `json:"segmentId,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// WAL provides append-only durability for incoming writes and segment materialization.
type WAL struct {
	path string
	file *os.File
	mu   sync.Mutex
}

// OpenWAL ensures the WAL file exists and is ready for appends.
// It returns the opened WAL and its current size (next offset).
func OpenWAL(basePath string) (*WAL, int64, error) {
	if err := os.MkdirAll(basePath, 0o755); err != nil {
		return nil, 0, fmt.Errorf("create wal directory: %w", err)
	}

	path := filepath.Join(basePath, walFilename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, 0, fmt.Errorf("open wal: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat wal: %w", err)
	}

	return &WAL{path: path, file: file}, info.Size(), nil
}

// Append writes a length-prefixed JSON record to the WAL and fsyncs the file.
// It returns the offset immediately after the record has been persisted.
func (w *WAL) Append(record WALRecord) (int64, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("marshal wal record: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return 0, fmt.Errorf("seek wal end: %w", err)
	}

	offset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("wal offset: %w", err)
	}

	if err := binary.Write(w.file, binary.LittleEndian, uint32(len(data))); err != nil {
		return 0, fmt.Errorf("write wal length: %w", err)
	}

	if _, err := w.file.Write(data); err != nil {
		return 0, fmt.Errorf("write wal body: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return 0, fmt.Errorf("fsync wal: %w", err)
	}

	return offset + int64(4+len(data)), nil
}

// Recover reads records after the provided offset (typically manifest.AppliedWALOffset).
// It stops at the first incomplete or corrupt record to guarantee idempotent replay.
func (w *WAL) Recover(fromOffset int64) ([]WALRecord, int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(fromOffset, io.SeekStart); err != nil {
		return nil, fromOffset, fmt.Errorf("seek wal: %w", err)
	}

	reader := bufio.NewReader(w.file)
	var records []WALRecord
	currentOffset := fromOffset

	for {
		lengthBuf := make([]byte, 4)
		if _, err := io.ReadFull(reader, lengthBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return records, currentOffset, nil
			}
			return records, currentOffset, fmt.Errorf("read wal length: %w", err)
		}

		length := binary.LittleEndian.Uint32(lengthBuf)
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return records, currentOffset, nil
			}
			return records, currentOffset, fmt.Errorf("read wal payload: %w", err)
		}

		var record WALRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			return records, currentOffset, fmt.Errorf("decode wal record: %w", err)
		}

		currentOffset += int64(4 + length)
		records = append(records, record)
	}
}

// Close closes the underlying file handle.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
