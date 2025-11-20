package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendAndRecover(t *testing.T) {
	dir := t.TempDir()
	wal, _, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer wal.Close()

	first := WALRecord{Operation: "index", Index: "articles", Document: map[string]any{"id": "1"}}
	second := WALRecord{Operation: "index", Index: "articles", Document: map[string]any{"id": "2"}}

	offset, err := wal.Append(first)
	if err != nil {
		t.Fatalf("append first: %v", err)
	}
	offset, err = wal.Append(second)
	if err != nil {
		t.Fatalf("append second: %v", err)
	}

	records, next, err := wal.Recover(0)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}

	if got, want := len(records), 2; got != want {
		t.Fatalf("expected %d records, got %d", want, got)
	}

	if next != offset {
		t.Fatalf("expected recovery offset %d, got %d", offset, next)
	}

	// Simulate partial write to ensure recovery stops cleanly.
	f, err := os.OpenFile(filepath.Join(dir, walFilename), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open wal raw: %v", err)
	}
	if _, err := f.Write([]byte{0xFF}); err != nil {
		t.Fatalf("append garbage: %v", err)
	}
	f.Close()

	records, next, err = wal.Recover(offset)
	if err != nil {
		t.Fatalf("recover after garbage: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected no new records after garbage, got %d", len(records))
	}
	if next != offset {
		t.Fatalf("expected offset to remain %d, got %d", offset, next)
	}
}
