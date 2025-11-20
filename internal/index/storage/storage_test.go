package storage

import (
	"os"
	"path/filepath"
	"testing"

	"astersearch/internal/index"
)

func TestEngineStorageRecoveryAndManifest(t *testing.T) {
	base := t.TempDir()

	storage, pending, err := OpenEngineStorage(base)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending records on empty storage")
	}

	// Append a record and reopen to ensure it is replayed.
	walOffset, err := storage.WAL.Append(WALRecord{Operation: "index", Index: "articles", Document: map[string]any{"id": "1"}})
	if err != nil {
		t.Fatalf("append record: %v", err)
	}
	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	storage, pending, err = OpenEngineStorage(base)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	if got, want := len(pending), 1; got != want {
		t.Fatalf("expected %d pending record, got %d", want, got)
	}

	// Mark WAL offset as applied and persist a segment.
	segmentMeta := index.SegmentMetadata{ID: "seg-1", DocumentCount: 1}
	if err := WriteSegment(storage.SegmentsPath(), SegmentFiles{ID: segmentMeta.ID}, SegmentWriteOptions{}); err != nil {
		t.Fatalf("write segment stub: %v", err)
	}
	if err := storage.Manifest.AddSegment(segmentMeta, walOffset); err != nil {
		t.Fatalf("add segment: %v", err)
	}

	// Reopen again and ensure no pending records remain.
	storage.Close()
	storage, pending, err = OpenEngineStorage(base)
	if err != nil {
		t.Fatalf("reopen after manifest: %v", err)
	}
	defer storage.Close()

	if len(pending) != 0 {
		t.Fatalf("expected wal to be fully applied, got %d pending", len(pending))
	}

	if storage.Manifest.AppliedWALOffset != walOffset {
		t.Fatalf("expected manifest offset %d, got %d", walOffset, storage.Manifest.AppliedWALOffset)
	}

	if len(storage.Manifest.Segments) != 1 || storage.Manifest.Segments[0].ID != segmentMeta.ID {
		t.Fatalf("unexpected manifest segments %+v", storage.Manifest.Segments)
	}

	// Cleanup helper should remove directories.
	if err := storage.RemoveAll(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, "wal")); !os.IsNotExist(err) {
		t.Fatalf("expected wal directory to be removed")
	}
}
