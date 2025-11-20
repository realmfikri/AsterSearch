package storage

import (
	"testing"

	"astersearch/internal/index"
)

func TestSegmentManifestLifecycle(t *testing.T) {
	dir := t.TempDir()

	manifest, err := LoadSegmentManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}

	if manifest.AppliedWALOffset != 0 || len(manifest.Segments) != 0 {
		t.Fatalf("expected empty manifest, got %+v", manifest)
	}

	meta := index.SegmentMetadata{ID: "seg-1", DocumentCount: 10}
	if err := manifest.AddSegment(meta, 128); err != nil {
		t.Fatalf("add segment: %v", err)
	}

	if manifest.AppliedWALOffset != 128 {
		t.Fatalf("expected wal offset 128, got %d", manifest.AppliedWALOffset)
	}
	if len(manifest.Segments) != 1 || manifest.Segments[0].ID != "seg-1" {
		t.Fatalf("unexpected segments %+v", manifest.Segments)
	}

	// Ensure it round-trips to disk.
	manifestReloaded, err := LoadSegmentManifest(dir)
	if err != nil {
		t.Fatalf("reload manifest: %v", err)
	}
	if manifestReloaded.AppliedWALOffset != 128 || len(manifestReloaded.Segments) != 1 {
		t.Fatalf("unexpected reloaded manifest %+v", manifestReloaded)
	}
}
