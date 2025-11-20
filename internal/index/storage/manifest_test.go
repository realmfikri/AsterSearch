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

func TestSegmentManifestReplaceSegments(t *testing.T) {
	dir := t.TempDir()

	manifest, err := LoadSegmentManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}

	segs := []index.SegmentMetadata{
		{ID: "seg-1", DocumentCount: 1},
		{ID: "seg-2", DocumentCount: 1},
	}
	for _, seg := range segs {
		if err := manifest.AddSegment(seg, int64(len(seg.ID))); err != nil {
			t.Fatalf("add segment: %v", err)
		}
	}

	merged := index.SegmentMetadata{ID: "seg-merged", DocumentCount: 2}
	if err := manifest.ReplaceSegments([]string{"seg-1", "seg-2"}, []index.SegmentMetadata{merged}, 99); err != nil {
		t.Fatalf("replace segments: %v", err)
	}

	if manifest.AppliedWALOffset != 99 {
		t.Fatalf("expected wal offset to update, got %d", manifest.AppliedWALOffset)
	}

	if len(manifest.Segments) != 1 || manifest.Segments[0].ID != merged.ID {
		t.Fatalf("expected only merged segment to remain, got %+v", manifest.Segments)
	}
}
