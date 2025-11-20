package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// EngineStorage glues together the WAL and manifest around a segment directory layout.
type EngineStorage struct {
	WAL      *WAL
	Manifest *SegmentManifest

	walPath      string
	segmentsPath string
}

// OpenEngineStorage initializes durability primitives and returns pending WAL entries for replay.
func OpenEngineStorage(basePath string) (*EngineStorage, []WALRecord, error) {
	walPath := filepath.Join(basePath, "wal")
	segmentsPath := filepath.Join(basePath, "segments")

	wal, walSize, err := OpenWAL(walPath)
	if err != nil {
		return nil, nil, err
	}

	manifest, err := LoadSegmentManifest(segmentsPath)
	if err != nil {
		_ = wal.Close()
		return nil, nil, err
	}

	// Replay only the portion of the WAL that hasn't been materialized into immutable segments.
	pending, nextOffset, err := wal.Recover(manifest.AppliedWALOffset)
	if err != nil {
		_ = wal.Close()
		return nil, nil, err
	}

	// Keep manifest watermark in sync with the WAL size even if nothing to replay (clean shutdown case).
	if nextOffset == walSize && manifest.AppliedWALOffset != walSize {
		if err := manifest.UpdateOffset(walSize); err != nil {
			_ = wal.Close()
			return nil, nil, err
		}
	}

	return &EngineStorage{WAL: wal, Manifest: manifest, walPath: walPath, segmentsPath: segmentsPath}, pending, nil
}

// SegmentsPath exposes the directory where immutable segment files live.
func (s *EngineStorage) SegmentsPath() string {
	return s.segmentsPath
}

// Close releases underlying handles.
func (s *EngineStorage) Close() error {
	if s.WAL != nil {
		return s.WAL.Close()
	}
	return nil
}

// RemoveAll clears the storage directories (useful for tests only).
func (s *EngineStorage) RemoveAll() error {
	if err := os.RemoveAll(s.walPath); err != nil {
		return fmt.Errorf("cleanup wal: %w", err)
	}
	if err := os.RemoveAll(s.segmentsPath); err != nil {
		return fmt.Errorf("cleanup segments: %w", err)
	}
	return nil
}
