package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"astersearch/internal/index"
)

const manifestFilename = "manifest.json"

// SegmentManifest tracks immutable segments and the WAL offset they are derived from.
type SegmentManifest struct {
	Segments         []index.SegmentMetadata `json:"segments"`
	AppliedWALOffset int64                   `json:"appliedWalOffset"`

	path string
	mu   sync.Mutex
}

// LoadSegmentManifest reads the manifest file or initializes an empty one when absent.
func LoadSegmentManifest(basePath string) (*SegmentManifest, error) {
	if err := os.MkdirAll(basePath, 0o755); err != nil {
		return nil, fmt.Errorf("create manifest dir: %w", err)
	}

	path := filepath.Join(basePath, manifestFilename)
	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			m := &SegmentManifest{path: path}
			if err := m.persist(); err != nil {
				return nil, err
			}
			return m, nil
		}
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	var manifest SegmentManifest
	if err := json.Unmarshal(content, &manifest); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}

	manifest.path = path
	return &manifest, nil
}

// AddSegment appends a new immutable segment entry and persists the manifest.
func (m *SegmentManifest) AddSegment(meta index.SegmentMetadata, appliedOffset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Segments = append(m.Segments, meta)
	sort.Slice(m.Segments, func(i, j int) bool {
		return m.Segments[i].ID < m.Segments[j].ID
	})
	m.AppliedWALOffset = appliedOffset
	return m.persist()
}

// ReplaceSegments atomically swaps a set of existing segments with a new collection and persists the change.
// This is useful for compaction/merge operations where deleted or superseded segments should disappear together
// with the publication of a new merged segment.
func (m *SegmentManifest) ReplaceSegments(removeIDs []string, add []index.SegmentMetadata, appliedOffset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	removals := make(map[string]struct{}, len(removeIDs))
	for _, id := range removeIDs {
		removals[id] = struct{}{}
	}

	filtered := make([]index.SegmentMetadata, 0, len(m.Segments)+len(add))
	for _, seg := range m.Segments {
		if _, drop := removals[seg.ID]; drop {
			continue
		}
		filtered = append(filtered, seg)
	}

	filtered = append(filtered, add...)
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].ID < filtered[j].ID })

	m.Segments = filtered
	m.AppliedWALOffset = appliedOffset
	return m.persist()
}

// UpdateOffset updates the WAL watermark without modifying segments.
func (m *SegmentManifest) UpdateOffset(offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.AppliedWALOffset = offset
	return m.persist()
}

func (m *SegmentManifest) persist() error {
	content, err := json.MarshalIndent(struct {
		Segments         []index.SegmentMetadata `json:"segments"`
		AppliedWALOffset int64                   `json:"appliedWalOffset"`
	}{Segments: m.Segments, AppliedWALOffset: m.AppliedWALOffset}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}

	if err := os.WriteFile(m.path, content, 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	return nil
}
