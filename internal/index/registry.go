package index

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// FieldType represents the supported indexable field types.
type FieldType string

const (
	FieldTypeText    FieldType = "text"
	FieldTypeKeyword FieldType = "keyword"
)

// FieldDefinition describes how a field should be indexed.
type FieldDefinition struct {
	Type       FieldType `json:"type"`
	Weight     float64   `json:"weight,omitempty"`
	FilterOnly bool      `json:"filterOnly,omitempty"`
}

// BM25Parameters stores tunable ranking parameters.
type BM25Parameters struct {
	K1 float64 `json:"k1"`
	B  float64 `json:"b"`
}

// SegmentMetadata captures immutable segment level information.
type SegmentMetadata struct {
	ID            string `json:"id"`
	DocumentCount int    `json:"documentCount"`
}

// IndexMetadata exposes runtime statistics for an index.
type IndexMetadata struct {
	DocCount int               `json:"docCount"`
	Segments []SegmentMetadata `json:"segments"`
}

// Definition represents a fully resolved index definition.
type Definition struct {
	Name      string                     `json:"name"`
	Fields    map[string]FieldDefinition `json:"fields"`
	Tokenizer string                     `json:"tokenizer"`
	BM25      BM25Parameters             `json:"bm25"`
	Metadata  IndexMetadata              `json:"metadata"`
}

// CreateRequest captures the payload for creating an index.
type CreateRequest struct {
	Name      string                     `json:"name"`
	Fields    map[string]FieldDefinition `json:"fields"`
	Tokenizer string                     `json:"tokenizer"`
	BM25      *BM25Parameters            `json:"bm25,omitempty"`
}

// Registry persists index definitions on disk and serves them at runtime.
type Registry struct {
	basePath string
	indexes  map[string]Definition
	mu       sync.RWMutex
}

const (
	defaultTokenizer = "standard"
	defaultK1        = 1.2
	defaultB         = 0.75
	maxFields        = 64
	maxNameLength    = 64
)

// NewRegistry loads existing index definitions from disk, ensuring the storage path exists.
func NewRegistry(basePath string) (*Registry, error) {
	if err := os.MkdirAll(basePath, 0o755); err != nil {
		return nil, fmt.Errorf("create index directory: %w", err)
	}

	r := &Registry{
		basePath: basePath,
		indexes:  make(map[string]Definition),
	}

	if err := r.loadFromDisk(); err != nil {
		return nil, err
	}

	return r, nil
}

// Create registers and persists a new index definition.
func (r *Registry) Create(req CreateRequest) (Definition, error) {
	if err := req.validate(); err != nil {
		return Definition{}, err
	}

	def := Definition{
		Name:      strings.TrimSpace(req.Name),
		Fields:    normalizeFields(req.Fields),
		Tokenizer: defaultTokenizer,
		BM25:      BM25Parameters{K1: defaultK1, B: defaultB},
		Metadata:  IndexMetadata{DocCount: 0, Segments: []SegmentMetadata{}},
	}

	if req.Tokenizer != "" {
		def.Tokenizer = req.Tokenizer
	}

	if req.BM25 != nil {
		def.BM25 = *req.BM25
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.indexes[def.Name]; exists {
		return Definition{}, fmt.Errorf("index '%s' already exists", def.Name)
	}

	if err := r.persist(def); err != nil {
		return Definition{}, err
	}

	r.indexes[def.Name] = def
	return def, nil
}

// List returns all known index definitions.
func (r *Registry) List() []Definition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	definitions := make([]Definition, 0, len(r.indexes))
	for _, def := range r.indexes {
		definitions = append(definitions, def)
	}

	return definitions
}

// Get retrieves an index definition by name.
func (r *Registry) Get(name string) (Definition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	def, ok := r.indexes[name]
	return def, ok
}

// UpdateDefinition persists changes to an existing definition (e.g. runtime metadata updates).
func (r *Registry) UpdateDefinition(def Definition) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.indexes[def.Name]; !ok {
		return fmt.Errorf("index '%s' not found", def.Name)
	}

	if err := r.persist(def); err != nil {
		return err
	}

	r.indexes[def.Name] = def
	return nil
}

func (r *Registry) persist(def Definition) error {
	path := filepath.Join(r.basePath, fmt.Sprintf("%s.json", def.Name))
	content, err := json.MarshalIndent(def, "", "  ")
	if err != nil {
		return fmt.Errorf("serialize definition: %w", err)
	}

	if err := os.WriteFile(path, content, 0o644); err != nil {
		return fmt.Errorf("write definition: %w", err)
	}

	return nil
}

func (r *Registry) loadFromDisk() error {
	entries, err := os.ReadDir(r.basePath)
	if err != nil {
		return fmt.Errorf("read index directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		path := filepath.Join(r.basePath, entry.Name())
		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read definition %s: %w", entry.Name(), err)
		}

		var def Definition
		if err := json.Unmarshal(content, &def); err != nil {
			return fmt.Errorf("decode definition %s: %w", entry.Name(), err)
		}

		if def.Name == "" {
			return fmt.Errorf("definition file %s missing name", entry.Name())
		}

		if err := validateFields(def.Fields); err != nil {
			return fmt.Errorf("definition %s invalid fields: %w", def.Name, err)
		}

		if err := validateBM25(def.BM25); err != nil {
			return fmt.Errorf("definition %s invalid bm25: %w", def.Name, err)
		}

		r.indexes[def.Name] = def
	}

	return nil
}

func normalizeFields(fields map[string]FieldDefinition) map[string]FieldDefinition {
	normalized := make(map[string]FieldDefinition, len(fields))
	for name, def := range fields {
		key := strings.TrimSpace(name)
		weight := def.Weight
		if weight == 0 {
			weight = 1.0
		}
		normalized[key] = FieldDefinition{
			Type:       def.Type,
			Weight:     weight,
			FilterOnly: def.FilterOnly,
		}
	}
	return normalized
}

func (req CreateRequest) validate() error {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return errors.New("name is required")
	}

	if len(name) > maxNameLength {
		return fmt.Errorf("name must be <= %d characters", maxNameLength)
	}

	if len(req.Fields) == 0 {
		return errors.New("at least one field is required")
	}

	if len(req.Fields) > maxFields {
		return fmt.Errorf("field count exceeds limit of %d", maxFields)
	}

	if err := validateFields(req.Fields); err != nil {
		return err
	}

	if req.Tokenizer == "" {
		// default will be applied, no validation needed
	}

	if req.BM25 != nil {
		if err := validateBM25(*req.BM25); err != nil {
			return err
		}
	}

	return nil
}

func validateFields(fields map[string]FieldDefinition) error {
	for name, def := range fields {
		key := strings.TrimSpace(name)
		if key == "" {
			return fmt.Errorf("field name cannot be empty")
		}

		switch def.Type {
		case FieldTypeText, FieldTypeKeyword:
		default:
			return fmt.Errorf("field '%s' has invalid type '%s'", key, def.Type)
		}

		if def.Weight < 0 {
			return fmt.Errorf("field '%s' weight must be non-negative", key)
		}
	}

	return nil
}

func validateBM25(bm25 BM25Parameters) error {
	if bm25.K1 <= 0 {
		return errors.New("bm25.k1 must be > 0")
	}

	if bm25.B < 0 || bm25.B > 1 {
		return errors.New("bm25.b must be between 0 and 1")
	}

	return nil
}

// LoadFromFS reconstructs a registry from an fs.FS, useful for testing.
func LoadFromFS(fsys fs.FS, basePath string) (*Registry, error) {
	r := &Registry{
		basePath: basePath,
		indexes:  make(map[string]Definition),
	}

	err := fs.WalkDir(fsys, basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(d.Name()) != ".json" {
			return nil
		}

		content, err := fs.ReadFile(fsys, path)
		if err != nil {
			return err
		}

		var def Definition
		if err := json.Unmarshal(content, &def); err != nil {
			return err
		}

		if def.Name == "" {
			return fmt.Errorf("definition file %s missing name", d.Name())
		}

		if err := validateFields(def.Fields); err != nil {
			return fmt.Errorf("definition %s invalid fields: %w", def.Name, err)
		}

		if err := validateBM25(def.BM25); err != nil {
			return fmt.Errorf("definition %s invalid bm25: %w", def.Name, err)
		}

		r.indexes[def.Name] = def
		return nil
	})

	if err != nil {
		return nil, err
	}

	return r, nil
}
