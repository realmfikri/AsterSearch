package index

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCreateAndGetDefinition(t *testing.T) {
	dir := t.TempDir()
	registry, err := NewRegistry(dir)
	if err != nil {
		t.Fatalf("failed to init registry: %v", err)
	}

	req := CreateRequest{
		Name: "articles",
		Fields: map[string]FieldDefinition{
			"title": {Type: FieldTypeText, Weight: 2},
			"tags":  {Type: FieldTypeKeyword, FilterOnly: true},
		},
		Tokenizer: "standard_en",
		BM25:      &BM25Parameters{K1: 1.5, B: 0.7},
	}

	def, err := registry.Create(req)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	if def.Name != req.Name {
		t.Errorf("expected name %s got %s", req.Name, def.Name)
	}

	loaded, ok := registry.Get("articles")
	if !ok {
		t.Fatalf("expected index to be retrievable")
	}

	if loaded.BM25 != *req.BM25 {
		t.Errorf("bm25 mismatch: %+v vs %+v", loaded.BM25, *req.BM25)
	}

	// Verify persisted
	path := filepath.Join(dir, "articles.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected persisted definition: %v", err)
	}

	var persisted Definition
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal persisted: %v", err)
	}

	if persisted.Metadata.DocCount != 0 || len(persisted.Metadata.Segments) != 0 {
		t.Errorf("unexpected metadata: %+v", persisted.Metadata)
	}
}

func TestValidation(t *testing.T) {
	cases := []struct {
		name    string
		req     CreateRequest
		wantErr bool
	}{
		{"missing name", CreateRequest{}, true},
		{"missing fields", CreateRequest{Name: "abc"}, true},
		{"too many fields", CreateRequest{Name: "abc", Fields: generateFields(maxFields + 1)}, true},
		{"bad field type", CreateRequest{Name: "abc", Fields: map[string]FieldDefinition{"title": {Type: "number"}}}, true},
		{"bad bm25", CreateRequest{Name: "abc", Fields: map[string]FieldDefinition{"title": {Type: FieldTypeText}}, BM25: &BM25Parameters{K1: 0, B: 0.5}}, true},
		{"valid", CreateRequest{Name: "abc", Fields: map[string]FieldDefinition{"title": {Type: FieldTypeText}}}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.req.validate()
			if tc.wantErr && err == nil {
				t.Fatalf("expected error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestListReturnsAll(t *testing.T) {
	dir := t.TempDir()
	registry, err := NewRegistry(dir)
	if err != nil {
		t.Fatalf("init registry: %v", err)
	}

	_, _ = registry.Create(CreateRequest{ //nolint:errcheck
		Name: "a",
		Fields: map[string]FieldDefinition{
			"title": {Type: FieldTypeText},
		},
	})

	_, _ = registry.Create(CreateRequest{ //nolint:errcheck
		Name: "b",
		Fields: map[string]FieldDefinition{
			"title": {Type: FieldTypeText},
		},
	})

	indexes := registry.List()
	if len(indexes) != 2 {
		t.Fatalf("expected 2 indexes got %d", len(indexes))
	}
}

func generateFields(n int) map[string]FieldDefinition {
	fields := make(map[string]FieldDefinition, n)
	for i := 0; i < n; i++ {
		fields["f"+string(rune('a'+i%26))+string(rune('a'+(i/26)%26))] = FieldDefinition{Type: FieldTypeText}
	}
	return fields
}
