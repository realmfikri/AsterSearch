package index

import (
	"strings"
	"testing"
)

func TestSearchSupportsBooleanAndPhrases(t *testing.T) {
	def := Definition{Fields: map[string]FieldDefinition{
		"title": {Type: FieldTypeText, Weight: 2},
		"body":  {Type: FieldTypeText, Weight: 1},
		"tags":  {Type: FieldTypeKeyword, FilterOnly: true},
	}, BM25: BM25Parameters{K1: 1.2, B: 0.75}}

	tokenizer := NewSimpleTokenizer(nil)
	idx := NewInMemoryIndex(def, tokenizer, FlushThresholds{})

	docs := []map[string]any{
		{"id": "1", "title": "Quick brown fox", "body": "jumps over lazy dog", "tags": []string{"animals", "story"}, "views": 100, "rating": 4.5},
		{"id": "2", "title": "Fast fox", "body": "quick movements and agile", "tags": []string{"animals"}, "views": 200, "rating": 4.8},
		{"id": "3", "title": "Lazy dog", "body": "sleepy dog lies down", "tags": []string{"pets"}, "views": 50, "rating": 3.0},
	}

	for _, doc := range docs {
		if err := idx.IndexDocument(doc); err != nil {
			t.Fatalf("index document: %v", err)
		}
	}

	snapshot := idx.Flush()
	searcher := NewSearcher(def, snapshot, tokenizer)

	// Default AND between terms
	res := searcher.Search(SearchRequest{Query: "quick fox"})
	if res.TotalHits != 2 {
		t.Fatalf("expected 2 hits for 'quick fox' got %d", res.TotalHits)
	}
	if res.Hits[0].ID != "1" {
		t.Fatalf("expected doc 1 to rank first got %s", res.Hits[0].ID)
	}

	// Must / must_not filters
	res = searcher.Search(SearchRequest{Query: "+fox -lazy"})
	if res.TotalHits != 1 || res.Hits[0].ID != "2" {
		t.Fatalf("expected only doc 2 after applying must/must_not")
	}

	// Phrase query
	res = searcher.Search(SearchRequest{Query: "\"quick brown\""})
	if res.TotalHits != 1 || res.Hits[0].ID != "1" {
		t.Fatalf("expected phrase to match only doc 1")
	}
}

func TestSearchFiltersAndHighlights(t *testing.T) {
	def := Definition{Fields: map[string]FieldDefinition{
		"title": {Type: FieldTypeText, Weight: 2},
		"body":  {Type: FieldTypeText, Weight: 1},
		"tags":  {Type: FieldTypeKeyword, FilterOnly: true},
	}, BM25: BM25Parameters{K1: 1.2, B: 0.75}}

	tokenizer := NewSimpleTokenizer(nil)
	idx := NewInMemoryIndex(def, tokenizer, FlushThresholds{})

	docs := []map[string]any{
		{"id": "1", "title": "Quick brown fox", "body": "jumps over lazy dog", "tags": []string{"animals", "story"}, "views": 100, "rating": 4.5},
		{"id": "2", "title": "Fast fox", "body": "quick movements and agile", "tags": []string{"animals"}, "views": 200, "rating": 4.8},
		{"id": "3", "title": "Lazy dog", "body": "sleepy dog lies down", "tags": []string{"pets"}, "views": 50, "rating": 3.0},
	}

	for _, doc := range docs {
		if err := idx.IndexDocument(doc); err != nil {
			t.Fatalf("index document: %v", err)
		}
	}

	snapshot := idx.Flush()
	searcher := NewSearcher(def, snapshot, tokenizer)

	// Numeric filter
	res := searcher.Search(SearchRequest{Query: "fox", Filters: "views>150"})
	if res.TotalHits != 1 || res.Hits[0].ID != "2" {
		t.Fatalf("expected numeric filter to keep only doc 2 got %v hits", res.TotalHits)
	}

	// Range filter
	res = searcher.Search(SearchRequest{Query: "fox", Filters: "rating:4-5"})
	if res.TotalHits != 2 {
		t.Fatalf("expected two docs within rating range got %d", res.TotalHits)
	}

	// Highlight generation
	hit := res.Hits[0]
	if len(hit.Highlights) == 0 {
		t.Fatalf("expected highlights for matched fields")
	}
	titleHighlight := hit.Highlights["title"]
	if !strings.Contains(titleHighlight, "<em>") {
		t.Fatalf("expected highlighted terms in title snippet got %s", titleHighlight)
	}
}
