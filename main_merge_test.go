package main

import (
	"testing"

	"astersearch/internal/index"
)

func TestMergeSegmentsPrunesDeletesAndRebuildsStats(t *testing.T) {
	def := index.Definition{Fields: map[string]index.FieldDefinition{
		"title": {Type: index.FieldTypeText},
	}}
	tokenizer := index.NewSimpleTokenizer(nil)

	seg1 := index.SegmentSnapshot{
		Postings: map[string][]index.Posting{
			"fox": {{DocID: "1", Positions: []int{0}}},
		},
		Docs:  []map[string]any{{"id": "1", "title": "quick fox"}},
		Stats: index.BM25Stats{TotalDocs: 1, AvgFieldLengths: map[string]float64{"title": 2}},
	}

	seg2 := index.SegmentSnapshot{
		Postings: map[string][]index.Posting{
			"fox": {{DocID: "2", Positions: []int{0}}, {DocID: "1", Positions: []int{0, 1}}},
		},
		Docs:  []map[string]any{{"id": "1", "title": "quick fox", "_deleted": true}, {"id": "2", "title": "new fox"}},
		Stats: index.BM25Stats{TotalDocs: 2, AvgFieldLengths: map[string]float64{"title": 2}},
	}

	merged := mergeSegments(def, tokenizer, []index.SegmentSnapshot{seg1, seg2})

	if merged.Stats.TotalDocs != 1 {
		t.Fatalf("expected only one live doc, got %d", merged.Stats.TotalDocs)
	}

	if got := len(merged.Docs); got != 1 || merged.Docs[0]["id"] != "2" {
		t.Fatalf("expected only doc 2 to survive deletion, got %+v", merged.Docs)
	}

	foxPostings := merged.Postings["fox"]
	if len(foxPostings) != 1 || foxPostings[0].DocID != "2" {
		t.Fatalf("expected postings to drop deleted doc, got %+v", foxPostings)
	}

	if avg := merged.Stats.AvgFieldLengths["title"]; avg != 2 {
		t.Fatalf("expected recomputed avg length of 2, got %f", avg)
	}
}

func TestIndexEngineCompactionReplacesSegmentsAndMetadata(t *testing.T) {
	def := index.Definition{Fields: map[string]index.FieldDefinition{"title": {Type: index.FieldTypeText}}}
	eng := newIndexEngine(def, nil, indexEngineConfig{})
	defer close(eng.stopCh)

	segA := index.SegmentSnapshot{Docs: []map[string]any{{"id": "1", "title": "first"}}, Stats: index.BM25Stats{TotalDocs: 1}}
	segB := index.SegmentSnapshot{Docs: []map[string]any{{"id": "2", "title": "second"}}, Stats: index.BM25Stats{TotalDocs: 1}}

	eng.mu.Lock()
	eng.segments = []index.SegmentSnapshot{segA, segB}
	eng.mu.Unlock()

	eng.compactSegments()

	eng.mu.RLock()
	defer eng.mu.RUnlock()

	if got := len(eng.segments); got != 1 {
		t.Fatalf("expected a single merged segment, got %d", got)
	}
	if eng.def.Metadata.DocCount != 2 {
		t.Fatalf("expected metadata doc count to reflect merge, got %d", eng.def.Metadata.DocCount)
	}
	if got := len(eng.def.Metadata.Segments); got != 1 {
		t.Fatalf("expected metadata to be replaced with new segment, got %d entries", got)
	}
}
