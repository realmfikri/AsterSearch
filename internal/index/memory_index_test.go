package index

import "testing"

func TestSimpleTokenizerWithStopwords(t *testing.T) {
	tokenizer := NewSimpleTokenizer([]string{"the", "and"})
	tokens := tokenizer.Tokenize("The quick brown fox and the dog")

	wantTerms := []Token{{Term: "quick", Position: 1}, {Term: "brown", Position: 2}, {Term: "fox", Position: 3}, {Term: "dog", Position: 6}}
	if len(tokens) != len(wantTerms) {
		t.Fatalf("expected %d tokens got %d", len(wantTerms), len(tokens))
	}
	for i, tok := range tokens {
		if tok != wantTerms[i] {
			t.Fatalf("token %d mismatch: %+v vs %+v", i, tok, wantTerms[i])
		}
	}
}

func TestIndexDocumentTracksPostingsAndStats(t *testing.T) {
	def := Definition{
		Fields: map[string]FieldDefinition{
			"title": {Type: FieldTypeText, Weight: 2},
			"tags":  {Type: FieldTypeKeyword, Weight: 1},
		},
	}

	tokenizer := NewSimpleTokenizer([]string{"the"})
	idx := NewInMemoryIndex(def, tokenizer, FlushThresholds{MaxDocuments: 2})

	doc := map[string]any{
		"id":    "1",
		"title": "The quick brown fox",
		"tags":  []string{"Go", "Rust"},
	}

	if err := idx.IndexDocument(doc); err != nil {
		t.Fatalf("index document: %v", err)
	}

	if idx.ShouldFlush() {
		t.Fatalf("should not flush after one doc with threshold 2")
	}

	postings := idx.inverted["quick"]["1"]
	if postings == nil {
		t.Fatalf("expected postings for 'quick'")
	}
	if postings.TermFreq != 2 {
		t.Fatalf("expected weighted term freq 2 got %v", postings.TermFreq)
	}

	if len(postings.Positions) != 1 || postings.Positions[0] != 1 {
		t.Fatalf("unexpected positions: %+v", postings.Positions)
	}

	if idx.fieldTotals["title"] != 3 { // "the" is a stopword and ignored
		t.Fatalf("expected field length 3 got %d", idx.fieldTotals["title"])
	}

	doc2 := map[string]any{"id": "2", "title": "brown dog", "tags": "Go"}
	if err := idx.IndexDocument(doc2); err != nil {
		t.Fatalf("index document 2: %v", err)
	}

	if !idx.ShouldFlush() {
		t.Fatalf("should flush after reaching max documents")
	}

	snapshot := idx.Flush()
	if snapshot.Stats.TotalDocs != 2 {
		t.Fatalf("expected 2 docs in snapshot stats got %d", snapshot.Stats.TotalDocs)
	}

	if got := snapshot.Stats.AvgFieldLengths["title"]; got != 2.5 {
		t.Fatalf("unexpected avg field length got %v", got)
	}

	if len(snapshot.Docs) != 2 {
		t.Fatalf("expected doc store snapshot to contain 2 docs")
	}

	if idx.totalDocs != 0 || len(idx.inverted) != 0 {
		t.Fatalf("expected state reset after flush")
	}
}
