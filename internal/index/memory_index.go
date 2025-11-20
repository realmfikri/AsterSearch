package index

import (
	"fmt"
	"sort"
	"strings"
)

// Posting captures inverted index information for a single document.
type Posting struct {
	DocID     string
	TermFreq  float64
	Positions []int
}

// BM25Stats tracks the corpus statistics required for BM25 scoring.
type BM25Stats struct {
	TotalDocs       int
	AvgFieldLengths map[string]float64
}

// FlushThresholds controls when the in-memory index should be materialized into an immutable segment.
type FlushThresholds struct {
	MaxDocuments int
	MaxPostings  int
}

// SegmentSnapshot materializes the current mutable state so it can be persisted as an immutable segment.
type SegmentSnapshot struct {
	Postings map[string][]Posting
	Docs     []map[string]any
	Stats    BM25Stats
}

// InMemoryIndex accumulates documents and postings prior to flushing them to disk segments.
type InMemoryIndex struct {
	def         Definition
	tokenizer   Tokenizer
	thresholds  FlushThresholds
	inverted    map[string]map[string]*Posting
	docStore    []map[string]any
	fieldTotals map[string]int
	totalDocs   int
	totalTerms  int
}

// NewInMemoryIndex wires together tokenization, stopword removal, and field weighting for the supplied index definition.
func NewInMemoryIndex(def Definition, tokenizer Tokenizer, thresholds FlushThresholds) *InMemoryIndex {
	return &InMemoryIndex{
		def:         def,
		tokenizer:   tokenizer,
		thresholds:  thresholds,
		inverted:    make(map[string]map[string]*Posting),
		docStore:    []map[string]any{},
		fieldTotals: make(map[string]int),
	}
}

// IndexDocument ingests the provided document and updates postings, document store, and BM25 statistics.
func (idx *InMemoryIndex) IndexDocument(doc map[string]any) error {
	idRaw, ok := doc["id"]
	if !ok {
		return fmt.Errorf("document missing id")
	}

	docID, ok := idRaw.(string)
	if !ok || docID == "" {
		return fmt.Errorf("document id must be a non-empty string")
	}

	idx.totalDocs++
	idx.docStore = append(idx.docStore, cloneDocument(doc))

	for fieldName, fieldDef := range idx.def.Fields {
		value, exists := doc[fieldName]
		if !exists {
			continue
		}

		switch fieldDef.Type {
		case FieldTypeText:
			idx.indexTextField(docID, fieldName, value, fieldDef.Weight)
		case FieldTypeKeyword:
			idx.indexKeywordField(docID, fieldName, value, fieldDef.Weight)
		}
	}

	return nil
}

// ShouldFlush reports whether current mutable state exceeds any configured thresholds.
func (idx *InMemoryIndex) ShouldFlush() bool {
	if idx.thresholds.MaxDocuments > 0 && idx.totalDocs >= idx.thresholds.MaxDocuments {
		return true
	}
	if idx.thresholds.MaxPostings > 0 && idx.totalTerms >= idx.thresholds.MaxPostings {
		return true
	}
	return false
}

// Flush snapshots the mutable structures into an immutable representation and resets the index.
func (idx *InMemoryIndex) Flush() SegmentSnapshot {
	snapshot := SegmentSnapshot{
		Postings: make(map[string][]Posting, len(idx.inverted)),
		Docs:     idx.docStore,
		Stats: BM25Stats{
			TotalDocs:       idx.totalDocs,
			AvgFieldLengths: make(map[string]float64, len(idx.fieldTotals)),
		},
	}

	for term, postingsByDoc := range idx.inverted {
		postings := make([]Posting, 0, len(postingsByDoc))
		for _, p := range postingsByDoc {
			// Ensure deterministic ordering of positions within postings
			sort.Ints(p.Positions)
			postings = append(postings, *p)
		}
		sort.Slice(postings, func(i, j int) bool {
			return postings[i].DocID < postings[j].DocID
		})
		snapshot.Postings[term] = postings
	}

	for field, total := range idx.fieldTotals {
		snapshot.Stats.AvgFieldLengths[field] = float64(total) / float64(idx.totalDocs)
	}

	idx.inverted = make(map[string]map[string]*Posting)
	idx.docStore = []map[string]any{}
	idx.fieldTotals = make(map[string]int)
	idx.totalDocs = 0
	idx.totalTerms = 0

	return snapshot
}

func (idx *InMemoryIndex) indexTextField(docID, field string, value any, weight float64) {
	tokens := idx.tokenizer.Tokenize(fmt.Sprint(value))
	idx.fieldTotals[field] += len(tokens)
	idx.totalTerms += len(tokens)

	for _, token := range tokens {
		idx.addPosting(token.Term, docID, token.Position, weight)
	}
}

func (idx *InMemoryIndex) indexKeywordField(docID, field string, value any, weight float64) {
	switch v := value.(type) {
	case string:
		idx.addPosting(strings.ToLower(v), docID, 0, weight)
		idx.fieldTotals[field]++
		idx.totalTerms++
	case []string:
		for i, entry := range v {
			idx.addPosting(strings.ToLower(entry), docID, i, weight)
			idx.fieldTotals[field]++
			idx.totalTerms++
		}
	}
}

func (idx *InMemoryIndex) addPosting(term, docID string, position int, weight float64) {
	if term == "" {
		return
	}
	postingsByDoc, exists := idx.inverted[term]
	if !exists {
		postingsByDoc = make(map[string]*Posting)
		idx.inverted[term] = postingsByDoc
	}

	posting, exists := postingsByDoc[docID]
	if !exists {
		posting = &Posting{DocID: docID}
		postingsByDoc[docID] = posting
	}

	posting.TermFreq += weight
	posting.Positions = append(posting.Positions, position)
}

func cloneDocument(doc map[string]any) map[string]any {
	clone := make(map[string]any, len(doc))
	for k, v := range doc {
		clone[k] = v
	}
	return clone
}
