package index

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// SearchRequest captures the parameters for executing a search against an index snapshot.
type SearchRequest struct {
	Query    string
	Page     int
	PageSize int
	Filters  string
}

// SearchResponse contains the ranked hits plus paging metadata.
type SearchResponse struct {
	TotalHits int         `json:"totalHits"`
	Page      int         `json:"page"`
	PageSize  int         `json:"pageSize"`
	Hits      []SearchHit `json:"hits"`
}

// SearchHit represents a single matched document and its associated metadata.
type SearchHit struct {
	ID         string            `json:"id"`
	Score      float64           `json:"score"`
	Highlights map[string]string `json:"highlights,omitempty"`
	Source     map[string]any    `json:"source,omitempty"`
}

// QueryTerm encapsulates either a single term or a quoted phrase as part of the parsed query string.
type QueryTerm struct {
	Term    string
	Phrase  []string
	Must    bool
	MustNot bool
}

// FilterOperator enumerates the supported filter comparisons.
type FilterOperator string

const (
	FilterEquals FilterOperator = "eq"
	FilterGT     FilterOperator = "gt"
	FilterGTE    FilterOperator = "gte"
	FilterLT     FilterOperator = "lt"
	FilterLTE    FilterOperator = "lte"
	FilterRange  FilterOperator = "range"

	defaultPageSize = 10
	maxPageSize     = 100
)

// Filter captures a structured filter expression derived from the query string or filters parameter.
type Filter struct {
	Field string
	Op    FilterOperator
	Value any
	Min   *float64
	Max   *float64
}

// ParsedQuery is the intermediate representation of a user query after tokenization and parsing.
type ParsedQuery struct {
	Terms   []QueryTerm
	Filters []Filter
}

// Searcher executes BM25-ranked queries over an immutable snapshot of postings and a document store.
type Searcher struct {
	def          Definition
	postings     map[string][]Posting
	docs         map[string]map[string]any
	stats        BM25Stats
	tokenizer    Tokenizer
	docLengths   map[string]int
	avgDocLength float64
}

// NewSearcher constructs a Searcher from a segment snapshot.
func NewSearcher(def Definition, snapshot SegmentSnapshot, tokenizer Tokenizer) *Searcher {
	docs := make(map[string]map[string]any, len(snapshot.Docs))
	for _, doc := range snapshot.Docs {
		if id, ok := doc["id"].(string); ok {
			docs[id] = doc
		}
	}

	docLengths := make(map[string]int)
	totalTerms := 0
	for _, postings := range snapshot.Postings {
		for _, p := range postings {
			docLengths[p.DocID] += len(p.Positions)
			totalTerms += len(p.Positions)
		}
	}

	avgDocLength := 0.0
	if len(docLengths) > 0 {
		avgDocLength = float64(totalTerms) / float64(len(docLengths))
	}

	if tokenizer == nil {
		tokenizer = NewSimpleTokenizer(nil)
	}

	return &Searcher{
		def:          def,
		postings:     snapshot.Postings,
		docs:         docs,
		stats:        snapshot.Stats,
		tokenizer:    tokenizer,
		docLengths:   docLengths,
		avgDocLength: avgDocLength,
	}
}

// ParseQuery breaks the raw query string (and optional filters string) into structured components.
func ParseQuery(raw string, filters string) ParsedQuery {
	tokens := tokenizeQuery(raw)
	if filters != "" {
		tokens = append(tokens, tokenizeQuery(filters)...)
	}

	parsed := ParsedQuery{}
	for _, tok := range tokens {
		term := tok
		must := true // default AND semantics
		mustNot := false

		if strings.HasPrefix(term, "+") {
			term = strings.TrimPrefix(term, "+")
			must = true
		} else if strings.HasPrefix(term, "-") {
			term = strings.TrimPrefix(term, "-")
			must = false
			mustNot = true
		}

		if f, ok := parseFilterToken(term); ok {
			parsed.Filters = append(parsed.Filters, f)
			continue
		}

		if phrase := parsePhrase(term); len(phrase) > 0 {
			parsed.Terms = append(parsed.Terms, QueryTerm{Phrase: phrase, Must: must, MustNot: mustNot})
			continue
		}

		normalized := strings.ToLower(strings.TrimSpace(term))
		if normalized != "" {
			parsed.Terms = append(parsed.Terms, QueryTerm{Term: normalized, Must: must, MustNot: mustNot})
		}
	}

	return parsed
}

// Search executes the ranked query and applies filters, pagination, and highlighting.
func (s *Searcher) Search(req SearchRequest) SearchResponse {
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = defaultPageSize
	}
	if req.PageSize > maxPageSize {
		req.PageSize = maxPageSize
	}

	parsed := ParseQuery(req.Query, req.Filters)
	candidates := s.evaluateCandidates(parsed)
	scores := s.scoreDocuments(parsed, candidates)
	filtered := s.applyFilters(parsed.Filters, scores)

	hits := make([]SearchHit, 0, len(filtered))
	for docID, score := range filtered {
		doc := s.docs[docID]
		hits = append(hits, SearchHit{
			ID:         docID,
			Score:      score,
			Highlights: s.buildHighlights(doc, parsed),
			Source:     doc,
		})
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score == hits[j].Score {
			return hits[i].ID < hits[j].ID
		}
		return hits[i].Score > hits[j].Score
	})

	total := len(hits)
	start := (req.Page - 1) * req.PageSize
	if start > total {
		start = total
	}
	end := start + req.PageSize
	if end > total {
		end = total
	}

	return SearchResponse{TotalHits: total, Page: req.Page, PageSize: req.PageSize, Hits: hits[start:end]}
}

func (s *Searcher) evaluateCandidates(parsed ParsedQuery) map[string]struct{} {
	var mustTerms []QueryTerm
	var optionalTerms []QueryTerm

	for _, t := range parsed.Terms {
		if t.MustNot {
			continue
		}
		if t.Must {
			mustTerms = append(mustTerms, t)
		} else {
			optionalTerms = append(optionalTerms, t)
		}
	}

	candidates := make(map[string]struct{})
	initialized := false

	for _, term := range mustTerms {
		ids := s.lookupTerm(term)
		if !initialized {
			for id := range ids {
				candidates[id] = struct{}{}
			}
			initialized = true
			continue
		}
		for id := range candidates {
			if _, ok := ids[id]; !ok {
				delete(candidates, id)
			}
		}
	}

	// Default AND means optional terms only contribute when no must terms were provided.
	if !initialized && len(optionalTerms) > 0 {
		ids := s.lookupTerm(optionalTerms[0])
		for id := range ids {
			candidates[id] = struct{}{}
		}
		for _, term := range optionalTerms[1:] {
			ids := s.lookupTerm(term)
			for id := range candidates {
				if _, ok := ids[id]; !ok {
					delete(candidates, id)
				}
			}
		}
	}

	// Apply must_not exclusions.
	for _, term := range parsed.Terms {
		if !term.MustNot {
			continue
		}
		ids := s.lookupTerm(term)
		for id := range ids {
			delete(candidates, id)
		}
	}

	return candidates
}

func (s *Searcher) lookupTerm(term QueryTerm) map[string]struct{} {
	if len(term.Phrase) > 0 {
		return s.matchPhrase(term.Phrase)
	}
	matched := make(map[string]struct{})
	postings := s.postings[term.Term]
	for _, p := range postings {
		matched[p.DocID] = struct{}{}
	}
	return matched
}

func (s *Searcher) matchPhrase(terms []string) map[string]struct{} {
	if len(terms) == 0 {
		return map[string]struct{}{}
	}

	postings := s.postings[terms[0]]
	matches := make(map[string]struct{})

	for _, p := range postings {
		positions := make(map[int]struct{}, len(p.Positions))
		for _, pos := range p.Positions {
			positions[pos] = struct{}{}
		}

		match := true
		for i := 1; i < len(terms); i++ {
			nextPostings := postingPositionsForDoc(s.postings[terms[i]], p.DocID)
			if len(nextPostings) == 0 {
				match = false
				break
			}
			stepMatch := false
			for pos := range positions {
				if containsPosition(nextPostings, pos+i) {
					stepMatch = true
					break
				}
			}
			if !stepMatch {
				match = false
				break
			}
		}

		if match {
			matches[p.DocID] = struct{}{}
		}
	}

	return matches
}

func postingPositionsForDoc(postings []Posting, docID string) []int {
	for _, p := range postings {
		if p.DocID == docID {
			return p.Positions
		}
	}
	return nil
}

func containsPosition(positions []int, target int) bool {
	for _, pos := range positions {
		if pos == target {
			return true
		}
	}
	return false
}

func (s *Searcher) scoreDocuments(parsed ParsedQuery, candidates map[string]struct{}) map[string]float64 {
	scores := make(map[string]float64, len(candidates))
	if len(candidates) == 0 {
		return scores
	}

	terms := make([]string, 0, len(parsed.Terms))
	for _, t := range parsed.Terms {
		if t.MustNot {
			continue
		}
		if len(t.Phrase) > 0 {
			terms = append(terms, t.Phrase...)
		} else if t.Term != "" {
			terms = append(terms, t.Term)
		}
	}

	uniqueTerms := uniqueStrings(terms)

	k1 := s.def.BM25.K1
	b := s.def.BM25.B
	avgDL := s.avgDocLength
	if avgDL == 0 {
		avgDL = 1
	}

	for _, term := range uniqueTerms {
		postings := s.postings[term]
		if len(postings) == 0 {
			continue
		}
		df := float64(len(postings))
		idf := math.Log((float64(s.stats.TotalDocs)-df+0.5)/(df+0.5) + 1)

		for _, p := range postings {
			if _, ok := candidates[p.DocID]; !ok {
				continue
			}
			tf := p.TermFreq
			dl := float64(s.docLengths[p.DocID])
			score := idf * ((tf * (k1 + 1)) / (tf + k1*(1-b+b*(dl/avgDL))))
			scores[p.DocID] += score
		}
	}

	return scores
}

func (s *Searcher) applyFilters(filters []Filter, scores map[string]float64) map[string]float64 {
	if len(filters) == 0 || len(scores) == 0 {
		return scores
	}

	filtered := make(map[string]float64, len(scores))
	for docID, score := range scores {
		doc := s.docs[docID]
		if doc == nil {
			continue
		}
		if matchesAllFilters(doc, filters) {
			filtered[docID] = score
		}
	}
	return filtered
}

func matchesAllFilters(doc map[string]any, filters []Filter) bool {
	for _, f := range filters {
		value, ok := doc[f.Field]
		if !ok {
			return false
		}

		switch f.Op {
		case FilterEquals:
			if !valuesEqual(value, f.Value) {
				return false
			}
		case FilterGT, FilterGTE, FilterLT, FilterLTE, FilterRange:
			num, ok := numericValue(value)
			if !ok {
				return false
			}
			if !evaluateNumericFilter(num, f) {
				return false
			}
		}
	}
	return true
}

func valuesEqual(a, b any) bool {
	if na, ok := numericValue(a); ok {
		if nb, ok := numericValue(b); ok {
			return na == nb
		}
	}
	switch va := a.(type) {
	case string:
		vb, ok := b.(string)
		return ok && strings.EqualFold(strings.TrimSpace(va), strings.TrimSpace(vb))
	case []string:
		vb, ok := b.(string)
		if !ok {
			return false
		}
		normalized := strings.ToLower(strings.TrimSpace(vb))
		for _, entry := range va {
			if strings.ToLower(strings.TrimSpace(entry)) == normalized {
				return true
			}
		}
		return false
	}
	return false
}

func numericValue(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case jsonNumber:
		f, err := strconv.ParseFloat(string(n), 64)
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		if err == nil {
			return f, true
		}
	}
	return 0, false
}

func evaluateNumericFilter(value float64, filter Filter) bool {
	switch filter.Op {
	case FilterGT:
		target, _ := numericValue(filter.Value)
		return value > target
	case FilterGTE:
		target, _ := numericValue(filter.Value)
		return value >= target
	case FilterLT:
		target, _ := numericValue(filter.Value)
		return value < target
	case FilterLTE:
		target, _ := numericValue(filter.Value)
		return value <= target
	case FilterRange:
		if filter.Min != nil && value < *filter.Min {
			return false
		}
		if filter.Max != nil && value > *filter.Max {
			return false
		}
		return true
	default:
		return false
	}
}

func tokenizeQuery(input string) []string {
	var tokens []string
	var current strings.Builder
	inQuotes := false

	pushToken := func() {
		if current.Len() > 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
	}

	for _, r := range input {
		switch {
		case r == '"':
			if inQuotes {
				pushToken()
				inQuotes = false
			} else {
				pushToken()
				inQuotes = true
			}
		case unicode.IsSpace(r) && !inQuotes:
			pushToken()
		default:
			current.WriteRune(r)
		}
	}
	pushToken()
	return tokens
}

func parsePhrase(token string) []string {
	if !strings.Contains(token, " ") {
		return nil
	}
	parts := strings.Fields(token)
	if len(parts) == 0 {
		return nil
	}
	phrase := make([]string, 0, len(parts))
	for _, p := range parts {
		normalized := strings.ToLower(strings.TrimSpace(p))
		if normalized != "" {
			phrase = append(phrase, normalized)
		}
	}
	return phrase
}

type jsonNumber string

func parseFilterToken(token string) (Filter, bool) {
	// Comparison operators
	ops := []string{">=", "<=", ">", "<"}
	for _, op := range ops {
		if strings.Contains(token, op) {
			parts := strings.SplitN(token, op, 2)
			if len(parts) != 2 {
				continue
			}
			field := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			if field == "" || value == "" {
				continue
			}
			var operator FilterOperator
			switch op {
			case ">=":
				operator = FilterGTE
			case "<=":
				operator = FilterLTE
			case ">":
				operator = FilterGT
			case "<":
				operator = FilterLT
			}
			return Filter{Field: field, Op: operator, Value: jsonNumber(value)}, true
		}
	}

	if strings.Contains(token, ":") {
		parts := strings.SplitN(token, ":", 2)
		field := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if field == "" || value == "" {
			return Filter{}, false
		}

		// Numeric range expressed as a-b
		if strings.Contains(value, "-") {
			rangeParts := strings.SplitN(value, "-", 2)
			min, minErr := strconv.ParseFloat(rangeParts[0], 64)
			max, maxErr := strconv.ParseFloat(rangeParts[1], 64)
			if minErr == nil || maxErr == nil {
				var minPtr, maxPtr *float64
				if minErr == nil {
					minPtr = &min
				}
				if maxErr == nil {
					maxPtr = &max
				}
				return Filter{Field: field, Op: FilterRange, Min: minPtr, Max: maxPtr}, true
			}
		}

		// Equality (string or numeric)
		if num, err := strconv.ParseFloat(value, 64); err == nil {
			return Filter{Field: field, Op: FilterEquals, Value: num}, true
		}
		return Filter{Field: field, Op: FilterEquals, Value: value}, true
	}

	return Filter{}, false
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func (s *Searcher) buildHighlights(doc map[string]any, parsed ParsedQuery) map[string]string {
	if doc == nil {
		return nil
	}

	highlightTerms := make(map[string]struct{})
	for _, t := range parsed.Terms {
		if t.MustNot {
			continue
		}
		if len(t.Phrase) > 0 {
			for _, pt := range t.Phrase {
				highlightTerms[pt] = struct{}{}
			}
			continue
		}
		if t.Term != "" {
			highlightTerms[t.Term] = struct{}{}
		}
	}

	if len(highlightTerms) == 0 {
		return nil
	}

	highlights := make(map[string]string)
	for field, def := range s.def.Fields {
		if def.Type != FieldTypeText {
			continue
		}
		raw, ok := doc[field].(string)
		if !ok || raw == "" {
			continue
		}
		snippet := buildSnippet(raw, highlightTerms)
		if snippet != "" {
			highlights[field] = snippet
		}
	}

	if len(highlights) == 0 {
		return nil
	}
	return highlights
}

func buildSnippet(text string, terms map[string]struct{}) string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return ""
	}

	matchIdx := -1
	for i, w := range words {
		normalized := strings.ToLower(strings.Trim(w, ",.!?:;"))
		if _, ok := terms[normalized]; ok {
			matchIdx = i
			break
		}
	}

	if matchIdx == -1 {
		return ""
	}

	start := matchIdx - 3
	if start < 0 {
		start = 0
	}
	end := matchIdx + 4
	if end > len(words) {
		end = len(words)
	}

	var snippet []string
	for i := start; i < end; i++ {
		word := words[i]
		normalized := strings.ToLower(strings.Trim(word, ",.!?:;"))
		if _, ok := terms[normalized]; ok {
			snippet = append(snippet, "<em>"+word+"</em>")
		} else {
			snippet = append(snippet, word)
		}
	}

	return strings.Join(snippet, " ")
}
