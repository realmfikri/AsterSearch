package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"astersearch/internal/index"
)

func main() {
	registryPath := os.Getenv("ASTERSEARCH_INDEX_PATH")
	if registryPath == "" {
		registryPath = "data/indexes"
	}

	registry, err := index.NewRegistry(registryPath)
	if err != nil {
		log.Fatalf("failed to initialize index registry: %v", err)
	}

	server := newAPIServer(registry)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/indexes", server.handleIndexes)
	mux.HandleFunc("/v1/indexes/", server.handleIndexByName)
	mux.HandleFunc("/v1/search", server.handleSearch)
	mux.HandleFunc("/v1/health", server.handleHealth)

	addr := ":8080"
	log.Printf("AsterSearch API listening on %s", addr)
	if err := http.ListenAndServe(addr, withJSONHeaders(mux)); err != nil {
		log.Fatal(err)
	}
}

func withJSONHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

type apiServer struct {
	registry *index.Registry

	engines map[string]*indexEngine
	mu      sync.RWMutex
}

func newAPIServer(registry *index.Registry) *apiServer {
	server := &apiServer{
		registry: registry,
		engines:  make(map[string]*indexEngine),
	}

	for _, def := range registry.List() {
		server.engines[def.Name] = newIndexEngine(def, registry)
	}

	return server
}

func (s *apiServer) handleIndexes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.createIndex(w, r)
	case http.MethodGet:
		s.listIndexes(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *apiServer) handleIndexByName(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/v1/indexes/") {
		http.NotFound(w, r)
		return
	}

	trimmed := strings.TrimPrefix(r.URL.Path, "/v1/indexes/")
	segments := strings.Split(strings.Trim(trimmed, "/"), "/")
	if len(segments) == 0 || segments[0] == "" {
		http.NotFound(w, r)
		return
	}

	name := segments[0]
	if len(segments) == 1 {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		def, ok := s.registry.Get(name)
		if !ok {
			http.NotFound(w, r)
			return
		}

		respond(w, http.StatusOK, def)
		return
	}

	switch segments[1] {
	case "documents":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.indexDocuments(w, r, name)
	case "stats":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.indexStats(w, r, name)
	default:
		http.NotFound(w, r)
	}
}

func (s *apiServer) createIndex(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	var req index.CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json payload", start)
		return
	}

	def, err := s.registry.Create(req)
	if err != nil {
		respondError(w, httpStatusForError(err), err.Error(), start)
		return
	}

	s.mu.Lock()
	s.engines[def.Name] = newIndexEngine(def, s.registry)
	s.mu.Unlock()

	respond(w, http.StatusCreated, map[string]any{"index": def, "timingMs": time.Since(start).Milliseconds()})
}

func (s *apiServer) listIndexes(w http.ResponseWriter, _ *http.Request) {
	start := time.Now()
	indexes := s.registry.List()
	respond(w, http.StatusOK, map[string]any{"indexes": indexes, "timingMs": time.Since(start).Milliseconds()})
}

func (s *apiServer) indexDocuments(w http.ResponseWriter, r *http.Request, name string) {
	start := time.Now()

	engine, ok := s.getEngine(name)
	if !ok {
		respondError(w, http.StatusNotFound, "index not found", start)
		return
	}

	var payload struct {
		Documents []map[string]any `json:"documents"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json payload", start)
		return
	}
	if len(payload.Documents) == 0 {
		respondError(w, http.StatusBadRequest, "no documents provided", start)
		return
	}

	indexed, segmentID, errs := engine.indexDocuments(payload.Documents, s.registry)
	status := http.StatusOK
	if indexed == 0 {
		status = http.StatusBadRequest
	}

	respond(w, status, map[string]any{
		"indexed":   indexed,
		"errors":    errs,
		"segmentId": segmentID,
		"timingMs":  time.Since(start).Milliseconds(),
	})
}

func (s *apiServer) indexStats(w http.ResponseWriter, _ *http.Request, name string) {
	start := time.Now()

	def, ok := s.registry.Get(name)
	if !ok {
		respondError(w, http.StatusNotFound, "index not found", start)
		return
	}

	respond(w, http.StatusOK, map[string]any{
		"name":     def.Name,
		"docCount": def.Metadata.DocCount,
		"segments": def.Metadata.Segments,
		"timingMs": time.Since(start).Milliseconds(),
	})
}

func (s *apiServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	indexName := r.URL.Query().Get("index")
	if indexName == "" {
		respondError(w, http.StatusBadRequest, "index parameter is required", start)
		return
	}

	query := r.URL.Query().Get("q")
	if query == "" {
		respondError(w, http.StatusBadRequest, "q parameter is required", start)
		return
	}

	engine, ok := s.getEngine(indexName)
	if !ok {
		respondError(w, http.StatusNotFound, "index not found", start)
		return
	}

	page, err := parseIntDefault(r.URL.Query().Get("page"), 1)
	if err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid page: %v", err), start)
		return
	}
	pageSize, err := parseIntDefault(r.URL.Query().Get("pageSize"), 10)
	if err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid pageSize: %v", err), start)
		return
	}

	req := index.SearchRequest{
		Query:    query,
		Page:     page,
		PageSize: pageSize,
		Filters:  r.URL.Query().Get("filters"),
	}

	resp := engine.search(req)

	respond(w, http.StatusOK, map[string]any{
		"index":     indexName,
		"query":     query,
		"totalHits": resp.TotalHits,
		"page":      resp.Page,
		"pageSize":  resp.PageSize,
		"results":   resp.Hits,
		"timingMs":  time.Since(start).Milliseconds(),
	})
}

func (s *apiServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	start := time.Now()
	respond(w, http.StatusOK, map[string]any{"status": "ok", "timingMs": time.Since(start).Milliseconds()})
}

func (s *apiServer) getEngine(name string) (*indexEngine, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	eng, ok := s.engines[name]
	return eng, ok
}

func respond(w http.ResponseWriter, status int, payload any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func respondError(w http.ResponseWriter, status int, message string, start time.Time) {
	respond(w, status, map[string]any{"error": message, "timingMs": time.Since(start).Milliseconds()})
}

func parseIntDefault(raw string, defaultVal int) (int, error) {
	if raw == "" {
		return defaultVal, nil
	}
	val, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	return val, nil
}

type indexEngine struct {
	def       index.Definition
	registry  *index.Registry
	tokenizer index.Tokenizer
	writer    *index.InMemoryIndex
	segments  []index.SegmentSnapshot

	mergeInterval  time.Duration
	mergeThreshold int
	mergeCh        chan struct{}
	stopCh         chan struct{}

	mu sync.RWMutex
}

func newIndexEngine(def index.Definition, registry *index.Registry) *indexEngine {
	tokenizer := index.NewSimpleTokenizer(nil)
	eng := &indexEngine{
		def:            def,
		registry:       registry,
		tokenizer:      tokenizer,
		writer:         index.NewInMemoryIndex(def, tokenizer, index.FlushThresholds{}),
		mergeInterval:  30 * time.Second,
		mergeThreshold: 4,
		mergeCh:        make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
	}
	go eng.mergeLoop()
	return eng
}

func (e *indexEngine) indexDocuments(docs []map[string]any, registry *index.Registry) (int, string, []string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var errs []string
	indexed := 0

	for i, doc := range docs {
		if err := e.writer.IndexDocument(doc); err != nil {
			errs = append(errs, fmt.Sprintf("doc %d: %v", i, err))
			continue
		}
		indexed++
	}

	if indexed == 0 {
		return 0, "", errs
	}

	snapshot := e.writer.Flush()
	e.segments = append(e.segments, snapshot)

	segmentID := fmt.Sprintf("seg-%d", time.Now().UnixNano())
	e.def.Metadata.DocCount += snapshot.Stats.TotalDocs
	e.def.Metadata.Segments = append(e.def.Metadata.Segments, index.SegmentMetadata{ID: segmentID, DocumentCount: snapshot.Stats.TotalDocs})

	if registry != nil {
		_ = registry.UpdateDefinition(e.def)
	}

	e.maybeScheduleMergeLocked()

	return indexed, segmentID, errs
}

func (e *indexEngine) maybeScheduleMergeLocked() {
	if len(e.segments) <= 1 {
		return
	}
	if len(e.segments) >= e.mergeThreshold {
		e.enqueueMerge()
		return
	}

	smallSegments := 0
	for _, seg := range e.segments {
		if seg.Stats.TotalDocs < 10 {
			smallSegments++
		}
	}
	if smallSegments >= 2 {
		e.enqueueMerge()
	}
}

func (e *indexEngine) enqueueMerge() {
	select {
	case e.mergeCh <- struct{}{}:
	default:
	}
}

func (e *indexEngine) mergeLoop() {
	ticker := time.NewTicker(e.mergeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.compactSegments()
		case <-e.mergeCh:
			e.compactSegments()
		case <-e.stopCh:
			return
		}
	}
}

func (e *indexEngine) compactSegments() {
	e.mu.RLock()
	segments := append([]index.SegmentSnapshot{}, e.segments...)
	def := e.def
	registry := e.registry
	tokenizer := e.tokenizer
	e.mu.RUnlock()

	if len(segments) <= 1 {
		return
	}

	merged := mergeSegments(def, tokenizer, segments)
	mergedID := fmt.Sprintf("merge-%d", time.Now().UnixNano())

	e.mu.Lock()
	defer e.mu.Unlock()
	// Re-check length after acquiring the write lock to avoid racing with a concurrent writer that already merged.
	if len(e.segments) <= 1 {
		return
	}

	e.segments = []index.SegmentSnapshot{merged}
	e.def.Metadata.DocCount = merged.Stats.TotalDocs
	e.def.Metadata.Segments = []index.SegmentMetadata{{ID: mergedID, DocumentCount: merged.Stats.TotalDocs}}

	if registry != nil {
		_ = registry.UpdateDefinition(e.def)
	}
}

func (e *indexEngine) search(req index.SearchRequest) index.SearchResponse {
	e.mu.RLock()
	segments := append([]index.SegmentSnapshot{}, e.segments...)
	def := e.def
	e.mu.RUnlock()

	snapshot := mergeSegments(def, e.tokenizer, segments)
	searcher := index.NewSearcher(def, snapshot, e.tokenizer)
	return searcher.Search(req)
}

func mergeSegments(def index.Definition, tokenizer index.Tokenizer, segments []index.SegmentSnapshot) index.SegmentSnapshot {
	merged := index.SegmentSnapshot{
		Postings: make(map[string][]index.Posting),
		Docs:     []map[string]any{},
		Stats: index.BM25Stats{
			AvgFieldLengths: make(map[string]float64),
		},
	}

	if len(segments) == 0 {
		return merged
	}

	liveDocs := make(map[string]map[string]any)
	deletedDocs := make(map[string]struct{})

	for _, seg := range segments {
		for _, doc := range seg.Docs {
			id, ok := doc["id"].(string)
			if !ok || id == "" {
				continue
			}

			if isDeletedDoc(doc) {
				delete(liveDocs, id)
				deletedDocs[id] = struct{}{}
				continue
			}

			delete(deletedDocs, id)
			liveDocs[id] = doc
		}
	}

	for _, seg := range segments {
		for term, postings := range seg.Postings {
			for _, p := range postings {
				if _, removed := deletedDocs[p.DocID]; removed {
					continue
				}
				if _, exists := liveDocs[p.DocID]; !exists {
					continue
				}
				merged.Postings[term] = append(merged.Postings[term], p)
			}
		}
	}

	for term, postings := range merged.Postings {
		sort.Slice(postings, func(i, j int) bool {
			return postings[i].DocID < postings[j].DocID
		})
		merged.Postings[term] = postings
	}

	for _, doc := range liveDocs {
		merged.Docs = append(merged.Docs, doc)
	}

	merged.Stats = rebuildStats(def, tokenizer, merged.Docs)

	return merged
}

func rebuildStats(def index.Definition, tokenizer index.Tokenizer, docs []map[string]any) index.BM25Stats {
	if tokenizer == nil {
		tokenizer = index.NewSimpleTokenizer(nil)
	}

	totals := make(map[string]int)

	for _, doc := range docs {
		for fieldName, fieldDef := range def.Fields {
			value, ok := doc[fieldName]
			if !ok {
				continue
			}

			switch fieldDef.Type {
			case index.FieldTypeText:
				totals[fieldName] += len(tokenizer.Tokenize(fmt.Sprint(value)))
			case index.FieldTypeKeyword:
				switch v := value.(type) {
				case string:
					if strings.TrimSpace(v) != "" {
						totals[fieldName]++
					}
				case []string:
					totals[fieldName] += len(v)
				}
			}
		}
	}

	stats := index.BM25Stats{TotalDocs: len(docs), AvgFieldLengths: make(map[string]float64)}
	if stats.TotalDocs == 0 {
		return stats
	}

	for field, total := range totals {
		stats.AvgFieldLengths[field] = float64(total) / float64(stats.TotalDocs)
	}

	return stats
}

func isDeletedDoc(doc map[string]any) bool {
	deleted, ok := doc["_deleted"]
	if !ok {
		return false
	}
	flag, ok := deleted.(bool)
	return ok && flag
}

func httpStatusForError(err error) int {
	if strings.Contains(err.Error(), "exists") {
		return http.StatusConflict
	}
	return http.StatusBadRequest
}
