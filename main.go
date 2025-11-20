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
		server.engines[def.Name] = newIndexEngine(def)
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
	s.engines[def.Name] = newIndexEngine(def)
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
	tokenizer index.Tokenizer
	writer    *index.InMemoryIndex
	segments  []index.SegmentSnapshot
	mu        sync.RWMutex
}

func newIndexEngine(def index.Definition) *indexEngine {
	tokenizer := index.NewSimpleTokenizer(nil)
	return &indexEngine{
		def:       def,
		tokenizer: tokenizer,
		writer:    index.NewInMemoryIndex(def, tokenizer, index.FlushThresholds{}),
	}
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

	return indexed, segmentID, errs
}

func (e *indexEngine) search(req index.SearchRequest) index.SearchResponse {
	e.mu.RLock()
	segments := append([]index.SegmentSnapshot{}, e.segments...)
	def := e.def
	e.mu.RUnlock()

	snapshot := mergeSegments(segments)
	searcher := index.NewSearcher(def, snapshot, e.tokenizer)
	return searcher.Search(req)
}

func mergeSegments(segments []index.SegmentSnapshot) index.SegmentSnapshot {
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

	fieldTotals := make(map[string]float64)
	docsByID := make(map[string]map[string]any)

	for _, seg := range segments {
		merged.Stats.TotalDocs += seg.Stats.TotalDocs
		for field, avg := range seg.Stats.AvgFieldLengths {
			fieldTotals[field] += avg * float64(seg.Stats.TotalDocs)
		}

		for term, postings := range seg.Postings {
			merged.Postings[term] = append(merged.Postings[term], postings...)
		}

		for _, doc := range seg.Docs {
			if id, ok := doc["id"].(string); ok {
				docsByID[id] = doc
			}
		}
	}

	if merged.Stats.TotalDocs > 0 {
		for field, total := range fieldTotals {
			merged.Stats.AvgFieldLengths[field] = total / float64(merged.Stats.TotalDocs)
		}
	}

	for _, postings := range merged.Postings {
		sort.Slice(postings, func(i, j int) bool {
			return postings[i].DocID < postings[j].DocID
		})
	}

	for _, doc := range docsByID {
		merged.Docs = append(merged.Docs, doc)
	}

	return merged
}

func httpStatusForError(err error) int {
	if strings.Contains(err.Error(), "exists") {
		return http.StatusConflict
	}
	return http.StatusBadRequest
}
