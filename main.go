package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"astersearch/internal/config"
	"astersearch/internal/index"
)

func main() {
	cfg := config.DefaultConfig()

	configPath := flag.String("config", "", "Path to a TOML or YAML config file")
	listen := flag.String("listen", "", "Override the listen address (e.g. :8080)")
	indexPath := flag.String("index-path", "", "Override the index storage directory")
	flag.Parse()

	if *configPath != "" {
		loaded, err := config.Load(*configPath)
		if err != nil {
			log.Fatalf("failed to load config: %v", err)
		}
		cfg = loaded
	}

	if envPath := os.Getenv("ASTERSEARCH_INDEX_PATH"); envPath != "" {
		cfg.Paths.IndexDir = envPath
	}

	if *listen != "" {
		cfg.Server.Listen = *listen
	}
	if *indexPath != "" {
		cfg.Paths.IndexDir = *indexPath
	}

	bm25K1, bm25B := cfg.ToBM25()
	registry, err := index.NewRegistryWithDefaults(cfg.Paths.IndexDir, index.CreateDefaults{
		Tokenizer: cfg.IndexDefaults.Tokenizer,
		BM25:      index.BM25Parameters{K1: bm25K1, B: bm25B},
	})
	if err != nil {
		log.Fatalf("failed to initialize index registry: %v", err)
	}

	engineCfg := indexEngineConfig{
		mergeInterval:  cfg.IndexDefaults.MergeInterval,
		mergeThreshold: cfg.IndexDefaults.MergeThreshold,
		flushThresholds: index.FlushThresholds{
			MaxDocuments: cfg.IndexDefaults.FlushMaxDocs,
			MaxPostings:  cfg.IndexDefaults.FlushMaxPosts,
		},
	}

	telemetry := newTelemetry(cfg.Metrics.Enabled != nil && *cfg.Metrics.Enabled)
	server := newAPIServer(registry, engineCfg, telemetry)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/indexes", server.handleIndexes)
	mux.HandleFunc("/v1/indexes/", server.handleIndexByName)
	mux.HandleFunc("/v1/search", server.handleSearch)
	mux.HandleFunc("/v1/health", server.handleHealth)
	if telemetry.enabled {
		mux.HandleFunc("/v1/metrics", telemetry.handleMetrics)
	}

	handler := withJSONHeaders(mux)
	handler = withTelemetry(handler, telemetry, cfg.Logging.RequestLogs == nil || (cfg.Logging.RequestLogs != nil && *cfg.Logging.RequestLogs))

	log.Printf("AsterSearch API listening on %s (index path: %s)", cfg.Server.Listen, cfg.Paths.IndexDir)
	if err := http.ListenAndServe(cfg.Server.Listen, handler); err != nil {
		log.Fatal(err)
	}
}

func withJSONHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

type telemetry struct {
	enabled     bool
	reqCount    atomic.Int64
	errCount    atomic.Int64
	lastStatus  atomic.Int64
	lastLatency atomic.Int64
}

func newTelemetry(enabled bool) *telemetry {
	return &telemetry{enabled: enabled}
}

func (t *telemetry) record(status int, duration time.Duration) {
	if !t.enabled {
		return
	}
	t.reqCount.Add(1)
	t.lastStatus.Store(int64(status))
	t.lastLatency.Store(duration.Milliseconds())
	if status >= http.StatusBadRequest {
		t.errCount.Add(1)
	}
}

func (t *telemetry) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	if !t.enabled {
		respond(w, http.StatusOK, map[string]any{"enabled": false})
		return
	}

	snapshot := map[string]any{
		"enabled":         true,
		"requests":        t.reqCount.Load(),
		"errors":          t.errCount.Load(),
		"lastStatus":      t.lastStatus.Load(),
		"lastLatencyMs":   t.lastLatency.Load(),
		"uptimeTimestamp": time.Now().UTC().Format(time.RFC3339),
	}
	respond(w, http.StatusOK, snapshot)
}

func withTelemetry(next http.Handler, telemetry *telemetry, logRequests bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(recorder, r)
		duration := time.Since(start)

		if telemetry != nil {
			telemetry.record(recorder.status, duration)
		}
		if logRequests {
			log.Printf("%s %s -> %d (%s)", r.Method, r.URL.Path, recorder.status, duration)
		}
	})
}

type apiServer struct {
	registry *index.Registry

	engines   map[string]*indexEngine
	engineCfg indexEngineConfig
	telemetry *telemetry
	mu        sync.RWMutex
}

type indexEngineConfig struct {
	mergeInterval   time.Duration
	mergeThreshold  int
	flushThresholds index.FlushThresholds
}

func newAPIServer(registry *index.Registry, engCfg indexEngineConfig, telemetry *telemetry) *apiServer {
	server := &apiServer{
		registry:  registry,
		engines:   make(map[string]*indexEngine),
		engineCfg: engCfg,
		telemetry: telemetry,
	}

	for _, def := range registry.List() {
		server.engines[def.Name] = newIndexEngine(def, registry, engCfg)
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
	s.engines[def.Name] = newIndexEngine(def, s.registry, s.engineCfg)
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

	indexed, segmentIDs, errs := engine.indexDocuments(payload.Documents, s.registry)
	status := http.StatusOK
	if indexed == 0 {
		status = http.StatusBadRequest
	}

	segmentID := ""
	if len(segmentIDs) > 0 {
		segmentID = segmentIDs[len(segmentIDs)-1]
	}

	respond(w, status, map[string]any{
		"indexed":    indexed,
		"errors":     errs,
		"segmentId":  segmentID,
		"segmentIds": segmentIDs,
		"timingMs":   time.Since(start).Milliseconds(),
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

func newIndexEngine(def index.Definition, registry *index.Registry, cfg indexEngineConfig) *indexEngine {
	tokenizer := index.TokenizerFor(def.Tokenizer)
	mergeInterval := cfg.mergeInterval
	if mergeInterval == 0 {
		mergeInterval = 30 * time.Second
	}
	mergeThreshold := cfg.mergeThreshold
	if mergeThreshold == 0 {
		mergeThreshold = 4
	}

	eng := &indexEngine{
		def:            def,
		registry:       registry,
		tokenizer:      tokenizer,
		writer:         index.NewInMemoryIndex(def, tokenizer, cfg.flushThresholds),
		mergeInterval:  mergeInterval,
		mergeThreshold: mergeThreshold,
		mergeCh:        make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
	}
	go eng.mergeLoop()
	return eng
}

func (e *indexEngine) indexDocuments(docs []map[string]any, registry *index.Registry) (int, []string, []string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var errs []string
	indexed := 0
	var segmentIDs []string
	pendingWrite := false

	flush := func() {
		snapshot := e.writer.Flush()
		e.segments = append(e.segments, snapshot)

		segmentID := fmt.Sprintf("seg-%d-%d", time.Now().UnixNano(), len(segmentIDs))
		e.def.Metadata.DocCount += snapshot.Stats.TotalDocs
		e.def.Metadata.Segments = append(e.def.Metadata.Segments, index.SegmentMetadata{ID: segmentID, DocumentCount: snapshot.Stats.TotalDocs})
		segmentIDs = append(segmentIDs, segmentID)
		pendingWrite = false
	}

	for i, doc := range docs {
		if err := e.writer.IndexDocument(doc); err != nil {
			errs = append(errs, fmt.Sprintf("doc %d: %v", i, err))
			continue
		}
		indexed++

		pendingWrite = true
		if e.writer.ShouldFlush() {
			flush()
		}
	}

	if indexed == 0 {
		return 0, nil, errs
	}

	if pendingWrite {
		flush()
	}

	if registry != nil && len(segmentIDs) > 0 {
		_ = registry.UpdateDefinition(e.def)
	}

	e.maybeScheduleMergeLocked()

	return indexed, segmentIDs, errs
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
