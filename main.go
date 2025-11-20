package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	prometheusotel "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	cfg := config.DefaultConfig()

	configPath := flag.String("config", "", "Path to a TOML or YAML config file")
	listen := flag.String("listen", "", "Override the listen address (e.g. :8080)")
	indexPath := flag.String("index-path", "", "Override the index storage directory")
	flag.Parse()

	if *configPath != "" {
		loaded, err := config.Load(*configPath)
		if err != nil {
			logger.Error("failed to load config", "error", err)
			os.Exit(1)
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
		logger.Error("failed to initialize index registry", "error", err)
		os.Exit(1)
	}

	engineCfg := indexEngineConfig{
		mergeInterval:  cfg.IndexDefaults.MergeInterval,
		mergeThreshold: cfg.IndexDefaults.MergeThreshold,
		flushThresholds: index.FlushThresholds{
			MaxDocuments: cfg.IndexDefaults.FlushMaxDocs,
			MaxPostings:  cfg.IndexDefaults.FlushMaxPosts,
		},
	}

	telemetry := newTelemetry(ctx, logger, cfg.Metrics.Enabled != nil && *cfg.Metrics.Enabled)
	server := newAPIServer(registry, engineCfg, telemetry, logger)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/indexes", server.handleIndexes)
	mux.HandleFunc("/v1/indexes/", server.handleIndexByName)
	mux.HandleFunc("/v1/search", server.handleSearch)
	mux.HandleFunc("/v1/health", server.handleHealth)
	mux.HandleFunc("/v1/ready", server.handleReadiness)
	if telemetry.enabled {
		mux.HandleFunc("/v1/metrics", telemetry.handleMetrics)
	}

	handler := withJSONHeaders(mux)
	handler = withTelemetry(handler, telemetry, cfg.Logging.RequestLogs == nil || (cfg.Logging.RequestLogs != nil && *cfg.Logging.RequestLogs))

	logger.Info("AsterSearch API listening", "listen", cfg.Server.Listen, "indexPath", cfg.Paths.IndexDir)
	if err := http.ListenAndServe(cfg.Server.Listen, handler); err != nil {
		logger.Error("server stopped", "error", err)
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
	enabled bool
	logger  *slog.Logger

	registry       *prometheus.Registry
	metricsHandler http.Handler
	meter          metric.Meter

	reqCount    atomic.Int64
	errCount    atomic.Int64
	lastStatus  atomic.Int64
	lastLatency atomic.Int64

	httpRequests  metric.Int64Counter
	httpErrors    metric.Int64Counter
	httpLatency   metric.Float64Histogram
	indexDocs     metric.Int64Counter
	indexLatency  metric.Float64Histogram
	searchOps     metric.Int64Counter
	searchLatency metric.Float64Histogram

	segmentGauge *prometheus.GaugeVec
	walGauge     *prometheus.GaugeVec
}

func newTelemetry(ctx context.Context, logger *slog.Logger, enabled bool) *telemetry {
	telemetry := &telemetry{enabled: enabled, logger: logger}
	if !enabled {
		return telemetry
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	exporter, err := prometheusotel.New(prometheusotel.WithRegisterer(registry))
	if err != nil {
		logger.Error("failed to initialize prometheus exporter", "error", err)
		return telemetry
	}

	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	otel.SetMeterProvider(provider)
	meter := provider.Meter("astersearch")

	httpReq, _ := meter.Int64Counter("http_requests_total", metric.WithDescription("Total HTTP requests"))
	httpErr, _ := meter.Int64Counter("http_errors_total", metric.WithDescription("HTTP requests that returned an error status"))
	httpLatency, _ := meter.Float64Histogram("http_request_duration_ms", metric.WithDescription("Latency of HTTP requests in milliseconds"), metric.WithUnit("ms"))
	indexDocs, _ := meter.Int64Counter("index_documents_total", metric.WithDescription("Documents processed by the indexing pipeline"))
	indexLatency, _ := meter.Float64Histogram("index_latency_ms", metric.WithDescription("Latency of index mutations"), metric.WithUnit("ms"))
	searchOps, _ := meter.Int64Counter("search_requests_total", metric.WithDescription("Search operations executed"))
	searchLatency, _ := meter.Float64Histogram("search_latency_ms", metric.WithDescription("Latency of search operations"), metric.WithUnit("ms"))

	segmentGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "astersearch", Name: "segments", Help: "Segments currently tracked per index"}, []string{"index"})
	walGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "astersearch", Name: "wal_offset_bytes", Help: "Last recorded WAL offset"}, []string{"index"})
	registry.MustRegister(segmentGauge, walGauge)

	telemetry.registry = registry
	telemetry.metricsHandler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	telemetry.meter = meter
	telemetry.httpRequests = httpReq
	telemetry.httpErrors = httpErr
	telemetry.httpLatency = httpLatency
	telemetry.indexDocs = indexDocs
	telemetry.indexLatency = indexLatency
	telemetry.searchOps = searchOps
	telemetry.searchLatency = searchLatency
	telemetry.segmentGauge = segmentGauge
	telemetry.walGauge = walGauge

	telemetry.logger.Info("telemetry initialized", "prometheus", true)
	telemetry.httpRequests.Add(ctx, 0) // ensure metric is created eagerly
	return telemetry
}

func (t *telemetry) recordRequest(ctx context.Context, method, path string, status int, duration time.Duration) {
	if !t.enabled {
		return
	}

	attrs := metric.WithAttributes(
		attribute.String("method", method),
		attribute.String("path", path),
		attribute.Int("status", status),
	)
	t.httpRequests.Add(ctx, 1, attrs)
	t.httpLatency.Record(ctx, float64(duration.Milliseconds()), attrs)
	if status >= http.StatusBadRequest {
		t.httpErrors.Add(ctx, 1, attrs)
	}

	t.reqCount.Add(1)
	t.lastStatus.Store(int64(status))
	t.lastLatency.Store(duration.Milliseconds())
	if status >= http.StatusBadRequest {
		t.errCount.Add(1)
	}
}

func (t *telemetry) recordIndexing(ctx context.Context, indexName string, documents int, segments int, errs int, duration time.Duration) {
	if !t.enabled {
		return
	}

	attrs := metric.WithAttributes(attribute.String("index", indexName))
	t.indexDocs.Add(ctx, int64(documents), attrs)
	t.indexLatency.Record(ctx, float64(duration.Milliseconds()), attrs)
	t.segmentGauge.WithLabelValues(indexName).Set(float64(segments))
	if errs > 0 {
		t.httpErrors.Add(ctx, int64(errs), attrs)
	}
}

func (t *telemetry) recordSearch(ctx context.Context, indexName string, hits int, duration time.Duration) {
	if !t.enabled {
		return
	}

	attrs := metric.WithAttributes(attribute.String("index", indexName))
	t.searchOps.Add(ctx, 1, attrs)
	t.searchLatency.Record(ctx, float64(duration.Milliseconds()), attrs)
}

func (t *telemetry) observeWAL(indexName string, offset int64) {
	if !t.enabled {
		return
	}

	t.walGauge.WithLabelValues(indexName).Set(float64(offset))
}

func (t *telemetry) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if !t.enabled || t.registry == nil {
		respond(w, http.StatusOK, map[string]any{"enabled": false})
		return
	}

	t.metricsHandler.ServeHTTP(w, r)
}

func withTelemetry(next http.Handler, telemetry *telemetry, logRequests bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(recorder, r)
		duration := time.Since(start)

		if telemetry != nil {
			telemetry.recordRequest(r.Context(), r.Method, r.URL.Path, recorder.status, duration)
		}
		if logRequests && telemetry != nil && telemetry.logger != nil {
			telemetry.logger.Info("request completed", "method", r.Method, "path", r.URL.Path, "status", recorder.status, "duration_ms", duration.Milliseconds())
		}
	})
}

type apiServer struct {
	registry *index.Registry

	engines   map[string]*indexEngine
	engineCfg indexEngineConfig
	telemetry *telemetry
	logger    *slog.Logger
	mu        sync.RWMutex
	ready     atomic.Bool
}

type indexEngineConfig struct {
	mergeInterval   time.Duration
	mergeThreshold  int
	flushThresholds index.FlushThresholds
}

func newAPIServer(registry *index.Registry, engCfg indexEngineConfig, telemetry *telemetry, logger *slog.Logger) *apiServer {
	server := &apiServer{
		registry:  registry,
		engines:   make(map[string]*indexEngine),
		engineCfg: engCfg,
		telemetry: telemetry,
		logger:    logger,
	}

	for _, def := range registry.List() {
		server.engines[def.Name] = newIndexEngine(def, registry, engCfg, telemetry, logger)
	}
	server.ready.Store(true)

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
	s.engines[def.Name] = newIndexEngine(def, s.registry, s.engineCfg, s.telemetry, s.logger)
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
	ctx := r.Context()

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

	indexed, segmentIDs, errs := engine.indexDocuments(ctx, payload.Documents, s.registry)
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

	if s.logger != nil {
		s.logger.Info("index request processed", "index", name, "documents", len(payload.Documents), "indexed", indexed, "status", status, "duration_ms", time.Since(start).Milliseconds())
	}
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

	resp := engine.search(r.Context(), req)

	respond(w, http.StatusOK, map[string]any{
		"index":     indexName,
		"query":     query,
		"totalHits": resp.TotalHits,
		"page":      resp.Page,
		"pageSize":  resp.PageSize,
		"results":   resp.Hits,
		"timingMs":  time.Since(start).Milliseconds(),
	})

	if s.logger != nil {
		s.logger.Info("search completed", "index", indexName, "query", query, "hits", resp.TotalHits, "duration_ms", time.Since(start).Milliseconds())
	}
}

func (s *apiServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	start := time.Now()
	respond(w, http.StatusOK, map[string]any{"status": "ok", "timingMs": time.Since(start).Milliseconds()})
}

func (s *apiServer) handleReadiness(w http.ResponseWriter, _ *http.Request) {
	start := time.Now()
	ready := s.ready.Load()
	engineCount := len(s.engines)
	if ready && engineCount != len(s.registry.List()) {
		ready = false
	}

	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}

	respond(w, status, map[string]any{
		"status":     map[bool]string{true: "ready", false: "initializing"}[ready],
		"engines":    engineCount,
		"timingMs":   time.Since(start).Milliseconds(),
		"registry":   len(s.registry.List()),
		"lastStatus": status,
	})
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
	telemetry *telemetry
	logger    *slog.Logger

	mergeInterval  time.Duration
	mergeThreshold int
	mergeCh        chan struct{}
	stopCh         chan struct{}

	mu sync.RWMutex
}

func newIndexEngine(def index.Definition, registry *index.Registry, cfg indexEngineConfig, telemetry *telemetry, logger *slog.Logger) *indexEngine {
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
		telemetry:      telemetry,
		logger:         logger,
		mergeInterval:  mergeInterval,
		mergeThreshold: mergeThreshold,
		mergeCh:        make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
	}
	go eng.mergeLoop()
	return eng
}

func (e *indexEngine) indexDocuments(ctx context.Context, docs []map[string]any, registry *index.Registry) (int, []string, []string) {
	start := time.Now()
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

	if e.telemetry != nil {
		e.telemetry.recordIndexing(ctx, e.def.Name, indexed, len(e.segments), len(errs), time.Since(start))
	}
	if e.logger != nil {
		e.logger.Info("indexed documents", "index", e.def.Name, "documents", len(docs), "indexed", indexed, "errors", len(errs), "segments", len(e.segments), "duration_ms", time.Since(start).Milliseconds())
	}

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
	start := time.Now()

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

	if e.telemetry != nil {
		e.telemetry.recordIndexing(context.Background(), e.def.Name, merged.Stats.TotalDocs, len(e.segments), 0, time.Since(start))
	}
	if e.logger != nil {
		e.logger.Info("segments compacted", "index", e.def.Name, "mergedSegments", len(segments), "docCount", merged.Stats.TotalDocs, "duration_ms", time.Since(start).Milliseconds())
	}
}

func (e *indexEngine) search(ctx context.Context, req index.SearchRequest) index.SearchResponse {
	start := time.Now()
	e.mu.RLock()
	segments := append([]index.SegmentSnapshot{}, e.segments...)
	def := e.def
	e.mu.RUnlock()

	snapshot := mergeSegments(def, e.tokenizer, segments)
	searcher := index.NewSearcher(def, snapshot, e.tokenizer)
	resp := searcher.Search(req)

	if e.telemetry != nil {
		e.telemetry.recordSearch(ctx, e.def.Name, resp.TotalHits, time.Since(start))
	}
	if e.logger != nil {
		e.logger.Info("search pipeline executed", "index", e.def.Name, "hits", resp.TotalHits, "duration_ms", time.Since(start).Milliseconds())
	}
	return resp
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
