package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"astersearch/internal/config"
	"astersearch/internal/index"
	"log/slog"
)

func TestAdminEndpointsRequireAuth(t *testing.T) {
	registry, engCfg := newTestRegistry(t)
	telemetry := newTelemetry(context.Background(), slog.Default(), false)
	srv := newAPIServer(registry, engCfg, telemetry, slog.Default(), config.SecurityConfig{AdminTokens: []string{"secret"}, RateLimit: config.RateLimitConfig{RequestsPerMin: 10}})
	ts := newTestHTTPServer(t, srv, telemetry)

	payload := []byte(`{"name":"new-index","fields":{"title":{"type":"text"}}}`)
	resp, err := http.Post(ts.URL+"/v1/indexes", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("POST /v1/indexes: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing admin token, got %d", resp.StatusCode)
	}
}

func TestIndexingRequiresAuthAndRateLimit(t *testing.T) {
	registry, engCfg := newTestRegistry(t)
	_, err := registry.Create(index.CreateRequest{Name: "docs", Fields: map[string]index.FieldDefinition{"title": {Type: index.FieldTypeText}}})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}

	telemetry := newTelemetry(context.Background(), slog.Default(), false)
	securityCfg := config.SecurityConfig{
		AdminTokens: []string{"admin-key"},
		IndexTokens: []string{"writer-key"},
		RateLimit:   config.RateLimitConfig{RequestsPerMin: 1},
	}
	srv := newAPIServer(registry, engCfg, telemetry, slog.Default(), securityCfg)
	ts := newTestHTTPServer(t, srv, telemetry)

	docPayload := map[string]any{"documents": []map[string]any{{"id": "1", "title": "hello"}}}
	body, _ := json.Marshal(docPayload)

	// Missing token should be rejected.
	resp, err := http.Post(ts.URL+"/v1/indexes/docs/documents", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("unauthorized post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", resp.StatusCode)
	}

	// First authorized request should succeed.
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/indexes/docs/documents", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer writer-key")
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("authorized post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for first authorized request, got %d", resp.StatusCode)
	}

	// Second request within the same window should hit the limiter.
	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/indexes/docs/documents", bytes.NewReader(body))
	req2.Header.Set("Authorization", "Bearer writer-key")
	req2.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("rate limited post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after exceeding limit, got %d", resp.StatusCode)
	}
}

func newTestRegistry(t *testing.T) (*index.Registry, indexEngineConfig) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	registry, err := index.NewRegistryWithDefaults(dir, index.CreateDefaults{Tokenizer: cfg.IndexDefaults.Tokenizer, BM25: index.BM25Parameters{K1: cfg.IndexDefaults.BM25.K1, B: cfg.IndexDefaults.BM25.B}})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	return registry, indexEngineConfig{
		mergeInterval:   cfg.IndexDefaults.MergeInterval,
		mergeThreshold:  cfg.IndexDefaults.MergeThreshold,
		flushThresholds: index.FlushThresholds{MaxDocuments: cfg.IndexDefaults.FlushMaxDocs, MaxPostings: cfg.IndexDefaults.FlushMaxPosts},
	}
}

func newTestHTTPServer(t *testing.T, api *apiServer, telemetry *telemetry) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/indexes", api.handleIndexes)
	mux.HandleFunc("/v1/indexes/", api.handleIndexByName)
	mux.HandleFunc("/v1/search", api.handleSearch)
	mux.HandleFunc("/v1/health", api.handleHealth)
	mux.HandleFunc("/v1/ready", api.handleReadiness)

	handler := withJSONHeaders(mux)
	handler = withTelemetry(handler, telemetry, false)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	return ts
}
