package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

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

	server := &apiServer{registry: registry}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/indexes", server.handleIndexes)
	mux.HandleFunc("/v1/indexes/", server.handleIndexByName)

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

	name := strings.TrimPrefix(r.URL.Path, "/v1/indexes/")
	if name == "" {
		http.NotFound(w, r)
		return
	}

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
}

func (s *apiServer) createIndex(w http.ResponseWriter, r *http.Request) {
	var req index.CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json payload", http.StatusBadRequest)
		return
	}

	def, err := s.registry.Create(req)
	if err != nil {
		http.Error(w, err.Error(), httpStatusForError(err))
		return
	}

	respond(w, http.StatusCreated, def)
}

func (s *apiServer) listIndexes(w http.ResponseWriter, _ *http.Request) {
	indexes := s.registry.List()
	respond(w, http.StatusOK, map[string]any{"indexes": indexes})
}

func respond(w http.ResponseWriter, status int, payload any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func httpStatusForError(err error) int {
	if strings.Contains(err.Error(), "exists") {
		return http.StatusConflict
	}
	return http.StatusBadRequest
}
