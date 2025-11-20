# AsterSearch

---

# Project: **AsterSearch** — Full-Text Search Engine (BM25 + Inverted Index)

## 1. What It Is

**AsterSearch** is a backend-centric, standalone search engine that indexes documents and serves ranked search results using a classic **inverted index + BM25** ranking model.

It’s designed as:

* A **library + HTTP service** you can embed or run standalone
* Focused on **developer use** (like a tiny self-hosted Elastic/Lucene)
* Optimized for **text-heavy documents**: articles, notes, blog posts, docs

---

## 2. Core Concepts

* **Index**: named collection of documents (e.g. `articles`, `notes`, `products`)
* **Document**: JSON object with a unique `id` and one or more **text fields**
* **Field**: named attribute (e.g. `title`, `body`, `tags`) with optional weights
* **Token**: normalized term after tokenization/stopword removal
* **Posting List**: list of `(docId, termFrequency, positions)` for a token
* **Segment**: immutable chunk of index on disk (allows incremental indexing + merge)

---

## 3. Inputs / Outputs

### 3.1 Inputs

1. **Indexing Input (HTTP API)**

   * Endpoint: `POST /v1/indexes/{indexName}/documents`
   * Body (JSON, batched):

     ```json
     {
       "documents": [
         {
           "id": "doc-123",
           "title": "Understanding BM25 for Search",
           "body": "BM25 is a ranking function used by search engines...",
           "tags": ["search", "ranking"]
         }
       ]
     }
     ```

2. **Search Input (HTTP API)**

   * Endpoint: `GET /v1/search`
   * Query params:

     * `index` (string, required) – which index to search
     * `q` (string, required) – user query, e.g. `"bm25 search ranking"`
     * `page` (int, default `1`)
     * `pageSize` (int, default `10`, max `100`)
     * `filters` (JSON or simple syntax, optional), e.g. `tags:search`

3. **Admin / Schema Input**

   * Endpoint: `POST /v1/indexes`
   * Body:

     ```json
     {
       "name": "articles",
       "fields": {
         "title": { "type": "text", "weight": 2.0 },
         "body": { "type": "text", "weight": 1.0 },
         "tags": { "type": "keyword", "filterOnly": true }
       },
       "tokenizer": "standard_en_id"
     }
     ```

---

### 3.2 Outputs

1. **Search Output**

   * Response:

     ```json
     {
       "index": "articles",
       "query": "bm25 search ranking",
       "totalHits": 178,
       "page": 1,
       "pageSize": 10,
       "results": [
         {
           "id": "doc-123",
           "score": 12.84,
           "highlights": {
             "title": "Understanding <em>BM25</em> for <em>Search</em>",
             "body": "... <em>BM25</em> is a ranking function used by many <em>search</em> engines ..."
           },
           "metadata": {
             "tags": ["search", "ranking"]
           }
         }
       ],
       "timingMs": 7
     }
     ```

2. **Indexing Output**

   * Response:

     ```json
     {
       "indexed": 1,
       "errors": [],
       "segmentId": "seg-2025-11-20T12:00:01Z"
     }
     ```

3. **Admin Output**

   * `GET /v1/indexes/{indexName}` → schema, stats, disk usage, doc count, segments

---

## 4. Features / High-Level Spec

### 4.1 Functional Features

* **Multiple named indexes** in one process
* **Schema-aware indexing**:

  * text fields: tokenized + BM25 scoring
  * keyword fields: exact match / filter / aggregation only
* **BM25 ranking**:

  * Tunable `k1`, `b` per index
  * Field weights (e.g. title > body)
* **Boolean queries**:

  * Default: AND between terms
  * Support `+must -must_not "phrases"` in the query string
* **Highlights/snippets**:

  * Term position tracking → highlight matched tokens
  * Snippet extraction around best-matching spans
* **Filters**:

  * Exact match filters (e.g. `tags:search`, `lang:en`)
  * Numeric range filters (e.g. `views > 1000`)
* **Pagination / ranked results**:

  * `offset/limit` or `page/pageSize`

### 4.2 Non-Functional Requirements

* **Latency**:

  * P50 < 20 ms for index with 100k docs
  * P95 < 100 ms for index with 1M docs
* **Throughput**:

  * > 100 queries/s on a single node (target)
* **Indexing**:

  * Support at least 1M docs on a single node
* **Durability**:

  * Write-ahead log (WAL) or append-only segment files
  * Crash-safe after WAL flush

---

## 5. Internal Architecture (Backend Focus)

### 5.1 Components

* **HTTP API Layer**

  * Validates requests
  * Translates to internal commands (IndexDocument, SearchQuery, etc.)

* **Index Manager**

  * Manages multiple indexes
  * Keeps in-memory registry: schema, BM25 params, segment list

* **Index Writer**

  * Tokenizes documents
  * Updates **in-memory posting lists**
  * Periodically flushes to **segment files** (immutable)
  * Maintains **term dictionary** + **doc store** (for highlights/snippets)

* **Searcher**

  * Parses query string → tokens + operators
  * Looks up posting lists
  * Applies BM25
  * Applies filters
  * Produces top-K results with scores

* **Storage Engine**

  * On disk:

    * `segments/seg-xxx.postings` (compressed posting lists)
    * `segments/seg-xxx.docs` (document store: original fields)
    * `segments/seg-xxx.meta` (headers, stats, doc count)
  * Optional:

    * memory-mapped files for fast access

* **Background Jobs**

  * **Segment merge** (compacts many small segments into fewer big ones)
  * **Index optimization** (rebuild stats, prune tombstone docs)

---

## 6. Data Structures (High Level)

* **Posting List Entry**:

  ```ts
  struct Posting {
    docId: u32;
    termFreq: u16;
    positions: Vec<u16>; // token positions in field
  }
  ```

* **Inverted Index**:

  ```ts
  Map<Term, Map<FieldName, Vec<Posting>>>
  ```

* **Document Store**:

  * Map `docId → serialized JSON` or binary encoded doc (for retrieval & highlights)

* **Index Metadata**:

  ```ts
  struct IndexStats {
    docCount: u64;
    avgFieldLength: Map<FieldName, f64>;
    totalDocsDeleted: u64;
  }
  ```

---

## 7. Technology & Implementation Notes

*(You can change this later, but this is a solid “high spec” default.)*

* **Language**: Go or Rust (for perf & systems feel)
* **Storage**:

  * Raw files on disk (your own layout) OR
  * BoltDB/Badger for key-value (term → postings location, docId → doc)
* **Config**:

  * TOML/YAML config for global defaults & per-index overrides
* **Deployment**:

  * Single binary `astersearch`
  * Runs as systemd service on VPS
  * Exposes HTTP on `:8080` behind Nginx/Caddy if needed

---

## 8. External Interfaces Summary

* **HTTP REST API** for:

  * Index creation/listing
  * Document indexing/updating/deleting
  * Search queries
  * Stats/health (`/v1/health`, `/v1/indexes/{name}/stats`)

* (Optional later) **gRPC** or **embedded library API**:

  * `Search(index, query)`
  * `IndexDocuments(index, docs)`

---

## 9. Configuration & Operations

### CLI & config files

* Defaults: listens on `:8080`, stores indexes in `data/indexes`, enables request logs + metrics.
* Flags: `--config` (TOML/YAML), `--listen`, `--index-path`.
* Environment: `ASTERSEARCH_INDEX_PATH` overrides the storage directory (kept for backward compatibility).
* Config examples live in `config/examples/config.toml` and `config/examples/config.yaml` and support per-index defaults (tokenizer, BM25 `k1/b`, merge interval/threshold, flush_max_documents/flush_max_postings) as well as logging/metrics toggles.

### Running the server directly

```bash
go run ./... --config ./config/examples/config.toml
# or override inline
go run ./... --listen :8080 --index-path ./data/indexes
```

### Observability hooks

* JSON request logging is on by default; disable via `logging.request_logs = false`.
* `/v1/metrics` returns basic counters (requests, errors, last status/latency) when enabled.
* Standard `/v1/health` endpoint is included for liveness probes.

### Load testing

The `loadtest/` directory provides a k6 scenario that drives `POST /v1/indexes/{index}/documents` and `GET /v1/search` with realistic payloads. Thresholds are pre-wired for the latency/QPS targets above; see `loadtest/README.md` for commands and expected outputs.

### Systemd unit (for :8080)

* Install the binary at `/usr/local/bin/astersearch` and copy `deploy/systemd/astersearch.service` + `deploy/systemd/astersearch.env` to `/etc/astersearch/`.
* Ensure the working directory (default `/var/lib/astersearch`) exists and is writable by the `astersearch` user.
* Reload + start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now astersearch.service
sudo journalctl -u astersearch -f
```

### Reverse proxy (optional)

For TLS or path normalization, place Nginx in front of the service on `:8080`:

```nginx
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

Keep `/v1/health` and `/v1/metrics` reachable for monitoring.



