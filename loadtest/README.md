# AsterSearch load testing

This folder contains a k6 scenario that stress tests the two hot paths:

* `POST /v1/indexes/{index}/documents`
* `GET /v1/search`

It targets the latency goals from the project README (P50 < 20 ms, P95 < 100 ms, and ~100 QPS for search) and writes a structured summary to `loadtest/results/summary.json`.

## Prerequisites

* A running AsterSearch server (for local runs: `go run ./... --config ./config/examples/config.toml`).
* [k6](https://k6.io/docs/get-started/installation/) installed locally.

## Running the scenarios

From the repository root:

```bash
# optional: tweak performance defaults (BM25, tokenizer, flush size) via config/examples/config.toml
# start the API server in a separate shell
# go run ./... --config ./config/examples/config.toml

# execute the load test
k6 run loadtest/k6/astersearch.js \
  -e BASE_URL=http://localhost:8080 \
  -e INDEX_NAME=articles \
  -e INDEX_RPS=25 \
  -e SEARCH_RPS=120 \
  -e INDEX_BATCH=32 \
  -e INDEX_DURATION=2m \
  -e SEARCH_DURATION=2m
```

The script will:

1. Create the index (idempotent) with `standard_en` tokenization and tuned BM25 defaults.
2. Seed a handful of realistic documents from `loadtest/data/articles.json`.
3. Run concurrent indexing and search scenarios with configurable request rates.
4. Enforce thresholds for search P50/P95 latency and QPS (see `options.thresholds`).
5. Emit metrics to `stdout` and to `loadtest/results/summary.json` for later inspection.

If thresholds are exceeded, k6 will exit with a non-zero code and the summary will include the failing checks to make retuning easy.

## Tuning notes

* BM25 defaults (k1=1.4, b=0.7) and `standard_en` tokenizer are chosen for balanced precision/recall under English-heavy content.
* Segment flush settings in `config/examples/config.{toml,yaml}` (`flush_max_documents`, `flush_max_postings`, `merge_interval`, `merge_threshold`) keep segments at a predictable size, which stabilizes search latency.
* For high-QPS search runs, prefer `SEARCH_RPS` ≥ 120 to match the `>100 queries/s` target.
