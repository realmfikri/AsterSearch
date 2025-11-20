import http from 'k6/http'
import { check, sleep } from 'k6'
import { Rate, Trend } from 'k6/metrics'
import { randomItem, randomSeed } from 'k6/utils'

const articleFixtures = JSON.parse(open('../data/articles.json'))
const searchQueries = articleFixtures.flatMap((doc) => [doc.title, doc.tags[0], doc.body.split(' ')[0]])

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080'
const INDEX_NAME = __ENV.INDEX_NAME || 'articles'
const INDEX_RPS = parseInt(__ENV.INDEX_RPS || '20', 10)
const SEARCH_RPS = parseInt(__ENV.SEARCH_RPS || '120', 10)
const INDEX_DURATION = __ENV.INDEX_DURATION || '1m'
const SEARCH_DURATION = __ENV.SEARCH_DURATION || '1m'
const INDEX_BATCH = parseInt(__ENV.INDEX_BATCH || '25', 10)
const HEADERS = { 'Content-Type': 'application/json' }

randomSeed(42)

export const options = {
  scenarios: {
    indexing: {
      executor: 'constant-arrival-rate',
      exec: 'indexDocuments',
      rate: INDEX_RPS,
      timeUnit: '1s',
      duration: INDEX_DURATION,
      preAllocatedVUs: 10,
      maxVUs: 50,
    },
    search: {
      executor: 'constant-arrival-rate',
      exec: 'searchQueries',
      startTime: '10s',
      rate: SEARCH_RPS,
      timeUnit: '1s',
      duration: SEARCH_DURATION,
      preAllocatedVUs: 20,
      maxVUs: 100,
    },
  },
  thresholds: {
    'http_req_duration{scenario:search}': ['p(50)<20', 'p(95)<100'],
    'http_req_duration{scenario:indexing}': ['p(95)<100'],
    search_rps: ['rate>100'],
  },
}

const searchRate = new Rate('search_rps')
const indexRate = new Rate('index_rps')
const searchLatency = new Trend('search_latency_ms')
const indexLatency = new Trend('index_latency_ms')
let docCounter = 0

export function setup() {
  createIndex()
  seedDocuments(articleFixtures)
  return { seeded: articleFixtures.length }
}

function createIndex() {
  const payload = {
    name: INDEX_NAME,
    tokenizer: 'standard_en',
    bm25: { k1: 1.4, b: 0.7 },
    fields: {
      title: { type: 'text', weight: 3 },
      body: { type: 'text', weight: 1 },
      tags: { type: 'keyword', weight: 1 },
      lang: { type: 'keyword', weight: 1 },
      views: { type: 'keyword', filterOnly: true },
    },
  }

  const res = http.post(`${BASE_URL}/v1/indexes`, JSON.stringify(payload), { headers: HEADERS })
  check(res, {
    'index created or exists': (r) => r.status === 201 || r.status === 409,
  })
}

function seedDocuments(docs) {
  for (let i = 0; i < docs.length; i += INDEX_BATCH) {
    const batch = docs.slice(i, i + INDEX_BATCH).map((doc) => ({ ...doc, id: `${doc.id}-seed-${i}` }))
    const res = http.post(
      `${BASE_URL}/v1/indexes/${INDEX_NAME}/documents`,
      JSON.stringify({ documents: batch }),
      { headers: HEADERS, tags: { seed: 'true' } },
    )
    check(res, {
      'seed batch accepted': (r) => r.status === 200,
    })
  }
}

function buildDocPayload(batchSize) {
  const docs = []
  for (let i = 0; i < batchSize; i++) {
    const base = randomItem(articleFixtures)
    docCounter += 1
    docs.push({
      ...base,
      id: `load-${__VU}-${docCounter}`,
      views: base.views + docCounter,
    })
  }
  return docs
}

export function indexDocuments() {
  const batch = buildDocPayload(INDEX_BATCH)
  const res = http.post(
    `${BASE_URL}/v1/indexes/${INDEX_NAME}/documents`,
    JSON.stringify({ documents: batch }),
    { headers: HEADERS, tags: { scenario: 'indexing' } },
  )

  indexRate.add(1)
  indexLatency.add(res.timings.duration)
  check(res, {
    'indexing ok': (r) => r.status === 200,
  })
  sleep(0.01)
}

export function searchQueries() {
  const query = randomItem(searchQueries)
  const res = http.get(
    `${BASE_URL}/v1/search?index=${INDEX_NAME}&q=${encodeURIComponent(query)}&pageSize=10`,
    { tags: { scenario: 'search' } },
  )

  searchRate.add(1)
  searchLatency.add(res.timings.duration)
  check(res, {
    'search ok': (r) => r.status === 200,
    'has hits': (r) => r.json('hits') !== undefined,
  })
  sleep(0.01)
}

export function handleSummary(data) {
  const summary = {
    scenarios: Object.keys(options.scenarios),
    metrics: {
      search_p50_ms: data.metrics['http_req_duration{scenario:search}']?.values['50'],
      search_p95_ms: data.metrics['http_req_duration{scenario:search}']?.values['95'],
      search_rps: data.metrics.search_rps?.rate,
      index_p95_ms: data.metrics['http_req_duration{scenario:indexing}']?.values['95'],
    },
    thresholds: data.thresholds,
  }

  return {
    'loadtest/results/summary.json': JSON.stringify(summary, null, 2),
    stdout: JSON.stringify(summary, null, 2),
  }
}
