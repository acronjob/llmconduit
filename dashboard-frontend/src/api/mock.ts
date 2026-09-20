/**
 * In-browser mock backend — lets all four views ship before the Rust contract is live.
 *
 * Provides:
 *  - `mockFetch`: a `fetch`-compatible function answering the D13 REST routes + D7 auth.
 *  - `MockWebSocket` + `mockWsFactory`: a WS that emits a snapshot then a live stream of
 *    batched `DashboardFrame`s, INCLUDING a multi-message `Monitor` frame (sibling
 *    `DebugWsMessage`s under one seq) — the `DebugUpdate`-equivalent the dedup test asserts.
 *
 * A dev flag (`isMockEnabled`) selects mock vs. real (see ../config/env.ts).
 */
import type {
  AuthApiKey,
  AuthAuditEvent,
  AuthGroup,
  AuthPolicy,
  AuthPricingRow,
  AuthRole,
  AuthSession,
  AuthUsageRow,
  AuthUser,
  CatalogEntry,
  DashboardFrame,
  DebugWsMessage,
  FlowDetail,
  FlowSummary,
  FlowsResponse,
  MetricsResponse,
  MonitorPayload,
  ProviderCacheMetrics,
  ProviderHealth,
  ProviderInventoryEntry,
  ProviderLatency,
  SnapshotFrame,
  SnapshotResponse,
  TopologyResponse,
  WsServerMessage,
} from './types';
import type { WsLike } from './ws';

const MOCK_CSRF = 'mock-csrf-token';

let AUTH_USERS: AuthUser[] = [
  { id: 'usr_ops', kind: 'user', display_name: 'Operations', enabled: true, created_at: '2026-06-01T09:00:00Z' },
  { id: 'usr_batch', kind: 'service_account', display_name: 'Batch inference', enabled: true, created_at: '2026-06-02T12:00:00Z' },
];
let AUTH_GROUPS: AuthGroup[] = [
  { id: 'grp_prod', name: 'Production', enabled: true, member_count: 2 },
];
let AUTH_ROLES: AuthRole[] = [
  { id: 'role_operator', name: 'Operator', enabled: true, permissions: ['auth.keys.read', 'auth.keys.create', 'auth.usage.read', 'auth.sessions.read'] },
];
let AUTH_POLICIES: AuthPolicy[] = [
  { id: 'pol_prod', name: 'Production models', effect: 'allow', enabled: true, subjects: ['grp_prod'], endpoints: ['responses', 'chat'], models: [], requested_models: ['gpt-*'], served_models: ['gpt-4.1'], providers: ['openai'], routes: ['cloud'], time_windows: [{ weekday_mask: 31, start_minute: 480, end_minute: 1080, absolute_start_ms: null, absolute_end_ms: null }], max_concurrent_sessions: 4, max_daily_session_starts: 100, management_permissions: [] },
  { id: 'pol_deny_local', name: 'Block local fallback', effect: 'deny', enabled: true, subjects: ['grp_prod'], endpoints: ['*'], models: [], requested_models: ['*'], served_models: ['*'], providers: ['vllm-b'], routes: ['local'], time_windows: [], max_concurrent_sessions: null, max_daily_session_starts: null, management_permissions: [] },
];
let AUTH_KEYS: AuthApiKey[] = [
  { id: 'key_ops', principal_id: 'usr_ops', name: 'operator laptop', prefix: 'llmc_7ad2', enabled: true, created_at: '2026-06-03T10:00:00Z', expires_at: null, last_used_at: '2026-06-21T14:19:40Z' },
];
let AUTH_SESSIONS: AuthSession[] = [
  { id: 'sess_admin', kind: 'dashboard', principal_id: 'usr_ops', key_id: 'key_ops', endpoint: null, requested_model: null, started_at: '2026-06-21T14:00:00Z', expires_at: '2026-06-21T22:00:00Z' },
  { id: 'sess_infer', kind: 'inference', principal_id: 'usr_batch', key_id: 'key_batch', endpoint: 'responses', requested_model: 'gpt-4.1', started_at: '2026-06-21T14:19:57Z', expires_at: null },
];
const AUTH_USAGE: AuthUsageRow[] = [
  { dimension: 'key', value: 'key_ops', requests: 142, prompt_tokens: 82120, completion_tokens: 11940, cached_tokens: 30220, reasoning_tokens: null, cost: 1.8241, cost_confidence: 'confident' },
  { dimension: 'group', value: 'grp_prod', requests: 18, prompt_tokens: null, completion_tokens: null, cached_tokens: null, reasoning_tokens: null, cost: null, cost_confidence: 'unavailable' },
];
const AUTH_AUDIT: AuthAuditEvent[] = [
  { id: 'audit_001', timestamp: '2026-06-21T14:18:00Z', actor: 'usr_ops', action: 'api_key.create', target: 'key_ops', outcome: 'ok', metadata: { prefix: 'llmc_7ad2' } },
  { id: 'audit_002', timestamp: '2026-06-21T14:19:00Z', actor: 'key_ops', action: 'inference.authorize', target: 'vllm-b', outcome: 'denied', metadata: { policy: 'pol_deny_local' } },
];
const AUTH_PRICING: AuthPricingRow[] = [
  { model: 'gpt-4.1', provider: 'openai', source: 'operator', fetched_at: '2026-06-20T00:00:00Z', input_per_1k: '0.002', output_per_1k: '0.008', confidence: 'confident' },
];

// ---------------------------------------------------------------------------
// Seed data — shapes mirror the REAL Rust DTOs (D1 FlowSummary, D4 ProviderHealth).
// ---------------------------------------------------------------------------

const NODES: ProviderHealth[] = [
  { id: 'vllm-a', name: 'vllm-a (8001)', route: null, base_url: 'http://localhost:8001', status: 'healthy', cooling_until_ms: null, last_error: null, served_count: 1280, failover_count: 0, consecutive_failures: 0, catalog_fetched_ms: Date.now() - 5000, catalog_size: 12 },
  { id: 'vllm-b', name: 'vllm-b (8002)', route: null, base_url: 'http://localhost:8002', status: 'cooling', cooling_until_ms: Date.now() + 8000, last_error: 'connection refused', served_count: 610, failover_count: 3, consecutive_failures: 2, catalog_fetched_ms: Date.now() - 9000, catalog_size: 8 },
  { id: 'openai', name: 'openai-proxy', route: 'cloud', base_url: 'https://api.openai.com', status: 'down', cooling_until_ms: Date.now() + 30000, last_error: '503 upstream', served_count: 42, failover_count: 9, consecutive_failures: 7, catalog_fetched_ms: null, catalog_size: 0 },
];

// Gap 12/13: per-provider latency + error distribution, keyed by node id. Present ONLY on the REST
// `/topology` + `/snapshot` nodes (the WS `topology_update` frame STRIPS it — `topologyFrame`
// below — mirroring the Rust `from_health` leaving `per_provider: None` on the live WS path).
// Exercises the three node/tile states the gap-13 e2e asserts:
//  - vllm-a (healthy): all-served ⇒ a MEASURED `error_rate: 0` (a real `0%`, DISTINCT from absent),
//    no error distribution. Node emphasis: nominal (base size, no ring).
//  - vllm-b (cooling): a DEGRADING provider — elevated error rate (16%) with a per-class
//    distribution (connect + timeout). Node emphasis: degrading (enlarged + an error ring).
//  - openai (down): ABSENT (no in-window samples) ⇒ the tile renders `—` and the node is NEUTRAL —
//    NOT a 0-sized or falsely-healthy node (don't-lie-with-zeros). Omitted from this map.
const PER_PROVIDER: Record<string, ProviderLatency> = {
  'vllm-a': {
    provider: 'vllm-a', data_quality: 'derived', samples: 248, served: 248, failed: 0,
    p50: 88, p95: 210, p99: 320, error_rate: 0, errors: {},
  },
  'vllm-b': {
    provider: 'vllm-b', data_quality: 'derived', samples: 75, served: 63, failed: 12,
    p50: 240, p95: 1100, p99: 2400, error_rate: 16, errors: { connect: 7, timeout: 5 },
  },
  // openai: intentionally ABSENT (zero in-window samples → unavailable tile + neutral node).
};

const PRICE_TABLE: TopologyResponse['price_table'] = {
  // Gap 07: gpt-4o has a CONFIGURED cache rate (presence true) → cached charges are confident.
  'gpt-4o': { input_per_1k: 0.005, output_per_1k: 0.015, cached_per_1k: 0.0025, cached_price_configured: true },
  // llama has NO configured cache rate (presence false, numeric defaults to 0) → a flow that
  // bills cached tokens on it is `estimated`, not `confident` (exercises the labelling path).
  'llama-3.1-70b': { input_per_1k: 0.0008, output_per_1k: 0.0008, cached_per_1k: 0, cached_price_configured: false },
};

const CATALOG: CatalogEntry[] = [
  { id: 'gpt-4o', context_limit: 128000 },
  { id: 'llama-3.1-70b', context_limit: 131072 },
  { id: 'qwen2.5-coder-32b', context_limit: 32768 },
  // gap 06: a model whose upstream advertises NO window ⇒ `null` (unavailable),
  // distinct from a real `0`. Renderers show `—`, never `0`.
  { id: 'mystery-model', context_limit: null },
];

const PROVIDERS: ProviderInventoryEntry[] = [
  {
    provider_id: 'vllm-a',
    provider_name: 'vllm-a',
    resource_id: 'gpu-a',
    route: null,
    base_url: 'http://localhost:8001',
    models: [
      { id: 'llama-3.1-70b', context_limit: 131072 },
      { id: 'qwen2.5-coder-32b', context_limit: 32768 },
    ],
    availability: {
      timezone: 'America/Chicago',
      default_capacity: 0,
      weekly: [{ days: ['monday', 'tuesday', 'wednesday', 'thursday', 'friday'], start_local: '08:00', end_local: '18:00', capacity: 8 }],
      exceptions: [],
    },
    capacity_limit: 8,
    active_requests: 2,
    accepting_requests: true,
    healthy: true,
  },
  {
    provider_id: 'vllm-b',
    provider_name: 'vllm-b',
    resource_id: 'gpu-b',
    route: null,
    base_url: 'http://localhost:8002',
    models: [
      { id: 'gpt-4o', context_limit: 128000 },
      { id: 'mystery-model', context_limit: null },
    ],
    availability: null,
    capacity_limit: 4,
    active_requests: 4,
    accepting_requests: false,
    healthy: false,
  },
  {
    provider_id: 'openai',
    provider_name: 'openai-proxy',
    resource_id: null,
    route: 'cloud',
    base_url: 'https://api.openai.com',
    models: [{ id: 'gpt-4o', context_limit: 128000 }],
    availability: null,
    capacity_limit: null,
    active_requests: null,
    accepting_requests: false,
    healthy: false,
  },
];

const PROVIDER_METRICS: ProviderCacheMetrics[] = [
  { provider: 'vllm-a', source: 'vllm', fetched_at_ms: Date.now() - 4000, cache_hits: 820, cache_queries: 1200, cache_hit_rate: 0.683, kv_cache_usage: 0.42, data_quality: 'derived' },
  { provider: 'vllm-b', source: 'sglang', fetched_at_ms: Date.now() - 9000, cache_hits: null, cache_queries: null, cache_hit_rate: 0.31, kv_cache_usage: 0.91, data_quality: 'derived' },
];

/**
 * Gap 15 review round 3 — a ~4 KiB User-Agent `client_label` injected ONLY when `?longclient=1` is in
 * the URL (an opt-in e2e seam; absent for every other test so seeds + the flows screenshot are
 * untouched). It proves the filter-bar chip BOUNDS an unbounded label in REAL layout (the long chip's
 * label span clips — `scrollWidth > clientWidth` — while the chip button width stays bounded).
 */
function longClientEnabled(): boolean {
  return typeof window !== 'undefined' && new URLSearchParams(window.location.search).get('longclient') === '1';
}

function seedFlows(): FlowSummary[] {
  const now = Date.now();
  const flows: FlowSummary[] = [
    // Gap 07: llama has cached tokens (128) but NO configured cache rate ⇒ cost is ESTIMATED
    // (labelled as such in the UI), not confident.
    // Gap 10: FULL phase spine + a served attempt with a wire first byte (open, still streaming) ⇒
    // the latency breakdown reads a MEASURED TTFT (first_content_delta) + wire TTFB + every segment.
    {
      api_call_id: 'api_001', response_id: 'resp_001', method: 'POST', uri: '/v1/responses', status: 'open',
      model_requested: 'gpt-4o', model_served: 'llama-3.1-70b', upstream_target: 'vllm-a',
      usage: { prompt: 812, completion: 240, total: 1052, cached: 128, reasoning: 0 },
      started_ms: now - 2400, finished_ms: null, elapsed_ms: 2400, terminal_reason: null,
      cost: 0.0061, cost_confidence: 'estimated',
      // Gap 15: a STRONG key-hash client (a non-reversible `key-<hex>` digest — NOT a raw key). Shared
      // with api_002 below so the "by client" roll-up groups two flows under one stable identity.
      client_label: 'key-9f3a1c0b2d4e', client_source: 'key_hash',
      // ingress → +30ms normalize → +20ms routing → +220ms wire TTFB → +180ms first content
      // (still streaming: no stream_end/finalize yet ⇒ generation/finalize segments unavailable).
      ingress_ms: now - 2400, normalization_done_ms: now - 2370, routing_decision_ms: now - 2350,
      first_content_delta_ms: now - 1950, first_upstream_byte_ms: now - 2130,
      attempts: [
        { provider: 'vllm-a', model: 'llama-3.1-70b', start_ms: now - 2350, end_ms: now - 2130, first_upstream_byte_ms: now - 2130, status: 'served' },
      ],
    },
    // Gap 07: cached/reasoning UNREPORTED (absent ⇒ renders `—`, never `0`); unpriced cache ⇒ estimated.
    // Gap 10: a COMPLETED flow with the full phase spine ⇒ every segment measured, tok/s derived.
    {
      api_call_id: 'api_002', response_id: 'resp_002', method: 'POST', uri: '/v1/chat/completions', status: 'completed',
      model_requested: 'llama-3.1-70b', model_served: 'llama-3.1-70b', upstream_target: 'vllm-a',
      usage: { prompt: 1500, completion: 980, total: 2480 },
      started_ms: now - 12000, finished_ms: now - 7800, elapsed_ms: 4200, terminal_reason: 'response.completed',
      cost: 0.0019, cost_confidence: 'estimated',
      // Gap 15: SAME key-hash as api_001 ⇒ the roll-up rolls both flows up under one client (2 flows,
      // priced cost summed, mean latency derived) — proving cost/latency aggregation by client.
      client_label: 'key-9f3a1c0b2d4e', client_source: 'key_hash',
      ingress_ms: now - 12000, normalization_done_ms: now - 11960, routing_decision_ms: now - 11940,
      first_upstream_byte_ms: now - 11700, first_content_delta_ms: now - 11500,
      stream_end_ms: now - 7820, finalize_ms: now - 7800,
      attempts: [
        { provider: 'vllm-a', model: 'llama-3.1-70b', start_ms: now - 11940, end_ms: now - 11700, first_upstream_byte_ms: now - 11700, status: 'served' },
      ],
    },
    // Gap 07: no usage + a failed call ⇒ cost null ⇒ cost_confidence UNAVAILABLE (renders `—`).
    // Gap 10: ERRORED BEFORE CONTENT — ingress/normalize/routing/finalize stamped, but NO
    // first_content_delta_ms / stream_end_ms ⇒ the prefill + generation segments are UNAVAILABLE
    // (`—`, never 0ms); the attempt FAILED pre-headers ⇒ no wire TTFB. TTFT/tok/s ⇒ `—`.
    {
      api_call_id: 'api_003', response_id: null, method: 'POST', uri: '/v1/responses', status: 'failed',
      model_requested: 'gpt-4o', model_served: 'gpt-4o', upstream_target: 'openai',
      usage: null, started_ms: now - 30000, finished_ms: now - 29200, elapsed_ms: 800,
      terminal_reason: 'upstream 503', cost: null, cost_confidence: 'unavailable',
      // Gap 15: a STRONG configured caller-id (an operator-configured `x-client-id` header value) —
      // an explicit, caller-asserted identity. This client's only flow FAILED ⇒ a derived 100% err rate
      // in the roll-up, and an UNPRICED cost ⇒ `—` (don't-lie-with-zeros, never a fabricated `$0.00`).
      client_label: 'svc-checkout', client_source: 'configured_header',
      ingress_ms: now - 30000, normalization_done_ms: now - 29970, routing_decision_ms: now - 29950,
      finalize_ms: now - 29200,
      attempts: [
        { provider: 'openai', model: 'gpt-4o', start_ms: now - 29950, end_ms: now - 29200, status: 'failed', error_class: 'http_status', failover_reason: 'terminal_no_failover' },
      ],
    },
    // Gap 09: served by a model whose upstream advertises NO context window (`mystery-model` ⇒
    // catalog context_limit null) but WITH reported usage. The context gauge must render `—`
    // (UNKNOWN capacity), NEVER a fabricated 0%/100% — distinct from a flow on a known-window model.
    // Gap 10: full phases but NO attempts/first_upstream_byte ⇒ the upstream-wait segment is
    // unavailable and the prefill segment is a SEPARATELY-LABELLED `derived` routing→first-token
    // span (the no-TTFB path — never a measured prefill, since the wire first byte is absent).
    {
      api_call_id: 'api_004', response_id: 'resp_004', method: 'POST', uri: '/v1/chat/completions', status: 'completed',
      model_requested: 'mystery-model', model_served: 'mystery-model', upstream_target: 'vllm-b',
      usage: { prompt: 4096, completion: 512, total: 4608 },
      started_ms: now - 18000, finished_ms: now - 16000, elapsed_ms: 2000, terminal_reason: 'response.completed',
      cost: null, cost_confidence: 'unavailable',
      // Gap 15: the WEAK User-Agent fallback (no key, no configured id — just a spoofable UA). The
      // CLIENT cell + roll-up render this visibly weaker (italic/dimmed + a `ua` badge, data-quality
      // `derived`) and labelled — NEVER as a confirmed identity (a key-hash/configured-id is `measured`).
      client_label: 'python-httpx/0.27', client_source: 'user_agent',
      ingress_ms: now - 18000, normalization_done_ms: now - 17960, routing_decision_ms: now - 17940,
      first_content_delta_ms: now - 17600, stream_end_ms: now - 16020, finalize_ms: now - 16000,
    },
    // Gap 11: a FAILOVER flow — the FIRST provider (vllm-b) FAILED (http_status, ~600ms before any
    // header ⇒ no first_upstream_byte), then OpenAI SERVED. The attempt-trace stepper must render a
    // 2-node chain (A failed → B served), the served node visually distinct, the failed node's
    // first byte `—` (never 0). Routed via `/v1/responses` on `openai` (the served target).
    {
      api_call_id: 'api_005', response_id: 'resp_005', method: 'POST', uri: '/v1/responses', status: 'completed',
      model_requested: 'gpt-4o', model_served: 'gpt-4o', upstream_target: 'openai',
      usage: { prompt: 640, completion: 320, total: 960, cached: 0, reasoning: 0 },
      started_ms: now - 9000, finished_ms: now - 6200, elapsed_ms: 2800, terminal_reason: 'response.completed',
      cost: 0.0080, cost_confidence: 'confident',
      // Gap 15: a DISTINCT strong key-hash client (a different caller key) ⇒ a separate roll-up row.
      client_label: 'key-deadbeef0012', client_source: 'key_hash',
      ingress_ms: now - 9000, normalization_done_ms: now - 8970, routing_decision_ms: now - 8950,
      first_upstream_byte_ms: now - 8050, first_content_delta_ms: now - 7900,
      stream_end_ms: now - 6220, finalize_ms: now - 6200,
      attempts: [
        // A — vllm-b failed (HTTP 503), no first byte (failed pre-headers) ⇒ first byte `—`.
        { provider: 'vllm-b', model: 'gpt-4o', start_ms: now - 8950, end_ms: now - 8350, status: 'failed', error_class: 'http_status', failover_reason: 'provider_failed' },
        // B — openai served (first wire byte arrived 300ms into its attempt).
        { provider: 'openai', model: 'gpt-4o', start_ms: now - 8350, end_ms: now - 6200, first_upstream_byte_ms: now - 8050, status: 'served' },
      ],
    },
    // Gap 14: a FAILED flow on vllm-b/llama with a terminal_reason but NO captured upstream_response
    // (capture is OFF for it) — the ErrorTab must show an explicit "capture disabled" state, NOT a
    // blank implying "no error". Also feeds the aggregate taxonomy (vllm-b group, timeout reason).
    {
      // Gap 15: NO client attribution (no key, no configured id, no UA) ⇒ `client_label`/`client_source`
      // ABSENT ⇒ the CLIENT cell renders `—` (don't-lie-with-zeros, never a fabricated id) and the flow
      // bumps the roll-up's explicit "unattributed" count rather than inventing a client.
      api_call_id: 'api_006', response_id: null, method: 'POST', uri: '/v1/chat/completions', status: 'failed',
      model_requested: 'llama-3.1-70b', model_served: 'llama-3.1-70b', upstream_target: 'vllm-b',
      usage: null, started_ms: now - 40000, finished_ms: now - 39000, elapsed_ms: 1000,
      terminal_reason: 'upstream timeout', cost: null, cost_confidence: 'unavailable',
      ingress_ms: now - 40000, normalization_done_ms: now - 39970, routing_decision_ms: now - 39950,
      finalize_ms: now - 39000,
      attempts: [
        { provider: 'vllm-b', model: 'llama-3.1-70b', start_ms: now - 39950, end_ms: now - 39000, status: 'failed', error_class: 'timeout', failover_reason: 'terminal_no_failover' },
      ],
    },
  ];
  if (longClientEnabled()) {
    // Gap 15 review round 3 (opt-in e2e only): a ~4 KiB UA `client_label` — the worst case the
    // filter-bar chip must bound. WEAK source so it also exercises the UA tagging on a huge label.
    flows.push({
      api_call_id: 'api_long', response_id: 'resp_long', method: 'POST', uri: '/v1/chat/completions', status: 'completed',
      model_requested: 'gpt-4o', model_served: 'gpt-4o', upstream_target: 'vllm-a',
      usage: { prompt: 10, completion: 10, total: 20 },
      started_ms: now - 5000, finished_ms: now - 4000, elapsed_ms: 1000, terminal_reason: 'response.completed',
      cost: null, cost_confidence: 'unavailable',
      client_label: `python-httpx/${'x'.repeat(4096)}`, client_source: 'user_agent',
    });
  }
  return flows;
}

/**
 * Gap 14 — the captured upstream RESPONSE/ERROR body the mock attaches to the LIVE `/flows/:id`
 * detail (NEVER the list rows / snapshot summaries — body-free invariant), keyed by `api_call_id`,
 * mirroring the Rust `FlowDetailBody.upstream_response`. ONLY `api_003` has capture ON (a real error
 * body); every other id is ABSENT ⇒ the ErrorTab reads "capture disabled" (don't-lie-with-zeros). This
 * proves both the capture-ON (body shown) and capture-OFF (explicit disabled) states in e2e.
 */
const UPSTREAM_RESPONSE_BY_ID: Record<string, FlowDetail['upstream_response']> = {
  api_003: {
    body: { error: { message: 'Service Unavailable: upstream pool exhausted', type: 'server_error', code: 503 } },
    truncated: false,
  },
};

function buildMetrics(): MetricsResponse {
  const win = (m: number) => {
    const samples = Math.round(252 * m);
    return {
      reqs_per_sec: 4.2 * m, active_streams: Math.round(3 * m), error_pct: 1.1,
      p50: 180, p95: 920, p99: 1840, tokens_per_sec: 142 * m, cost_per_min: 0.21 * m,
      samples,
      // The mock's window is fully measured: every finalized flow reported usage on a
      // priced model, so all three denominators equal `samples` (tok/s + $/min measurable).
      usage_samples: samples,
      priced_samples: samples,
      // Gap 07: the priced llama model has no configured cache rate (and the seed flow on it
      // bills/omits cached) ⇒ the aggregate $/min is an ESTIMATE, labelled as such.
      cost_confidence: 'estimated' as const,
    };
  };
  const m1 = win(1);
  return {
    metrics_seq: 1,
    reqs_per_sec: 4.2, active_streams: 3, error_pct: 1.1,
    p50: 180, p95: 920, p99: 1840, tokens_per_sec: 142, cost_per_min: 0.21,
    samples: m1.samples,
    usage_samples: m1.usage_samples,
    priced_samples: m1.priced_samples,
    cost_confidence: m1.cost_confidence,
    windows: { m1, m5: win(0.9), h1: win(0.7) },
  };
}

function buildTopology(): TopologyResponse {
  return {
    topology_seq: 1,
    // Gap 12/13: the REST `/topology` (+ snapshot) node carries the additive `per_provider` when the
    // provider had in-window samples; an absent entry (openai) leaves the field off (don't-lie-with
    // -zeros). The WS `topology_update` frame strips it (see `topologyFrame`).
    nodes: NODES.map((n) => (PER_PROVIDER[n.id] ? { ...n, per_provider: PER_PROVIDER[n.id] } : n)),
    edges: [
      { from: 'gateway', to: 'vllm-a', throughput: 4.2, tokens_per_sec: 142, cost_per_sec: 0.003 },
      { from: 'gateway', to: 'vllm-b', throughput: 1.0, tokens_per_sec: 61, cost_per_sec: 0.001 },
    ],
    price_table: PRICE_TABLE,
  };
}

function buildSnapshot(): SnapshotFrame {
  return {
    type: 'snapshot',
    cursors: { flow_seq: 3, metrics_seq: 1, topology_seq: 1, monitor_seq: 5 },
    flows: seedFlows(),
    metrics: buildMetrics(),
    topology: buildTopology(),
  };
}

/**
 * A multi-message `Monitor` frame: ONE envelope (seq = originating DebugUpdate.sequence)
 * whose `batch` carries several sibling `DashboardPayload::Monitor`s, each NESTING a real
 * itself-`type`-tagged `DebugWsMessage` (request_upsert / segment_append / request_status —
 * the actual `src/monitor.rs` arms). The dedup test asserts ALL apply (none dropped).
 */
export function buildMonitorFrame(seq = 6, responseId = 'resp_001'): DashboardFrame {
  const now = Date.now();
  const messages: DebugWsMessage[] = [
    {
      type: 'request_upsert',
      request: {
        response_id: responseId, model: 'llama-3.1-70b', started_at_ms: now, updated_at_ms: now,
        completed_at_ms: null, status: 'running',
        stats: { input_items: 3, tool_count: 0, turn_count: 1, user_messages: 1, assistant_messages: 0, system_messages: 1, developer_messages: 0, reasoning_items: 0, function_calls: 0, function_outputs: 0, tool_items: 0, input_chars: 42, instructions_chars: 0 },
        error: null,
      },
    },
    { type: 'segment_append', response_id: responseId, segment: { timestamp_ms: now, kind: 'output', text: 'Hello' } },
    { type: 'segment_append', response_id: responseId, segment: { timestamp_ms: now, kind: 'output', text: ', world' } },
    { type: 'request_status', response_id: responseId, status: 'completed', completed_at_ms: now, error: null },
  ];
  return {
    domain: 'monitor',
    seq,
    // NESTED wire form: each payload is `{type:'monitor', message:<DebugWsMessage>}`
    // (the message is itself `type`-tagged) — see types.ts WIRE CONTRACT.
    batch: messages.map((message): MonitorPayload => ({ type: 'monitor', message })),
  };
}

/** A standalone `usage` frame (flow domain) — exercises the `usage` payload arm (finding 9). */
export function buildUsageFrame(seq: number, apiCallId = 'api_001'): DashboardFrame {
  return {
    domain: 'flow',
    seq,
    batch: [
      { type: 'usage', api_call_id: apiCallId, response_id: 'resp_001', prompt: 812, completion: 512, total: 1324, cached: 128, reasoning: 16 },
    ],
  };
}

// ---------------------------------------------------------------------------
// Mock REST
// ---------------------------------------------------------------------------

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

/** Captures kill POSTs so tests can assert the CSRF header round-tripped. */
export const mockKillLog: { id: string; csrf: string | null }[] = [];

/**
 * Reserved api_call_id whose kill route answers 401 (with a valid CSRF) — models a kill that
 * races a session expiry. Lets the 401-teardown path be driven through the real client wiring.
 */
export const MOCK_KILL_UNAUTHORIZED_ID = 'api_session_expired';

/** A `fetch`-compatible mock answering D13 REST + D7 auth. */
export const mockFetch: typeof fetch = async (input, init): Promise<Response> => {
  const url = typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url;
  const method = (init?.method ?? 'GET').toUpperCase();
  const path = url.replace(/^https?:\/\/[^/]+/, '').split('?')[0] ?? url;
  const qs = new URLSearchParams(url.includes('?') ? url.slice(url.indexOf('?')) : '');

  // -- Auth --
  if (path === '/dashboard/login' && method === 'POST') {
    return json({ ok: true });
  }
  if (path === '/dashboard/auth/key-login' && method === 'POST') {
    return json({ ok: true });
  }
  if (path === '/dashboard/api/auth/summary' && method === 'GET') {
    return json({
      policy_epoch: 7,
      actor: {
        kind: 'bootstrap', principal_id: null, display_name: 'Bootstrap administrator',
        permissions: [],
      },
      counts: {
        users: AUTH_USERS.length, groups: AUTH_GROUPS.length, roles: AUTH_ROLES.length,
        policies: AUTH_POLICIES.length, api_keys: AUTH_KEYS.length,
        active_sessions: AUTH_SESSIONS.length,
      },
    });
  }
  if (path === '/dashboard/api/auth/users') {
    if (method === 'POST') {
      const body = JSON.parse(String(init?.body ?? '{}')) as { display_name?: string; kind?: AuthUser['kind'] };
      if (!body.display_name || !body.kind) return json({ error: 'invalid user' }, 400);
      AUTH_USERS = [...AUTH_USERS, {
        id: `usr_${AUTH_USERS.length + 1}`, display_name: body.display_name,
        kind: body.kind, enabled: true, created_at: new Date().toISOString(),
      }];
    }
    return json({ users: AUTH_USERS });
  }
  if (path === '/dashboard/api/auth/groups') {
    if (method === 'POST') {
      const body = JSON.parse(String(init?.body ?? '{}')) as { name?: string; members?: string[] };
      if (!body.name || !Array.isArray(body.members)) return json({ error: 'invalid group' }, 400);
      AUTH_GROUPS = [...AUTH_GROUPS, { id: `grp_${AUTH_GROUPS.length + 1}`, name: body.name, enabled: true, member_count: body.members.length }];
    }
    return json({ groups: AUTH_GROUPS });
  }
  if (path === '/dashboard/api/auth/roles') {
    if (method === 'POST') {
      const body = JSON.parse(String(init?.body ?? '{}')) as { name?: string; permissions?: AuthRole['permissions'] };
      if (!body.name || !Array.isArray(body.permissions)) return json({ error: 'invalid role' }, 400);
      AUTH_ROLES = [...AUTH_ROLES, { id: `role_${AUTH_ROLES.length + 1}`, name: body.name, enabled: true, permissions: body.permissions }];
    }
    return json({ roles: AUTH_ROLES });
  }
  if (path === '/dashboard/api/auth/policies') {
    if (method === 'POST') {
      const body = JSON.parse(String(init?.body ?? '{}')) as Omit<AuthPolicy, 'id' | 'enabled'>;
      if (!body.name || !['allow', 'deny'].includes(body.effect)) return json({ error: 'invalid policy' }, 400);
      AUTH_POLICIES = [...AUTH_POLICIES, { ...body, id: `pol_${AUTH_POLICIES.length + 1}`, enabled: true }];
    }
    return json({ policies: AUTH_POLICIES });
  }
  if (path === '/dashboard/api/auth/api-keys') {
    if (method === 'POST') {
      const body = JSON.parse(String(init?.body ?? '{}')) as { principal_id?: string; name?: string; expires_at?: string | null };
      if (!body.principal_id || !body.name) return json({ error: 'invalid key' }, 400);
      const apiKey: AuthApiKey = { id: `key_${AUTH_KEYS.length + 1}`, principal_id: body.principal_id, name: body.name, prefix: 'llmc_demo', enabled: true, created_at: new Date().toISOString(), expires_at: body.expires_at ?? null, last_used_at: null };
      AUTH_KEYS = [...AUTH_KEYS, apiKey];
      return json({ ...apiKey, raw_key: `llmc_mock_${apiKey.id}_copy_once` });
    }
    return json({ api_keys: AUTH_KEYS });
  }
  const keyAction = path.match(/^\/dashboard\/api\/auth\/api-keys\/([^/]+)\/(revoke|rotate)$/);
  if (keyAction && method === 'POST') {
    if (!headerValue(init?.headers, 'X-CSRF-Token')) return json({ error: 'missing csrf' }, 403);
    const id = decodeURIComponent(keyAction[1] ?? '');
    const found = AUTH_KEYS.find((key) => key.id === id);
    if (!found) return json({ error: 'unknown key' }, 404);
    if (keyAction[2] === 'revoke') {
      AUTH_KEYS = AUTH_KEYS.map((key) => key.id === id ? { ...key, enabled: false } : key);
      return json({ api_keys: AUTH_KEYS });
    }
    const rotated = { ...found, id: `${found.id}_rotated`, prefix: 'llmc_rot8', created_at: new Date().toISOString(), last_used_at: null };
    AUTH_KEYS = [...AUTH_KEYS.filter((key) => key.id !== id), rotated];
    return json({ ...rotated, raw_key: `llmc_mock_${rotated.id}_copy_once` });
  }
  if (path === '/dashboard/api/auth/sessions') return json({ sessions: AUTH_SESSIONS });
  const sessionRevoke = path.match(/^\/dashboard\/api\/auth\/sessions\/([^/]+)\/revoke$/);
  if (sessionRevoke && method === 'POST') {
    if (!headerValue(init?.headers, 'X-CSRF-Token')) return json({ error: 'missing csrf' }, 403);
    const id = decodeURIComponent(sessionRevoke[1] ?? '');
    AUTH_SESSIONS = AUTH_SESSIONS.filter((session) => session.id !== id);
    return json({ sessions: AUTH_SESSIONS });
  }
  if (path === '/dashboard/api/auth/usage') return json({ usage: AUTH_USAGE });
  if (path === '/dashboard/api/auth/audit') return json({ events: AUTH_AUDIT });
  if (path === '/dashboard/api/auth/pricing') return json({ pricing: AUTH_PRICING });

  // -- Kill (CSRF) -- `:id` == api_call_id ONLY (D13 contract). CSRF checked first
  // (security gate), then the id must be a seeded api_call_id else 404 (finding 7).
  const killMatch = path.match(/^\/dashboard\/api\/flows\/([^/]+)\/kill$/);
  if (killMatch && method === 'POST') {
    const id = decodeURIComponent(killMatch[1] ?? '');
    const csrf = headerValue(init?.headers, 'X-CSRF-Token');
    mockKillLog.push({ id, csrf });
    if (!csrf) return json({ error: 'missing csrf' }, 403);
    // Test affordance: the reserved id `api_session_expired` models a kill that races a
    // session loss → 401, so the auth-teardown-vs-rollback contract can be exercised end to end.
    if (id === MOCK_KILL_UNAUTHORIZED_ID) return json({ error: 'unauthorized' }, 401);
    if (!isSeededApiCallId(id)) return json({ error: 'unknown api_call_id' }, 404);
    return json({ api_call_id: id, killed: true });
  }

  // -- Reads --
  if (path === '/dashboard/api/flows') {
    let flows = seedFlows();
    const status = qs.get('status');
    if (status) flows = flows.filter((f) => f.status === status);
    const model = qs.get('model');
    if (model) flows = flows.filter((f) => f.model_requested === model || f.model_served === model);
    const upstream = qs.get('upstream');
    if (upstream) flows = flows.filter((f) => f.upstream_target === upstream);
    const resp: FlowsResponse = { flows, total: flows.length, flow_seq: 3 };
    return json(resp);
  }
  const detailMatch = path.match(/^\/dashboard\/api\/flows\/([^/]+)$/);
  if (detailMatch) {
    const id = decodeURIComponent(detailMatch[1] ?? '');
    const detail = buildFlowDetail(id); // by api_call_id ONLY
    return detail ? json(detail) : json({ error: 'unknown api_call_id' }, 404);
  }
  if (path === '/dashboard/api/metrics') return json(buildMetrics());
  if (path === '/dashboard/api/topology') return json(buildTopology());
  if (path === '/dashboard/api/providers') return json({ providers: PROVIDERS });
  if (path === '/dashboard/api/provider-metrics') return json({ generated_at_ms: Date.now(), providers: PROVIDER_METRICS });
  if (path === '/dashboard/api/catalog') return json(CATALOG);
  if (path === '/dashboard/api/snapshot') {
    const atMs = Number(qs.get('at') ?? Date.now());
    // Snapshot summaries are body-free FlowSummary objects (identical shape, D1).
    const snap: SnapshotResponse = {
      cursors: { flow_seq: 3, metrics_seq: 1, topology_seq: 1, monitor_seq: 5 },
      at_ms: atMs,
      summaries: seedFlows(),
      metrics: buildMetrics(),
      topology: buildTopology(),
    };
    return json(snap);
  }

  return json({ error: `mock: no route for ${method} ${path}` }, 404);
};

/** True when `id` is one of the seeded `api_call_id`s (D13 `:id = api_call_id`) — finding 7. */
function isSeededApiCallId(id: string): boolean {
  return seedFlows().some((f) => f.api_call_id === id);
}

/**
 * Build the flow-detail body for a seeded `api_call_id`. Resolves by `api_call_id` ONLY
 * (NOT response_id) per the D13 `:id = api_call_id` contract; returns `null` for an unknown
 * id so the route answers 404 (finding 7).
 */
function buildFlowDetail(id: string): FlowDetail | null {
  const base = seedFlows().find((f) => f.api_call_id === id);
  if (!base) return null;
  return {
    flow_seq: 3,
    api_call_id: base.api_call_id,
    response_id: base.response_id,
    inbound_body: { model: base.model_requested, messages: [{ role: 'user', content: 'Hi' }] },
    inbound_headers: { 'content-type': 'application/json', authorization: 'Bearer ***' },
    normalized: { model: base.model_served, input: [{ role: 'user', content: 'Hi' }] },
    upstream_body: { model: base.model_served, messages: [{ role: 'user', content: 'Hi' }], stream: true },
    // Gap 14: project the captured upstream error body onto the LIVE detail (only for ids with
    // capture ON; absent otherwise ⇒ the ErrorTab's "capture disabled" state). Mirrors the Rust
    // `FlowDetailBody.upstream_response` (live-detail only, never the body-free summaries).
    upstream_response: UPSTREAM_RESPONSE_BY_ID[base.api_call_id],
    model_requested: base.model_requested,
    model_served: base.model_served,
    upstream_target: base.upstream_target,
    usage: base.usage,
    status: base.status,
    deltas: [
      { sequence: 1, kind: 'response.created', payload: {}, ts_ms: base.started_ms },
      { sequence: 2, kind: 'response.delta', payload: { text: 'Hello' }, ts_ms: base.started_ms + 200 },
      { sequence: 3, kind: 'response.delta', payload: { text: ', world' }, ts_ms: base.started_ms + 400 },
    ],
    terminal_reason: base.terminal_reason,
    started_ms: base.started_ms,
    finished_ms: base.finished_ms,
    elapsed_ms: base.elapsed_ms,
    cost: base.cost ?? null,
    cost_confidence: base.cost_confidence,
    // Gap 10: project the gap-02 phase epochs + gap-03 attempts/wire-TTFB from the seed flow onto
    // the inspector detail (mirrors how the live row carries them) so the latency breakdown reads
    // the MEASURED spine when the flow is opened via the REST detail path.
    ingress_ms: base.ingress_ms,
    normalization_done_ms: base.normalization_done_ms,
    routing_decision_ms: base.routing_decision_ms,
    first_content_delta_ms: base.first_content_delta_ms,
    stream_end_ms: base.stream_end_ms,
    finalize_ms: base.finalize_ms,
    attempts: base.attempts,
    first_upstream_byte_ms: base.first_upstream_byte_ms,
  };
}

function headerValue(headers: HeadersInit | undefined, name: string): string | null {
  if (!headers) return null;
  if (headers instanceof Headers) return headers.get(name);
  if (Array.isArray(headers)) {
    const found = headers.find(([k]) => k.toLowerCase() === name.toLowerCase());
    return found ? found[1] : null;
  }
  const rec = headers as Record<string, string>;
  const key = Object.keys(rec).find((k) => k.toLowerCase() === name.toLowerCase());
  return key ? rec[key]! : null;
}

// ---------------------------------------------------------------------------
// Mock WebSocket
// ---------------------------------------------------------------------------

/**
 * A scripted WS: on connect it pushes a snapshot, then a flow_status frame, a metrics
 * frame, a topology frame, and the multi-message Monitor frame. `emit()` lets tests/dev
 * drive additional frames. Timers are used so React can render between frames; tests can
 * call the exposed `pushScript()` synchronously instead.
 */
export class MockWebSocket implements WsLike {
  onopen: ((ev: unknown) => void) | null = null;
  onclose: ((ev: { code?: number } | undefined) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;

  private timers: ReturnType<typeof setTimeout>[] = [];
  private seq = { flow: 3, metrics: 1, topology: 1, monitor: 5 };

  constructor(_url: string) {
    // Defer so handlers attach before frames flow.
    this.timers.push(setTimeout(() => this.start(), 0));
  }

  private start(): void {
    this.onopen?.({});
    this.deliver(buildSnapshot());
    // Stagger live frames. flow_status THEN a standalone usage frame (finding 9: the
    // `usage` payload arm must be exercised on the live path), both flow-domain so their
    // seqs stay monotonic.
    let t = 50;
    const live: WsServerMessage[] = [
      this.flowFrame(),
      buildUsageFrame(++this.seq.flow),
      this.metricsFrame(),
      this.topologyFrame(),
      buildMonitorFrame(++this.seq.monitor),
    ];
    for (const frame of live) {
      this.timers.push(setTimeout(() => this.deliver(frame), t));
      t += 60;
    }
  }

  private deliver(msg: WsServerMessage): void {
    this.onmessage?.({ data: JSON.stringify(msg) });
  }

  /** Push an arbitrary frame (dev/testing). */
  emit(msg: WsServerMessage): void {
    this.deliver(msg);
  }

  private flowFrame(): DashboardFrame {
    // Gap 10b: a LIVE `flow_status` frame now carries the spine fields the Rust
    // `flow_status_payload` projects off the live record — the gap-02 phases (flattened
    // siblings), the gap-03 served attempt, and the wire TTFB — so the live path exercises
    // the REAL projected wire shape (a row's waterfall + stepper light up off the socket,
    // not only off the REST detail). Anchored to a single `started` so the phases stay
    // monotonic (ingress ≤ … ≤ finalize).
    const started = Date.now() - 3100;
    return {
      domain: 'flow',
      seq: ++this.seq.flow,
      batch: [{
        type: 'flow_status',
        api_call_id: 'api_001',
        response_id: 'resp_001',
        status: 'completed',
        model_requested: 'gpt-4o',
        model_served: 'llama-3.1-70b',
        upstream_target: 'vllm-a',
        usage: { prompt: 812, completion: 512, total: 1324, cached: 128, reasoning: 0 },
        started_ms: started,
        elapsed_ms: 3100,
        ingress_ms: started,
        normalization_done_ms: started + 30,
        routing_decision_ms: started + 50,
        first_upstream_byte_ms: started + 260,
        first_content_delta_ms: started + 440,
        stream_end_ms: started + 3080,
        finalize_ms: started + 3100,
        attempts: [
          { provider: 'vllm-a', model: 'llama-3.1-70b', start_ms: started + 50, end_ms: started + 260, first_upstream_byte_ms: started + 260, status: 'served' },
        ],
      }],
    };
  }

  private metricsFrame(): DashboardFrame {
    const m = buildMetrics();
    return {
      domain: 'metrics',
      seq: ++this.seq.metrics,
      batch: [{
        type: 'metric_tick',
        reqs_per_sec: m.reqs_per_sec, active_streams: m.active_streams, error_pct: m.error_pct,
        p50: m.p50, p95: m.p95, p99: m.p99, tokens_per_sec: m.tokens_per_sec, cost_per_min: m.cost_per_min,
        samples: m.samples,
        usage_samples: m.usage_samples,
        priced_samples: m.priced_samples,
        cost_confidence: m.cost_confidence,
        windows: m.windows,
      }],
    };
  }

  private topologyFrame(): DashboardFrame {
    const t = buildTopology();
    // The LIVE WS `topology_update` frame does NOT join the metrics window (gap-12 discovery), so it
    // carries `per_provider` ABSENT — strip it here to mirror the Rust `from_health` (the REST
    // `/topology` + `/snapshot` are the per-provider source). Stripping (not just omitting) proves
    // the frontend reads the REST path, not the WS frame, for the per-provider tile.
    const nodes = t.nodes.map(({ per_provider: _omit, ...rest }) => rest);
    return {
      domain: 'topology',
      seq: ++this.seq.topology,
      batch: [{ type: 'topology_update', nodes, edges: t.edges }],
    };
  }

  send(_data: string): void {
    // The dashboard WS is server-push only; client sends are ignored by the mock.
  }

  close(code?: number): void {
    for (const id of this.timers) clearTimeout(id);
    this.timers = [];
    this.onclose?.({ code });
  }
}

export const mockWsFactory = (url: string): WsLike => new MockWebSocket(url);

/** The CSRF token the mock bootstrap exposes (mirrors the cookie the Rust shell sets). */
export const mockBootstrapCsrf = MOCK_CSRF;
