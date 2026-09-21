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
  ActivityBucket,
  ActivityResponse,
  ApiKeyRecord,
  CatalogEntry,
  DashboardFrame,
  DebugWsMessage,
  FlowDetail,
  FlowSummary,
  ActiveSessionsResponse,
  FlowsResponse,
  HistoryMetricsResponse,
  HistoryRequest,
  MetricSample,
  MetricsResponse,
  MonitorPayload,
  ProviderHealth,
  ProviderLatency,
  SessionDetailResponse,
  SessionRequestStub,
  SessionRow,
  SessionUser,
  SessionsResponse,
  SnapshotFrame,
  SnapshotResponse,
  ThroughputBucket,
  ThroughputResponse,
  TopologyResponse,
  UserRecord,
  WsServerMessage,
} from './types';
import type { WsLike } from './ws';

const MOCK_CSRF = 'mock-csrf-token';

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
      // Sessions: the newest turn of the seeded claude-code session — a pure append onto api_002.
      harness: 'claude-code', harness_version: '2.1.205', session_id: 'sess_root',
      chain_parent_request_id: 'api_002', divergence_kind: 'append', cache_bust: false,
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
      // Sessions: the first turn of the seeded claude-code session (a new chain, no predecessor).
      harness: 'claude-code', harness_version: '2.1.205', session_id: 'sess_root',
      chain_parent_request_id: null, divergence_kind: 'new_chain', cache_bust: false,
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
      // Sessions: a codex flow whose tool list changed mid-conversation ⇒ a cache bust on its chain.
      harness: 'codex', harness_version: '0.104.0', session_id: 'sess_codex',
      chain_parent_request_id: 'api_005', divergence_kind: 'tools_changed', cache_bust: true,
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
      harness: 'codex', harness_version: '0.104.0', session_id: 'sess_codex',
      chain_parent_request_id: null, divergence_kind: 'new_chain', cache_bust: false,
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

  // -- Auth -- a username/password login is accepted for the seeded users (any password
  // of 8+ chars), a token login for any non-empty token; the response carries the user.
  if (path === '/dashboard/login' && method === 'POST') {
    const body = parseBody(init?.body);
    if (typeof body.username === 'string') {
      const user = MOCK_USERS.find((u) => u.username === body.username);
      if (!user || typeof body.password !== 'string' || body.password.length < 8) return json({ error: 'invalid username or password' }, 401);
      mockSessionUser = { id: user.id, username: user.username, is_admin: user.is_admin };
      return json({ authenticated: true, user: mockSessionUser });
    }
    if (typeof body.token !== 'string' || body.token.length === 0) return json({ error: 'invalid token' }, 401);
    mockSessionUser = null;
    return json({ authenticated: true, user: null });
  }
  // -- Accounts --
  if (path === '/dashboard/api/me') {
    return json({ user: mockSessionUser, is_admin: mockSessionUser?.is_admin ?? true, auth_mode: 'users', accounts_enabled: true });
  }
  if (path === '/dashboard/api/users' && method === 'GET') {
    if (mockSessionUser && !mockSessionUser.is_admin) return json({ error: 'administrator role required' }, 403);
    return json({ users: MOCK_USERS });
  }
  if (path === '/dashboard/api/users' && method === 'POST') {
    if (!headerValue(init?.headers, 'X-CSRF-Token')) return json({ error: 'missing csrf' }, 403);
    const body = parseBody(init?.body);
    const username = typeof body.username === 'string' ? body.username.trim() : '';
    if (!username) return json({ error: 'username must be 1-64 characters' }, 400);
    if (MOCK_USERS.some((u) => u.username === username)) return json({ error: 'username already exists' }, 409);
    const user: UserRecord = { id: `user_${MOCK_USERS.length + 1}`, username, is_admin: body.is_admin === true, created_at_ms: Date.now(), updated_at_ms: Date.now() };
    MOCK_USERS.push(user);
    return json(user, 201);
  }
  const userMatch = path.match(/^\/dashboard\/api\/users\/([^/]+)$/);
  if (userMatch && (method === 'PATCH' || method === 'DELETE')) {
    if (!headerValue(init?.headers, 'X-CSRF-Token')) return json({ error: 'missing csrf' }, 403);
    const id = decodeURIComponent(userMatch[1] ?? '');
    const index = MOCK_USERS.findIndex((u) => u.id === id);
    if (index < 0) return json({ error: 'user not found' }, 404);
    if (method === 'DELETE') {
      MOCK_USERS.splice(index, 1);
      const before = MOCK_KEYS.length;
      for (let i = MOCK_KEYS.length - 1; i >= 0; i--) if (MOCK_KEYS[i]!.user_id === id) MOCK_KEYS.splice(i, 1);
      return json({ id, deleted: true, keys_revoked: before - MOCK_KEYS.length });
    }
    const body = parseBody(init?.body);
    if (typeof body.is_admin === 'boolean') MOCK_USERS[index] = { ...MOCK_USERS[index]!, is_admin: body.is_admin };
    return json({ id, updated: true });
  }
  if (path === '/dashboard/api/keys' && method === 'GET') {
    const scope = qs.get('user_id');
    const keys = scope === 'all' || (!scope && !mockSessionUser) ? MOCK_KEYS : MOCK_KEYS.filter((k) => k.user_id === (scope ?? mockSessionUser?.id));
    return json({ keys });
  }
  if (path === '/dashboard/api/keys' && method === 'POST') {
    if (!headerValue(init?.headers, 'X-CSRF-Token')) return json({ error: 'missing csrf' }, 403);
    const body = parseBody(init?.body);
    const key: ApiKeyRecord = {
      id: `key_${MOCK_KEYS.length + 1}`,
      label: typeof body.label === 'string' && body.label ? body.label : null,
      user_id: typeof body.user_id === 'string' ? body.user_id : mockSessionUser?.id ?? null,
      allowed_models: Array.isArray(body.allowed_models) ? (body.allowed_models as string[]) : [],
      created_at_ms: Date.now(),
      updated_at_ms: Date.now(),
    };
    MOCK_KEYS.push(key);
    return json({ key, secret: `llmc_${'0'.repeat(56)}${MOCK_KEYS.length.toString().padStart(8, '0')}`, registered_keys: MOCK_KEYS.length }, 201);
  }
  const keyMatch = path.match(/^\/dashboard\/api\/keys\/([^/]+)$/);
  if (keyMatch && method === 'DELETE') {
    if (!headerValue(init?.headers, 'X-CSRF-Token')) return json({ error: 'missing csrf' }, 403);
    const id = decodeURIComponent(keyMatch[1] ?? '');
    const index = MOCK_KEYS.findIndex((k) => k.id === id);
    if (index < 0) return json({ error: 'key not found' }, 404);
    MOCK_KEYS.splice(index, 1);
    return json({ id, revoked: true, registered_keys: MOCK_KEYS.length });
  }
  // -- History series --
  if (path === '/dashboard/api/history/throughput') {
    const bucketSecs = Number(qs.get('bucket_secs') ?? 60);
    const resp: ThroughputResponse = { buckets: seedThroughput(bucketSecs * 1000), since_ms: 0, bucket_ms: bucketSecs * 1000, limit: 2000, truncated: false };
    return json(resp);
  }
  if (path === '/dashboard/api/history/activity') {
    const bucketSecs = Number(qs.get('bucket_secs') ?? 300);
    const resp: ActivityResponse = { buckets: seedActivity(bucketSecs * 1000), since_ms: 0, bucket_ms: bucketSecs * 1000, limit: 2000, truncated: false };
    return json(resp);
  }
  if (path === '/dashboard/api/history/metrics') {
    const resp: HistoryMetricsResponse = { samples: seedUpstreamSamples(), since_ms: 0, limit: 5000, truncated: false };
    return json(resp);
  }
  if (path === '/dashboard/logout' && method === 'POST') {
    return json({ ok: true });
  }

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

  // -- Durable history (sessions view + chain diff) --
  if (path === '/dashboard/api/sessions/active') {
    return json(MOCK_ACTIVE_SESSIONS());
  }
  if (path === '/dashboard/api/history/sessions') {
    const roots = qs.get('roots') !== 'false';
    const sessions = roots ? MOCK_SESSIONS.filter((s) => s.parent_id === null) : MOCK_SESSIONS;
    const resp: SessionsResponse = { sessions, since_ms: 0, limit: 100, truncated: false };
    return json(resp);
  }
  const sessionMatch = path.match(/^\/dashboard\/api\/history\/sessions\/([^/]+)$/);
  if (sessionMatch) {
    const id = decodeURIComponent(sessionMatch[1] ?? '');
    const session = MOCK_SESSIONS.find((s) => s.id === id);
    if (!session) return json({ error: 'session not found' }, 404);
    const ancestors: SessionRow[] = [];
    let cursor = session.parent_id;
    while (cursor) {
      const parent = MOCK_SESSIONS.find((s) => s.id === cursor);
      if (!parent) break;
      ancestors.push(parent);
      cursor = parent.parent_id;
    }
    const resp: SessionDetailResponse = {
      session,
      ancestors,
      children: MOCK_SESSIONS.filter((s) => s.parent_id === id),
      requests: MOCK_HISTORY_REQUESTS.filter((r) => r.session_id === id),
      requests_truncated: false,
    };
    return json(resp);
  }
  const bodyMatch = path.match(/^\/dashboard\/api\/history\/requests\/([^/]+)\/body$/);
  if (bodyMatch) {
    const id = decodeURIComponent(bodyMatch[1] ?? '');
    const body = MOCK_REQUEST_BODIES[id];
    return body ? json(body) : json({ error: 'request body not stored' }, 404);
  }

  return json({ error: `mock: no route for ${method} ${path}` }, 404);
};

// ---------------------------------------------------------------------------
// Durable history seed: a claude-code session with an inferred sub-agent under it, a codex
// session with a cache bust, and reassembled bodies for the chain diff.
// ---------------------------------------------------------------------------

const SEED_NOW = Date.now();

function mockSession(over: Partial<SessionRow> & Pick<SessionRow, 'id' | 'harness'>): SessionRow {
  return {
    parent_id: null,
    kind: 'declared',
    harness_version: null,
    external_id: null,
    user_id: 'user_dev',
    session_kind: null,
    client_label: 'key-9f3a1c0b2d4e',
    virtual_key_id: 'key_dev_ci',
    depth: 0,
    root_request_id: null,
    spawned_by_request_id: null,
    first_seen_ms: SEED_NOW - 60_000,
    last_seen_ms: SEED_NOW - 2_400,
    request_count: 2,
    ...over,
  };
}


/** The live hub cut: root + agent sessions active, the codex one aged out (mirrors
 *  the 15-minute window — its last activity is 40+ seconds old but still inside;
 *  kept to exercise window counting). Window counts derive from the stub ring. */
function MOCK_ACTIVE_SESSIONS(): ActiveSessionsResponse {
  const now = Date.now();
  // Rebase the module-load-frozen fixture times to NOW so the board never ages
  // out mid-session (the seeds are minutes-old offsets, not wall-clock facts).
  const rebase = (ms: number) => now - (SEED_NOW - ms);
  const stubsFor = (session: SessionRow): SessionRequestStub[] =>
    MOCK_HISTORY_REQUESTS
      .filter((r) => r.session_id === session.id)
      .map((r) => ({
        api_call_id: r.id,
        client_model: r.client_model,
        created_at_ms: rebase(r.created_at_ms),
        status: r.status,
        input_tokens: r.input_tokens,
        output_tokens: r.output_tokens,
        cached_tokens: r.cached_tokens,
        reasoning_tokens: null,
        error: r.error,
        terminal_reason: null,
      }))
      .sort((a, b) => b.created_at_ms - a.created_at_ms);
  const windows = (stubs: SessionRequestStub[]) => ({
    requests_1m: stubs.filter((s) => s.created_at_ms >= now - 60_000).length,
    requests_5m: stubs.filter((s) => s.created_at_ms >= now - 300_000).length,
    requests_10m: stubs.filter((s) => s.created_at_ms >= now - 600_000).length,
    requests_15m: stubs.filter((s) => s.created_at_ms >= now - 900_000).length,
  });
  const rows = ['sess_root', 'sess_agent', 'sess_codex']
    .map((id) => MOCK_SESSIONS.find((s) => s.id === id))
    .filter((s): s is SessionRow => !!s)
    .map((session) => {
      const stubs = stubsFor(session);
      const aggregate = session.user_id
        ? {
            session_id: session.id,
            input_tokens: stubs.reduce((sum, s) => sum + (s.input_tokens ?? 0), 0) || null,
            output_tokens: stubs.reduce((sum, s) => sum + (s.output_tokens ?? 0), 0) || null,
            cached_tokens: stubs.reduce((sum, s) => sum + (s.cached_tokens ?? 0), 0) || null,
            reasoning_tokens: null,
          }
        : null;
      return { ...session, requests: stubs, ...windows(stubs), aggregate };
    });
  return { sessions: rows, seq: 1 };
}
export const MOCK_SESSIONS: SessionRow[] = [
  mockSession({ id: 'sess_root', harness: 'claude-code', harness_version: '2.1.205', external_id: '11111111-1111-4111-8111-111111111111', root_request_id: 'api_002', request_count: 2 }),
  mockSession({ id: 'sess_agent', harness: 'claude-code', harness_version: '2.1.205', parent_id: 'sess_root', kind: 'inferred', depth: 1, spawned_by_request_id: 'api_002', root_request_id: 'api_agent_1', request_count: 1, first_seen_ms: SEED_NOW - 30_000, last_seen_ms: SEED_NOW - 29_000 }),
  mockSession({ id: 'sess_codex', harness: 'codex', harness_version: '0.104.0', external_id: 'codex-session-1', root_request_id: 'api_005', request_count: 2, client_label: 'python-httpx/0.27', first_seen_ms: SEED_NOW - 120_000, last_seen_ms: SEED_NOW - 40_000 }),
];

function mockRequest(over: Partial<HistoryRequest> & Pick<HistoryRequest, 'id' | 'session_id' | 'created_at_ms'>): HistoryRequest {
  return {
    response_id: null,
    client_protocol: 'anthropic_messages',
    client_model: 'claude-x',
    backend: 'vllm-a',
    resolved_model: 'llama-3.1-70b',
    status: 'completed',
    completed_at_ms: over.created_at_ms + 3_000,
    first_token_at_ms: over.created_at_ms + 400,
    input_tokens: 1200,
    output_tokens: 300,
    cached_tokens: 900,
    error: null,
    client_label: 'key-9f3a1c0b2d4e',
    harness: 'claude-code',
    harness_version: '2.1.205',
    chain_parent_request_id: null,
    item_count: 4,
    shared_prefix_items: null,
    divergence_kind: 'new_chain',
    divergence_index: null,
    cache_bust: false,
    ...over,
  };
}

export const MOCK_HISTORY_REQUESTS: HistoryRequest[] = [
  mockRequest({ id: 'api_002', session_id: 'sess_root', created_at_ms: SEED_NOW - 12_000, item_count: 3 }),
  mockRequest({ id: 'api_001', session_id: 'sess_root', created_at_ms: SEED_NOW - 2_400, status: 'running', completed_at_ms: null, chain_parent_request_id: 'api_002', divergence_kind: 'append', shared_prefix_items: 3, item_count: 5 }),
  mockRequest({ id: 'api_agent_1', session_id: 'sess_agent', created_at_ms: SEED_NOW - 30_000, item_count: 3 }),
  mockRequest({ id: 'api_005', session_id: 'sess_codex', created_at_ms: SEED_NOW - 120_000, harness: 'codex', harness_version: '0.104.0', client_protocol: 'responses', client_label: 'python-httpx/0.27', item_count: 4 }),
  mockRequest({ id: 'api_004', session_id: 'sess_codex', created_at_ms: SEED_NOW - 40_000, harness: 'codex', harness_version: '0.104.0', client_protocol: 'responses', client_label: 'python-httpx/0.27', chain_parent_request_id: 'api_005', divergence_kind: 'tools_changed', divergence_index: 1, shared_prefix_items: 1, item_count: 7, cache_bust: true }),
];

/** Reassembled inbound bodies (what `/history/requests/:id/body` returns) for the chain diff. */
export const MOCK_REQUEST_BODIES: Record<string, unknown> = {
  api_005: {
    model: 'codex-large',
    instructions: 'You are Codex.',
    tools: [{ type: 'function', name: 'read_file', parameters: { type: 'object' } }],
    input: [{ type: 'message', role: 'user', content: 'List the files.' }],
  },
  api_004: {
    model: 'codex-large',
    instructions: 'You are Codex.',
    tools: [
      { type: 'function', name: 'read_file', parameters: { type: 'object' } },
      { type: 'function', name: 'write_file', parameters: { type: 'object' } },
    ],
    input: [
      { type: 'message', role: 'user', content: 'List the files.' },
      { type: 'message', role: 'assistant', content: 'a.rs, b.rs' },
      { type: 'message', role: 'user', content: 'Now edit a.rs' },
    ],
  },
  api_002: { model: 'claude-x', system: 'You are Claude Code.', messages: [{ role: 'user', content: 'hi' }] },
  api_001: { model: 'claude-x', system: 'You are Claude Code.', messages: [{ role: 'user', content: 'hi' }, { role: 'assistant', content: 'hello' }, { role: 'user', content: 'more' }] },
};

function parseBody(body: BodyInit | null | undefined): Record<string, unknown> {
  if (typeof body !== 'string') return {};
  try {
    const parsed = JSON.parse(body) as unknown;
    return typeof parsed === 'object' && parsed !== null ? (parsed as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

/** The mock's signed-in user (null after a token login). */
let mockSessionUser: SessionUser | null = null;

export const MOCK_USERS: UserRecord[] = [
  { id: 'user_admin', username: 'admin', is_admin: true, created_at_ms: Date.now() - 86_400_000 * 30, updated_at_ms: Date.now() - 86_400_000 * 30 },
  { id: 'user_dev', username: 'dev', is_admin: false, created_at_ms: Date.now() - 86_400_000 * 3, updated_at_ms: Date.now() - 86_400_000 * 3 },
];

export const MOCK_KEYS: ApiKeyRecord[] = [
  { id: 'key_admin_laptop', label: 'laptop', user_id: 'user_admin', allowed_models: [], created_at_ms: Date.now() - 86_400_000 * 20, updated_at_ms: Date.now() - 86_400_000 * 20 },
  { id: 'key_dev_ci', label: 'ci', user_id: 'user_dev', allowed_models: ['local'], created_at_ms: Date.now() - 86_400_000 * 2, updated_at_ms: Date.now() - 86_400_000 * 2 },
];

/** Reset the mutable mock account state (tests). */
export function resetMockAccounts(): void {
  mockSessionUser = null;
  MOCK_USERS.splice(0, MOCK_USERS.length,
    { id: 'user_admin', username: 'admin', is_admin: true, created_at_ms: Date.now() - 86_400_000 * 30, updated_at_ms: Date.now() - 86_400_000 * 30 },
    { id: 'user_dev', username: 'dev', is_admin: false, created_at_ms: Date.now() - 86_400_000 * 3, updated_at_ms: Date.now() - 86_400_000 * 3 },
  );
  MOCK_KEYS.splice(0, MOCK_KEYS.length,
    { id: 'key_admin_laptop', label: 'laptop', user_id: 'user_admin', allowed_models: [], created_at_ms: Date.now() - 86_400_000 * 20, updated_at_ms: Date.now() - 86_400_000 * 20 },
    { id: 'key_dev_ci', label: 'ci', user_id: 'user_dev', allowed_models: ['local'], created_at_ms: Date.now() - 86_400_000 * 2, updated_at_ms: Date.now() - 86_400_000 * 2 },
  );
}

/** Deterministic per-model throughput buckets for the last hour. */
function seedThroughput(bucketMs: number): ThroughputBucket[] {
  const now = Date.now();
  const start = Math.floor((now - 60 * 60_000) / bucketMs) * bucketMs;
  const out: ThroughputBucket[] = [];
  for (let t = start, i = 0; t <= now; t += bucketMs, i++) {
    const wave = 1 + Math.sin(i / 3) * 0.4;
    out.push({
      bucket_ms: t, model: 'llama-3.1-70b', backend: 'vllm-a',
      requests: Math.round(12 * wave), completed: Math.round(11 * wave),
      input_tokens: Math.round(24_000 * wave), output_tokens: Math.round(4_800 * wave), cached_tokens: Math.round(18_000 * wave),
      ttft_ms_sum: Math.round(12 * wave * 380), ttft_count: Math.round(12 * wave), prefill_tokens: Math.round(24_000 * wave),
      decode_ms_sum: Math.round(11 * wave * 2_600), decode_tokens: Math.round(4_400 * wave), decode_count: Math.round(11 * wave),
    });
    out.push({
      bucket_ms: t, model: 'gpt-4o', backend: 'openai',
      requests: Math.round(4 * wave), completed: Math.round(4 * wave),
      input_tokens: Math.round(6_000 * wave), output_tokens: Math.round(1_200 * wave), cached_tokens: 0,
      ttft_ms_sum: Math.round(4 * wave * 900), ttft_count: Math.round(4 * wave), prefill_tokens: Math.round(6_000 * wave),
      decode_ms_sum: Math.round(4 * wave * 3_000), decode_tokens: Math.round(1_200 * wave), decode_count: Math.round(4 * wave),
    });
  }
  return out;
}

/** Deterministic per-user/key activity buckets for the last day. */
function seedActivity(bucketMs: number): ActivityBucket[] {
  const now = Date.now();
  const start = Math.floor((now - 24 * 60 * 60_000) / bucketMs) * bucketMs;
  const out: ActivityBucket[] = [];
  for (let t = start, i = 0; t <= now; t += bucketMs, i++) {
    const wave = 1 + Math.cos(i / 4) * 0.5;
    out.push({ bucket_ms: t, user_id: 'user_admin', virtual_key_id: 'key_admin_laptop', requests: Math.round(30 * wave), failed: i % 5 === 0 ? 1 : 0, input_tokens: Math.round(60_000 * wave), output_tokens: Math.round(9_000 * wave), cached_tokens: Math.round(40_000 * wave) });
    out.push({ bucket_ms: t, user_id: 'user_dev', virtual_key_id: 'key_dev_ci', requests: Math.round(8 * wave), failed: 0, input_tokens: Math.round(9_000 * wave), output_tokens: Math.round(2_000 * wave), cached_tokens: 0 });
    if (i % 3 === 0) out.push({ bucket_ms: t, user_id: null, virtual_key_id: null, requests: 2, failed: 0, input_tokens: 800, output_tokens: 200, cached_tokens: 0 });
  }
  return out;
}

/** Two consecutive vLLM scrapes for vllm-a so rates can be derived, plus one health sample. */
function seedUpstreamSamples(): MetricSample[] {
  const now = Date.now();
  const sample = (ts: number, prompt: number, gen: number, running: number) => ({
    backend: 'vllm-a', ts_ms: ts,
    data: JSON.stringify({ kind: 'upstream', engine: 'vllm', backend: 'vllm-a', scraped_at_ms: ts, kept_lines: 12, models: { 'llama-3.1-70b': { values: { running, waiting: 1, kv_usage: 0.62, prompt_tokens_total: prompt, generation_tokens_total: gen, prefix_cache_hits_total: 1_500_000, prefix_cache_queries_total: 2_000_000, ttft_seconds_sum: 900, ttft_count: 2_400 } } } }),
  });
  const out: MetricSample[] = [];
  for (let i = 8; i >= 1; i--) {
    const ts = now - i * 15_000;
    out.push(sample(ts, 5_000_000 + (8 - i) * 30_000, 900_000 + (8 - i) * 6_000, 3 + (i % 3)));
  }
  out.push({ backend: 'vllm-a', ts_ms: now - 60_000, data: JSON.stringify({ kind: 'health', name: 'vllm-a', status: 'healthy' }) });
  return out;
}

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
  private seq = { flow: 3, metrics: 1, topology: 1, monitor: 5, sessions: 0 };
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
      this.sessionsFrame(),
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

  private sessionsFrame(): DashboardFrame {
    // Mirrors the Rust sessions-domain push: a tiny touched-ids tick (no body)
    // that drives the SPA to refetch `/sessions/active`.
    return {
      domain: 'sessions',
      seq: ++this.seq.sessions,
      batch: [{ type: 'session_update', touched: ['sess_root'] }],
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
