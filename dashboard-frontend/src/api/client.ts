/**
 * Typed REST client for the D13 endpoints. Cross-cutting behavior:
 *  - All reads carry the session cookie automatically (`credentials: 'include'`).
 *  - The kill POST attaches `X-CSRF-Token` (D7 double-submit; value from bootstrap/cookie).
 *  - A 401 on ANY fetch fires the `onUnauthorized` signal so the shell bounces to login.
 *
 * The client is transport-agnostic: pass a `fetchImpl` (default `globalThis.fetch`) and
 * a `csrfToken` getter so the mock backend + tests can inject their own.
 */
import type {
  ActivityResponse,
  ApiKeyRecord,
  CatalogEntry,
  CreatedKeyResponse,
  FlowDetail,
  FlowsQuery,
  FlowsResponse,
  HistoryBodyHop,
  HistoryMetricsResponse,
  KillResponse,
  LoginRequest,
  MeResponse,
  MetricsResponse,
  SessionDetailResponse,
  SessionUser,
  SessionsResponse,
  SnapshotResponse,
  ThroughputResponse,
  TopologyResponse,
  UserRecord,
} from './types';

export type FetchImpl = typeof fetch;

/** Raised when a fetch returns 401; the shell listens for this to bounce to login. */
export class UnauthorizedError extends Error {
  constructor() {
    super('unauthorized');
    this.name = 'UnauthorizedError';
  }
}

export interface DashboardClientOptions {
  /** Base path the API is mounted under. Default `/dashboard/api`. */
  basePath?: string;
  /** Injected fetch (mock/test). Default `globalThis.fetch`. */
  fetchImpl?: FetchImpl;
  /** Returns the current double-submit CSRF token (read from cookie/bootstrap). */
  getCsrfToken?: () => string | null;
  /** Fired on ANY 401 so the app can bounce to the login shell. */
  onUnauthorized?: () => void;
}

export class DashboardClient {
  private readonly basePath: string;
  private readonly fetchImpl: FetchImpl;
  private readonly getCsrfToken: () => string | null;
  private readonly onUnauthorized: (() => void) | undefined;

  constructor(opts: DashboardClientOptions = {}) {
    this.basePath = opts.basePath ?? '/dashboard/api';
    // Bind to globalThis so the default impl isn't called with a `this` of the class.
    this.fetchImpl = opts.fetchImpl ?? ((...a: Parameters<FetchImpl>) => globalThis.fetch(...a));
    this.getCsrfToken = opts.getCsrfToken ?? (() => null);
    this.onUnauthorized = opts.onUnauthorized;
  }

  private async request<T>(path: string, init?: RequestInit): Promise<T> {
    const res = await this.fetchImpl(`${this.basePath}${path}`, {
      credentials: 'include',
      ...init,
    });
    if (res.status === 401) {
      // Bounce-to-login signal: notify, then throw so callers stop.
      this.onUnauthorized?.();
      throw new UnauthorizedError();
    }
    if (!res.ok) {
      throw new Error(`${init?.method ?? 'GET'} ${path} failed: ${res.status}`);
    }
    // 204/empty bodies decode to `undefined as T` at the call sites that allow it.
    const text = await res.text();
    return (text ? JSON.parse(text) : undefined) as T;
  }

  // -- Auth -----------------------------------------------------------------

  /**
   * `POST /dashboard/login` — note: login lives at /dashboard, NOT under /api. Returns the
   * signed-in user (null for a token login) as the server reports it.
   */
  async login(body: LoginRequest): Promise<{ user: SessionUser | null }> {
    const res = await this.fetchImpl('/dashboard/login', {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`login failed: ${res.status}`);
    }
    try {
      const parsed = (await res.json()) as { user?: SessionUser | null };
      return { user: parsed.user ?? null };
    } catch {
      return { user: null };
    }
  }

  /** A CSRF-carrying mutation under /dashboard/api. */
  private mutate<T>(path: string, method: 'POST' | 'PATCH' | 'DELETE', body?: unknown): Promise<T> {
    const csrf = this.getCsrfToken();
    const headers: Record<string, string> = {};
    if (csrf) headers['X-CSRF-Token'] = csrf;
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    return this.request<T>(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  }

  // -- Accounts -------------------------------------------------------------

  me(): Promise<MeResponse> {
    return this.request<MeResponse>('/me');
  }

  listUsers(): Promise<{ users: UserRecord[] }> {
    return this.request<{ users: UserRecord[] }>('/users');
  }

  createUser(body: { username: string; password: string; is_admin: boolean }): Promise<UserRecord> {
    return this.mutate<UserRecord>('/users', 'POST', body);
  }

  updateUser(id: string, body: { password?: string; is_admin?: boolean }): Promise<{ id: string; updated: boolean }> {
    return this.mutate(`/users/${encodeURIComponent(id)}`, 'PATCH', body);
  }

  deleteUser(id: string): Promise<{ id: string; deleted: boolean; keys_revoked: number }> {
    return this.mutate(`/users/${encodeURIComponent(id)}`, 'DELETE');
  }

  /** `GET /keys[?user_id=]` — own keys by default; admins may pass a user id or `all`. */
  listKeys(userId?: string): Promise<{ keys: ApiKeyRecord[] }> {
    return this.request<{ keys: ApiKeyRecord[] }>(`/keys${userId ? `?user_id=${encodeURIComponent(userId)}` : ''}`);
  }

  createKey(body: { label?: string; allowed_models?: string[]; user_id?: string }): Promise<CreatedKeyResponse> {
    return this.mutate<CreatedKeyResponse>('/keys', 'POST', body);
  }

  deleteKey(id: string): Promise<{ id: string; revoked: boolean }> {
    return this.mutate(`/keys/${encodeURIComponent(id)}`, 'DELETE');
  }

  // -- History series -------------------------------------------------------

  historyThroughput(query: { since_ms?: number; bucket_secs?: number; limit?: number } = {}): Promise<ThroughputResponse> {
    return this.request<ThroughputResponse>(`/history/throughput${buildQuery(query)}`);
  }

  historyActivity(query: { since_ms?: number; bucket_secs?: number; limit?: number } = {}): Promise<ActivityResponse> {
    return this.request<ActivityResponse>(`/history/activity${buildQuery(query)}`);
  }

  historyMetrics(query: { since_ms?: number; limit?: number } = {}): Promise<HistoryMetricsResponse> {
    return this.request<HistoryMetricsResponse>(`/history/metrics${buildQuery(query)}`);
  }

  /** `POST /dashboard/logout` — clears the session cookie. */
  async logout(): Promise<void> {
    await this.fetchImpl('/dashboard/logout', { method: 'POST', credentials: 'include' });
  }

  /**
   * Lightweight protected-endpoint probe (finding 7): a cheap GET used by the WS layer
   * after a transient drop to decide reconnect-vs-logout. Returns `true` if the session is
   * still valid, `false` ONLY on a `401`. It does NOT fire `onUnauthorized` itself (the
   * caller decides); a non-401 error (network) resolves `true` so a blip reconnects rather
   * than logging the user out. Probes `/metrics` (a small, always-present read).
   */
  async probeAuth(): Promise<boolean> {
    try {
      const res = await this.fetchImpl(`${this.basePath}/metrics`, { credentials: 'include' });
      return res.status !== 401;
    } catch {
      // Network failure ≠ auth failure: stay logged in, let the socket reconnect.
      return true;
    }
  }

  // -- Reads (cursor-bearing) ----------------------------------------------

  flows(query: FlowsQuery = {}): Promise<FlowsResponse> {
    const qs = buildQuery(query);
    return this.request<FlowsResponse>(`/flows${qs}`);
  }

  flowDetail(id: string): Promise<FlowDetail> {
    return this.request<FlowDetail>(`/flows/${encodeURIComponent(id)}`);
  }

  metrics(): Promise<MetricsResponse> {
    return this.request<MetricsResponse>('/metrics');
  }

  topology(): Promise<TopologyResponse> {
    return this.request<TopologyResponse>('/topology');
  }

  /** Bare array — no cursor (D13: static-ish catalog read). */
  catalog(): Promise<CatalogEntry[]> {
    return this.request<CatalogEntry[]>('/catalog');
  }

  snapshot(atMs: number): Promise<SnapshotResponse> {
    return this.request<SnapshotResponse>(`/snapshot?at=${encodeURIComponent(String(atMs))}`);
  }

  // -- Durable history (SQL-backed; 503 when no SQL store is configured) ----

  /** `GET /history/sessions` — recent session-tree nodes (roots only unless `roots: false`). */
  historySessions(query: { since_ms?: number; limit?: number; roots?: boolean } = {}): Promise<SessionsResponse> {
    return this.request<SessionsResponse>(`/history/sessions${buildQuery(query)}`);
  }

  /** `GET /history/sessions/:id` — one node with its ancestors, children and newest requests. */
  historySession(id: string, query: { limit?: number } = {}): Promise<SessionDetailResponse> {
    return this.request<SessionDetailResponse>(`/history/sessions/${encodeURIComponent(id)}${buildQuery(query)}`);
  }

  /**
   * `GET /history/requests/:id/body?hop=` — the FULL reassembled request body of one hop (the
   * content store joins the skeleton with its items). The response IS the body, not an envelope.
   */
  historyRequestBody(id: string, hop: HistoryBodyHop = 'client_in'): Promise<unknown> {
    return this.request<unknown>(`/history/requests/${encodeURIComponent(id)}/body?hop=${hop}`);
  }

  // -- Mutation (CSRF-gated) ------------------------------------------------

  /** `POST /flows/:id/kill` — attaches `X-CSRF-Token` (D7). */
  kill(id: string): Promise<KillResponse> {
    const csrf = this.getCsrfToken();
    const headers: Record<string, string> = {};
    if (csrf) headers['X-CSRF-Token'] = csrf;
    return this.request<KillResponse>(`/flows/${encodeURIComponent(id)}/kill`, {
      method: 'POST',
      headers,
    });
  }
}

/** Serializes a query object into a `?a=b&c=d` string, dropping undefined/null values. */
function buildQuery(query: object): string {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(query as Record<string, unknown>)) {
    if (v !== undefined && v !== null) params.set(k, String(v));
  }
  const s = params.toString();
  return s ? `?${s}` : '';
}

/**
 * Reads the double-submit CSRF token from a non-HttpOnly cookie (D7). The Rust shell
 * sets `csrf_token` in both a cookie and the SPA bootstrap; this reads the cookie form.
 */
export function readCsrfCookie(cookieName = 'llmconduit_csrf'): string | null {
  if (typeof document === 'undefined') return null;
  const match = document.cookie.split('; ').find((c) => c.startsWith(`${cookieName}=`));
  return match ? decodeURIComponent(match.slice(cookieName.length + 1)) : null;
}
