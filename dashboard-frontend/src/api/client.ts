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
  AuthApiKeysResponse,
  AuthAuditResponse,
  AuthGroupsResponse,
  AuthPoliciesResponse,
  AuthPricingResponse,
  AuthRolesResponse,
  AuthSessionsResponse,
  AuthSummary,
  AuthUsageResponse,
  AuthUsersResponse,
  CatalogEntry,
  CreateAuthApiKeyRequest,
  CreateAuthUserRequest,
  CreatedAuthApiKey,
  FlowDetail,
  FlowsQuery,
  FlowsResponse,
  KillResponse,
  LoginRequest,
  MetricsResponse,
  SnapshotResponse,
  TopologyResponse,
  AuthSummary,
  ApiKeyListResponse,
  CreatedApiKey,
  CreateApiKeyRequest,
} from './types';
import {
  isAuthApiKeysResponse,
  isAuthAuditResponse,
  isAuthGroupsResponse,
  isAuthPoliciesResponse,
  isAuthPricingResponse,
  isAuthRolesResponse,
  isAuthSessionsResponse,
  isAuthSummary,
  isAuthUsageResponse,
  isAuthUsersResponse,
  isCreatedAuthApiKey,
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

  private async request<T>(path: string, init?: RequestInit, guard?: (value: unknown) => value is T): Promise<T> {
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
    const value: unknown = text ? JSON.parse(text) : undefined;
    if (guard && !guard(value)) throw new Error(`${path} returned an invalid response`);
    return value as T;
  }

  // -- Auth -----------------------------------------------------------------

  /** `POST /dashboard/login` — note: login lives at /dashboard, NOT under /api. */
  async login(body: LoginRequest): Promise<void> {
    const res = await this.fetchImpl('/dashboard/login', {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`login failed: ${res.status}`);
    }
  }

  /** Delegated management login backed by an API key. */
  async keyLogin(apiKey: string): Promise<void> {
    const res = await this.fetchImpl('/dashboard/auth/key-login', {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: apiKey }),
    });
    if (!res.ok) throw new Error(`key login failed: ${res.status}`);
  }

  /** `POST /dashboard/auth/logout` — revokes delegated sessions and clears cookies. */
  async logout(): Promise<void> {
    const csrf = this.getCsrfToken();
    await this.fetchImpl('/dashboard/auth/logout', {
      method: 'POST',
      credentials: 'include',
      headers: csrf ? { 'X-CSRF-Token': csrf } : undefined,
    });
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

  // -- Access management ----------------------------------------------------

  authSummary(): Promise<AuthSummary> {
    return this.request('/auth/summary', undefined, isAuthSummary);
  }
  authUsers(): Promise<AuthUsersResponse> {
    return this.request('/auth/users', undefined, isAuthUsersResponse);
  }
  authGroups(): Promise<AuthGroupsResponse> {
    return this.request('/auth/groups', undefined, isAuthGroupsResponse);
  }
  authRoles(): Promise<AuthRolesResponse> {
    return this.request('/auth/roles', undefined, isAuthRolesResponse);
  }
  authPolicies(): Promise<AuthPoliciesResponse> {
    return this.request('/auth/policies', undefined, isAuthPoliciesResponse);
  }
  authApiKeys(): Promise<AuthApiKeysResponse> {
    return this.request('/auth/api-keys', undefined, isAuthApiKeysResponse);
  }
  authSessions(): Promise<AuthSessionsResponse> {
    return this.request('/auth/sessions', undefined, isAuthSessionsResponse);
  }
  authUsage(): Promise<AuthUsageResponse> {
    return this.request('/auth/usage', undefined, isAuthUsageResponse);
  }
  authAudit(): Promise<AuthAuditResponse> {
    return this.request('/auth/audit', undefined, isAuthAuditResponse);
  }
  authPricing(): Promise<AuthPricingResponse> {
    return this.request('/auth/pricing', undefined, isAuthPricingResponse);
  }

  createAuthUser(body: CreateAuthUserRequest): Promise<AuthUsersResponse> {
    return this.authMutation('/auth/users', body, isAuthUsersResponse);
  }
  createAuthApiKey(body: CreateAuthApiKeyRequest): Promise<CreatedAuthApiKey> {
    return this.authMutation('/auth/api-keys', body, isCreatedAuthApiKey);
  }
  revokeAuthApiKey(id: string): Promise<AuthApiKeysResponse> {
    return this.authMutation(`/auth/api-keys/${encodeURIComponent(id)}/revoke`, undefined, isAuthApiKeysResponse);
  }
  rotateAuthApiKey(id: string): Promise<CreatedAuthApiKey> {
    return this.authMutation(`/auth/api-keys/${encodeURIComponent(id)}/rotate`, undefined, isCreatedAuthApiKey);
  }
  revokeAuthSession(id: string): Promise<AuthSessionsResponse> {
    return this.authMutation(`/auth/sessions/${encodeURIComponent(id)}/revoke`, undefined, isAuthSessionsResponse);
  }

  private authMutation<T>(
    path: string,
    body: unknown,
    guard: (value: unknown) => value is T,
  ): Promise<T> {
    const csrf = this.getCsrfToken();
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (csrf) headers['X-CSRF-Token'] = csrf;
    return this.request(path, {
      method: 'POST',
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    }, guard);
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

  authSummary(): Promise<AuthSummary> {
    return this.request<AuthSummary>('/auth/summary');
  }

  apiKeys(): Promise<ApiKeyListResponse> {
    return this.request<ApiKeyListResponse>('/auth/api-keys');
  }

  createApiKey(body: CreateApiKeyRequest): Promise<CreatedApiKey> {
    return this.authMutation<CreatedApiKey>('/auth/api-keys', body);
  }

  revokeApiKey(id: string): Promise<void> {
    return this.authMutation<void>(`/auth/api-keys/${encodeURIComponent(id)}/revoke`);
  }

  private authMutation<T>(path: string, body?: unknown): Promise<T> {
    const csrf = this.getCsrfToken();
    const headers: Record<string, string> = {};
    if (csrf) headers['X-CSRF-Token'] = csrf;
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    return this.request<T>(path, {
      method: 'POST',
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  }
}

/** Serializes a flows query into a `?a=b&c=d` string, dropping undefined values. */
function buildQuery(query: FlowsQuery): string {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(query)) {
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
