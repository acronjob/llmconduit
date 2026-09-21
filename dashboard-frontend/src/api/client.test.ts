import { describe, it, expect, beforeEach, vi } from 'vitest';
import { DashboardClient, UnauthorizedError, readCsrfCookie } from './client';
import { mockFetch, mockKillLog } from './mock';

describe('DashboardClient — kill includes X-CSRF-Token', () => {
  beforeEach(() => {
    mockKillLog.length = 0;
  });

  it('attaches the CSRF token header on the kill POST (via the mock backend)', async () => {
    const client = new DashboardClient({
      fetchImpl: mockFetch,
      getCsrfToken: () => 'mock-csrf-token',
    });
    // `:id` == api_call_id (D13 contract).
    const res = await client.kill('api_001');
    expect(res.killed).toBe(true);
    expect(mockKillLog).toHaveLength(1);
    expect(mockKillLog[0]).toEqual({ id: 'api_001', csrf: 'mock-csrf-token' });
  });

  it('mock backend rejects a kill with no CSRF token (403)', async () => {
    const client = new DashboardClient({
      fetchImpl: mockFetch,
      getCsrfToken: () => null,
    });
    await expect(client.kill('api_001')).rejects.toThrow(/403/);
    expect(mockKillLog[0]?.csrf).toBeNull();
  });

  it('mock backend 404s a kill for an unknown api_call_id (finding 7)', async () => {
    const client = new DashboardClient({
      fetchImpl: mockFetch,
      getCsrfToken: () => 'mock-csrf-token',
    });
    // A response_id is NOT a valid kill key — `:id` must be api_call_id.
    await expect(client.kill('resp_001')).rejects.toThrow(/404/);
  });
});

describe('DashboardClient — 401 bounce-to-login', () => {
  it('fires onUnauthorized and throws UnauthorizedError on any 401', async () => {
    const onUnauthorized = vi.fn();
    const fetch401: typeof globalThis.fetch = async () =>
      new Response('nope', { status: 401 });
    const client = new DashboardClient({
      fetchImpl: fetch401,
      onUnauthorized,
    });
    await expect(client.flows()).rejects.toBeInstanceOf(UnauthorizedError);
    expect(onUnauthorized).toHaveBeenCalledOnce();
  });
});

describe('DashboardClient — typed reads against the D13 shapes (mock)', () => {
  it('flows() returns the cursor-bearing FlowsResponse', async () => {
    const client = new DashboardClient({ fetchImpl: mockFetch });
    const res = await client.flows();
    expect(typeof res.flow_seq).toBe('number');
    expect(Array.isArray(res.flows)).toBe(true);
  });

  it('catalog() returns a BARE array (no cursor) with a NULLABLE context_limit', async () => {
    const client = new DashboardClient({ fetchImpl: mockFetch });
    const cat = await client.catalog();
    expect(Array.isArray(cat)).toBe(true);
    const first = cat[0];
    expect(first).toBeDefined();
    expect(first).toHaveProperty('context_limit');
    // gap 06: a real window surfaces as a number...
    expect(typeof first?.context_limit).toBe('number');
    // ...and a model with no advertised window surfaces as `null` (unavailable),
    // NEVER a non-null `0` (the lie-with-zeros the gap removed).
    const unavailable = cat.find((e) => e.id === 'mystery-model');
    expect(unavailable).toBeDefined();
    expect(unavailable?.context_limit ?? null).toBeNull();
    expect(unavailable?.context_limit).not.toBe(0);
  });

  it('providers() and providerMetrics() return exact provider inventory and cache samples', async () => {
    const client = new DashboardClient({ fetchImpl: mockFetch });
    const providers = await client.providers();
    expect(providers.providers.find((provider) => provider.provider_id === 'vllm-a')?.models.map((model) => model.id)).toContain('llama-3.1-70b');
    expect(providers.providers.find((provider) => provider.provider_id === 'vllm-a')?.availability?.timezone).toBe('America/Chicago');

    const metrics = await client.providerMetrics();
    const vllm = metrics.providers.find((provider) => provider.provider === 'vllm-a');
    expect(vllm?.source).toBe('vllm');
    expect(vllm?.cache_hit_rate).toBeGreaterThan(0);
  });
});

describe('DashboardClient — access management runtime validation', () => {
  it('validates management responses and sends CSRF for mutations', async () => {
    const client = new DashboardClient({ fetchImpl: mockFetch, getCsrfToken: () => 'test-csrf' });
    const summary = await client.authSummary();
    expect(summary.policy_epoch).toBeGreaterThan(0);
    const users = await client.authUsers();
    const created = await client.createAuthApiKey({ principal_id: users.users[0]!.id, name: 'test' });
    expect(created.raw_key).toMatch(/^llmc_/);
    const revoked = await client.revokeAuthApiKey(created.id);
    expect(revoked.api_keys.find((key) => key.id === created.id)?.enabled).toBe(false);
  });

  it('rejects an invalid management response before it reaches the UI', async () => {
    const fetchInvalid: typeof fetch = async () => new Response(JSON.stringify({ users: [{ id: 7 }] }), { status: 200 });
    const client = new DashboardClient({ fetchImpl: fetchInvalid });
    await expect(client.authUsers()).rejects.toThrow(/invalid response/);
  });

  it('accepts the backend audit outcome used for successful management events', async () => {
    const fetchAudit: typeof fetch = async () => new Response(JSON.stringify({
      events: [{
        id: '1',
        timestamp: '2026-09-20T12:06:19+00:00',
        actor: 'bootstrap',
        action: 'bootstrap.created',
        target: 'key_bootstrap',
        outcome: 'ok',
        metadata: {},
      }],
    }), { status: 200 });
    const client = new DashboardClient({ fetchImpl: fetchAudit });

    await expect(client.authAudit()).resolves.toMatchObject({
      events: [{ outcome: 'ok' }],
    });
  });
});

describe('readCsrfCookie', () => {
  it('reads the double-submit token from the non-HttpOnly cookie', () => {
    document.cookie = 'llmconduit_csrf=abc123';
    expect(readCsrfCookie()).toBe('abc123');
  });
});
