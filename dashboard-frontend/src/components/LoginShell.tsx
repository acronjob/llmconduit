/**
 * Login shell — rendered when the SPA loads unauthenticated (D7/D9). A token-entry form
 * accepts either the bootstrap dashboard token or an `llmc_` delegated management key.
 * On success the server sets a session cookie and the dashboard mounts.
 */
import { useState, type FormEvent } from 'react';
import { Panel } from './ui/Panel';
import { Button } from './ui/Button';
import { authStore } from '../store/authStore';
import type { DashboardClient } from '../api/client';

export function LoginShell({ client }: { client: DashboardClient }) {
  const [token, setToken] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      if (token.startsWith('llmc_')) await client.keyLogin(token);
      else await client.login({ token });
      // Server set the session cookie; reflect it in the store to mount the dashboard.
      authStore.getState().setAuthenticated(true);
    } catch {
      setError('Invalid token.');
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="flex h-full items-center justify-center bg-bg p-4">
      <Panel className="w-full max-w-sm p-6">
        <h1 className="mb-1 text-lg font-semibold text-text">llmconduit</h1>
        <p className="mb-4 text-sm text-text-muted">Bootstrap token or management-enabled API key required.</p>
        <form onSubmit={onSubmit} className="flex flex-col gap-3">
          <label className="flex flex-col gap-1 text-sm">
            <span className="text-text-muted">Token</span>
            <input
              type="password"
              autoFocus
              value={token}
              onChange={(e) => setToken(e.target.value)}
              className="rounded-md border border-line bg-panel-raised px-3 py-2 font-mono text-sm text-text outline-none focus:border-accent"
              placeholder="Dashboard token or llmc_…"
              aria-label="Dashboard token"
            />
          </label>
          {error && <p role="alert" className="text-sm text-status-down">{error}</p>}
          <Button type="submit" disabled={busy || token.length === 0}>
            {busy ? 'Signing in…' : 'Sign in'}
          </Button>
        </form>
      </Panel>
    </div>
  );
}
