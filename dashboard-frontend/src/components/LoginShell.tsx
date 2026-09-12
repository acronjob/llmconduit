/**
 * Login shell — rendered when the SPA loads unauthenticated (D7/D9). Username + password by
 * default once accounts exist (`auth_mode: users`), the shared access token otherwise; either
 * form can be switched to. On success the server sets the session cookie and we flip the auth
 * store (and remember the user) so the dashboard mounts. Any 401 elsewhere bounces back here.
 */
import { useState, type FormEvent } from 'react';
import { Panel } from './ui/Panel';
import { Button } from './ui/Button';
import { authStore } from '../store/authStore';
import { useAuth } from '../store/hooks';
import type { DashboardClient } from '../api/client';

const INPUT = 'rounded-md border border-line bg-panel-raised px-3 py-2 font-mono text-sm text-text outline-none focus:border-accent';

export function LoginShell({ client }: { client: DashboardClient }) {
  const authMode = useAuth((s) => s.authMode);
  const [tokenMode, setTokenMode] = useState(authMode !== 'users');
  const [token, setToken] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const { user } = await client.login(tokenMode ? { token } : { username, password });
      // Server set the session cookie; reflect it in the store to mount the dashboard.
      authStore.getState().setUser(user);
      authStore.getState().setAuthenticated(true);
    } catch {
      setError(tokenMode ? 'Invalid token.' : 'Invalid username or password.');
    } finally {
      setBusy(false);
    }
  }

  const canSubmit = tokenMode ? token.length > 0 : username.length > 0 && password.length > 0;

  return (
    <div className="flex h-full items-center justify-center bg-bg p-4">
      <Panel className="w-full max-w-sm p-6">
        <h1 className="mb-1 text-lg font-semibold text-text">llmconduit</h1>
        <p className="mb-4 text-sm text-text-muted" data-testid="login-subtitle">
          {tokenMode ? 'Dashboard access token required.' : 'Sign in with your username and password.'}
        </p>
        <form onSubmit={onSubmit} className="flex flex-col gap-3" data-testid="login-form" data-mode={tokenMode ? 'token' : 'users'}>
          {tokenMode ? (
            <label className="flex flex-col gap-1 text-sm">
              <span className="text-text-muted">Token</span>
              <input type="password" autoFocus value={token} onChange={(e) => setToken(e.target.value)} className={INPUT} placeholder="LLMCONDUIT_DASHBOARD_TOKEN" aria-label="Dashboard token" />
            </label>
          ) : (
            <>
              <label className="flex flex-col gap-1 text-sm">
                <span className="text-text-muted">Username</span>
                <input type="text" autoFocus autoComplete="username" value={username} onChange={(e) => setUsername(e.target.value)} className={INPUT} aria-label="Username" />
              </label>
              <label className="flex flex-col gap-1 text-sm">
                <span className="text-text-muted">Password</span>
                <input type="password" autoComplete="current-password" value={password} onChange={(e) => setPassword(e.target.value)} className={INPUT} aria-label="Password" />
              </label>
            </>
          )}
          {error && <p role="alert" className="text-sm text-status-down">{error}</p>}
          <Button type="submit" disabled={busy || !canSubmit}>
            {busy ? 'Signing in…' : 'Sign in'}
          </Button>
          <button type="button" className="text-left text-xs text-text-muted hover:text-text" onClick={() => { setTokenMode((m) => !m); setError(null); }} data-testid="login-toggle">
            {tokenMode ? 'Use a username and password instead' : 'Use an access token instead'}
          </button>
        </form>
      </Panel>
    </div>
  );
}
