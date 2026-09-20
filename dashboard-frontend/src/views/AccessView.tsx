import { useCallback, useEffect, useState } from 'react';
import { getConnection } from '../api/connection';
import type { ApiKeySummary, AuthSummary, CreatedApiKey } from '../api/types';
import { Button } from '../components/ui/Button';
import { Panel } from '../components/ui/Panel';

export function AccessView() {
  const { client } = getConnection();
  const [summary, setSummary] = useState<AuthSummary | null>(null);
  const [keys, setKeys] = useState<ApiKeySummary[]>([]);
  const [principal, setPrincipal] = useState('service account');
  const [name, setName] = useState('default');
  const [models, setModels] = useState('*');
  const [created, setCreated] = useState<CreatedApiKey | null>(null);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [nextSummary, response] = await Promise.all([client.authSummary(), client.apiKeys()]);
      setSummary(nextSummary);
      setKeys(response.data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unable to load access controls');
    }
  }, [client]);

  useEffect(() => { void refresh(); }, [refresh]);

  const create = async () => {
    try {
      const key = await client.createApiKey({
        principal_name: principal,
        name,
        endpoints: ['*'],
        models: models.split(',').map((model) => model.trim()).filter(Boolean),
      });
      setCreated(key);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unable to create API key');
    }
  };

  const revoke = async (id: string) => {
    try {
      await client.revokeApiKey(id);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unable to revoke API key');
    }
  };

  return (
    <main className="space-y-4 p-5" data-testid="access-view">
      <header>
        <h1 className="font-ui text-xl font-semibold text-text">Access control</h1>
        <p className="mt-1 text-xs text-text-muted">
          API-key lifecycle and model grants · {summary?.mode ?? 'loading'}
        </p>
      </header>
      {error && <div className="rounded border border-red-500/40 bg-red-500/10 p-3 text-xs text-red-300">{error}</div>}
      {created && (
        <Panel className="p-4" data-testid="created-key">
          <div className="text-xs font-semibold uppercase tracking-wider text-amber-300">Copy once</div>
          <code className="mt-2 block break-all rounded bg-bg p-3 text-xs text-text">{created.raw_key}</code>
          <Button className="mt-2" variant="ghost" onClick={() => setCreated(null)}>Dismiss secret</Button>
        </Panel>
      )}
      <Panel className="p-4">
        <h2 className="mb-3 text-sm font-semibold text-text">Create key</h2>
        <div className="grid gap-3 md:grid-cols-3">
          <input className="rounded border border-line bg-bg px-3 py-2 text-xs text-text" value={principal} onChange={(e) => setPrincipal(e.target.value)} aria-label="Principal name" />
          <input className="rounded border border-line bg-bg px-3 py-2 text-xs text-text" value={name} onChange={(e) => setName(e.target.value)} aria-label="Key name" />
          <input className="rounded border border-line bg-bg px-3 py-2 text-xs text-text" value={models} onChange={(e) => setModels(e.target.value)} aria-label="Allowed models" />
        </div>
        <Button className="mt-3" onClick={() => void create()}>Create API key</Button>
      </Panel>
      <Panel className="p-4">
        <h2 className="mb-3 text-sm font-semibold text-text">API keys</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="text-text-muted"><tr><th className="py-2">Name</th><th>Principal</th><th>Prefix</th><th>Status</th><th /></tr></thead>
            <tbody>
              {keys.map((key) => (
                <tr key={key.id} className="border-t border-line">
                  <td className="py-2 text-text">{key.name}</td><td className="font-mono text-text-muted">{key.principal_id}</td>
                  <td className="font-mono text-text">{key.prefix}…</td><td>{key.enabled ? 'active' : 'revoked'}</td>
                  <td className="text-right">{key.enabled && <Button variant="ghost" onClick={() => void revoke(key.id)}>Revoke</Button>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </main>
  );
}
