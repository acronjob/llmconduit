import { useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getConnection } from '../../api/connection';
import type {
  AuthApiKey,
  AuthPolicy,
  AuthSession,
  AuthUsageRow,
  CreatedAuthApiKey,
} from '../../api/types';
import { Button } from '../../components/ui/Button';
import { Panel } from '../../components/ui/Panel';
import { cn } from '../../lib/cn';

const accessQueryKey = ['auth', 'access'] as const;

async function loadAccess() {
  const { client } = getConnection();
  const [summary, users, groups, roles, policies, apiKeys, sessions, usage, audit, pricing] = await Promise.all([
    client.authSummary(), client.authUsers(), client.authGroups(), client.authRoles(),
    client.authPolicies(), client.authApiKeys(), client.authSessions(), client.authUsage(),
    client.authAudit(), client.authPricing(),
  ]);
  return { summary, users, groups, roles, policies, apiKeys, sessions, usage, audit, pricing };
}

function Cell({ children, muted }: { children: ReactNode; muted?: boolean }) {
  return <td className={cn('border-t border-line px-3 py-2 align-top', muted && 'text-text-muted')}>{children}</td>;
}

function Section({ title, count, children }: { title: string; count?: number; children: ReactNode }) {
  return (
    <Panel className="min-w-0 overflow-hidden">
      <div className="flex items-center justify-between border-b border-line px-3 py-2">
        <h2 className="text-xs font-semibold uppercase tracking-[0.14em]">{title}</h2>
        {count !== undefined && <span className="font-mono text-xs text-text-muted">{count}</span>}
      </div>
      {children}
    </Panel>
  );
}

function Status({ enabled }: { enabled: boolean }) {
  return <span className={cn('rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase', enabled ? 'bg-status-healthy/15 text-status-healthy' : 'bg-status-down/15 text-status-down')}>{enabled ? 'active' : 'disabled'}</span>;
}

function timestamp(value: string | null): string {
  return value ? value.replace('T', ' ').replace('Z', ' UTC') : '—';
}

function cost(row: AuthUsageRow): string {
  return row.cost === null ? '—' : `$${row.cost.toFixed(4)}`;
}

export function AccessView() {
  const { client, queryClient } = getConnection();
  const query = useQuery({ queryKey: accessQueryKey, queryFn: loadAccess });
  const [userName, setUserName] = useState('');
  const [keyName, setKeyName] = useState('');
  const [keyPrincipal, setKeyPrincipal] = useState('');
  const [revealedKey, setRevealedKey] = useState<CreatedAuthApiKey | null>(null);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [draftEffect, setDraftEffect] = useState<AuthPolicy['effect']>('allow');
  const [draftName, setDraftName] = useState('New policy');
  const [draftModels, setDraftModels] = useState('gpt-*');
  const [draftProviders, setDraftProviders] = useState('openai');

  const users = query.data?.users.users ?? [];
  const principal = keyPrincipal || users[0]?.id || '';
  const draftPreview = useMemo(() => ({
    effect: draftEffect,
    models: draftModels.split(',').map((v) => v.trim()).filter(Boolean),
    providers: draftProviders.split(',').map((v) => v.trim()).filter(Boolean),
  }), [draftEffect, draftModels, draftProviders]);

  async function run(action: () => Promise<unknown>) {
    setBusy(true);
    setActionError(null);
    try {
      await action();
      await queryClient.invalidateQueries({ queryKey: accessQueryKey });
    } catch (error) {
      setActionError(error instanceof Error ? error.message : 'management action failed');
    } finally {
      setBusy(false);
    }
  }

  if (query.isLoading) return <div className="p-5 text-sm text-text-muted">Loading access control…</div>;
  if (query.isError || !query.data) return <div className="p-5 text-sm text-status-down">Access control unavailable: {query.error instanceof Error ? query.error.message : 'invalid response'}</div>;
  const data = query.data;

  return (
    <div className="min-h-0 flex-1 overflow-auto p-4" data-testid="access-view">
      <div className="mb-4 flex items-end justify-between">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-[0.2em] text-accent">Access control</div>
          <h1 className="mt-1 text-xl font-semibold">Principals, keys, policy, and accountability</h1>
          <p className="mt-1 text-xs text-text-muted">Policy epoch {data.summary.policy_epoch} · signed in as {data.summary.actor.display_name}</p>
        </div>
        <div className="font-mono text-xs text-text-muted">{data.summary.counts.active_sessions} active sessions</div>
      </div>
      {actionError && <div role="alert" className="mb-3 rounded border border-status-down/40 bg-status-down/10 px-3 py-2 text-xs text-status-down">{actionError}</div>}

      <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
        <Section title="Users and service accounts" count={users.length}>
          <form className="flex gap-2 p-3" onSubmit={(event) => { event.preventDefault(); if (!userName.trim()) return; void run(async () => { await client.createAuthUser({ display_name: userName.trim(), kind: 'user' }); setUserName(''); }); }}>
            <input aria-label="User display name" value={userName} onChange={(event) => setUserName(event.target.value)} placeholder="Display name" className="min-w-0 flex-1 rounded border border-line bg-bg px-2 py-1.5 text-sm outline-none focus:border-accent" />
            <Button disabled={busy || !userName.trim()} type="submit">Create user</Button>
          </form>
          <table className="w-full text-left text-xs"><thead className="text-text-muted"><tr><th className="px-3 py-1">Name</th><th className="px-3 py-1">Kind</th><th className="px-3 py-1">Status</th></tr></thead><tbody>
            {users.map((user) => <tr key={user.id}><Cell><div>{user.display_name}</div><div className="font-mono text-[10px] text-text-muted">{user.id}</div></Cell><Cell muted>{user.kind}</Cell><Cell><Status enabled={user.enabled} /></Cell></tr>)}
          </tbody></table>
        </Section>

        <Section title="API keys" count={data.apiKeys.api_keys.length}>
          <form className="grid grid-cols-[1fr_1fr_auto] gap-2 p-3" onSubmit={(event) => { event.preventDefault(); if (!principal || !keyName.trim()) return; void run(async () => { const created = await client.createAuthApiKey({ principal_id: principal, name: keyName.trim() }); setRevealedKey(created); setKeyName(''); }); }}>
            <select aria-label="Key owner" value={principal} onChange={(event) => setKeyPrincipal(event.target.value)} className="rounded border border-line bg-bg px-2 py-1.5 text-xs">{users.map((user) => <option key={user.id} value={user.id}>{user.display_name}</option>)}</select>
            <input aria-label="Key name" value={keyName} onChange={(event) => setKeyName(event.target.value)} placeholder="Key name" className="rounded border border-line bg-bg px-2 py-1.5 text-sm" />
            <Button disabled={busy || !principal || !keyName.trim()} type="submit">Create key</Button>
          </form>
          <table className="w-full text-left text-xs"><thead className="text-text-muted"><tr><th className="px-3 py-1">Key</th><th className="px-3 py-1">Last used</th><th className="px-3 py-1">Status</th><th className="px-3 py-1" /></tr></thead><tbody>
            {data.apiKeys.api_keys.map((key) => <KeyRow key={key.id} apiKey={key} busy={busy} onRevoke={() => void run(() => client.revokeAuthApiKey(key.id))} onRotate={() => void run(async () => setRevealedKey(await client.rotateAuthApiKey(key.id)))} />)}
          </tbody></table>
        </Section>

        <Section title="Groups and roles" count={data.groups.groups.length + data.roles.roles.length}>
          <div className="grid gap-2 p-3 sm:grid-cols-2">
            {data.groups.groups.map((group) => <div key={group.id} className="rounded border border-line bg-bg p-2 text-xs"><div className="font-medium">{group.name}</div><div className="mt-1 font-mono text-[10px] text-text-muted">{group.id} · {group.member_count} members</div></div>)}
            {data.roles.roles.map((role) => <div key={role.id} className="rounded border border-line bg-bg p-2 text-xs"><div className="flex justify-between"><span className="font-medium">{role.name}</span><Status enabled={role.enabled} /></div><div className="mt-2 flex flex-wrap gap-1">{role.permissions.map((permission) => <span key={permission} className="rounded bg-line/50 px-1 font-mono text-[9px]">{permission}</span>)}</div></div>)}
          </div>
        </Section>

        <Section title="Policy editor and effective preview" count={data.policies.policies.length}>
          <div className="grid gap-3 p-3 lg:grid-cols-2">
            <div className="space-y-2 text-xs">
              <label className="block">Name<input aria-label="Policy name" value={draftName} onChange={(event) => setDraftName(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5" /></label>
              <label className="block">Effect<select aria-label="Policy effect" value={draftEffect} onChange={(event) => setDraftEffect(event.target.value as AuthPolicy['effect'])} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5"><option value="allow">Allow</option><option value="deny">Deny</option></select></label>
              <label className="block">Models<input aria-label="Policy models" value={draftModels} onChange={(event) => setDraftModels(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5" /></label>
              <label className="block">Providers<input aria-label="Policy providers" value={draftProviders} onChange={(event) => setDraftProviders(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5" /></label>
              <Button disabled={busy || !draftName.trim()} onClick={() => void run(() => client.createAuthPolicy({ name: draftName.trim(), effect: draftEffect, subjects: ['grp_prod'], endpoints: ['*'], models: draftPreview.models, providers: draftPreview.providers }))}>Save reviewed policy</Button>
            </div>
            <div className={cn('rounded border p-3 text-xs', draftEffect === 'deny' ? 'border-status-down/50 bg-status-down/10' : 'border-status-healthy/40 bg-status-healthy/10')}>
              <div className="flex items-center justify-between"><span className="font-semibold uppercase tracking-wide">Effective preview</span><span className={cn('rounded px-1.5 py-0.5 font-bold uppercase', draftEffect === 'deny' ? 'bg-status-down/20 text-status-down' : 'bg-status-healthy/20 text-status-healthy')}>{draftEffect}</span></div>
              <div className="mt-3 text-text-muted">Models</div><div className="font-mono">{draftPreview.models.join(', ') || '—'}</div>
              <div className="mt-2 text-text-muted">Providers</div><div className="font-mono">{draftPreview.providers.join(', ') || '—'}</div>
            </div>
          </div>
          <div className="border-t border-line p-3 text-xs">{data.policies.policies.map((policy) => <div key={policy.id} className="mb-2 flex items-center gap-2"><span className={cn('rounded px-1.5 py-0.5 text-[10px] font-bold uppercase', policy.effect === 'deny' ? 'bg-status-down/20 text-status-down' : 'bg-status-healthy/15 text-status-healthy')}>{policy.effect}</span><span>{policy.name}</span><span className="ml-auto font-mono text-[10px] text-text-muted">{policy.models.join(', ')} · {policy.providers.join(', ')}</span></div>)}</div>
        </Section>

        <Section title="Usage and cost" count={data.usage.usage.length}>
          <table className="w-full text-left text-xs"><thead className="text-text-muted"><tr><th className="px-3 py-1">Attribution</th><th className="px-3 py-1">Requests</th><th className="px-3 py-1">Tokens</th><th className="px-3 py-1">Cost</th></tr></thead><tbody>{data.usage.usage.map((row) => <tr key={`${row.dimension}:${row.value}`}><Cell>{row.dimension}<div className="font-mono text-[10px] text-text-muted">{row.value}</div></Cell><Cell>{row.requests}</Cell><Cell>{row.prompt_tokens === null || row.completion_tokens === null ? '—' : (row.prompt_tokens + row.completion_tokens).toLocaleString()}</Cell><Cell><div>{cost(row)}</div><div className="text-[10px] uppercase text-text-muted">{row.cost_confidence}</div></Cell></tr>)}</tbody></table>
        </Section>

        <Section title="Active sessions" count={data.sessions.sessions.length}>
          <table className="w-full text-left text-xs"><thead className="text-text-muted"><tr><th className="px-3 py-1">Session</th><th className="px-3 py-1">Target</th><th className="px-3 py-1">Started</th><th className="px-3 py-1" /></tr></thead><tbody>{data.sessions.sessions.map((session) => <SessionRow key={session.id} session={session} busy={busy} onRevoke={() => void run(() => client.revokeAuthSession(session.id))} />)}</tbody></table>
        </Section>

        <Section title="Audit log" count={data.audit.events.length}>
          <table className="w-full text-left text-xs"><tbody>{data.audit.events.map((event) => <tr key={event.id}><Cell muted>{timestamp(event.timestamp)}</Cell><Cell><div>{event.action}</div><div className="font-mono text-[10px] text-text-muted">{event.actor} → {event.target}</div></Cell><Cell><span className={event.outcome === 'denied' ? 'text-status-down' : 'text-status-healthy'}>{event.outcome}</span></Cell></tr>)}</tbody></table>
        </Section>

        <Section title="Pricing provenance" count={data.pricing.pricing.length}>
          <table className="w-full text-left text-xs"><tbody>{data.pricing.pricing.map((row) => <tr key={`${row.provider}:${row.model}`}><Cell>{row.model}<div className="font-mono text-[10px] text-text-muted">{row.provider}</div></Cell><Cell>${row.input_per_1k} in / ${row.output_per_1k} out</Cell><Cell><div className="uppercase">{row.confidence}</div><div className="text-[10px] text-text-muted">{row.source}</div></Cell></tr>)}</tbody></table>
        </Section>
      </div>

      {revealedKey && <CopyOnceDialog created={revealedKey} onClose={() => setRevealedKey(null)} />}
    </div>
  );
}

function KeyRow({ apiKey, busy, onRevoke, onRotate }: { apiKey: AuthApiKey; busy: boolean; onRevoke: () => void; onRotate: () => void }) {
  return <tr><Cell><div>{apiKey.name}</div><div className="font-mono text-[10px] text-text-muted">{apiKey.prefix}… · {apiKey.id}</div></Cell><Cell muted>{timestamp(apiKey.last_used_at)}</Cell><Cell><Status enabled={apiKey.enabled} /></Cell><Cell><div className="flex justify-end gap-1"><Button variant="ghost" disabled={busy || !apiKey.enabled} onClick={onRotate}>Rotate</Button><Button variant="danger" disabled={busy || !apiKey.enabled} onClick={onRevoke}>Revoke</Button></div></Cell></tr>;
}

function SessionRow({ session, busy, onRevoke }: { session: AuthSession; busy: boolean; onRevoke: () => void }) {
  return <tr><Cell><div>{session.kind}</div><div className="font-mono text-[10px] text-text-muted">{session.id}</div></Cell><Cell>{session.endpoint ?? 'dashboard'}<div className="font-mono text-[10px] text-text-muted">{session.requested_model ?? session.principal_id}</div></Cell><Cell muted>{timestamp(session.started_at)}</Cell><Cell><Button variant="danger" disabled={busy} onClick={onRevoke}>Terminate</Button></Cell></tr>;
}

function CopyOnceDialog({ created, onClose }: { created: CreatedAuthApiKey; onClose: () => void }) {
  const [copied, setCopied] = useState(false);
  return <div role="dialog" aria-modal="true" aria-label="Copy API key" className="fixed inset-0 z-50 grid place-items-center bg-bg/80 p-4"><Panel raised className="w-full max-w-xl p-5 shadow-2xl"><div className="text-[10px] font-bold uppercase tracking-[0.18em] text-status-cooling">Copy once</div><h2 className="mt-1 text-lg font-semibold">API key created</h2><p className="mt-2 text-xs text-text-muted">This raw secret will not be shown or returned by the API again.</p><code className="mt-4 block break-all rounded border border-line bg-bg p-3 text-sm text-accent" data-testid="raw-api-key">{created.raw_key}</code><div className="mt-4 flex justify-end gap-2"><Button onClick={() => { void navigator.clipboard?.writeText(created.raw_key); setCopied(true); }}>{copied ? 'Copied' : 'Copy key'}</Button><Button variant="ghost" onClick={onClose}>I stored it</Button></div></Panel></div>;
}
