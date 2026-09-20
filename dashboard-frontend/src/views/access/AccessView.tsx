import { useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getConnection } from '../../api/connection';
import type {
  AuthApiKey,
  AuthPolicy,
  AuthSession,
  AuthUsageRow,
  AuthUser,
  CreateAuthPolicyRequest,
  CreatedAuthApiKey,
  ManagementPermission,
} from '../../api/types';
import { Button } from '../../components/ui/Button';
import { Panel } from '../../components/ui/Panel';
import { cn } from '../../lib/cn';
import {
  buildAccessOverview,
  formatAccessList,
  type AccessAttentionItem,
  type AccessPrincipalRow,
} from './accessOverviewModel';
import { validatePolicy } from './policyEditorModel';

const accessQueryKey = ['auth', 'access'] as const;
const managementPermissions: ManagementPermission[] = [
  'auth.keys.read', 'auth.keys.create', 'auth.keys.revoke', 'auth.keys.rotate',
  'auth.principals.read', 'auth.principals.write', 'auth.groups.read', 'auth.groups.write',
  'auth.roles.read', 'auth.roles.write', 'auth.policies.read', 'auth.policies.write',
  'auth.usage.read', 'auth.audit.read', 'auth.pricing.read', 'auth.pricing.sync',
  'auth.pricing.write', 'auth.sessions.read', 'auth.sessions.terminate',
];
const accessTabs = ['overview', 'keys', 'people', 'policies', 'sessions', 'audit'] as const;
type AccessTab = typeof accessTabs[number];
type WizardStep = 'identity' | 'permissions' | 'limits' | 'review';

const csv = (value: string) => value.split(',').map((item) => item.trim()).filter(Boolean);
const positiveInteger = (value: string): number | null => value === '' ? null : Number(value);
const weekdays = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];
const weekdayMask = (value: string): number => csv(value).reduce((mask, day) => {
  const index = weekdays.indexOf(day.toLowerCase());
  return index < 0 ? mask : mask | (1 << index);
}, 0);
const minuteOfDay = (value: string): number => {
  const [hour = Number.NaN, minute = Number.NaN] = value.split(':').map(Number);
  return hour * 60 + minute;
};
const dataOrEmpty = <T,>(value: T[] | undefined): T[] => value ?? [];

async function loadAccess() {
  const { client } = getConnection();
  const [summary, users, groups, roles, policies, apiKeys, sessions, usage, audit, pricing] = await Promise.all([
    client.authSummary(), client.authUsers(), client.authGroups(), client.authRoles(),
    client.authPolicies(), client.authApiKeys(), client.authSessions(), client.authUsage(),
    client.authAudit(), client.authPricing(),
  ]);
  return { summary, users, groups, roles, policies, apiKeys, sessions, usage, audit, pricing };
}

export function AccessView() {
  const { client, queryClient } = getConnection();
  const query = useQuery({ queryKey: accessQueryKey, queryFn: loadAccess });
  const [tab, setTab] = useState<AccessTab>('overview');
  const [drawerOpen, setDrawerOpen] = useState(true);
  const [wizardStep, setWizardStep] = useState<WizardStep>('identity');
  const [userName, setUserName] = useState('');
  const [groupName, setGroupName] = useState('');
  const [groupMembers, setGroupMembers] = useState<string[]>([]);
  const [roleName, setRoleName] = useState('');
  const [rolePermissions, setRolePermissions] = useState<ManagementPermission[]>([]);
  const [keyName, setKeyName] = useState('');
  const [keyPrincipal, setKeyPrincipal] = useState('');
  const [revealedKey, setRevealedKey] = useState<CreatedAuthApiKey | null>(null);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [draftEffect, setDraftEffect] = useState<AuthPolicy['effect']>('allow');
  const [draftName, setDraftName] = useState('New policy');
  const [draftSubjects, setDraftSubjects] = useState<string[]>([]);
  const [draftEndpoints, setDraftEndpoints] = useState('responses');
  const [draftModels, setDraftModels] = useState('gpt-*');
  const [draftRequestedModels, setDraftRequestedModels] = useState('gpt-*');
  const [draftServedModels, setDraftServedModels] = useState('gpt-4.1');
  const [draftProviders, setDraftProviders] = useState('openai');
  const [draftRoutes, setDraftRoutes] = useState('cloud');
  const [draftWindowDays, setDraftWindowDays] = useState('mon,tue,wed,thu,fri');
  const [draftWindowStart, setDraftWindowStart] = useState('08:00');
  const [draftWindowEnd, setDraftWindowEnd] = useState('18:00');
  const [draftConcurrent, setDraftConcurrent] = useState('4');
  const [draftDailyStarts, setDraftDailyStarts] = useState('100');
  const [draftManagementPermissions, setDraftManagementPermissions] = useState<ManagementPermission[]>([]);

  const [wizardMode, setWizardMode] = useState<'existing' | 'new'>('new');
  const [wizardExistingPrincipal, setWizardExistingPrincipal] = useState('');
  const [wizardDisplayName, setWizardDisplayName] = useState('');
  const [wizardKind, setWizardKind] = useState<AuthUser['kind']>('user');
  const [wizardKeyName, setWizardKeyName] = useState('default key');
  const [wizardPolicyName, setWizardPolicyName] = useState('Standard inference access');
  const [wizardEndpoints, setWizardEndpoints] = useState('responses,chat');
  const [wizardModels, setWizardModels] = useState('gpt-*');
  const [wizardProviders, setWizardProviders] = useState('openai');
  const [wizardRoutes, setWizardRoutes] = useState('cloud');
  const [wizardDays, setWizardDays] = useState('mon,tue,wed,thu,fri');
  const [wizardStart, setWizardStart] = useState('08:00');
  const [wizardEnd, setWizardEnd] = useState('18:00');
  const [wizardConcurrent, setWizardConcurrent] = useState('4');
  const [wizardDailyStarts, setWizardDailyStarts] = useState('100');

  const users = useMemo(() => query.data?.users.users ?? [], [query.data?.users.users]);
  const principal = keyPrincipal || users[0]?.id || '';
  const selectedWizardPrincipal = wizardExistingPrincipal || users[0]?.id || '';
  const subjectOptions = useMemo(() => [
    ...users.map((item) => ({ id: `principal:${item.id}`, label: `principal · ${item.display_name}` })),
    ...dataOrEmpty(query.data?.groups.groups).map((item) => ({ id: `group:${item.id}`, label: `group · ${item.name}` })),
    ...dataOrEmpty(query.data?.roles.roles).map((item) => ({ id: `role:${item.id}`, label: `role · ${item.name}` })),
    ...dataOrEmpty(query.data?.apiKeys.api_keys).map((item) => ({ id: `key:${item.id}`, label: `key · ${item.name}` })),
  ], [query.data, users]);
  const draftPreview = useMemo<CreateAuthPolicyRequest>(() => ({
    name: draftName.trim(), effect: draftEffect, subjects: draftSubjects,
    endpoints: csv(draftEndpoints), requested_models: csv(draftRequestedModels),
    models: csv(draftModels),
    served_models: csv(draftServedModels), providers: csv(draftProviders), routes: csv(draftRoutes),
    time_windows: csv(draftWindowDays).length && draftWindowStart && draftWindowEnd
      ? [{ weekday_mask: weekdayMask(draftWindowDays), start_minute: minuteOfDay(draftWindowStart), end_minute: minuteOfDay(draftWindowEnd), absolute_start_ms: null, absolute_end_ms: null }]
      : [],
    max_concurrent_sessions: positiveInteger(draftConcurrent),
    max_daily_session_starts: positiveInteger(draftDailyStarts),
    management_permissions: draftManagementPermissions,
  }), [draftConcurrent, draftDailyStarts, draftEffect, draftEndpoints, draftName, draftProviders,
    draftManagementPermissions, draftModels, draftRequestedModels, draftRoutes, draftServedModels, draftSubjects, draftWindowDays,
    draftWindowEnd, draftWindowStart]);
  const wizardPolicy = useMemo<CreateAuthPolicyRequest>(() => ({
    name: wizardPolicyName.trim(),
    effect: 'allow',
    subjects: [],
    endpoints: csv(wizardEndpoints),
    models: csv(wizardModels),
    requested_models: csv(wizardModels),
    served_models: csv(wizardModels),
    providers: csv(wizardProviders),
    routes: csv(wizardRoutes),
    time_windows: csv(wizardDays).length && wizardStart && wizardEnd
      ? [{ weekday_mask: weekdayMask(wizardDays), start_minute: minuteOfDay(wizardStart), end_minute: minuteOfDay(wizardEnd), absolute_start_ms: null, absolute_end_ms: null }]
      : [],
    max_concurrent_sessions: positiveInteger(wizardConcurrent),
    max_daily_session_starts: positiveInteger(wizardDailyStarts),
    management_permissions: [],
  }), [wizardConcurrent, wizardDailyStarts, wizardDays, wizardEndpoints, wizardEnd, wizardModels, wizardPolicyName, wizardProviders, wizardRoutes, wizardStart]);
  const policyValidation = useMemo(() => validatePolicy(draftPreview), [draftPreview]);

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

  async function createWizardAccess() {
    await run(async () => {
      let principalId = selectedWizardPrincipal;
      if (wizardMode === 'new') {
        const existingIds = new Set(users.map((user) => user.id));
        const created = await client.createAuthUser({ display_name: wizardDisplayName.trim(), kind: wizardKind });
        const createdUser = created.users.find((user) => !existingIds.has(user.id));
        if (!createdUser) throw new Error('The identity was created, but its identifier was not returned. Create its policy and key from the existing-identity flow.');
        principalId = createdUser.id;
      }
      const policy: CreateAuthPolicyRequest = { ...wizardPolicy, subjects: [`principal:${principalId}`] };
      const validation = validatePolicy(policy);
      if (validation.length > 0) throw new Error(validation.join(' '));
      await client.createAuthPolicy(policy);
      const createdKey = await client.createAuthApiKey({ principal_id: principalId, name: wizardKeyName.trim() || 'default key' });
      setRevealedKey(createdKey);
      setDrawerOpen(false);
      setWizardStep('identity');
    });
  }

  if (query.isLoading) return <div className="p-5 text-sm text-text-muted">Loading access control...</div>;
  if (query.isError || !query.data) return <div className="p-5 text-sm text-status-down">Access control unavailable: {query.error instanceof Error ? query.error.message : 'invalid response'}</div>;
  const data = query.data;
  const overview = buildAccessOverview({
    users,
    groups: data.groups.groups,
    roles: data.roles.roles,
    policies: data.policies.policies,
    apiKeys: data.apiKeys.api_keys,
    sessions: data.sessions.sessions,
    usage: data.usage.usage,
  });

  return (
    <div className={cn('min-h-0 flex-1 overflow-auto p-5 transition-[margin]', drawerOpen && 'xl:mr-[28rem]')} data-testid="access-view">
      <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold">Access control</h1>
          <p className="mt-1 text-sm text-text-muted">Manage who can use which models, when, and how much.</p>
        </div>
        <div className="flex gap-2"><Button className="bg-accent text-white hover:bg-accent/85" onClick={() => { setDrawerOpen(true); setWizardMode('new'); setWizardStep('identity'); }}>＋ Create access</Button><Button variant="ghost" className="border-line" onClick={() => setTab('policies')}>＋ Create policy</Button></div>
      </div>

      {actionError && <div role="alert" className="mb-3 rounded border border-status-down/40 bg-status-down/10 px-3 py-2 text-xs text-status-down">{actionError}</div>}

      {tab === 'overview' && <AccessSummaryCards overview={overview} deniedToday={data.audit.events.filter((event) => event.outcome === 'denied').length} />}

      <div className="mb-3 mt-3 flex flex-wrap gap-2 border-b border-line" role="tablist" aria-label="Access sections">
        {accessTabs.map((item) => (
          <button
            key={item}
            role="tab"
            aria-selected={tab === item}
            className={cn('border-b-2 px-3 py-2 text-xs font-medium transition-colors', tab === item ? 'border-accent text-accent' : 'border-transparent text-text-muted hover:text-text')}
            onClick={() => setTab(item)}
          >
            {tabLabel(item)}
          </button>
        ))}
      </div>

      {tab === 'overview' && <AccessOverview overview={overview} onNavigate={setTab} />}
      {tab === 'keys' && (
        <ApiKeysSection
          users={users}
          apiKeys={data.apiKeys.api_keys}
          principal={principal}
          keyName={keyName}
          busy={busy}
          setKeyPrincipal={setKeyPrincipal}
          setKeyName={setKeyName}
          onCreateKey={() => void run(async () => {
            const created = await client.createAuthApiKey({ principal_id: principal, name: keyName.trim() });
            setRevealedKey(created);
            setKeyName('');
          })}
          onRevoke={(key) => void run(() => client.revokeAuthApiKey(key.id))}
          onRotate={(key) => void run(async () => setRevealedKey(await client.rotateAuthApiKey(key.id)))}
        />
      )}
      {tab === 'people' && (
        <PeopleSection
          users={users}
          groups={data.groups.groups}
          roles={data.roles.roles}
          userName={userName}
          groupName={groupName}
          groupMembers={groupMembers}
          roleName={roleName}
          rolePermissions={rolePermissions}
          busy={busy}
          setUserName={setUserName}
          setGroupName={setGroupName}
          setGroupMembers={setGroupMembers}
          setRoleName={setRoleName}
          setRolePermissions={setRolePermissions}
          onCreateUser={() => void run(async () => {
            await client.createAuthUser({ display_name: userName.trim(), kind: 'user' });
            setUserName('');
          })}
          onCreateGroup={() => void run(async () => {
            await client.createAuthGroup({ name: groupName.trim(), members: groupMembers });
            setGroupName('');
            setGroupMembers([]);
          })}
          onCreateRole={() => void run(async () => {
            await client.createAuthRole({ name: roleName.trim(), permissions: rolePermissions });
            setRoleName('');
            setRolePermissions([]);
          })}
        />
      )}
      {tab === 'policies' && (
        <PolicySection
          policies={data.policies.policies}
          subjectOptions={subjectOptions}
          draftPreview={draftPreview}
          policyValidation={policyValidation}
          busy={busy}
          draftEffect={draftEffect}
          draftName={draftName}
          draftSubjects={draftSubjects}
          draftEndpoints={draftEndpoints}
          draftModels={draftModels}
          draftRequestedModels={draftRequestedModels}
          draftServedModels={draftServedModels}
          draftProviders={draftProviders}
          draftRoutes={draftRoutes}
          draftWindowDays={draftWindowDays}
          draftWindowStart={draftWindowStart}
          draftWindowEnd={draftWindowEnd}
          draftConcurrent={draftConcurrent}
          draftDailyStarts={draftDailyStarts}
          draftManagementPermissions={draftManagementPermissions}
          setDraftEffect={setDraftEffect}
          setDraftName={setDraftName}
          setDraftSubjects={setDraftSubjects}
          setDraftEndpoints={setDraftEndpoints}
          setDraftModels={setDraftModels}
          setDraftRequestedModels={setDraftRequestedModels}
          setDraftServedModels={setDraftServedModels}
          setDraftProviders={setDraftProviders}
          setDraftRoutes={setDraftRoutes}
          setDraftWindowDays={setDraftWindowDays}
          setDraftWindowStart={setDraftWindowStart}
          setDraftWindowEnd={setDraftWindowEnd}
          setDraftConcurrent={setDraftConcurrent}
          setDraftDailyStarts={setDraftDailyStarts}
          setDraftManagementPermissions={setDraftManagementPermissions}
          onSavePolicy={() => void run(() => client.createAuthPolicy(draftPreview))}
        />
      )}
      {tab === 'sessions' && (
        <SessionsSection
          sessions={data.sessions.sessions}
          busy={busy}
          onRevoke={(session) => void run(() => client.revokeAuthSession(session.id))}
        />
      )}
      {tab === 'audit' && <AuditCostSection usage={data.usage.usage} audit={data.audit.events} pricing={data.pricing.pricing} />}

      {drawerOpen && (
        <CreateAccessDrawer
          users={users}
          step={wizardStep}
          setStep={setWizardStep}
          busy={busy}
          mode={wizardMode}
          setMode={setWizardMode}
          existingPrincipal={selectedWizardPrincipal}
          setExistingPrincipal={setWizardExistingPrincipal}
          displayName={wizardDisplayName}
          setDisplayName={setWizardDisplayName}
          kind={wizardKind}
          setKind={setWizardKind}
          keyName={wizardKeyName}
          setKeyName={setWizardKeyName}
          policyName={wizardPolicyName}
          setPolicyName={setWizardPolicyName}
          endpoints={wizardEndpoints}
          setEndpoints={setWizardEndpoints}
          models={wizardModels}
          setModels={setWizardModels}
          providers={wizardProviders}
          setProviders={setWizardProviders}
          routes={wizardRoutes}
          setRoutes={setWizardRoutes}
          days={wizardDays}
          setDays={setWizardDays}
          start={wizardStart}
          setStart={setWizardStart}
          end={wizardEnd}
          setEnd={setWizardEnd}
          concurrent={wizardConcurrent}
          setConcurrent={setWizardConcurrent}
          dailyStarts={wizardDailyStarts}
          setDailyStarts={setWizardDailyStarts}
          onClose={() => setDrawerOpen(false)}
          onCreate={() => void createWizardAccess()}
        />
      )}

      {revealedKey && <CopyOnceDialog created={revealedKey} onClose={() => setRevealedKey(null)} />}
    </div>
  );
}

function AccessSummaryCards({ overview, deniedToday }: { overview: ReturnType<typeof buildAccessOverview>; deniedToday: number }) {
  const identities = overview.summaryCards.find((card) => card.id === 'identities');
  const keys = overview.summaryCards.find((card) => card.id === 'keys');
  const sessions = overview.summaryCards.find((card) => card.id === 'sessions');
  const cards = [
    { icon: '⌘', label: 'Active keys', value: keys?.value ?? 0, caption: keys?.caption ?? 'Unavailable', health: keys?.health ?? 'unavailable' },
    { icon: '♙', label: 'People & services', value: identities?.value ?? 0, caption: identities?.caption ?? 'Unavailable', health: identities?.health ?? 'unavailable' },
    { icon: '⌬', label: 'Active sessions', value: sessions?.value ?? 0, caption: sessions?.caption ?? 'Unavailable', health: sessions?.health ?? 'unavailable' },
    { icon: '◇', label: 'Denied today', value: deniedToday, caption: deniedToday === 0 ? 'No denied requests' : 'Review recent decisions', health: deniedToday === 0 ? 'healthy' : 'critical' },
  ] as const;
  return (
    <div className="grid gap-3 md:grid-cols-4">
      {cards.map((card) => {
        const status = card.health === 'critical' ? 'review' : card.health === 'unavailable' ? '—' : 'active';
        return (
          <Panel key={card.label} className="p-3">
            <div className="flex items-center gap-2">
              <span className={cn('text-xl', card.health === 'critical' ? 'text-status-down' : 'text-accent')}>{card.icon}</span>
              <div className="text-[10px] font-bold uppercase tracking-[0.16em] text-text-muted">{card.label}</div>
            </div>
            <div className="mt-1 flex items-end justify-between gap-2">
              <div className="font-mono text-2xl font-semibold">{card.value}</div>
              <div className={cn('text-[10px] font-semibold', card.health === 'critical' ? 'text-status-down' : card.health === 'unavailable' ? 'text-text-muted' : 'text-status-healthy')}>{status}</div>
            </div>
            <div className="text-[10px] text-text-muted">{card.caption}</div>
          </Panel>
        );
      })}
    </div>
  );
}

function AccessOverview({ overview, onNavigate }: { overview: ReturnType<typeof buildAccessOverview>; onNavigate: (tab: AccessTab) => void }) {
  const [search, setSearch] = useState('');
  const filteredRows = overview.principalRows.filter((row) => [row.name, row.id, ...row.allowedModels, ...row.providers].join(' ').toLowerCase().includes(search.trim().toLowerCase()));
  return <div className="space-y-4">
    <Panel className="p-4"><h2 className="text-sm font-semibold">How access works</h2><div className="mt-3 grid md:grid-cols-3"><StepCard index="1" title="Identity" detail="Add a person or service that needs access." /><StepCard index="2" title="Policy" detail="Choose which models they can use and set any limits." /><StepCard index="3" title="API key" detail="Create a secure key with the right permissions." /></div></Panel>
    <Panel className="overflow-hidden"><div className="flex flex-wrap items-center justify-between gap-3 border-b border-line px-4 py-3"><div><h2 className="text-sm font-semibold">Direct access</h2><p className="mt-0.5 text-[10px] text-text-muted">Policies assigned directly to each identity; group and role access is managed separately.</p></div><div className="flex items-center gap-2"><label className="relative"><span className="absolute left-2.5 top-1.5 text-text-muted">⌕</span><input aria-label="Search access" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search people, services, or models…" className="w-64 rounded border border-line bg-bg py-1.5 pl-7 pr-2 text-xs outline-none focus:border-accent" /></label><button className="rounded border border-line px-3 py-1.5 text-xs text-text-muted hover:text-text" onClick={() => setSearch('')}>⌁ Filter</button></div></div><div className="overflow-auto"><table className="w-full min-w-[860px] text-left text-xs"><thead className="text-text-muted"><tr><th className="px-4 py-2">Name</th><th className="px-3 py-2">Access</th><th className="px-3 py-2">Models</th><th className="px-3 py-2">Schedule</th><th className="px-3 py-2">Key health</th><th className="px-3 py-2">Last used</th><th className="px-2 py-2" /></tr></thead><tbody>{filteredRows.map((row) => <PrincipalAccessRow key={row.id} row={row} />)}</tbody></table>{filteredRows.length === 0 && <div className="p-6 text-center text-xs text-text-muted">No identities match this search.</div>}</div></Panel>
    <Panel className="overflow-hidden"><div className="flex items-center justify-between border-b border-line px-4 py-3"><h2 className="text-sm font-semibold">Needs attention</h2><span className="rounded-full bg-status-cooling/15 px-2 py-0.5 text-[10px] font-semibold text-status-cooling">{overview.attentionItems.length} items</span></div><div className="divide-y divide-line">{overview.attentionItems.length === 0 ? <div className="p-4 text-xs text-text-muted">No access-control issues detected.</div> : overview.attentionItems.map((item) => <AttentionItem key={item.id} item={item} onReview={() => onNavigate(item.id.startsWith('key:') || item.id.startsWith('nokey:') ? 'keys' : 'policies')} />)}</div></Panel>
  </div>;
}

function ApiKeysSection({ users, apiKeys, principal, keyName, busy, setKeyPrincipal, setKeyName, onCreateKey, onRevoke, onRotate }: {
  users: AuthUser[];
  apiKeys: AuthApiKey[];
  principal: string;
  keyName: string;
  busy: boolean;
  setKeyPrincipal: (value: string) => void;
  setKeyName: (value: string) => void;
  onCreateKey: () => void;
  onRevoke: (key: AuthApiKey) => void;
  onRotate: (key: AuthApiKey) => void;
}) {
  return (
    <Section title="API keys" count={apiKeys.length}>
      <form className="grid gap-2 p-3 md:grid-cols-[1fr_1fr_auto]" onSubmit={(event) => { event.preventDefault(); if (principal && keyName.trim()) onCreateKey(); }}>
        <select aria-label="Key owner" value={principal} onChange={(event) => setKeyPrincipal(event.target.value)} className="rounded border border-line bg-bg px-2 py-1.5 text-xs">{users.map((user) => <option key={user.id} value={user.id}>{user.display_name}</option>)}</select>
        <input aria-label="Key name" value={keyName} onChange={(event) => setKeyName(event.target.value)} placeholder="Key name" className="rounded border border-line bg-bg px-2 py-1.5 text-sm" />
        <Button disabled={busy || !principal || !keyName.trim()} type="submit">Create key</Button>
      </form>
      <Table headers={['Key', 'Last used', 'Status', '']}>{apiKeys.map((key) => <KeyRow key={key.id} apiKey={key} busy={busy} onRevoke={() => onRevoke(key)} onRotate={() => onRotate(key)} />)}</Table>
    </Section>
  );
}

function PeopleSection(props: {
  users: AuthUser[];
  groups: Array<{ id: string; name: string; member_count: number }>;
  roles: Array<{ id: string; name: string; enabled: boolean; permissions: ManagementPermission[] }>;
  userName: string;
  groupName: string;
  groupMembers: string[];
  roleName: string;
  rolePermissions: ManagementPermission[];
  busy: boolean;
  setUserName: (value: string) => void;
  setGroupName: (value: string) => void;
  setGroupMembers: (value: string[]) => void;
  setRoleName: (value: string) => void;
  setRolePermissions: (value: ManagementPermission[]) => void;
  onCreateUser: () => void;
  onCreateGroup: () => void;
  onCreateRole: () => void;
}) {
  return (
    <div className="grid gap-4 xl:grid-cols-2">
      <Section title="Users and service accounts" count={props.users.length}>
        <form className="flex gap-2 p-3" onSubmit={(event) => { event.preventDefault(); if (props.userName.trim()) props.onCreateUser(); }}>
          <input aria-label="User display name" value={props.userName} onChange={(event) => props.setUserName(event.target.value)} placeholder="Display name" className="min-w-0 flex-1 rounded border border-line bg-bg px-2 py-1.5 text-sm outline-none focus:border-accent" />
          <Button disabled={props.busy || !props.userName.trim()} type="submit">Create user</Button>
        </form>
        <Table headers={['Name', 'Kind', 'Status']}>{props.users.map((user) => <tr key={user.id}><Cell><div>{user.display_name}</div><div className="font-mono text-[10px] text-text-muted">{user.id}</div></Cell><Cell muted>{user.kind}</Cell><Cell><Status enabled={user.enabled} /></Cell></tr>)}</Table>
      </Section>
      <Section title="Groups and roles" count={props.groups.length + props.roles.length}>
        <div className="grid gap-3 border-b border-line p-3 sm:grid-cols-2">
          <form className="space-y-2" onSubmit={(event) => { event.preventDefault(); if (props.groupName.trim()) props.onCreateGroup(); }}>
            <input aria-label="Group name" value={props.groupName} onChange={(event) => props.setGroupName(event.target.value)} placeholder="Group name" className="w-full rounded border border-line bg-bg px-2 py-1.5 text-sm" />
            <select multiple aria-label="Group members" value={props.groupMembers} onChange={(event) => props.setGroupMembers(Array.from(event.target.selectedOptions, (option) => option.value))} className="h-20 w-full rounded border border-line bg-bg px-2 py-1 text-xs">
              {props.users.map((user) => <option key={user.id} value={user.id}>{user.display_name} · {user.id}</option>)}
            </select>
            <Button disabled={props.busy || !props.groupName.trim()} type="submit">Create group</Button>
          </form>
          <form className="space-y-2" onSubmit={(event) => { event.preventDefault(); if (props.roleName.trim() && props.rolePermissions.length > 0) props.onCreateRole(); }}>
            <input aria-label="Role name" value={props.roleName} onChange={(event) => props.setRoleName(event.target.value)} placeholder="Role name" className="w-full rounded border border-line bg-bg px-2 py-1.5 text-sm" />
            <select multiple aria-label="Role permissions" value={props.rolePermissions} onChange={(event) => props.setRolePermissions(Array.from(event.target.selectedOptions, (option) => option.value as ManagementPermission))} className="h-20 w-full rounded border border-line bg-bg px-2 py-1 font-mono text-[10px]">
              {managementPermissions.map((permission) => <option key={permission} value={permission}>{permission}</option>)}
            </select>
            <Button disabled={props.busy || !props.roleName.trim() || props.rolePermissions.length === 0} type="submit">Create role</Button>
          </form>
        </div>
        <div className="grid gap-2 p-3 sm:grid-cols-2">
          {props.groups.map((group) => <div key={group.id} className="rounded border border-line bg-bg p-2 text-xs"><div className="font-medium">{group.name}</div><div className="mt-1 font-mono text-[10px] text-text-muted">{group.id} · {group.member_count} members</div></div>)}
          {props.roles.map((role) => <div key={role.id} className="rounded border border-line bg-bg p-2 text-xs"><div className="flex justify-between"><span className="font-medium">{role.name}</span><Status enabled={role.enabled} /></div><div className="mt-2 flex flex-wrap gap-1">{role.permissions.map((permission) => <span key={permission} className="rounded bg-line/50 px-1 font-mono text-[9px]">{permission}</span>)}</div></div>)}
        </div>
      </Section>
    </div>
  );
}

function PolicySection(props: {
  policies: AuthPolicy[];
  subjectOptions: Array<{ id: string; label: string }>;
  draftPreview: CreateAuthPolicyRequest;
  policyValidation: string[];
  busy: boolean;
  draftEffect: AuthPolicy['effect'];
  draftName: string;
  draftSubjects: string[];
  draftEndpoints: string;
  draftModels: string;
  draftRequestedModels: string;
  draftServedModels: string;
  draftProviders: string;
  draftRoutes: string;
  draftWindowDays: string;
  draftWindowStart: string;
  draftWindowEnd: string;
  draftConcurrent: string;
  draftDailyStarts: string;
  draftManagementPermissions: ManagementPermission[];
  setDraftEffect: (value: AuthPolicy['effect']) => void;
  setDraftName: (value: string) => void;
  setDraftSubjects: (value: string[]) => void;
  setDraftEndpoints: (value: string) => void;
  setDraftModels: (value: string) => void;
  setDraftRequestedModels: (value: string) => void;
  setDraftServedModels: (value: string) => void;
  setDraftProviders: (value: string) => void;
  setDraftRoutes: (value: string) => void;
  setDraftWindowDays: (value: string) => void;
  setDraftWindowStart: (value: string) => void;
  setDraftWindowEnd: (value: string) => void;
  setDraftConcurrent: (value: string) => void;
  setDraftDailyStarts: (value: string) => void;
  setDraftManagementPermissions: (value: ManagementPermission[]) => void;
  onSavePolicy: () => void;
}) {
  return (
    <Section title="Policy editor and effective preview" count={props.policies.length}>
      <div className="grid gap-3 p-3 xl:grid-cols-[minmax(0,1fr)_22rem]">
        <div className="grid gap-3 text-xs md:grid-cols-2">
          <label className="block">Name<input aria-label="Policy name" value={props.draftName} onChange={(event) => props.setDraftName(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5" /></label>
          <label className="block">Effect<select aria-label="Policy effect" value={props.draftEffect} onChange={(event) => props.setDraftEffect(event.target.value as AuthPolicy['effect'])} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5"><option value="allow">Allow</option><option value="deny">Deny</option></select></label>
          <label className="block md:col-span-2">Subjects<select multiple aria-label="Policy subjects" value={props.draftSubjects} onChange={(event) => props.setDraftSubjects(Array.from(event.target.selectedOptions, (option) => option.value))} className="mt-1 h-24 w-full rounded border border-line bg-bg px-2 py-1">{props.subjectOptions.map((subject) => <option key={subject.id} value={subject.id}>{subject.label} · {subject.id}</option>)}</select></label>
          <label className="block">Endpoints<input aria-label="Policy endpoints" value={props.draftEndpoints} onChange={(event) => props.setDraftEndpoints(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5" /></label>
          <label className="block">Compatibility models<input aria-label="Policy models" value={props.draftModels} onChange={(event) => props.setDraftModels(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5" /></label>
          <label className="block">Requested models<input aria-label="Policy requested models" value={props.draftRequestedModels} onChange={(event) => props.setDraftRequestedModels(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5" /></label>
          <label className="block">Served models<input aria-label="Policy served models" value={props.draftServedModels} onChange={(event) => props.setDraftServedModels(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5" /></label>
          <label className="block">Providers<input aria-label="Policy providers" value={props.draftProviders} onChange={(event) => props.setDraftProviders(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5" /></label>
          <label className="block">Routes<input aria-label="Policy routes" value={props.draftRoutes} onChange={(event) => props.setDraftRoutes(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5" /></label>
          <label className="block md:col-span-2">Management permissions<select multiple aria-label="Policy management permissions" value={props.draftManagementPermissions} onChange={(event) => props.setDraftManagementPermissions(Array.from(event.target.selectedOptions, (option) => option.value as ManagementPermission))} className="mt-1 h-24 w-full rounded border border-line bg-bg px-2 py-1">{managementPermissions.map((permission) => <option key={permission} value={permission}>{permission}</option>)}</select></label>
          <fieldset className="grid grid-cols-3 gap-2 rounded border border-line p-2 md:col-span-2"><legend>UTC window</legend>
            <input aria-label="Policy window days" value={props.draftWindowDays} onChange={(event) => props.setDraftWindowDays(event.target.value)} placeholder="mon,tue" className="rounded border border-line bg-bg px-2 py-1" />
            <input aria-label="Policy window start" type="time" value={props.draftWindowStart} onChange={(event) => props.setDraftWindowStart(event.target.value)} className="rounded border border-line bg-bg px-2 py-1" />
            <input aria-label="Policy window end" type="time" value={props.draftWindowEnd} onChange={(event) => props.setDraftWindowEnd(event.target.value)} className="rounded border border-line bg-bg px-2 py-1" />
          </fieldset>
          <label>Max concurrent<input aria-label="Max concurrent sessions" type="number" min="1" value={props.draftConcurrent} onChange={(event) => props.setDraftConcurrent(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5" /></label>
          <label>Daily starts<input aria-label="Daily session starts" type="number" min="1" value={props.draftDailyStarts} onChange={(event) => props.setDraftDailyStarts(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5" /></label>
          {props.policyValidation.length > 0 && <ul aria-label="Policy validation" className="list-disc pl-4 text-status-down md:col-span-2">{props.policyValidation.map((error) => <li key={error}>{error}</li>)}</ul>}
          <Button className="w-fit" disabled={props.busy || props.policyValidation.length > 0} onClick={props.onSavePolicy}>Save reviewed policy</Button>
        </div>
        <div className={cn('h-fit rounded border p-3 text-xs', props.draftEffect === 'deny' ? 'border-status-down/50 bg-status-down/10' : 'border-status-healthy/40 bg-status-healthy/10')}>
          <div className="flex items-center justify-between"><span className="font-semibold uppercase tracking-wide">Effective preview</span><span className={cn('rounded px-1.5 py-0.5 font-bold uppercase', props.draftEffect === 'deny' ? 'bg-status-down/20 text-status-down' : 'bg-status-healthy/20 text-status-healthy')}>{props.draftEffect}</span></div>
          <div className="mt-2 text-[10px] text-text-muted">{props.draftPreview.models.join(', ')}</div>
          <pre data-testid="policy-payload-preview" className="mt-3 max-h-[34rem] overflow-auto whitespace-pre-wrap break-all rounded bg-bg p-2 font-mono text-[10px]">{JSON.stringify(props.draftPreview, null, 2)}</pre>
        </div>
      </div>
      <div className="border-t border-line p-3 text-xs">{props.policies.map((policy) => <div key={policy.id} className="mb-2 flex items-center gap-2"><span className={cn('rounded px-1.5 py-0.5 text-[10px] font-bold uppercase', policy.effect === 'deny' ? 'bg-status-down/20 text-status-down' : 'bg-status-healthy/15 text-status-healthy')}>{policy.effect}</span><span>{policy.name}</span><span className="ml-auto font-mono text-[10px] text-text-muted">{policy.requested_models.join(', ')} -&gt; {policy.served_models.join(', ')} · {policy.providers.join(', ')} / {policy.routes.join(', ')}</span></div>)}</div>
    </Section>
  );
}

function SessionsSection({ sessions, busy, onRevoke }: { sessions: AuthSession[]; busy: boolean; onRevoke: (session: AuthSession) => void }) {
  return <Section title="Active sessions" count={sessions.length}><Table headers={['Session', 'Target', 'Started', '']}>{sessions.map((session) => <SessionRow key={session.id} session={session} busy={busy} onRevoke={() => onRevoke(session)} />)}</Table></Section>;
}

function AuditCostSection({ usage, audit, pricing }: { usage: AuthUsageRow[]; audit: Array<{ id: string; timestamp: string; actor: string; action: string; target: string; outcome: 'ok' | 'denied' | 'error' }>; pricing: Array<{ provider: string; model: string; input_per_1k: string; output_per_1k: string; confidence: string; source: string }> }) {
  return (
    <div className="grid gap-4 xl:grid-cols-3">
      <Section title="Usage and cost" count={usage.length}><Table headers={['Attribution', 'Requests', 'Tokens', 'Cost']}>{usage.map((row) => <tr key={`${row.dimension}:${row.value}`}><Cell>{row.dimension}<div className="font-mono text-[10px] text-text-muted">{row.value}</div></Cell><Cell>{row.requests}</Cell><Cell>{row.prompt_tokens === null || row.completion_tokens === null ? '-' : (row.prompt_tokens + row.completion_tokens).toLocaleString()}</Cell><Cell><div>{cost(row)}</div><div className="text-[10px] uppercase text-text-muted">{row.cost_confidence}</div></Cell></tr>)}</Table></Section>
      <Section title="Audit log" count={audit.length}><Table headers={['Time', 'Action', 'Outcome']}>{audit.map((event) => <tr key={event.id}><Cell muted>{timestamp(event.timestamp)}</Cell><Cell><div>{event.action}</div><div className="font-mono text-[10px] text-text-muted">{event.actor} -&gt; {event.target}</div></Cell><Cell><span className={event.outcome === 'denied' ? 'text-status-down' : 'text-status-healthy'}>{event.outcome}</span></Cell></tr>)}</Table></Section>
      <Section title="Pricing provenance" count={pricing.length}><Table headers={['Model', 'Price', 'Quality']}>{pricing.map((row) => <tr key={`${row.provider}:${row.model}`}><Cell>{row.model}<div className="font-mono text-[10px] text-text-muted">{row.provider}</div></Cell><Cell>${row.input_per_1k} in / ${row.output_per_1k} out</Cell><Cell><div className="uppercase">{row.confidence}</div><div className="text-[10px] text-text-muted">{row.source}</div></Cell></tr>)}</Table></Section>
    </div>
  );
}

function CreateAccessDrawer(props: {
  users: AuthUser[];
  step: WizardStep;
  setStep: (step: WizardStep) => void;
  busy: boolean;
  mode: 'existing' | 'new';
  setMode: (mode: 'existing' | 'new') => void;
  existingPrincipal: string;
  setExistingPrincipal: (value: string) => void;
  displayName: string;
  setDisplayName: (value: string) => void;
  kind: AuthUser['kind'];
  setKind: (value: AuthUser['kind']) => void;
  keyName: string;
  setKeyName: (value: string) => void;
  policyName: string;
  setPolicyName: (value: string) => void;
  endpoints: string;
  setEndpoints: (value: string) => void;
  models: string;
  setModels: (value: string) => void;
  providers: string;
  setProviders: (value: string) => void;
  routes: string;
  setRoutes: (value: string) => void;
  days: string;
  setDays: (value: string) => void;
  start: string;
  setStart: (value: string) => void;
  end: string;
  setEnd: (value: string) => void;
  concurrent: string;
  setConcurrent: (value: string) => void;
  dailyStarts: string;
  setDailyStarts: (value: string) => void;
  onClose: () => void;
  onCreate: () => void;
}) {
  const steps: WizardStep[] = ['identity', 'permissions', 'limits', 'review'];
  const index = steps.indexOf(props.step);
  const canContinue = props.step !== 'identity' || props.mode === 'existing' || props.displayName.trim().length > 0;
  return (
    <div className="pointer-events-none fixed inset-0 z-40 flex justify-end">
      <Panel raised className="pointer-events-auto flex h-full w-full max-w-[28rem] flex-col rounded-none border-y-0 border-r-0 bg-panel-raised shadow-2xl">
        <div className="border-b border-line p-5">
          <div className="flex items-start justify-between gap-3">
            <div>
              <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-accent">Guided setup</div>
              <h2 className="mt-1 text-xl font-semibold">Create access</h2>
              <p className="mt-1 text-xs text-text-muted">Grant model access in a few simple steps.</p>
            </div>
            <button aria-label="Close create access" className="rounded p-2 text-lg text-text-muted hover:text-text" onClick={props.onClose}>×</button>
          </div>
          <div className="mt-5 grid grid-cols-4 text-[10px] text-text-muted">{steps.map((step, stepIndex) => <button key={step} className="relative flex flex-col items-center gap-1.5" onClick={() => props.setStep(step)}>{stepIndex > 0 && <span className={cn('absolute right-1/2 top-3 h-px w-full', stepIndex <= index ? 'bg-accent' : 'bg-line')} />}<span className={cn('relative z-10 grid h-7 w-7 place-items-center rounded-full border font-mono', stepIndex === index ? 'border-accent bg-accent text-bg' : stepIndex < index ? 'border-accent bg-accent/20 text-accent' : 'border-line bg-panel-raised')}>{stepIndex + 1}</span><span className={cn('capitalize', stepIndex === index && 'font-semibold text-text')}>{step}</span></button>)}</div>
        </div>
        <div className="flex-1 overflow-auto p-5">
          {props.step === 'identity' && (
            <div className="space-y-4 text-sm">
              <div><h3 className="font-semibold">Who needs access?</h3><p className="mt-1 text-xs text-text-muted">Choose whether this is a person or a service, and give it a name.</p></div>
              {props.mode === 'new' ? <>
                <Segmented value={props.kind} onChange={props.setKind} options={[['user', 'Person'], ['service_account', 'Service']]} />
                <label className="block text-xs font-medium">Display name <span className="text-status-down">*</span><input aria-label="Wizard display name" value={props.displayName} onChange={(event) => props.setDisplayName(event.target.value)} placeholder="e.g. Maya Chen" className="mt-1.5 w-full rounded border border-line bg-bg px-3 py-2.5 text-sm" /><span className="mt-1 block font-normal text-text-muted">A clear, human-readable name for this identity.</span></label>
                <button className="text-xs font-medium text-accent" onClick={() => props.setMode('existing')}>Use an existing identity instead</button>
              </> : <><label className="block text-xs font-medium">Existing identity<select aria-label="Wizard existing principal" value={props.existingPrincipal} onChange={(event) => props.setExistingPrincipal(event.target.value)} className="mt-1.5 w-full rounded border border-line bg-bg px-3 py-2.5 text-sm">{props.users.map((user) => <option key={user.id} value={user.id}>{user.display_name}</option>)}</select></label><button className="text-xs font-medium text-accent" onClick={() => props.setMode('new')}>Create a new identity instead</button></>}
              <label className="block text-xs font-medium">Key label <span className="text-status-down">*</span><input aria-label="Wizard key name" value={props.keyName} onChange={(event) => props.setKeyName(event.target.value)} placeholder="e.g. Laptop, CI pipeline, Research" className="mt-1.5 w-full rounded border border-line bg-bg px-3 py-2.5 text-sm" /><span className="mt-1 block font-normal text-text-muted">Helps you identify this key later. You can create more keys afterwards.</span></label>
              <div className="rounded-md border border-accent/40 bg-accent/10 p-4 text-xs"><div className="font-semibold text-accent">ⓘ &nbsp;The API key will appear once</div><p className="mt-2 pl-5 leading-relaxed text-text-muted">For your security, the raw key is shown only once after creation. Copy and store it securely.</p></div>
            </div>
          )}
          {props.step === 'permissions' && (
            <div className="space-y-3 text-sm">
              <label className="block text-xs">Policy name<input aria-label="Wizard policy name" value={props.policyName} onChange={(event) => props.setPolicyName(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-2 text-sm" /></label>
              <label className="block text-xs">Endpoints<input aria-label="Wizard endpoints" value={props.endpoints} onChange={(event) => props.setEndpoints(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-2 text-sm" /></label>
              <label className="block text-xs">Models<input aria-label="Wizard models" value={props.models} onChange={(event) => props.setModels(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-2 text-sm" /></label>
              <label className="block text-xs">Providers<input aria-label="Wizard providers" value={props.providers} onChange={(event) => props.setProviders(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-2 text-sm" /></label>
              <label className="block text-xs">Routes<input aria-label="Wizard routes" value={props.routes} onChange={(event) => props.setRoutes(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-2 text-sm" /></label>
            </div>
          )}
          {props.step === 'limits' && (
            <div className="space-y-3 text-sm">
              <fieldset className="grid grid-cols-3 gap-2 rounded border border-line p-3"><legend className="px-1 text-xs text-text-muted">UTC window</legend>
                <input aria-label="Wizard window days" value={props.days} onChange={(event) => props.setDays(event.target.value)} className="rounded border border-line bg-bg px-2 py-2 text-xs" />
                <input aria-label="Wizard window start" type="time" value={props.start} onChange={(event) => props.setStart(event.target.value)} className="rounded border border-line bg-bg px-2 py-2 text-xs" />
                <input aria-label="Wizard window end" type="time" value={props.end} onChange={(event) => props.setEnd(event.target.value)} className="rounded border border-line bg-bg px-2 py-2 text-xs" />
              </fieldset>
              <label className="block text-xs">Max concurrent sessions<input aria-label="Wizard max concurrent sessions" type="number" min="1" value={props.concurrent} onChange={(event) => props.setConcurrent(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-2 text-sm" /></label>
              <label className="block text-xs">Daily session starts<input aria-label="Wizard daily starts" type="number" min="1" value={props.dailyStarts} onChange={(event) => props.setDailyStarts(event.target.value)} className="mt-1 w-full rounded border border-line bg-bg px-2 py-2 text-sm" /></label>
            </div>
          )}
          {props.step === 'review' && (
            <div className="space-y-3 text-sm">
              <ReviewLine label="Identity" value={props.mode === 'existing' ? props.users.find((user) => user.id === props.existingPrincipal)?.display_name ?? props.existingPrincipal : `${props.displayName || 'New identity'} (${props.kind})`} />
              <ReviewLine label="Key" value={props.keyName || 'default key'} />
              <ReviewLine label="Models" value={props.models} />
              <ReviewLine label="Provider/route" value={`${props.providers} / ${props.routes}`} />
              <ReviewLine label="Window" value={`${props.days} ${props.start}-${props.end} UTC`} />
              <ReviewLine label="Sessions" value={`${props.concurrent || 'unlimited'} concurrent, ${props.dailyStarts || 'unlimited'} daily starts`} />
            </div>
          )}
        </div>
        <div className="flex justify-between border-t border-line p-5">
          <Button variant="ghost" disabled={index === 0} onClick={() => props.setStep(steps[index - 1] ?? 'identity')}>Back</Button>
          {props.step === 'review'
            ? <Button disabled={props.busy || !canContinue} onClick={props.onCreate}>Create access</Button>
            : <Button className="bg-accent px-5 text-white hover:bg-accent/85" disabled={!canContinue} onClick={() => props.setStep(steps[index + 1] ?? 'review')}>Continue →</Button>}
        </div>
      </Panel>
    </div>
  );
}

function PrincipalAccessRow({ row }: { row: AccessPrincipalRow }) {
  return (
    <tr>
      <Cell><div className="flex items-center gap-2"><span className="grid h-7 w-7 shrink-0 place-items-center rounded border border-accent/30 bg-accent/10 text-accent">{row.kind === 'service_account' ? '◇' : '♙'}</span><div><div className="font-medium">{row.name}</div><div className="font-mono text-[10px] text-text-muted">{row.kind.replace('_', ' ')} · {row.id}</div></div></div></Cell>
      <Cell><span className="rounded bg-accent/15 px-2 py-1 text-[10px] font-medium text-accent">{row.policyCount > 0 ? `${row.policyCount} direct ${row.policyCount === 1 ? 'policy' : 'policies'}` : 'Inherited / default'}</span><div className="mt-1 text-[10px] text-text-muted">{formatAccessList(row.endpoints, 'No direct API rule')}</div></Cell>
      <Cell><div className="flex flex-wrap gap-1">{(row.allowedModels.length ? row.allowedModels : ['Unspecified']).slice(0, 2).map((model) => <span key={model} className="rounded bg-line/60 px-1.5 py-0.5">{model}</span>)}</div>{row.deniedModels.length > 0 && <div className="text-[10px] text-status-down">denies {formatAccessList(row.deniedModels)}</div>}</Cell>
      <Cell><div>{row.schedule}</div><div className="text-[10px] text-text-muted">{row.sessionLimit} concurrent</div></Cell>
      <Cell><Status enabled={row.enabled && row.activeKeyCount > 0} /><div className="mt-1 text-[10px] text-text-muted">{row.activeKeyCount} active · {row.revokedKeyCount} disabled</div></Cell>
      <Cell><div>{row.usageTokens === null ? '—' : `${row.usageTokens.toLocaleString()} tokens`}</div><div className="text-[10px] text-text-muted">{row.usageCost === null ? 'Usage unavailable' : `$${row.usageCost.toFixed(4)} · ${row.usageConfidence}`}</div></Cell>
      <Cell><button aria-label={`Actions for ${row.name}`} className="rounded px-2 py-1 text-text-muted hover:bg-line/50 hover:text-text">•••</button></Cell>
    </tr>
  );
}

function StepCard({ index, title, detail }: { index: string; title: string; detail: string }) {
  return <div className={cn('relative flex items-center gap-3 px-3 py-2', index !== '3' && "md:after:absolute md:after:right-0 md:after:top-1/2 md:after:text-lg md:after:text-accent md:after:content-['→']")}><div className="grid h-10 w-10 shrink-0 place-items-center rounded-full border border-accent/40 bg-accent/15 font-mono text-sm font-bold text-accent">{index}</div><div><div className="font-medium">{title}</div><div className="mt-0.5 text-xs leading-relaxed text-text-muted">{detail}</div></div></div>;
}

function AttentionItem({ item, onReview }: { item: AccessAttentionItem; onReview: () => void }) {
  return <div className="grid items-center gap-3 bg-panel px-4 py-3 text-xs sm:grid-cols-[minmax(180px,0.7fr)_minmax(0,1.4fr)_auto]"><div className={cn('font-semibold', item.severity === 'critical' ? 'text-status-down' : 'text-status-cooling')}>● &nbsp;{item.title}</div><div className="leading-relaxed text-text-muted">{item.detail}</div><Button variant="ghost" className="border-line" onClick={onReview}>Review</Button></div>;
}

function Segmented<T extends string>({ value, onChange, options }: { value: T; onChange: (value: T) => void; options: Array<[T, string]> }) {
  return <div className="grid grid-cols-2 rounded border border-line bg-bg p-1">{options.map(([id, label]) => <button key={id} className={cn('rounded px-2 py-2 text-xs font-medium transition', value === id ? 'bg-accent text-bg shadow-sm' : 'text-text-muted hover:text-text')} onClick={() => onChange(id)}>{label}</button>)}</div>;
}

function ReviewLine({ label, value }: { label: string; value: string }) {
  return <div className="rounded border border-line bg-bg p-3"><div className="text-[10px] font-bold uppercase tracking-[0.14em] text-text-muted">{label}</div><div className="mt-1 break-words">{value}</div></div>;
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

function Table({ headers, children }: { headers: string[]; children: ReactNode }) {
  return <table className="w-full text-left text-xs"><thead className="text-text-muted"><tr>{headers.map((header) => <th key={header} className="px-3 py-1">{header}</th>)}</tr></thead><tbody>{children}</tbody></table>;
}

function Status({ enabled }: { enabled: boolean }) {
  return <span className={cn('rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase', enabled ? 'bg-status-healthy/15 text-status-healthy' : 'bg-status-down/15 text-status-down')}>{enabled ? 'active' : 'disabled'}</span>;
}

function timestamp(value: string | null): string {
  return value ? value.replace('T', ' ').replace('Z', ' UTC') : '-';
}

function cost(row: AuthUsageRow): string {
  return row.cost === null ? '-' : `$${row.cost.toFixed(4)}`;
}

function tabLabel(tab: AccessTab): string {
  switch (tab) {
    case 'overview': return 'Overview';
    case 'keys': return 'API keys';
    case 'people': return 'People & groups';
    case 'policies': return 'Policies';
    case 'sessions': return 'Sessions';
    case 'audit': return 'Audit & cost';
  }
}

function KeyRow({ apiKey, busy, onRevoke, onRotate }: { apiKey: AuthApiKey; busy: boolean; onRevoke: () => void; onRotate: () => void }) {
  return <tr><Cell><div>{apiKey.name}</div><div className="font-mono text-[10px] text-text-muted">{apiKey.prefix}... · {apiKey.id}</div></Cell><Cell muted>{timestamp(apiKey.last_used_at)}</Cell><Cell><Status enabled={apiKey.enabled} /></Cell><Cell><div className="flex justify-end gap-1"><Button variant="ghost" disabled={busy || !apiKey.enabled} onClick={onRotate}>Rotate</Button><Button variant="danger" disabled={busy || !apiKey.enabled} onClick={onRevoke}>Revoke</Button></div></Cell></tr>;
}

function SessionRow({ session, busy, onRevoke }: { session: AuthSession; busy: boolean; onRevoke: () => void }) {
  return <tr><Cell><div>{session.kind}</div><div className="font-mono text-[10px] text-text-muted">{session.id}</div></Cell><Cell>{session.endpoint ?? 'dashboard'}<div className="font-mono text-[10px] text-text-muted">{session.requested_model ?? session.principal_id}</div></Cell><Cell muted>{timestamp(session.started_at)}</Cell><Cell><Button variant="danger" disabled={busy} onClick={onRevoke}>Terminate</Button></Cell></tr>;
}

function CopyOnceDialog({ created, onClose }: { created: CreatedAuthApiKey; onClose: () => void }) {
  const [copied, setCopied] = useState(false);
  return <div role="dialog" aria-modal="true" aria-label="Copy API key" className="fixed inset-0 z-50 grid place-items-center bg-bg/80 p-4"><Panel raised className="w-full max-w-xl p-5 shadow-2xl"><div className="text-[10px] font-bold uppercase tracking-[0.18em] text-status-cooling">Copy once</div><h2 className="mt-1 text-lg font-semibold">API key created</h2><p className="mt-2 text-xs text-text-muted">This raw secret will not be shown or returned by the API again.</p><code className="mt-4 block break-all rounded border border-line bg-bg p-3 text-sm text-accent" data-testid="raw-api-key">{created.raw_key}</code><div className="mt-4 flex justify-end gap-2"><Button onClick={() => { void navigator.clipboard?.writeText(created.raw_key); setCopied(true); }}>{copied ? 'Copied' : 'Copy key'}</Button><Button variant="ghost" onClick={onClose}>I stored it</Button></div></Panel></div>;
}
