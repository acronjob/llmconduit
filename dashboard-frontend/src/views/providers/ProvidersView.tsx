import { useMemo, useState, type FormEvent, type ReactNode } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { getConnection, queryKeys } from '../../api/connection';
import type { FleetModelEntry, FleetModelsResponse, MeshAdminState, MeshDisabledModel, MeshJoinKey, MeshNode, ProviderHealth } from '../../api/types';
import { EMPTY_FILTERS } from '../../components/FlowTable/filterTypes';
import { useFlowRows } from '../../components/FlowTable/useFlowRows';
import { fmtCost, fmtElapsed, fmtTokens } from '../../components/FlowTable/format';
import { buildProviderLatency, fmtProviderLatencyMs } from '../../components/viz/providerLatency';
import { Panel } from '../../components/ui/Panel';
import { Button } from '../../components/ui/Button';
import { useAuth, useDashboard } from '../../store/hooks';
import { useTopologyQuery } from '../../store/useTopologyQuery';
import { cn } from '../../lib/cn';
import {
  buildProviderInventory,
  costQuality,
  formatPercent,
  type ProviderInventoryRow,
  type ProviderInventorySummary,
} from './providersModel';

const authPoliciesKey = ['auth', 'policies', 'providers-view'] as const;
const DASH = '—';
const INPUT = 'rounded-md border border-line bg-bg px-2 py-1.5 font-mono text-xs text-text outline-none focus:border-accent';

export function ProvidersView() {
  const [status, setStatus] = useState<'all' | ProviderHealth['status']>('all');
  const [query, setQuery] = useState('');
  const [createdToken, setCreatedToken] = useState<{ label: string | null; token: string } | null>(null);
  const { client } = getConnection();
  const queryClient = useQueryClient();
  const mutationsEnabled = useAuth((s) => s.mutationsEnabled);
  const nodes = useDashboard((s) => s.topologyNodes);
  const edges = useDashboard((s) => s.topologyEdges);
  const { rows: flows } = useFlowRows(EMPTY_FILTERS);
  const { perProviderById } = useTopologyQuery();

  const topologyQuery = useQuery({ queryKey: queryKeys.topology, queryFn: () => client.topology() });
  const providersQuery = useQuery({ queryKey: queryKeys.providers, queryFn: () => client.providers() });
  const meshQuery = useQuery({ queryKey: queryKeys.mesh, queryFn: () => client.mesh(), retry: false });
  const fleetQuery = useQuery({ queryKey: queryKeys.fleet, queryFn: () => client.fleet(), retry: false });
  const providerMetricsQuery = useQuery({ queryKey: queryKeys.providerMetrics, queryFn: () => client.providerMetrics() });
  const policiesQuery = useQuery({ queryKey: authPoliciesKey, queryFn: () => client.authPolicies() });
  const invalidateMesh = () => {
    void queryClient.invalidateQueries({ queryKey: queryKeys.mesh });
    void queryClient.invalidateQueries({ queryKey: queryKeys.providers });
  };
  const createJoinKey = useMutation({
    mutationFn: (body: { label?: string; max_uses?: number; expires_in_secs?: number }) => client.createMeshJoinKey(body),
    onSuccess: (created) => {
      setCreatedToken({ label: created.join_key.label ?? null, token: created.token });
      invalidateMesh();
    },
  });
  const revokeJoinKey = useMutation({ mutationFn: (id: string) => client.revokeMeshJoinKey(id), onSuccess: invalidateMesh });
  const setNodeEnabled = useMutation({ mutationFn: ({ endpointId, enabled }: { endpointId: string; enabled: boolean }) => client.setMeshNodeEnabled(endpointId, enabled), onSuccess: invalidateMesh });
  const setModelDisabled = useMutation({
    mutationFn: ({ endpointId, resourceId, model, disabled }: { endpointId: string; resourceId: string; model: string; disabled: boolean }) =>
      client.setMeshModelDisabled({ endpoint_id: endpointId, resource_id: resourceId, model }, disabled),
    onSuccess: invalidateMesh,
  });
  const invalidateFleet = () => {
    void queryClient.invalidateQueries({ queryKey: queryKeys.fleet });
    void queryClient.invalidateQueries({ queryKey: queryKeys.providers });
    void queryClient.invalidateQueries({ queryKey: queryKeys.topology });
  };
  const loadFleetModel = useMutation({ mutationFn: (id: string) => client.loadFleetModel(id), onSuccess: invalidateFleet });
  const unloadFleetModel = useMutation({ mutationFn: (id: string) => client.unloadFleetModel(id), onSuccess: invalidateFleet });

  const inventory = useMemo(() => buildProviderInventory({
    providers: providersQuery.data?.providers ?? [],
    health: nodes,
    edges,
    flows,
    policies: policiesQuery.data?.policies ?? [],
    cacheMetrics: providerMetricsQuery.data?.providers ?? [],
    priceTable: topologyQuery.data?.price_table ?? {},
    perProviderById,
  }), [providersQuery.data, nodes, edges, flows, policiesQuery.data, providerMetricsQuery.data, topologyQuery.data, perProviderById]);

  const filteredRows = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return inventory.rows.filter((row) => {
      if (status !== 'all' && row.status !== status) return false;
      if (!needle) return true;
      return [
        row.id,
        row.name,
        row.resourceId,
        row.route,
        row.baseUrl,
        ...row.advertisedModels,
        ...row.policy.subjects,
        ...row.policy.endpoints,
      ].filter(Boolean).join(' ').toLowerCase().includes(needle);
    });
  }, [inventory.rows, query, status]);

  if (providersQuery.isLoading && nodes.length === 0) {
    return <div className="p-5 text-sm text-text-muted">Loading provider inventory...</div>;
  }

  const error = providersQuery.error || providerMetricsQuery.error || topologyQuery.error || policiesQuery.error;

  return (
    <div className="min-h-0 min-w-0 flex-1 overflow-auto p-4" data-testid="providers-view">
      <div className="mb-3 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-sm font-semibold uppercase tracking-[0.18em] text-text">provider inventory</h1>
          <p className="mt-1 text-xs text-text-muted">
            upstream health · advertised catalog · access windows · limits · usage
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <div className="flex overflow-hidden rounded-md border border-line text-xs" role="group" aria-label="Provider status">
            {(['all', 'healthy', 'cooling', 'down'] as const).map((item) => (
              <button
                key={item}
                type="button"
                onClick={() => setStatus(item)}
                className={cn(
                  'px-3 py-1.5 uppercase tracking-[0.12em]',
                  status === item ? 'bg-accent/15 text-accent' : 'bg-panel text-text-muted hover:text-text',
                )}
              >
                {item}
              </button>
            ))}
          </div>
          <label className="relative">
            <span className="absolute left-2.5 top-1.5 text-xs text-text-muted">⌕</span>
            <input
              aria-label="Search providers"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search provider, model, subject..."
              className="w-72 rounded-md border border-line bg-bg py-1.5 pl-7 pr-2 text-xs outline-none focus:border-accent"
            />
          </label>
        </div>
      </div>

      {error && (
        <Panel className="mb-3 border-status-cooling/40 bg-status-cooling/10 p-3 text-xs text-status-cooling" data-testid="providers-warning">
          Some provider data is unavailable: {(error as Error).message}
        </Panel>
      )}

      <SummaryStrip summary={inventory.summary} />

      <div className="mt-3 space-y-3">
        <MeshAdminPanel
          mesh={meshQuery.data ?? null}
          rows={inventory.rows}
          loading={meshQuery.isLoading}
          error={meshQuery.error ? String(meshQuery.error) : null}
          mutationsEnabled={mutationsEnabled}
          createdToken={createdToken}
          busy={createJoinKey.isPending || revokeJoinKey.isPending || setNodeEnabled.isPending || setModelDisabled.isPending}
          mutationError={String(createJoinKey.error ?? revokeJoinKey.error ?? setNodeEnabled.error ?? setModelDisabled.error ?? '') || null}
          onCreate={(body) => createJoinKey.mutate(body)}
          onDismissToken={() => setCreatedToken(null)}
          onRevoke={(id) => revokeJoinKey.mutate(id)}
          onSetNode={(endpointId, enabled) => setNodeEnabled.mutate({ endpointId, enabled })}
          onSetModel={(endpointId, resourceId, model, disabled) => setModelDisabled.mutate({ endpointId, resourceId, model, disabled })}
        />
        <FleetPanel
          fleet={fleetQuery.data ?? null}
          loading={fleetQuery.isLoading}
          error={fleetQuery.error ? String(fleetQuery.error) : null}
          mutationsEnabled={mutationsEnabled}
          busy={loadFleetModel.isPending || unloadFleetModel.isPending}
          mutationError={String(loadFleetModel.error ?? unloadFleetModel.error ?? '') || null}
          onLoad={(id) => loadFleetModel.mutate(id)}
          onUnload={(id) => unloadFleetModel.mutate(id)}
        />
        <ProviderTable rows={filteredRows} total={inventory.rows.length} />
        <ModelsPanel models={inventory.unionCatalogModels} />
      </div>
    </div>
  );
}

function FleetPanel({
  fleet,
  loading,
  error,
  mutationsEnabled,
  busy,
  mutationError,
  onLoad,
  onUnload,
}: {
  fleet: FleetModelsResponse | null;
  loading: boolean;
  error: string | null;
  mutationsEnabled: boolean;
  busy: boolean;
  mutationError: string | null;
  onLoad: (id: string) => void;
  onUnload: (id: string) => void;
}) {
  const unavailable = error && error.includes('404');
  const active = fleet?.models.filter((entry) => isFleetActive(entry)).length ?? 0;
  return (
    <Panel className="p-4" data-testid="fleet-panel" data-available={fleet ? 'true' : 'false'}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Fleet GPU switching</h2>
          <p className="mt-1 text-[10px] leading-relaxed text-text-muted">
            Local Fleet models · loaded {active}/{fleet?.models.length ?? 0} · actions use the dashboard mutation gate
          </p>
        </div>
        {!mutationsEnabled && (
          <span className="rounded-sm bg-status-cooling/15 px-2 py-1 text-[10px] uppercase tracking-wide text-status-cooling" data-testid="fleet-mutations-disabled">
            mutations disabled
          </span>
        )}
      </div>

      {loading && <p className="mt-3 text-xs text-text-muted">Loading Fleet state...</p>}
      {unavailable && <p className="mt-3 text-xs text-text-muted" data-quality="unavailable">Local Fleet is not configured on this gateway.</p>}
      {error && !unavailable && <p className="mt-3 text-xs text-status-down">Could not load Fleet state: {error}</p>}
      {mutationError && <p className="mt-2 text-xs text-status-down">{mutationError}</p>}

      {fleet && (
        <div className="mt-3 grid gap-2 lg:grid-cols-2">
          {fleet.models.map((entry) => (
            <FleetModelCard
              key={entry.model.id}
              entry={entry}
              busy={busy}
              mutationsEnabled={mutationsEnabled}
              onLoad={onLoad}
              onUnload={onUnload}
            />
          ))}
          {fleet.models.length === 0 && <EmptyMeshLine>No Fleet models configured.</EmptyMeshLine>}
        </div>
      )}
    </Panel>
  );
}

function FleetModelCard({
  entry,
  busy,
  mutationsEnabled,
  onLoad,
  onUnload,
}: {
  entry: FleetModelEntry;
  busy: boolean;
  mutationsEnabled: boolean;
  onLoad: (id: string) => void;
  onUnload: (id: string) => void;
}) {
  const active = isFleetActive(entry);
  const transitioning = ['loading', 'stopping'].includes(entry.status.phase);
  const gpus = entry.status.assigned_gpus.length ? entry.status.assigned_gpus.map((gpu) => `GPU ${gpu}`).join(', ') : DASH;
  return (
    <div className="min-w-0 rounded border border-line/70 bg-bg p-3" data-testid="fleet-model-card">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="truncate font-mono text-xs text-text" title={entry.model.id}>{entry.model.id}</div>
          <div className="mt-1 truncate text-[10px] text-text-muted" title={entry.model.image}>{entry.model.image}</div>
        </div>
        <span className={cn('rounded-sm px-2 py-1 text-[10px] uppercase tracking-wide', fleetPhaseClass(entry.status.phase))}>
          {entry.status.phase}
        </span>
      </div>
      {entry.model.description && <p className="mt-2 text-[10px] leading-relaxed text-text-muted">{entry.model.description}</p>}
      <div className="mt-3 grid grid-cols-2 gap-2 text-[10px]">
        <FleetFact label="desired" value={entry.status.desired_state} />
        <FleetFact label="gpus" value={gpus} />
        <FleetFact label="health" value={entry.status.health ?? entry.status.container_status ?? DASH} />
        <FleetFact label="checked" value={entry.status.last_checked ? fmtFleetTime(entry.status.last_checked) : DASH} />
      </div>
      {entry.status.last_error && <p className="mt-2 text-[10px] text-status-down">{entry.status.last_error}</p>}
      <div className="mt-3 flex justify-end gap-2">
        <Button type="button" disabled={!mutationsEnabled || busy || active || transitioning} onClick={() => onLoad(entry.model.id)} className="px-2 py-1 text-[10px]">Load</Button>
        <Button type="button" variant="danger" disabled={!mutationsEnabled || busy || !active || transitioning} onClick={() => onUnload(entry.model.id)} className="px-2 py-1 text-[10px]">Unload</Button>
      </div>
    </div>
  );
}

function FleetFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 rounded border border-line/60 bg-panel px-2 py-1.5">
      <div className="uppercase tracking-[0.14em] text-text-muted">{label}</div>
      <div className="mt-1 truncate font-mono text-text" title={value} data-quality={value === DASH ? 'unavailable' : 'measured'}>{value}</div>
    </div>
  );
}

function isFleetActive(entry: FleetModelEntry): boolean {
  return entry.status.phase === 'ready' || entry.status.desired_state === 'loaded';
}

function fleetPhaseClass(phase: string): string {
  if (phase === 'ready') return 'bg-status-healthy/15 text-status-healthy';
  if (phase === 'loading' || phase === 'stopping') return 'bg-status-cooling/15 text-status-cooling';
  if (phase === 'failed' || phase === 'unhealthy') return 'bg-status-down/15 text-status-down';
  return 'bg-panel text-text-muted';
}

function MeshAdminPanel({
  mesh,
  rows,
  loading,
  error,
  mutationsEnabled,
  createdToken,
  busy,
  mutationError,
  onCreate,
  onDismissToken,
  onRevoke,
  onSetNode,
  onSetModel,
}: {
  mesh: MeshAdminState | null;
  rows: ProviderInventoryRow[];
  loading: boolean;
  error: string | null;
  mutationsEnabled: boolean;
  createdToken: { label: string | null; token: string } | null;
  busy: boolean;
  mutationError: string | null;
  onCreate: (body: { label?: string; max_uses?: number; expires_in_secs?: number }) => void;
  onDismissToken: () => void;
  onRevoke: (id: string) => void;
  onSetNode: (endpointId: string, enabled: boolean) => void;
  onSetModel: (endpointId: string, resourceId: string, model: string, disabled: boolean) => void;
}) {
  const [label, setLabel] = useState('');
  const [maxUses, setMaxUses] = useState('1');
  const [expiresHours, setExpiresHours] = useState('24');
  const disabledModels = new Set((mesh?.disabled_models ?? []).map(disabledModelKey));
  const meshEndpointIds = new Set((mesh?.nodes ?? []).map((node) => node.endpoint_id));
  const meshRows = rows
    .map((row) => ({ row, endpointId: endpointFromProvider(row.id) }))
    .filter(({ row, endpointId }) => endpointId && meshEndpointIds.has(endpointId) && row.resourceId);
  const submit = (event: FormEvent) => {
    event.preventDefault();
    onCreate({
      label: label.trim() || undefined,
      max_uses: numberField(maxUses),
      expires_in_secs: numberField(expiresHours) == null ? undefined : numberField(expiresHours)! * 3600,
    });
  };
  const unavailable = error && error.includes('404');

  return (
    <Panel className="p-4" data-testid="mesh-admin" data-available={mesh ? 'true' : 'false'}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Mesh enrollment</h2>
          <p className="mt-1 text-[10px] leading-relaxed text-text-muted">
            Provision provider tokens, revoke future enrollment, disable nodes, or suppress one advertised model.
          </p>
        </div>
        {!mutationsEnabled && (
          <span className="rounded-sm bg-status-cooling/15 px-2 py-1 text-[10px] uppercase tracking-wide text-status-cooling" data-testid="mesh-mutations-disabled">
            mutations disabled
          </span>
        )}
      </div>

      {loading && <p className="mt-3 text-xs text-text-muted">Loading mesh state...</p>}
      {unavailable && <p className="mt-3 text-xs text-text-muted" data-quality="unavailable">Mesh controller is not enabled on this gateway.</p>}
      {error && !unavailable && <p className="mt-3 text-xs text-status-down">Could not load mesh admin state: {error}</p>}

      {createdToken && (
        <div className="mt-3 rounded border border-status-healthy/40 bg-status-healthy/10 p-3" data-testid="mesh-created-token">
          <div className="text-[10px] uppercase tracking-[0.14em] text-status-healthy">new enrollment token{createdToken.label ? ` · ${createdToken.label}` : ''} · shown once</div>
          <code className="mt-1 block select-all break-all font-mono text-xs text-text">{createdToken.token}</code>
          <button type="button" className="mt-1 text-[10px] text-text-muted hover:text-text" onClick={onDismissToken}>dismiss</button>
        </div>
      )}

      {mesh && (
        <>
          <form className="mt-3 grid gap-2 md:grid-cols-[minmax(0,1fr)_7rem_7rem_auto]" onSubmit={submit}>
            <input aria-label="Enrollment label" value={label} onChange={(event) => setLabel(event.target.value)} placeholder="label" className={INPUT} />
            <input aria-label="Max uses" value={maxUses} onChange={(event) => setMaxUses(event.target.value)} inputMode="numeric" className={INPUT} />
            <input aria-label="Expires hours" value={expiresHours} onChange={(event) => setExpiresHours(event.target.value)} inputMode="numeric" className={INPUT} />
            <Button type="submit" disabled={!mutationsEnabled || busy} className="text-xs">Create token</Button>
          </form>
          {mutationError && <p className="mt-2 text-xs text-status-down">{mutationError}</p>}
          <div className="mt-3 grid gap-3 xl:grid-cols-3">
            <JoinKeyList keys={mesh.join_keys} busy={busy} mutationsEnabled={mutationsEnabled} onRevoke={onRevoke} />
            <NodeList nodes={mesh.nodes} busy={busy} mutationsEnabled={mutationsEnabled} onSetNode={onSetNode} />
            <ModelOverrideList rows={meshRows} disabledModels={disabledModels} busy={busy} mutationsEnabled={mutationsEnabled} onSetModel={onSetModel} />
          </div>
        </>
      )}
    </Panel>
  );
}

function JoinKeyList({ keys, busy, mutationsEnabled, onRevoke }: { keys: MeshJoinKey[]; busy: boolean; mutationsEnabled: boolean; onRevoke: (id: string) => void }) {
  return (
    <MeshList title="tokens" count={keys.length}>
      {keys.map((key) => (
        <div key={key.id} className="border-b border-line/70 py-2 last:border-0">
          <div className="flex items-center justify-between gap-2">
            <span className="truncate font-mono text-[10px]" title={key.id}>{key.label || key.id}</span>
            <Button type="button" variant="danger" disabled={!mutationsEnabled || busy || !key.enabled} onClick={() => onRevoke(key.id)} className="px-2 py-1 text-[10px]">Revoke</Button>
          </div>
          <div className="mt-1 text-[10px] text-text-muted">
            {key.enabled ? 'enabled' : 'revoked'} · uses {key.use_count}/{key.max_uses ?? DASH} · expires {key.expires_at_ms ? fmtWhen(key.expires_at_ms) : DASH}
          </div>
        </div>
      ))}
      {keys.length === 0 && <EmptyMeshLine>No enrollment tokens.</EmptyMeshLine>}
    </MeshList>
  );
}

function NodeList({ nodes, busy, mutationsEnabled, onSetNode }: { nodes: MeshNode[]; busy: boolean; mutationsEnabled: boolean; onSetNode: (endpointId: string, enabled: boolean) => void }) {
  return (
    <MeshList title="nodes" count={nodes.length}>
      {nodes.map((node) => (
        <div key={node.endpoint_id} className="border-b border-line/70 py-2 last:border-0">
          <div className="flex items-center justify-between gap-2">
            <span className="truncate font-mono text-[10px]" title={node.endpoint_id}>{node.label || node.endpoint_id}</span>
            <Button type="button" variant={node.enabled ? 'danger' : 'default'} disabled={!mutationsEnabled || busy} onClick={() => onSetNode(node.endpoint_id, !node.enabled)} className="px-2 py-1 text-[10px]">
              {node.enabled ? 'Disable' : 'Enable'}
            </Button>
          </div>
          <div className="mt-1 text-[10px] text-text-muted">
            {node.enabled ? 'enabled' : 'disabled'} · last seen {node.last_seen_at_ms ? fmtElapsed(Date.now() - node.last_seen_at_ms) + ' ago' : DASH}
          </div>
        </div>
      ))}
      {nodes.length === 0 && <EmptyMeshLine>No enrolled nodes.</EmptyMeshLine>}
    </MeshList>
  );
}

function ModelOverrideList({
  rows,
  disabledModels,
  busy,
  mutationsEnabled,
  onSetModel,
}: {
  rows: Array<{ row: ProviderInventoryRow; endpointId: string | null }>;
  disabledModels: Set<string>;
  busy: boolean;
  mutationsEnabled: boolean;
  onSetModel: (endpointId: string, resourceId: string, model: string, disabled: boolean) => void;
}) {
  const entries = rows.flatMap(({ row, endpointId }) => row.advertisedModels.map((model) => ({ row, endpointId, model }))).filter((entry): entry is { row: ProviderInventoryRow; endpointId: string; model: string } => Boolean(entry.endpointId && entry.row.resourceId));
  return (
    <MeshList title="model overrides" count={disabledModels.size}>
      {entries.slice(0, 12).map(({ row, endpointId, model }) => {
        const resourceId = row.resourceId ?? '';
        const disabled = disabledModels.has(disabledModelKey({ endpoint_id: endpointId, resource_id: resourceId, model }));
        return (
          <div key={`${endpointId}/${resourceId}/${model}`} className="border-b border-line/70 py-2 last:border-0">
            <div className="flex items-center justify-between gap-2">
              <span className="truncate font-mono text-[10px]" title={`${endpointId}/${resourceId}/${model}`}>{model}</span>
              <Button type="button" variant={disabled ? 'default' : 'danger'} disabled={!mutationsEnabled || busy} onClick={() => onSetModel(endpointId, resourceId, model, !disabled)} className="px-2 py-1 text-[10px]">
                {disabled ? 'Enable' : 'Disable'}
              </Button>
            </div>
            <div className="mt-1 truncate text-[10px] text-text-muted">{endpointId} · {resourceId} · {disabled ? 'disabled' : 'routable'}</div>
          </div>
        );
      })}
      {entries.length === 0 && <EmptyMeshLine>No mesh models advertised.</EmptyMeshLine>}
    </MeshList>
  );
}

function MeshList({ title, count, children }: { title: string; count: number; children: ReactNode }) {
  return (
    <div className="min-w-0 rounded border border-line/70 bg-bg px-3 py-2">
      <div className="flex items-center justify-between border-b border-line/70 pb-1">
        <h3 className="text-[10px] uppercase tracking-[0.14em] text-text-muted">{title}</h3>
        <span className="font-mono text-[10px] text-text-muted">{count}</span>
      </div>
      <div>{children}</div>
    </div>
  );
}

function EmptyMeshLine({ children }: { children: ReactNode }) {
  return <p className="py-4 text-center text-xs italic text-text-muted" data-quality="unavailable">{children}</p>;
}

function endpointFromProvider(providerId: string): string | null {
  return providerId.startsWith('mesh:') ? providerId.slice('mesh:'.length) : providerId || null;
}

function disabledModelKey(value: Pick<MeshDisabledModel, 'endpoint_id' | 'resource_id' | 'model'>): string {
  return `${value.endpoint_id}\n${value.resource_id}\n${value.model.toLowerCase()}`;
}

function numberField(value: string): number | undefined {
  const parsed = Number(value.trim());
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : undefined;
}

function fmtWhen(ms: number): string {
  return new Date(ms).toISOString().slice(0, 16).replace('T', ' ');
}

function fmtFleetTime(value: string): string {
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? fmtElapsed(Date.now() - parsed) + ' ago' : value;
}

function SummaryStrip({ summary }: { summary: ProviderInventorySummary }) {
  const cells = [
    { label: 'providers', value: String(summary.providers), quality: 'measured' },
    { label: 'healthy', value: String(summary.healthy), quality: 'measured', accent: 'text-status-healthy' },
    { label: 'cooling', value: String(summary.cooling), quality: 'measured', accent: 'text-status-cooling' },
    { label: 'down', value: String(summary.down), quality: 'measured', accent: 'text-status-down' },
    { label: 'models', value: summary.advertisedModels ? String(summary.advertisedModels) : DASH, quality: summary.advertisedModels ? 'derived' : 'unavailable' },
    { label: 'requests', value: String(summary.requests), quality: 'measured' },
    { label: 'active', value: String(summary.active), quality: 'measured', accent: 'text-accent' },
    { label: 'cost', value: fmtCost(summary.cost), quality: costQuality(summary.costConfidence), accent: 'text-meta' },
  ];
  return (
    <Panel className="grid grid-cols-2 gap-px overflow-hidden bg-line sm:grid-cols-4 xl:grid-cols-8" data-testid="providers-summary">
      {cells.map((cell) => (
        <div key={cell.label} className="bg-panel px-3 py-2">
          <div className="text-[9px] uppercase tracking-[0.14em] text-text-muted">{cell.label}</div>
          <div className={cn('mt-1 font-mono text-lg tabular-nums text-text', cell.accent)} data-quality={cell.quality}>
            {cell.value}
          </div>
        </div>
      ))}
    </Panel>
  );
}

function ProviderTable({ rows, total }: { rows: ProviderInventoryRow[]; total: number }) {
  return (
    <Panel className="overflow-hidden" data-testid="providers-table" data-available={rows.length > 0 ? 'true' : 'false'}>
      <div className="flex items-center justify-between border-b border-line px-4 py-3">
        <h2 className="text-sm font-semibold">Providers</h2>
        <span className="font-mono text-[10px] text-text-muted">{rows.length} / {total}</span>
      </div>
      <div className="overflow-auto">
        <table className="w-full min-w-[1180px] text-left text-xs">
          <thead className="bg-panel text-[10px] uppercase tracking-[0.12em] text-text-muted">
            <tr>
              <th className="px-4 py-2">Provider</th>
              <th className="px-3 py-2">Advertised models</th>
              <th className="px-3 py-2">Windows & limits</th>
              <th className="px-3 py-2">Usage</th>
              <th className="px-3 py-2">Latency</th>
              <th className="px-3 py-2">Traffic</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-line">
            {rows.map((row) => <ProviderRow key={row.key} row={row} />)}
          </tbody>
        </table>
      </div>
      {rows.length === 0 && (
        <div className="p-6 text-center text-xs italic text-text-muted" data-testid="providers-empty" data-quality="unavailable">
          No providers match this filter.
        </div>
      )}
    </Panel>
  );
}

function ProviderRow({ row }: { row: ProviderInventoryRow }) {
  const errorRate = row.usage.requests > 0 ? (row.usage.failures / row.usage.requests) * 100 : null;
  const latency = buildProviderLatency(row.perProvider, row.id);
  return (
    <tr data-testid="provider-row" data-provider={row.id} data-resource={row.resourceId ?? ''}>
      <td className="px-4 py-3 align-top">
        <div className="flex items-center gap-2">
          <span className={cn('h-2.5 w-2.5 rounded-full', statusDot(row.status))} aria-hidden />
          <div>
            <div className="font-medium text-text">{row.name}</div>
            <div className="mt-0.5 font-mono text-[10px] text-text-muted">
              {row.id}{row.resourceId ? ` · ${row.resourceId}` : ''}{row.route ? ` · ${row.route}` : ''}
            </div>
          </div>
        </div>
        <div className="mt-2 max-w-[18rem] truncate font-mono text-[10px] text-text-muted" title={row.baseUrl}>{row.baseUrl}</div>
        <div className="mt-1 text-[10px] text-text-muted">
          catalog {row.catalogSize ?? DASH} · fetched {row.catalogFetchedMs ? fmtElapsed(Date.now() - row.catalogFetchedMs) + ' ago' : DASH}
        </div>
        {row.lastError && <div className="mt-1 max-w-[18rem] truncate text-[10px] text-status-down" title={row.lastError}>{row.lastError}</div>}
      </td>
      <td className="px-3 py-3 align-top">
        <div className="flex max-w-[19rem] flex-wrap gap-1">
          {row.advertisedModels.slice(0, 6).map((model) => (
            <span key={model} className="rounded-sm bg-line/60 px-1.5 py-0.5 font-mono text-[10px]" title={model}>{model}</span>
          ))}
          {row.advertisedModels.length > 6 && <span className="rounded-sm bg-line/40 px-1.5 py-0.5 text-[10px] text-text-muted">+{row.advertisedModels.length - 6}</span>}
          {row.advertisedModels.length === 0 && <span className="text-text-muted" data-quality="unavailable">{DASH}</span>}
        </div>
        <div className="mt-2 space-y-0.5">
          {row.contextWindows.slice(0, 3).map((entry) => (
            <div key={entry.model} className="flex max-w-[18rem] justify-between gap-3 font-mono text-[10px] text-text-muted">
              <span className="truncate">{entry.model}</span>
              <span data-quality={entry.contextLimit == null ? 'unavailable' : 'measured'}>{entry.contextLimit == null ? DASH : fmtTokens(entry.contextLimit)}</span>
            </div>
          ))}
        </div>
        <div className="mt-2 text-[10px] text-text-muted">
          priced {row.priceCoverage.priced}/{row.priceCoverage.total || 0}
        </div>
      </td>
      <td className="px-3 py-3 align-top">
        <div className="mb-2 border-l-2 border-accent/40 pl-2">
          <div className="text-[9px] uppercase tracking-[0.12em] text-text-muted">capacity / availability</div>
          <div className="mt-1 text-[10px] text-text-muted">
            {row.capacity.active ?? DASH}/{row.capacity.limit ?? DASH} active · {row.capacity.accepting ? 'accepting' : 'closed'}
          </div>
          <div className="mt-1 text-[10px] text-text-muted">
            {row.availability.summary.length > 0
              ? row.availability.summary.slice(0, 2).join(' · ')
              : row.availability.defaultCapacity == null
                ? 'Schedule unavailable'
                : `Default capacity ${row.availability.defaultCapacity}`}
            {row.availability.timezone ? ` · ${row.availability.timezone}` : ''}
          </div>
        </div>
        <div className="text-[9px] uppercase tracking-[0.12em] text-text-muted">access policy</div>
        {row.policy.policyIds.length === 0 ? (
          <div className="mt-1 text-[11px] text-text-muted" data-quality="unavailable">No model access policies</div>
        ) : (
          <>
            <div className="flex flex-wrap gap-1">
              <span className="rounded-sm bg-status-healthy/15 px-1.5 py-0.5 text-[10px] text-status-healthy">{row.policy.allowCount} allow</span>
              <span className="rounded-sm bg-status-down/15 px-1.5 py-0.5 text-[10px] text-status-down">{row.policy.denyCount} deny</span>
            </div>
            <div className="mt-2 text-[10px] text-text-muted">
              {row.policy.windows.length > 0 ? row.policy.windows.slice(0, 2).join(' · ') : 'Any time'}
            </div>
            <div className="mt-1 text-[10px] text-text-muted">
              {row.policy.limits.length > 0 ? row.policy.limits.join(' · ') : 'No session limits'}
            </div>
            <div className="mt-1 max-w-[18rem] truncate text-[10px] text-text-muted" title={row.policy.subjects.join(', ')}>
              subjects {row.policy.subjects.length ? row.policy.subjects.join(', ') : DASH}
            </div>
          </>
        )}
      </td>
      <td className="px-3 py-3 align-top font-mono text-[10px] tabular-nums">
        <MetricLine label="req" value={String(row.usage.requests)} quality="measured" />
        <MetricLine label="err" value={formatPercent(errorRate)} quality={errorRate == null ? 'unavailable' : 'derived'} danger={(errorRate ?? 0) > 0} />
        <MetricLine label="tok" value={fmtTokens(row.usage.promptTokens == null && row.usage.completionTokens == null ? null : (row.usage.promptTokens ?? 0) + (row.usage.completionTokens ?? 0))} quality={row.usage.promptTokens == null && row.usage.completionTokens == null ? 'unavailable' : 'measured'} />
        <MetricLine label="cache" value={fmtTokens(row.usage.cachedTokens)} quality={row.usage.cachedTokens == null ? 'unavailable' : 'measured'} />
        <MetricLine label="cost" value={fmtCost(row.usage.cost)} quality={costQuality(row.usage.costConfidence)} />
        <MetricLine label="hit%" value={formatPercent(row.cacheMetrics?.cache_hit_rate == null ? null : row.cacheMetrics.cache_hit_rate * 100)} quality={row.cacheMetrics?.cache_hit_rate == null ? 'unavailable' : 'derived'} />
        <MetricLine label="kv" value={formatPercent(row.cacheMetrics?.kv_cache_usage == null ? null : row.cacheMetrics.kv_cache_usage * 100)} quality={row.cacheMetrics?.kv_cache_usage == null ? 'unavailable' : 'derived'} />
      </td>
      <td className="px-3 py-3 align-top font-mono text-[10px] tabular-nums">
        <MetricLine label="p50" value={latency.p50.text} quality={latency.p50.quality} />
        <MetricLine label="p95" value={latency.p95.text} quality={latency.p95.quality} />
        <MetricLine label="p99" value={latency.p99.text} quality={latency.p99.quality} danger={row.perProvider ? row.perProvider.p99 >= 1000 : false} />
        <MetricLine label="fail" value={latency.errorRate.text} quality={latency.errorRate.quality} danger={(row.perProvider?.error_rate ?? 0) > 0} />
      </td>
      <td className="px-3 py-3 align-top font-mono text-[10px] tabular-nums">
        <MetricLine label="req/s" value={row.edge ? row.edge.throughput.toFixed(2) : DASH} quality={row.edge ? 'derived' : 'unavailable'} />
        <MetricLine label="tok/s" value={row.edge ? row.edge.tokens_per_sec.toFixed(row.edge.tokens_per_sec < 10 ? 1 : 0) : DASH} quality={row.edge ? 'derived' : 'unavailable'} />
        <MetricLine label="$/s" value={row.edge ? fmtCost(row.edge.cost_per_sec) : DASH} quality={row.edge && row.edge.cost_per_sec > 0 ? 'derived' : 'unavailable'} />
        <MetricLine label="attempt p99" value={row.perProvider ? fmtProviderLatencyMs(row.perProvider.p99) : DASH} quality={row.perProvider ? 'derived' : 'unavailable'} />
      </td>
    </tr>
  );
}

function MetricLine({ label, value, quality, danger }: { label: string; value: string; quality: string; danger?: boolean }) {
  return (
    <div className="flex justify-between gap-3">
      <span className="text-text-muted">{label}</span>
      <span data-quality={quality} className={cn(danger ? 'text-status-down' : 'text-text')}>{value}</span>
    </div>
  );
}

function ModelsPanel({ models }: { models: Array<{ id: string; contextLimit: number | null; priced: boolean }> }) {
  return (
    <div>
      <Panel className="p-4" data-testid="providers-catalog">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold">Global advertised model catalog</h2>
          <span className="font-mono text-[10px] text-text-muted">{models.length} models</span>
        </div>
        <p className="mt-1 text-[10px] leading-relaxed text-text-muted">
          Exact provider-scoped model advertisements from /dashboard/api/providers.
        </p>
        <div className="mt-3 grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
          {models.map((entry) => (
            <div key={entry.id} className="grid grid-cols-[minmax(0,1fr)_4rem_3rem] items-center gap-2 rounded border border-line/70 bg-bg px-2 py-1.5 text-[10px]">
              <span className="truncate font-mono" title={entry.id}>{entry.id}</span>
              <span className="text-right font-mono text-text-muted" data-quality={entry.contextLimit == null ? 'unavailable' : 'measured'}>{entry.contextLimit == null ? DASH : fmtTokens(entry.contextLimit)}</span>
              <span className={cn('text-right uppercase tracking-wide', entry.priced ? 'text-status-healthy' : 'text-text-muted')}>{entry.priced ? 'priced' : DASH}</span>
            </div>
          ))}
          {models.length === 0 && (
            <p className="py-4 text-center text-xs italic text-text-muted" data-quality="unavailable">
              No catalog models advertised yet.
            </p>
          )}
        </div>
      </Panel>
    </div>
  );
}

function statusDot(status: ProviderHealth['status']): string {
  if (status === 'healthy') return 'bg-status-healthy';
  if (status === 'cooling') return 'bg-status-cooling';
  return 'bg-status-down';
}
