import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getConnection, queryKeys } from '../../api/connection';
import type { ProviderHealth } from '../../api/types';
import { EMPTY_FILTERS } from '../../components/FlowTable/filterTypes';
import { useFlowRows } from '../../components/FlowTable/useFlowRows';
import { fmtCost, fmtElapsed, fmtTokens } from '../../components/FlowTable/format';
import { buildProviderLatency, fmtProviderLatencyMs } from '../../components/viz/providerLatency';
import { Panel } from '../../components/ui/Panel';
import { useDashboard } from '../../store/hooks';
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

export function ProvidersView() {
  const [status, setStatus] = useState<'all' | ProviderHealth['status']>('all');
  const [query, setQuery] = useState('');
  const { client } = getConnection();
  const nodes = useDashboard((s) => s.topologyNodes);
  const edges = useDashboard((s) => s.topologyEdges);
  const { rows: flows } = useFlowRows(EMPTY_FILTERS);
  const { perProviderById } = useTopologyQuery();

  const topologyQuery = useQuery({ queryKey: queryKeys.topology, queryFn: () => client.topology() });
  const providersQuery = useQuery({ queryKey: queryKeys.providers, queryFn: () => client.providers() });
  const providerMetricsQuery = useQuery({ queryKey: queryKeys.providerMetrics, queryFn: () => client.providerMetrics() });
  const policiesQuery = useQuery({ queryKey: authPoliciesKey, queryFn: () => client.authPolicies() });

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
        <ProviderTable rows={filteredRows} total={inventory.rows.length} />
        <ModelsPanel models={inventory.unionCatalogModels} />
      </div>
    </div>
  );
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
