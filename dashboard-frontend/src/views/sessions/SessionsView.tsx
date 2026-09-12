/**
 * SessionsView — the session tree the gateway links requests into.
 *
 * Left: the most recently active ROOT sessions (harness badge, declared/inferred, request count,
 * last activity). Right: the selected node — its ancestors (breadcrumb), children (sub-sessions,
 * click to descend), and its newest requests with their chain lineage (divergence kind + cache
 * bust). "Show in Flows" cross-links into the flow table scoped to that session (deterministic SET,
 * like the other cross-links).
 *
 * Data is the durable history API only (SQL-backed). With no SQL store the endpoints answer 503 and
 * the view says so explicitly rather than rendering an empty tree that implies "no sessions".
 */
import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getConnection, queryKeys } from '../../api/connection';
import type { HistoryRequest, SessionRow } from '../../api/types';
import { flowFilterStore } from '../../store/flowFilterStore';
import { navigate } from '../../router/useHashRoute';
import { Panel } from '../../components/ui/Panel';
import { fmtClock, fmtTokens } from '../../components/FlowTable/format';
import { shortId } from '../../components/FlowTable/flowModel';
import { cn } from '../../lib/cn';
import { divergenceLabel, fmtAge, kindBadge, rollupRequests, sessionLabel, sortByActivity } from './sessionsModel';

const DASH = '—';

export function SessionsView() {
  const { client } = getConnection();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const roots = useQuery({
    queryKey: queryKeys.sessions,
    queryFn: () => client.historySessions({ roots: true }),
    refetchInterval: 10_000,
  });
  const detail = useQuery({
    queryKey: selectedId ? queryKeys.session(selectedId) : ['history', 'sessions', '__none__'],
    queryFn: () => client.historySession(selectedId as string),
    enabled: !!selectedId,
    refetchInterval: 10_000,
  });
  const rootRows = useMemo(() => sortByActivity(roots.data?.sessions ?? []), [roots.data]);
  const nowMs = Date.now();

  return (
    <div className="flex min-h-0 min-w-0 flex-1" data-testid="sessions-view">
      <div className="flex min-h-0 w-[38%] min-w-[320px] flex-col border-r border-line">
        <div className="flex items-baseline gap-2 border-b border-line bg-panel px-3 py-2">
          <h1 className="text-sm font-semibold uppercase tracking-[0.18em] text-text">sessions</h1>
          <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">harness · session · sub-session</span>
          <span className="ml-auto tabular-nums text-xs text-text-muted" data-testid="sessions-count">
            {roots.data ? `${rootRows.length} root${rootRows.length === 1 ? '' : 's'}${roots.data.truncated ? '+' : ''}` : ''}
          </span>
        </div>
        <div className="grid grid-cols-[minmax(120px,1fr)_auto_auto_56px_48px] gap-2 border-b border-line bg-panel-raised px-3 py-1.5 text-[10px] uppercase tracking-[0.14em] text-text-muted">
          <span>session</span>
          <span>harness</span>
          <span>kind</span>
          <span className="text-right">reqs</span>
          <span className="text-right">last</span>
        </div>
        <div className="min-h-0 flex-1 overflow-auto">
          {roots.isError && <Unavailable testId="sessions-unavailable" error={roots.error} />}
          {roots.data && rootRows.length === 0 && (
            <div className="px-3 py-6 text-center text-xs text-text-muted" data-testid="sessions-empty">
              No sessions in the last 7 days.
            </div>
          )}
          {rootRows.map((node) => (
            <SessionRowView key={node.id} node={node} nowMs={nowMs} selected={node.id === selectedId} onSelect={setSelectedId} />
          ))}
        </div>
      </div>
      <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-auto p-3">
        {!selectedId && (
          <div className="px-3 py-6 text-center text-xs text-text-muted" data-testid="session-detail-empty">
            Select a session to see its sub-sessions and requests.
          </div>
        )}
        {selectedId && detail.isError && <Unavailable testId="session-detail-unavailable" error={detail.error} />}
        {selectedId && detail.data && (
          <SessionDetailView
            data={detail.data}
            nowMs={nowMs}
            onSelect={setSelectedId}
          />
        )}
      </div>
    </div>
  );
}

function Unavailable({ testId, error }: { testId: string; error: unknown }) {
  const message = error instanceof Error ? error.message : String(error);
  const disabled = message.includes('503');
  return (
    <div className="px-3 py-6 text-center text-xs text-text-muted" data-testid={testId} data-reason={disabled ? 'disabled' : 'error'}>
      {disabled
        ? 'Durable history is disabled: configure control_plane.storage (sqlite or postgres) to see sessions.'
        : `Could not load sessions: ${message}`}
    </div>
  );
}

function HarnessBadge({ node }: { node: SessionRow }) {
  return (
    <span
      className="shrink-0 rounded-sm bg-accent/15 px-1 text-[9px] uppercase tracking-wide text-accent"
      data-testid="session-harness"
      title={node.harness_version ? `${node.harness} ${node.harness_version}` : node.harness}
    >
      {node.harness}
    </span>
  );
}

function KindBadge({ node }: { node: SessionRow }) {
  const badge = kindBadge(node);
  return (
    <span
      className={cn('shrink-0 rounded-sm px-1 text-[9px] uppercase tracking-wide', badge.inferred ? 'bg-status-cooling/15 text-status-cooling' : 'bg-line/40 text-text-muted')}
      data-testid="session-kind"
      data-kind={node.kind}
      title={badge.title}
    >
      {badge.label}
    </span>
  );
}

function SessionRowView({ node, nowMs, selected, onSelect }: { node: SessionRow; nowMs: number; selected: boolean; onSelect: (id: string) => void }) {
  return (
    <button
      type="button"
      onClick={() => onSelect(node.id)}
      data-testid="session-row"
      data-selected={selected || undefined}
      title={node.external_id ? `${node.harness} session ${node.external_id}` : `${node.harness} (no session id on the wire)`}
      className={cn(
        'grid w-full grid-cols-[minmax(120px,1fr)_auto_auto_56px_48px] items-center gap-2 border-b border-line/50 px-3 py-1.5 text-left text-xs transition-colors',
        selected ? 'bg-accent/12' : 'hover:bg-accent/[0.06]',
      )}
    >
      <span className="truncate font-mono text-text">{sessionLabel(node)}</span>
      <HarnessBadge node={node} />
      <KindBadge node={node} />
      <span className="text-right tabular-nums text-text-muted" data-testid="session-request-count" title="requests linked to this node (measured)">
        {node.request_count}
      </span>
      <span className="text-right tabular-nums text-text-muted" title={`last activity ${new Date(node.last_seen_ms).toISOString()}`}>
        {fmtAge(node.last_seen_ms, nowMs)}
      </span>
    </button>
  );
}

function SessionDetailView({
  data,
  nowMs,
  onSelect,
}: {
  data: { session: SessionRow; ancestors: SessionRow[]; children: SessionRow[]; requests: HistoryRequest[]; requests_truncated: boolean };
  nowMs: number;
  onSelect: (id: string) => void;
}) {
  const { session, ancestors, children, requests } = data;
  const rollup = useMemo(() => rollupRequests(requests), [requests]);
  const showInFlows = () => {
    flowFilterStore.getState().setSession(session.id);
    navigate('flows');
  };
  return (
    <div className="flex flex-col gap-3" data-testid="session-detail">
      {/* breadcrumb: root … parent › this */}
      <nav className="flex flex-wrap items-center gap-1 text-xs" aria-label="session ancestors" data-testid="session-breadcrumb">
        {[...ancestors].reverse().map((node) => (
          <span key={node.id} className="flex items-center gap-1">
            <button type="button" className="font-mono text-text-muted hover:text-text" onClick={() => onSelect(node.id)}>
              {sessionLabel(node)}
            </button>
            <span className="text-text-muted">›</span>
          </span>
        ))}
        <span className="font-mono text-text">{sessionLabel(session)}</span>
      </nav>

      <Panel raised className="flex flex-wrap items-center gap-x-4 gap-y-1 px-3 py-2">
        <HarnessBadge node={session} />
        <KindBadge node={session} />
        {session.session_kind && (
          <span className="text-xs text-text-muted" data-testid="session-session-kind" title="session kind declared by the harness">
            {session.session_kind}
          </span>
        )}
        <Stat testId="session-stat-requests" label="requests" value={String(session.request_count)} quality="measured" />
        <Stat testId="session-stat-depth" label="depth" value={String(session.depth)} quality="measured" />
        <Stat testId="session-stat-busts" label={`cache busts / ${rollup.count}`} value={rollup.quality === 'unavailable' ? DASH : String(rollup.busts)} quality={rollup.quality} accent={rollup.busts > 0 ? 'text-status-cooling' : undefined} />
        <Stat testId="session-stat-tokens" label="in · out · cached" value={rollup.inputTokens == null ? DASH : `${fmtTokens(rollup.inputTokens)} · ${fmtTokens(rollup.outputTokens)} · ${fmtTokens(rollup.cachedTokens)}`} quality={rollup.inputTokens == null ? 'unavailable' : 'derived'} />
        <span className="text-xs text-text-muted" title={session.client_label ?? 'no client attribution'}>
          {session.client_label ?? DASH}
        </span>
        <button
          type="button"
          onClick={showInFlows}
          className="ml-auto rounded-md border border-line px-2.5 py-1 text-xs text-text-muted transition-colors hover:text-text"
          data-testid="session-show-in-flows"
        >
          show in flows
        </button>
      </Panel>

      {children.length > 0 && (
        <section data-testid="session-children">
          <h2 className="mb-1 text-[10px] uppercase tracking-[0.14em] text-text-muted">sub-sessions · {children.length}</h2>
          <div className="rounded-md border border-line">
            {children.map((child) => (
              <SessionRowView key={child.id} node={child} nowMs={nowMs} selected={false} onSelect={onSelect} />
            ))}
          </div>
        </section>
      )}

      <section data-testid="session-requests">
        <h2 className="mb-1 text-[10px] uppercase tracking-[0.14em] text-text-muted">
          requests · {requests.length}{data.requests_truncated ? ' (newest shown)' : ''}
        </h2>
        {requests.length === 0 ? (
          <div className="px-3 py-3 text-xs italic text-text-muted" data-testid="session-requests-empty">No requests recorded on this node yet.</div>
        ) : (
          <div className="rounded-md border border-line">
            <div className="grid grid-cols-[88px_112px_minmax(120px,1fr)_96px_72px_72px_72px] gap-2 border-b border-line bg-panel-raised px-3 py-1.5 text-[10px] uppercase tracking-[0.14em] text-text-muted">
              <span>time</span><span>id</span><span>model</span><span>lineage</span>
              <span className="text-right">in</span><span className="text-right">cached</span><span className="text-right">out</span>
            </div>
            {requests.map((request) => (
              <RequestRowView key={request.id} request={request} />
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

function Stat({ testId, label, value, quality, accent }: { testId: string; label: string; value: string; quality: 'measured' | 'derived' | 'unavailable'; accent?: string }) {
  return (
    <span className="flex flex-col" data-testid={testId} data-quality={quality} title={`${label}: ${quality}`}>
      <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">{label}</span>
      <span className={cn('font-mono text-sm tabular-nums', quality === 'unavailable' ? 'text-text-muted' : accent ?? 'text-text')}>{value}</span>
    </span>
  );
}

function RequestRowView({ request }: { request: HistoryRequest }) {
  const lineage = divergenceLabel(request.divergence_kind);
  return (
    <div
      className="grid grid-cols-[88px_112px_minmax(120px,1fr)_96px_72px_72px_72px] items-center gap-2 border-b border-line/50 px-3 py-1.5 text-xs"
      data-testid="session-request"
      data-cache-bust={request.cache_bust === true ? 'true' : 'false'}
      title={request.chain_parent_request_id ? `extends ${request.chain_parent_request_id}` : 'chain start'}
    >
      <span className="tabular-nums text-text-muted">{fmtClock(request.created_at_ms)}</span>
      <span className="truncate font-mono text-text-muted">{shortId(request.id)}</span>
      <span className="truncate">{request.resolved_model ?? request.client_model}</span>
      <span
        className={cn('w-fit rounded-sm px-1 text-[9px] uppercase tracking-wide', lineage.bust ? 'bg-status-cooling/15 text-status-cooling' : 'bg-line/40 text-text-muted')}
        data-testid="session-request-lineage"
        data-kind={request.divergence_kind ?? undefined}
        title={`${lineage.title}${request.divergence_index != null ? ` (item ${request.divergence_index})` : ''}`}
      >
        {lineage.bust ? `bust · ${lineage.label}` : lineage.label}
      </span>
      <span className="text-right tabular-nums text-text-muted">{fmtTokens(request.input_tokens)}</span>
      <span className="text-right tabular-nums text-text-muted">{fmtTokens(request.cached_tokens)}</span>
      <span className="text-right tabular-nums text-text-muted">{fmtTokens(request.output_tokens)}</span>
    </div>
  );
}
