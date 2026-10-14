import type { GpuInventory } from '../../api/types';
import { fmtElapsed } from '../../components/FlowTable/format';
import { cn } from '../../lib/cn';
import {
  fmtGib,
  gpuFreshness,
  gpuGroupOf,
  gpuOccupantLabel,
  gpuOccupied,
  gpuUsedFraction,
  groupGpus,
  AUTO_GPU_PICK,
  type GpuPickAvailability,
  type GpuPickState,
  type GpuSelectionResult,
} from './gpuModel';

const DASH = '—';

/**
 * Compact per-GPU VRAM board. Absent inventory renders an explicit `unavailable` state —
 * never zero-filled bars (don't-lie-with-zeros).
 */
export function GpuVramPanel({ inventory, testId = 'gpu-vram-panel', nowMs = Date.now() }: {
  inventory: GpuInventory | undefined;
  testId?: string;
  nowMs?: number;
}) {
  if (!inventory) {
    return (
      <div className="mt-2 rounded border border-line/60 bg-panel px-2 py-1.5 text-[10px]" data-testid={testId} data-available="false">
        <span className="uppercase tracking-[0.14em] text-text-muted">gpu vram</span>
        <span className="ml-2 font-mono text-text-muted" data-quality="unavailable">{DASH} GPU info unavailable</span>
      </div>
    );
  }
  const freshness = gpuFreshness(inventory, nowMs);
  const groups = groupGpus(inventory);
  return (
    <div className="mt-2 rounded border border-line/60 bg-panel px-2 py-1.5 text-[10px]" data-testid={testId} data-available="true" data-stale={freshness.stale ? 'true' : 'false'}>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="uppercase tracking-[0.14em] text-text-muted">gpu vram · {inventory.gpus.length} GPU{inventory.gpus.length === 1 ? '' : 's'}</span>
        <span
          className={cn('rounded-sm px-1 text-[9px] uppercase tracking-wide', freshness.stale ? 'bg-status-cooling/15 text-status-cooling' : 'bg-line/40 text-text-muted')}
          data-testid="gpu-vram-freshness"
          data-quality={freshness.stale ? 'stale' : 'measured'}
          title={`sampled ${inventory.sampled_at}`}
        >
          {freshness.stale ? 'stale · ' : ''}{freshness.ageMs === undefined ? 'age unknown' : `${fmtElapsed(freshness.ageMs)} ago`}
        </span>
      </div>
      {inventory.gpus.length === 0 && <p className="mt-1 font-mono text-text-muted" data-quality="unavailable">{DASH} no GPUs reported</p>}
      {groups.map((group) => (
        <div key={group.group ?? 'none'} className="mt-1.5" data-testid="gpu-group" data-group={group.group ?? 'none'}>
          {groups.length > 1 && <div className="mb-0.5 text-[9px] uppercase tracking-[0.14em] text-text-muted">{group.label}</div>}
          <div className={cn('space-y-0.5', groups.length > 1 && 'border-l border-line/70 pl-1.5')}>
            {group.gpus.map((gpu) => {
              const fraction = gpuUsedFraction(gpu);
              const occupants = gpuOccupantLabel(gpu);
              const lowFree = fraction !== null && fraction > 0.9;
              return (
                <div key={gpu.index} className="grid grid-cols-[2.6rem_minmax(3rem,1fr)_auto] items-center gap-x-2" data-testid="gpu-row" data-gpu={gpu.index}>
                  <span className="font-mono text-text" title={gpu.name}>GPU {gpu.index}</span>
                  <div
                    className="flex h-1.5 w-full overflow-hidden rounded-full bg-line/40"
                    role="meter"
                    aria-label={`GPU ${gpu.index} VRAM used`}
                    aria-valuemin={0}
                    aria-valuemax={gpu.memory_total_mib}
                    aria-valuenow={gpu.memory_used_mib}
                    data-quality={fraction === null ? 'unavailable' : 'measured'}
                  >
                    {fraction !== null && <div className={cn('h-full', lowFree ? 'bg-status-cooling' : 'bg-accent')} style={{ width: `${fraction * 100}%` }} />}
                  </div>
                  <span className="whitespace-nowrap text-right font-mono tabular-nums text-text-muted" data-quality="measured">
                    <span className="text-text">{fmtGib(gpu.memory_used_mib)}</span>/{fmtGib(gpu.memory_total_mib)} GiB · <span className={lowFree ? 'text-status-cooling' : 'text-text'}>{fmtGib(gpu.memory_free_mib)}</span> free
                    {gpu.utilization_percent !== undefined && <span data-testid="gpu-util"> · {Math.round(gpu.utilization_percent)}%</span>}
                  </span>
                  <span className="col-span-3 -mt-0.5 truncate pl-[3.1rem] text-[9px] text-text-muted" title={occupants ?? 'idle'} data-testid="gpu-occupants">
                    {occupants ? `used by ${occupants}` : 'idle'}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}

/** Load-time GPU picker: "Auto (most free VRAM)" by default, or a manual checkbox pick. */
export function GpuPicker({ inventory, availability, value, validation, onChange, disabled }: {
  inventory: GpuInventory | undefined;
  availability: GpuPickAvailability;
  value: GpuPickState;
  validation: GpuSelectionResult | null;
  onChange: (value: GpuPickState) => void;
  disabled?: boolean;
}) {
  const manualAllowed = availability.kind === 'available';
  const groups = inventory ? groupGpus(inventory) : [];
  const toggle = (index: number, checked: boolean) => onChange({
    mode: 'manual',
    selected: checked ? [...value.selected, index] : value.selected.filter((gpu) => gpu !== index),
  });
  return (
    <fieldset className="mt-3 rounded border border-line/70 bg-bg px-3 py-2 text-xs" data-testid="gpu-picker" disabled={disabled}>
      <legend className="px-1 text-[10px] uppercase tracking-[0.14em] text-text-muted">gpu placement</legend>
      <div className="flex flex-wrap gap-3">
        <label className="flex items-center gap-1.5">
          <input type="radio" name="gpu-placement" checked={value.mode === 'auto'} onChange={() => onChange(AUTO_GPU_PICK)} />
          Auto (most free VRAM)
        </label>
        <label className={cn('flex items-center gap-1.5', !manualAllowed && 'text-text-muted')}>
          <input type="radio" name="gpu-placement" checked={value.mode === 'manual'} disabled={!manualAllowed} onChange={() => onChange({ mode: 'manual', selected: [] })} />
          Pick GPUs
        </label>
      </div>
      {!manualAllowed && <p className="mt-1 text-[10px] text-text-muted" data-testid="gpu-picker-unavailable">{availability.reason}</p>}
      {manualAllowed && value.mode === 'manual' && inventory && (
        <>
          <p className="mt-1 text-[10px] text-text-muted">Select {availability.required} GPU{availability.required === 1 ? '' : 's'}; Fleet assigns them in order.</p>
          <div className="mt-1 max-h-56 space-y-1 overflow-auto">
            {groups.map((group) => (
              <div key={group.group ?? 'none'}>
                {groups.length > 1 && <div className="text-[9px] uppercase tracking-[0.14em] text-text-muted">{group.label}</div>}
                {group.gpus.map((gpu) => {
                  const occupied = gpuOccupied(gpu);
                  const occupants = gpuOccupantLabel(gpu);
                  return (
                    <label key={gpu.index} className={cn('flex items-center justify-between gap-2 rounded px-1 py-0.5 text-[10px]', occupied ? 'text-text-muted' : 'hover:bg-line/30')}>
                      <span className="flex min-w-0 items-center gap-1.5">
                        <input
                          type="checkbox"
                          aria-label={`GPU ${gpu.index}`}
                          checked={value.selected.includes(gpu.index)}
                          disabled={occupied}
                          onChange={(event) => toggle(gpu.index, event.target.checked)}
                        />
                        <span className="font-mono">GPU {gpu.index}</span>
                        <span className="truncate" title={occupants ?? undefined}>{occupants ? `· ${occupants}` : ''}</span>
                      </span>
                      <span className="shrink-0 font-mono tabular-nums">{fmtGib(gpu.memory_free_mib)} GiB free{inventory && gpuGroupOf(inventory, gpu) !== undefined && groups.length > 1 ? ` · g${gpuGroupOf(inventory, gpu)}` : ''}</span>
                    </label>
                  );
                })}
              </div>
            ))}
          </div>
          {validation && !validation.ok && <p className="mt-1 text-[10px] text-status-down" data-testid="gpu-picker-error">{validation.error}</p>}
          {validation?.crossGroup && (
            <p className="mt-1 text-[10px] text-status-cooling" data-testid="gpu-picker-cross-group">
              Selection spans PCIe groups; tensor-parallel traffic will cross the slower link.
            </p>
          )}
        </>
      )}
    </fieldset>
  );
}

/** Fleet placement warnings returned with a successful load. */
export function LoadWarnings({ warnings, testId }: { warnings: string[] | undefined; testId: string }) {
  if (!warnings?.length) return null;
  return (
    <ul className="mt-2 space-y-0.5 rounded border border-status-cooling/40 bg-status-cooling/10 px-2 py-1 text-[10px] text-status-cooling" data-testid={testId}>
      {warnings.map((warning, index) => <li key={index}>Fleet warning: {warning}</li>)}
    </ul>
  );
}
