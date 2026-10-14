// Pure, DOM-free GPU inventory + placement model for the Providers view.
//
// The inventory comes from Fleet's `GET /v1/gpus` (local `/dashboard/api/fleet`) or from the
// latest copy a mesh worker pushed to the hub (`/dashboard/api/mesh`). Absent inventory means
// "GPU info unavailable" — never a board of zeros.

import type { GpuInfo, GpuInventory } from '../../api/types';

/** The hub receives at most one refresh per ~60 s per node; 3 missed refreshes = stale. */
export const GPU_STALE_AFTER_MS = 180_000;
/** Client-side bounds mirrored from the gateway (Fleet stays the final authority). */
export const MAX_GPU_PICK = 64;
export const MAX_GPU_INDEX = 255;

export const GPU_ERROR_MESSAGES: Record<string, string> = {
  gpus_unavailable: 'Requested GPUs are not available',
  gpus_not_supported: 'GPU placement is not supported by this Fleet or worker',
  gpus_not_applicable: 'GPU selection does not apply to this load',
  invalid_gpus: 'Invalid GPU selection',
};

/** MiB → GiB with one decimal (`97887` → `"95.6"`). */
export function fmtGib(mib: number): string {
  return (mib / 1024).toFixed(1);
}

export function gpuOccupied(gpu: GpuInfo): boolean {
  return gpu.assigned.length > 0 || (gpu.hidden_assignments ?? 0) > 0;
}

/** Distinct occupant model ids in first-seen order. */
export function gpuOccupants(gpu: GpuInfo): string[] {
  return Array.from(new Set(gpu.assigned.map((assignment) => assignment.model_id)));
}

/** Short occupant label, e.g. `qwen3-8b · +2 hidden`; `null` when the GPU is idle. */
export function gpuOccupantLabel(gpu: GpuInfo): string | null {
  const parts = gpuOccupants(gpu);
  const hidden = gpu.hidden_assignments ?? 0;
  if (hidden > 0) parts.push(`${hidden} hidden`);
  return parts.length ? parts.join(' · ') : null;
}

/** Used fraction in [0, 1]; `null` when the total is unknown (0), so no fake 0% bar. */
export function gpuUsedFraction(gpu: GpuInfo): number | null {
  if (gpu.memory_total_mib <= 0) return null;
  return Math.min(1, Math.max(0, gpu.memory_used_mib / gpu.memory_total_mib));
}

/** PCIe group of a GPU: its own `group`, else the first `groups` entry listing it. */
export function gpuGroupOf(inventory: GpuInventory, gpu: GpuInfo): number | undefined {
  if (gpu.group !== undefined) return gpu.group;
  const found = inventory.groups.findIndex((members) => members.includes(gpu.index));
  return found >= 0 ? found : undefined;
}

export interface GpuGroupView {
  /** Group index, or `null` for GPUs without a known group. */
  group: number | null;
  label: string;
  gpus: GpuInfo[];
}

/** GPUs bucketed by PCIe group (group order), ungrouped last; GPUs sorted by index. */
export function groupGpus(inventory: GpuInventory): GpuGroupView[] {
  const buckets = new Map<number | null, GpuInfo[]>();
  for (const gpu of inventory.gpus) {
    const group = gpuGroupOf(inventory, gpu) ?? null;
    const bucket = buckets.get(group) ?? [];
    bucket.push(gpu);
    buckets.set(group, bucket);
  }
  const keys = Array.from(buckets.keys()).sort((a, b) => (a === null ? 1 : b === null ? -1 : a - b));
  const hasGroups = keys.some((key) => key !== null);
  return keys.map((group) => ({
    group,
    label: group === null ? (hasGroups ? 'ungrouped' : 'GPUs') : `PCIe group ${group}`,
    gpus: (buckets.get(group) ?? []).slice().sort((a, b) => a.index - b.index),
  }));
}

export interface GpuFreshness {
  /** Sample age in ms, or `undefined` when neither `age_ms` nor a parseable `sampled_at` exists. */
  ageMs?: number;
  stale: boolean;
}

/** Prefers the server-computed `age_ms` (immune to client clock skew). */
export function gpuFreshness(inventory: GpuInventory, nowMs: number): GpuFreshness {
  let ageMs = inventory.age_ms;
  if (ageMs === undefined) {
    const sampled = Date.parse(inventory.sampled_at);
    if (Number.isFinite(sampled)) ageMs = Math.max(0, nowMs - sampled);
  }
  return { ageMs, stale: inventory.stale || ageMs === undefined || ageMs > GPU_STALE_AFTER_MS };
}

/**
 * Instances a load newly starts: the requested count for an inactive model, else the
 * increase over the current desired count (≤ 0 means GPU picking does not apply).
 */
export function instancesAdded(requested: number, active: boolean, currentDesired: number): number {
  return active ? requested - currentDesired : requested;
}

export type GpuPickAvailability =
  | { kind: 'available'; required: number }
  | { kind: 'unsupported'; reason: string }
  | { kind: 'not_applicable'; reason: string }
  | { kind: 'no_inventory'; reason: string };

/** Whether manual GPU picking is possible for this load, and how many GPUs it needs. */
export function gpuPickAvailability(
  inventory: GpuInventory | undefined,
  gpuCount: number | undefined,
  added: number,
): GpuPickAvailability {
  if (!gpuCount || gpuCount <= 0) {
    return { kind: 'unsupported', reason: 'This model has no GPU placement; Fleet picks its GPUs.' };
  }
  if (added <= 0) {
    return { kind: 'not_applicable', reason: 'No new instances start, so there are no GPUs to pick.' };
  }
  if (!inventory || inventory.gpus.length === 0) {
    return { kind: 'no_inventory', reason: 'GPU info unavailable; only automatic placement is possible.' };
  }
  return { kind: 'available', required: gpuCount * added };
}

export interface GpuPickState {
  mode: 'auto' | 'manual';
  selected: number[];
}

export const AUTO_GPU_PICK: GpuPickState = { mode: 'auto', selected: [] };

/**
 * The `gpus` to send: only for a manual pick that validated; `undefined` means automatic
 * placement (the field is omitted from the load body).
 */
export function gpusToSend(pick: GpuPickState, validation: GpuSelectionResult | null): number[] | undefined {
  if (pick.mode !== 'manual' || !validation?.ok || pick.selected.length === 0) return undefined;
  return pick.selected.slice();
}

export interface GpuSelectionResult {
  ok: boolean;
  error?: string;
  /** Selection spans more than one known PCIe group. */
  crossGroup: boolean;
}

/** Validates a manual pick against the bounds the gateway enforces and the live inventory. */
export function validateGpuSelection(selected: number[], required: number, inventory: GpuInventory | undefined): GpuSelectionResult {
  const crossGroup = inventory ? selectionSpansGroups(selected, inventory) : false;
  const fail = (error: string): GpuSelectionResult => ({ ok: false, error, crossGroup });
  if (selected.length > MAX_GPU_PICK) return fail(`at most ${MAX_GPU_PICK} GPUs`);
  if (selected.some((gpu) => !Number.isInteger(gpu) || gpu < 0 || gpu > MAX_GPU_INDEX)) return fail(`GPU indexes must be 0-${MAX_GPU_INDEX}`);
  if (new Set(selected).size !== selected.length) return fail('each GPU may be picked once');
  if (inventory) {
    const byIndex = new Map(inventory.gpus.map((gpu) => [gpu.index, gpu]));
    const unknown = selected.find((gpu) => !byIndex.has(gpu));
    if (unknown !== undefined) return fail(`GPU ${unknown} is not in this node's inventory`);
    const busy = selected.find((gpu) => gpuOccupied(byIndex.get(gpu)!));
    if (busy !== undefined) return fail(`GPU ${busy} is occupied`);
  }
  if (selected.length !== required) return fail(`select exactly ${required} GPU${required === 1 ? '' : 's'} (${selected.length} selected)`);
  return { ok: true, crossGroup };
}

export function selectionSpansGroups(selected: number[], inventory: GpuInventory): boolean {
  const groups = new Set<number>();
  for (const index of selected) {
    const gpu = inventory.gpus.find((entry) => entry.index === index);
    const group = gpu ? gpuGroupOf(inventory, gpu) : undefined;
    if (group !== undefined) groups.add(group);
  }
  return groups.size > 1;
}

/** Explains a load failure; GPU placement codes get a readable sentence plus the code. */
export function describeLoadError(error: unknown): string {
  if (!error) return '';
  const detail = error as { code?: unknown; serverMessage?: unknown; message?: unknown };
  const code = typeof detail.code === 'string' ? detail.code : undefined;
  const serverMessage = typeof detail.serverMessage === 'string' ? detail.serverMessage : undefined;
  if (code && GPU_ERROR_MESSAGES[code]) {
    const extra = serverMessage && serverMessage !== GPU_ERROR_MESSAGES[code] ? `: ${serverMessage}` : '';
    return `${GPU_ERROR_MESSAGES[code]}${extra} (${code})`;
  }
  if (serverMessage) return code ? `${serverMessage} (${code})` : serverMessage;
  return String(error);
}
