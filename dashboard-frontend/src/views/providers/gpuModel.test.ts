import { describe, expect, it } from 'vitest';
import type { GpuInfo, GpuInventory } from '../../api/types';
import {
  AUTO_GPU_PICK,
  GPU_STALE_AFTER_MS,
  describeLoadError,
  fmtGib,
  gpuFreshness,
  gpuOccupantLabel,
  gpuOccupied,
  gpuPickAvailability,
  gpuUsedFraction,
  gpusToSend,
  groupGpus,
  instancesAdded,
  selectionSpansGroups,
  validateGpuSelection,
} from './gpuModel';

const gpu = (index: number, overrides: Partial<GpuInfo> = {}): GpuInfo => ({
  index,
  name: 'NVIDIA RTX PRO 6000',
  memory_total_mib: 97_887,
  memory_used_mib: 1_234,
  memory_free_mib: 96_653,
  assigned: [],
  ...overrides,
});

const inventory = (overrides: Partial<GpuInventory> = {}): GpuInventory => ({
  sampled_at: '2026-10-10T00:00:00Z',
  stale: false,
  groups: [[0, 1, 6, 7], [2, 3, 4, 5]],
  gpus: [0, 1, 2, 3, 4, 5, 6, 7].map((index) => gpu(index)),
  ...overrides,
});

describe('gpuModel formatting', () => {
  it('formats MiB as one-decimal GiB', () => {
    expect(fmtGib(97_887)).toBe('95.6');
    expect(fmtGib(1024)).toBe('1.0');
    expect(fmtGib(0)).toBe('0.0');
  });

  it('reports no used fraction when the total is unknown instead of a fake 0%', () => {
    expect(gpuUsedFraction(gpu(0, { memory_total_mib: 0 }))).toBeNull();
    expect(gpuUsedFraction(gpu(0, { memory_used_mib: 48_943 }))).toBeCloseTo(0.5, 2);
    expect(gpuUsedFraction(gpu(0, { memory_used_mib: 200_000 }))).toBe(1);
  });

  it('labels occupants, deduplicating models and counting hidden ones', () => {
    const shared = gpu(0, { assigned: [{ model_id: 'a', instance_id: 'a-0' }, { model_id: 'a', instance_id: 'a-1' }, { model_id: 'b' }] });
    expect(gpuOccupantLabel(shared)).toBe('a · b');
    expect(gpuOccupantLabel(gpu(1, { hidden_assignments: 2 }))).toBe('2 hidden');
    expect(gpuOccupantLabel(gpu(2))).toBeNull();
    expect(gpuOccupied(gpu(1, { hidden_assignments: 1 }))).toBe(true);
    expect(gpuOccupied(gpu(2))).toBe(false);
  });
});

describe('groupGpus', () => {
  it('buckets by PCIe group in group order, sorting GPUs and putting unknown last', () => {
    const inv = inventory({
      groups: [[0, 1], [2, 3]],
      gpus: [gpu(3, { group: 1 }), gpu(0, { group: 0 }), gpu(2, { group: 1 }), gpu(1), gpu(9)],
    });
    const groups = groupGpus(inv);
    expect(groups.map((group) => group.label)).toEqual(['PCIe group 0', 'PCIe group 1', 'ungrouped']);
    // GPU 1 has no `group` field but is listed in groups[0].
    expect(groups[0]!.gpus.map((entry) => entry.index)).toEqual([0, 1]);
    expect(groups[1]!.gpus.map((entry) => entry.index)).toEqual([2, 3]);
    expect(groups[2]!.gpus.map((entry) => entry.index)).toEqual([9]);
  });

  it('uses a single plain bucket when topology is unknown', () => {
    const groups = groupGpus(inventory({ groups: [], gpus: [gpu(1), gpu(0)] }));
    expect(groups).toHaveLength(1);
    expect(groups[0]!.label).toBe('GPUs');
    expect(groups[0]!.gpus.map((entry) => entry.index)).toEqual([0, 1]);
  });
});

describe('gpuFreshness', () => {
  const now = Date.parse('2026-10-10T00:01:00Z');

  it('prefers the server-computed age over the client clock', () => {
    expect(gpuFreshness(inventory({ age_ms: 5_000 }), now)).toEqual({ ageMs: 5_000, stale: false });
  });

  it('falls back to sampled_at and flags stale past the threshold', () => {
    expect(gpuFreshness(inventory(), now)).toEqual({ ageMs: 60_000, stale: false });
    expect(gpuFreshness(inventory(), now + GPU_STALE_AFTER_MS).stale).toBe(true);
  });

  it('treats a Fleet-flagged or unparseable sample as stale', () => {
    expect(gpuFreshness(inventory({ stale: true, age_ms: 1 }), now).stale).toBe(true);
    expect(gpuFreshness(inventory({ sampled_at: 'garbage' }), now)).toEqual({ ageMs: undefined, stale: true });
  });
});

describe('placement availability and selection', () => {
  it('computes newly started instances', () => {
    expect(instancesAdded(2, false, 0)).toBe(2);
    expect(instancesAdded(3, true, 1)).toBe(2);
    expect(instancesAdded(1, true, 2)).toBe(-1);
  });

  it('only offers manual picking for placement models adding instances with an inventory', () => {
    expect(gpuPickAvailability(inventory(), 4, 2)).toEqual({ kind: 'available', required: 8 });
    expect(gpuPickAvailability(inventory(), 0, 1).kind).toBe('unsupported');
    expect(gpuPickAvailability(inventory(), undefined, 1).kind).toBe('unsupported');
    expect(gpuPickAvailability(inventory(), 2, 0).kind).toBe('not_applicable');
    expect(gpuPickAvailability(undefined, 2, 1).kind).toBe('no_inventory');
    expect(gpuPickAvailability(inventory({ gpus: [] }), 2, 1).kind).toBe('no_inventory');
  });

  it('validates count, bounds, duplicates, unknown and occupied GPUs', () => {
    const inv = inventory({ gpus: [gpu(0), gpu(1, { assigned: [{ model_id: 'busy' }] }), gpu(2), gpu(3), gpu(6)] });
    expect(validateGpuSelection([0, 6], 2, inv)).toEqual({ ok: true, crossGroup: false });
    expect(validateGpuSelection([0], 2, inv).error).toBe('select exactly 2 GPUs (1 selected)');
    expect(validateGpuSelection([0, 0], 2, inv).error).toBe('each GPU may be picked once');
    expect(validateGpuSelection([0, 256], 2, inv).error).toBe('GPU indexes must be 0-255');
    expect(validateGpuSelection([0, 7], 2, inv).error).toBe("GPU 7 is not in this node's inventory");
    expect(validateGpuSelection([0, 1], 2, inv).error).toBe('GPU 1 is occupied');
    expect(validateGpuSelection(Array.from({ length: 65 }, (_, index) => index), 65, undefined).error).toBe('at most 64 GPUs');
  });

  it('warns when a pick spans PCIe groups', () => {
    const inv = inventory();
    expect(selectionSpansGroups([0, 1], inv)).toBe(false);
    expect(selectionSpansGroups([0, 2], inv)).toBe(true);
    expect(validateGpuSelection([0, 2], 2, inv)).toEqual({ ok: true, crossGroup: true });
  });

  it('sends gpus only for a valid manual pick', () => {
    const ok = { ok: true, crossGroup: false };
    expect(gpusToSend(AUTO_GPU_PICK, null)).toBeUndefined();
    expect(gpusToSend({ mode: 'manual', selected: [2, 3] }, ok)).toEqual([2, 3]);
    expect(gpusToSend({ mode: 'manual', selected: [2] }, { ok: false, error: 'x', crossGroup: false })).toBeUndefined();
    expect(gpusToSend({ mode: 'manual', selected: [] }, ok)).toBeUndefined();
  });
});

describe('describeLoadError', () => {
  it('explains GPU placement codes and keeps other errors readable', () => {
    expect(describeLoadError({ message: 'POST x failed: 409', code: 'gpus_unavailable', serverMessage: 'Requested GPUs are not available' }))
      .toBe('Requested GPUs are not available (gpus_unavailable)');
    expect(describeLoadError({ message: 'POST x failed: 400', code: 'invalid_gpus', serverMessage: 'GPU 9 is not in the node inventory' }))
      .toBe('Invalid GPU selection: GPU 9 is not in the node inventory (invalid_gpus)');
    expect(describeLoadError({ message: 'POST x failed: 409', code: 'model_held', serverMessage: 'model is held' })).toBe('model is held (model_held)');
    expect(describeLoadError(new Error('boom'))).toBe('Error: boom');
    expect(describeLoadError(null)).toBe('');
  });
});
