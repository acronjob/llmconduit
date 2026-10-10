import { describe, expect, it } from 'vitest';
import { isFleetModelsResponse, isFleetOperationResponse, isGpuInventory, isMeshAdminState, isSwitchMeshModelResponse } from './types';

const inventory = {
  sampled_at: '2026-10-10T00:00:00Z',
  stale: false,
  groups: [[0, 1, 6, 7], [2, 3, 4, 5]],
  gpus: [{ index: 0, name: 'NVIDIA RTX PRO 6000', memory_total_mib: 97887, memory_used_mib: 1234, memory_free_mib: 96000, utilization_percent: 3, group: 0, assigned: [{ model_id: 'ds41-flash-tp4-ssd', instance_id: 'ds41-flash-tp4-ssd' }] }],
};

describe('GPU inventory wire guards', () => {
  it('accepts the documented shape and tolerates absent optional fields', () => {
    expect(isGpuInventory(inventory)).toBe(true);
    const minimal = { ...inventory, gpus: [{ index: 1, name: 'x', memory_total_mib: 1, memory_used_mib: 0, memory_free_mib: 1, assigned: [{ model_id: 'm' }] }] };
    expect(isGpuInventory(minimal)).toBe(true);
    expect(isGpuInventory({ ...inventory, age_ms: 1500 })).toBe(true);
    expect(isGpuInventory({ ...inventory, gpus: [{ ...inventory.gpus[0], memory_free_mib: -1 }] })).toBe(false);
    expect(isGpuInventory({ ...inventory, stale: undefined })).toBe(false);
  });

  it('keeps older servers (no inventory, no warnings) valid', () => {
    expect(isFleetModelsResponse({ models: [] })).toBe(true);
    expect(isFleetModelsResponse({ models: [], gpu_inventory: inventory })).toBe(true);
    expect(isFleetModelsResponse({ models: [], gpu_inventory: { gpus: 'nope' } })).toBe(false);
    expect(isFleetOperationResponse({ changed: true })).toBe(true);
    expect(isFleetOperationResponse({ changed: true, warnings: ['spans groups'] })).toBe(true);
    expect(isFleetOperationResponse({ changed: true, warnings: [1] })).toBe(false);
    expect(isSwitchMeshModelResponse({ endpoint_id: 'e', model_id: 'm', accepted: true, changed: true, warnings: ['w'] })).toBe(true);
  });

  it('validates the inventory nested in mesh model switching', () => {
    const node = { endpoint_id: 'e', enabled: true, joined_at_ms: 1, model_switching: { provider: 'lil-fleet', revision: 1, models: [], gpu_inventory: inventory } };
    expect(isMeshAdminState({ join_keys: [], nodes: [node], disabled_models: [] })).toBe(true);
    const bad = { ...node, model_switching: { ...node.model_switching, gpu_inventory: { ...inventory, groups: [['x']] } } };
    expect(isMeshAdminState({ join_keys: [], nodes: [bad], disabled_models: [] })).toBe(false);
  });
});
