import { afterEach, describe, expect, it } from 'vitest';
import { useState } from 'react';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import type { GpuInventory } from '../../api/types';
import { GpuPicker, GpuVramPanel, LoadWarnings } from './GpuVram';
import { AUTO_GPU_PICK, gpuPickAvailability, validateGpuSelection, type GpuPickState } from './gpuModel';

afterEach(cleanup);

const inventory: GpuInventory = {
  sampled_at: '2026-10-10T00:00:00Z',
  stale: false,
  age_ms: 2_000,
  groups: [[0, 1], [2, 3]],
  gpus: [0, 1, 2, 3].map((index) => ({
    index,
    name: 'GPU',
    memory_total_mib: 98_304,
    memory_used_mib: index === 1 ? 90_000 : 1_000,
    memory_free_mib: index === 1 ? 8_304 : 97_304,
    group: index < 2 ? 0 : 1,
    assigned: index === 1 ? [{ model_id: 'resident' }] : [],
  })),
};

function Harness({ required }: { required: number }) {
  const [pick, setPick] = useState<GpuPickState>(AUTO_GPU_PICK);
  const availability = gpuPickAvailability(inventory, required, 1);
  const validation = pick.mode === 'manual' && availability.kind === 'available' ? validateGpuSelection(pick.selected, availability.required, inventory) : null;
  return <GpuPicker inventory={inventory} availability={availability} value={pick} validation={validation} onChange={setPick} />;
}

describe('GpuVramPanel', () => {
  it('renders used/total/free in GiB per GPU', () => {
    render(<GpuVramPanel inventory={inventory} />);
    const row = screen.getAllByTestId('gpu-row')[1]!;
    expect(row).toHaveTextContent('87.9/96.0 GiB · 8.1 free');
    expect(row).toHaveTextContent('used by resident');
    expect(screen.getByRole('meter', { name: 'GPU 1 VRAM used' })).toHaveAttribute('aria-valuenow', '90000');
  });

  it('never renders zero bars without inventory', () => {
    render(<GpuVramPanel inventory={undefined} />);
    expect(screen.getByTestId('gpu-vram-panel')).toHaveTextContent('— GPU info unavailable');
    expect(screen.queryByRole('meter')).toBeNull();
  });
});

describe('GpuPicker', () => {
  it('defaults to auto and warns when a manual pick spans PCIe groups', () => {
    render(<Harness required={2} />);
    expect(screen.getByRole('radio', { name: 'Auto (most free VRAM)' })).toBeChecked();
    fireEvent.click(screen.getByRole('radio', { name: 'Pick GPUs' }));
    expect(screen.getByRole('checkbox', { name: 'GPU 1' })).toBeDisabled();
    fireEvent.click(screen.getByRole('checkbox', { name: 'GPU 0' }));
    expect(screen.getByTestId('gpu-picker-error')).toHaveTextContent('select exactly 2 GPUs (1 selected)');
    fireEvent.click(screen.getByRole('checkbox', { name: 'GPU 2' }));
    expect(screen.queryByTestId('gpu-picker-error')).toBeNull();
    expect(screen.getByTestId('gpu-picker-cross-group')).toBeVisible();
    fireEvent.click(screen.getByRole('radio', { name: 'Auto (most free VRAM)' }));
    expect(screen.queryByRole('checkbox', { name: 'GPU 0' })).toBeNull();
  });
});

describe('LoadWarnings', () => {
  it('renders nothing without warnings and lists them otherwise', () => {
    const { rerender } = render(<LoadWarnings warnings={undefined} testId="w" />);
    expect(screen.queryByTestId('w')).toBeNull();
    rerender(<LoadWarnings warnings={['spans groups']} testId="w" />);
    expect(screen.getByTestId('w')).toHaveTextContent('Fleet warning: spans groups');
  });
});
