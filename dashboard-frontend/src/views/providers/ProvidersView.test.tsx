import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, cleanup, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { getConnection, queryKeys } from '../../api/connection';
import type { ProviderInventoryEntry } from '../../api/types';
import { renderWithQuery, resetWorld } from '../../components/testHarness';
import { ProvidersView } from './ProvidersView';

beforeEach(() => resetWorld({ mock: true }));
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

const slot = (providerId: string, name: string, resourceId: string, healthy = true): ProviderInventoryEntry => ({
  provider_id: providerId,
  provider_name: name,
  resource_id: resourceId,
  route: null,
  base_url: 'https://inference.example/v1',
  models: [{ id: `${resourceId}-model`, context_limit: 32768 }],
  availability: null,
  capacity_limit: 4,
  active_requests: 0,
  accepting_requests: healthy,
  healthy,
});

function mockProviderSlots() {
  vi.spyOn(getConnection().client, 'providers').mockResolvedValue({ providers: [
    slot('mesh:south', 'South lab', 'gpu-c'),
    slot('mesh:north', 'North lab', 'gpu-a'),
    slot('mesh:north', 'North lab', 'gpu-b', false),
  ] });
}

describe('ProvidersView', () => {
  it('pages slots in stable provider/slot order and resets when filters or page size change', async () => {
    const providers = Array.from({ length: 12 }, (_, i) => slot(
      i < 8 ? 'mesh:north' : 'mesh:south', i < 8 ? 'North lab' : 'South lab',
      `gpu-${String(i).padStart(2, '0')}`, i !== 0,
    )).reverse();
    vi.spyOn(getConnection().client, 'providers').mockResolvedValue({ providers });
    renderWithQuery(<ProvidersView />);
    await waitFor(() => expect(screen.getAllByTestId('provider-row')).toHaveLength(5));
    const table = screen.getByTestId('providers-table');
    expect(table).toHaveTextContent('Showing 1–5 of 12 matching slots');
    expect(screen.getByRole('button', { name: 'Previous page' })).toBeDisabled();
    expect(screen.getAllByTestId('provider-row').map((row) => row.dataset.resource)).toEqual(['gpu-00', 'gpu-01', 'gpu-02', 'gpu-03', 'gpu-04']);
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    expect(screen.getAllByTestId('provider-group')).toHaveLength(2);
    expect(table).toHaveTextContent('Showing 6–10 of 12 matching slots');
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    expect(screen.getAllByTestId('provider-row')).toHaveLength(2);
    expect(screen.getByRole('button', { name: 'Next page' })).toBeDisabled();

    fireEvent.change(screen.getByRole('combobox', { name: 'Filter by provider' }), { target: { value: 'North lab' } });
    expect(table).toHaveTextContent('Showing 1–5 of 8 matching slots');
    expect(screen.getAllByTestId('provider-group')).toHaveLength(1);
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    fireEvent.change(screen.getByRole('combobox', { name: 'Slots per page' }), { target: { value: '10' } });
    expect(screen.getAllByTestId('provider-row')).toHaveLength(8);
    expect(table).toHaveTextContent('Page 1 of 1');

    fireEvent.change(screen.getByRole('textbox', { name: 'Search providers' }), { target: { value: ' GPU-00-MODEL ' } });
    expect(screen.getAllByTestId('provider-row')).toHaveLength(1);
    fireEvent.change(screen.getByRole('combobox', { name: 'Filter by availability' }), { target: { value: 'accepting' } });
    expect(screen.getByTestId('providers-empty')).toBeVisible();
    expect(table).toHaveTextContent('Showing 0 of 0 matching slots');
    fireEvent.click(screen.getByRole('button', { name: 'Clear filters' }));
    expect(screen.getAllByTestId('provider-row')).toHaveLength(10);
  });

  it('clamps pagination when live slots disappear without restoring a stale page later', async () => {
    const providers = Array.from({ length: 11 }, (_, i) => slot('mesh:north', 'North lab', `gpu-${i}`));
    vi.spyOn(getConnection().client, 'providers').mockResolvedValue({ providers });
    const { queryClient } = renderWithQuery(<ProvidersView />);
    await waitFor(() => expect(screen.getAllByTestId('provider-row')).toHaveLength(5));
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    await act(async () => { queryClient.setQueryData(queryKeys.providers, { providers: providers.slice(0, 2) }); });
    await waitFor(() => expect(screen.getAllByTestId('provider-row')).toHaveLength(2));
    expect(screen.getByTestId('providers-table')).toHaveTextContent('Page 1 of 1');
    await act(async () => { queryClient.setQueryData(queryKeys.providers, { providers }); });
    await waitFor(() => expect(screen.getByTestId('providers-table')).toHaveTextContent('Page 1 of 3'));
  });

  it('groups resource slots under provider-name headings without losing slot details', async () => {
    mockProviderSlots();
    renderWithQuery(<ProvidersView />);

    await waitFor(() => expect(screen.getAllByTestId('provider-group')).toHaveLength(2));
    const [north, south] = screen.getAllByTestId('provider-group');
    expect(within(north!).getByRole('heading', { name: 'North lab' })).toBeVisible();
    expect(within(north!).getAllByTestId('provider-row')).toHaveLength(2);
    expect(north).toHaveTextContent('2 / 2 slots');
    expect(north).toHaveTextContent('gpu-a-model');
    expect(north).toHaveTextContent('gpu-b-model');
    expect(north).not.toHaveTextContent('gpu-c-model');
    expect(within(south!).getByRole('heading', { name: 'South lab' })).toBeVisible();
    expect(within(south!).getAllByTestId('provider-row')).toHaveLength(1);
    expect(south).toHaveTextContent('1 / 1 slot');
  });

  it('filters slots within provider groups and hides groups without matches', async () => {
    mockProviderSlots();
    renderWithQuery(<ProvidersView />);
    await waitFor(() => expect(screen.getAllByTestId('provider-group')).toHaveLength(2));

    fireEvent.click(within(screen.getByRole('group', { name: 'Provider status' })).getByRole('button', { name: 'down' }));
    expect(screen.getAllByTestId('provider-group')).toHaveLength(1);
    const north = screen.getByTestId('provider-group');
    expect(north).toHaveTextContent('North lab');
    expect(north).toHaveTextContent('1 / 2 slots');
    expect(north).toHaveTextContent('gpu-b-model');
    expect(north).not.toHaveTextContent('gpu-a-model');

    fireEvent.click(within(screen.getByRole('group', { name: 'Provider status' })).getByRole('button', { name: 'all' }));
    fireEvent.change(screen.getByRole('textbox', { name: 'Search providers' }), { target: { value: 'South lab' } });
    expect(screen.getAllByTestId('provider-group')).toHaveLength(1);
    expect(screen.getByTestId('provider-group')).toHaveTextContent('gpu-c-model');

    fireEvent.change(screen.getByRole('textbox', { name: 'Search providers' }), { target: { value: 'missing-slot' } });
    expect(screen.queryByTestId('provider-group')).not.toBeInTheDocument();
    expect(screen.getByTestId('providers-empty')).toBeVisible();
  });

  it('renders exact advertised models, operational capacity, access limits, and cache metrics', async () => {
    renderWithQuery(<ProvidersView />);

    await screen.findByTestId('providers-view');
    await waitFor(() => expect(screen.getAllByTestId('provider-row').length).toBeGreaterThan(0));

    const table = screen.getByTestId('providers-table');
    expect(table).toHaveTextContent('vllm-a');
    expect(table).toHaveTextContent('llama-3.1-70b');
    expect(table).toHaveTextContent('2/8 active');
    expect(table).toHaveTextContent('America/Chicago');
    expect(table).toHaveTextContent('4 concurrent sessions');
    expect(table).toHaveTextContent('hit%');
    expect(table).toHaveTextContent('kv');
    expect(screen.getByTestId('providers-catalog')).toHaveTextContent('Exact provider-scoped model advertisements');
  });

  it('renders Fleet state and sends CSRF-gated load/unload actions', async () => {
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('fleet-panel');
    await waitFor(() => expect(within(panel).getAllByTestId('fleet-model-card').length).toBeGreaterThan(0));
    expect(panel).toHaveTextContent('qwen3-8b-flash');
    expect(panel).toHaveTextContent('GPU 0');

    const idleCard = within(panel).getByText('qwen3-32b').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    fireEvent.click(within(idleCard).getByRole('button', { name: 'Load' }));

    await waitFor(() => expect(idleCard).toHaveTextContent('loading'));
  });

  it('discovers and removes additional OpenAI-compatible providers', async () => {
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('configured-providers-panel');
    expect(panel).toHaveTextContent('Local lab');
    expect(panel).not.toHaveTextContent('secret-value');

    fireEvent.change(within(panel).getByLabelText('Provider name'), { target: { value: 'Remote lab' } });
    fireEvent.change(within(panel).getByLabelText('Provider URL'), { target: { value: 'https://inference.example/v1' } });
    fireEvent.change(within(panel).getByLabelText('Provider API key'), { target: { value: 'secret-value' } });
    fireEvent.click(within(panel).getByRole('button', { name: 'Discover & add' }));

    await waitFor(() => expect(panel).toHaveTextContent('Remote lab'));
    expect(panel).not.toHaveTextContent('secret-value');
  });

  it('renders and invokes model switching advertised by a downstream mesh worker', async () => {
    const client = getConnection().client;
    const mesh = await client.mesh();
    const endpoint = mesh.nodes.find((node) => node.model_switching)!.endpoint_id;
    vi.spyOn(client, 'providers').mockResolvedValue({ providers: [slot(`mesh:${endpoint}`, 'North lab', 'gpu-a')] });
    const switchModel = vi.spyOn(client, 'switchMeshModel');
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('mesh-admin');
    await waitFor(() => expect(panel).toHaveTextContent('remote model switching'));
    await waitFor(() => expect(within(screen.getByTestId('remote-model-switcher')).getByText('North lab')).toBeVisible());
    const model = within(panel).getByText('qwen3-32b').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    fireEvent.click(within(model).getByRole('button', { name: 'Switch' }));

    await waitFor(() => expect(model).toHaveTextContent('loading'));
    expect(switchModel).toHaveBeenCalledWith(endpoint, 'qwen3-32b');
  });
});
