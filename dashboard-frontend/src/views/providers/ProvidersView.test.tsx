import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, cleanup, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { getConnection, queryKeys } from '../../api/connection';
import type { FleetModelsResponse, ProviderInventoryEntry } from '../../api/types';
import { renderWithQuery, resetWorld } from '../../components/testHarness';
import { ProvidersView } from './ProvidersView';

beforeEach(() => resetWorld({ mock: true }));
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
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

const fleetStatusCases: FleetModelsResponse = {
  models: [
    {
      model: { id: 'fleet-ready', image: 'ready:image' },
      status: { model_id: 'fleet-ready', phase: 'ready', desired_state: 'ready', assigned_gpus: [0] },
    },
    {
      model: { id: 'fleet-loading', image: 'loading:image' },
      status: { model_id: 'fleet-loading', phase: 'loading', desired_state: 'ready', assigned_gpus: [1], container_status: 'running' },
    },
    {
      model: { id: 'fleet-stopping', image: 'stopping:image' },
      status: { model_id: 'fleet-stopping', phase: 'stopping', desired_state: 'unloaded', assigned_gpus: [2], container_status: 'running' },
    },
    {
      model: { id: 'fleet-failed', image: 'failed:image' },
      status: { model_id: 'fleet-failed', phase: 'failed', desired_state: 'ready', assigned_gpus: [], last_error: 'boot failed' },
    },
    {
      model: { id: 'fleet-unhealthy', image: 'unhealthy:image' },
      status: { model_id: 'fleet-unhealthy', phase: 'unhealthy', desired_state: 'ready', assigned_gpus: [], last_error: 'readiness failed' },
    },
    {
      model: { id: 'fleet-unloaded', image: 'unloaded:image' },
      status: { model_id: 'fleet-unloaded', phase: 'unloaded', desired_state: 'unloaded', assigned_gpus: [] },
    },
  ],
};

describe('ProvidersView', () => {
  it('defaults slots to healthy while allowing all statuses to be shown', async () => {
    mockProviderSlots();
    renderWithQuery(<ProvidersView />);

    const status = await screen.findByRole('group', { name: 'Provider status' });
    expect(within(status).getByRole('button', { name: 'healthy' })).toHaveAttribute('aria-pressed', 'true');
    expect(within(status).getByRole('button', { name: 'all' })).toHaveAttribute('aria-pressed', 'false');
    expect(screen.getAllByTestId('provider-row').map((row) => row.dataset.resource)).toEqual(['gpu-a', 'gpu-c']);
    expect(screen.getByTestId('providers-table')).not.toHaveTextContent('gpu-b-model');

    fireEvent.click(within(status).getByRole('button', { name: 'all' }));
    expect(screen.getAllByTestId('provider-row')).toHaveLength(3);
    expect(screen.getByTestId('providers-table')).toHaveTextContent('gpu-b-model');
  });

  it('shows an empty healthy view without hiding the option to inspect down slots', async () => {
    vi.spyOn(getConnection().client, 'providers').mockResolvedValue({ providers: [
      slot('mesh:north', 'North lab', 'gpu-down', false),
    ] });
    renderWithQuery(<ProvidersView />);

    await screen.findByTestId('providers-empty');
    expect(screen.queryByTestId('provider-row')).not.toBeInTheDocument();
    fireEvent.click(within(screen.getByRole('group', { name: 'Provider status' })).getByRole('button', { name: 'down' }));
    expect(screen.getByTestId('provider-row')).toHaveTextContent('gpu-down-model');
  });

  it('pages slots in stable provider/slot order and resets when filters or page size change', async () => {
    const providers = Array.from({ length: 12 }, (_, i) => slot(
      i < 8 ? 'mesh:north' : 'mesh:south', i < 8 ? 'North lab' : 'South lab',
      `gpu-${String(i).padStart(2, '0')}`, i !== 0,
    )).reverse();
    vi.spyOn(getConnection().client, 'providers').mockResolvedValue({ providers });
    renderWithQuery(<ProvidersView />);
    fireEvent.click(within(await screen.findByRole('group', { name: 'Provider status' })).getByRole('button', { name: 'all' }));
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
    fireEvent.click(within(screen.getByRole('group', { name: 'Provider status' })).getByRole('button', { name: 'all' }));
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
    fireEvent.click(within(screen.getByRole('group', { name: 'Provider status' })).getByRole('button', { name: 'all' }));

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
    fireEvent.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Confirm load' }));

    await waitFor(() => expect(idleCard).toHaveTextContent('loading'));
  });

  it('shows Fleet model status lights for loaded, loading, failed, and unloaded states', async () => {
    vi.spyOn(getConnection().client, 'fleet').mockResolvedValue(fleetStatusCases);
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('fleet-panel');
    await waitFor(() => expect(within(panel).getAllByTestId('fleet-model-card')).toHaveLength(6));

    const ready = within(panel).getByText('fleet-ready').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    const loading = within(panel).getByText('fleet-loading').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    const stopping = within(panel).getByText('fleet-stopping').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    const failed = within(panel).getByText('fleet-failed').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    const unhealthy = within(panel).getByText('fleet-unhealthy').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    const unloaded = within(panel).getByText('fleet-unloaded').closest('[data-testid="fleet-model-card"]') as HTMLElement;

    expect(within(ready).getByRole('status', { name: 'model status: loaded' })).toHaveAttribute('data-status', 'loaded');
    expect(within(loading).getByRole('status', { name: 'model status: loading (ready)' })).toHaveAttribute('data-status', 'loading');
    expect(within(stopping).getByRole('status', { name: 'model status: stopping (unloaded)' })).toHaveAttribute('data-status', 'loading');
    expect(within(failed).getByRole('status', { name: 'model status: failed (failed)' })).toHaveAttribute('data-status', 'failed');
    expect(within(unhealthy).getByRole('status', { name: 'model status: failed (unhealthy)' })).toHaveAttribute('data-status', 'failed');
    expect(within(unloaded).getByRole('status', { name: 'model status: unloaded' })).toHaveAttribute('data-status', 'unloaded');
    expect(within(loading).getByRole('button', { name: 'Unload' })).toBeDisabled();
    expect(within(stopping).getByRole('button', { name: 'Unload' })).toBeDisabled();
    expect(within(failed).getByRole('button', { name: 'Unload' })).toBeEnabled();
    expect(within(unhealthy).getByRole('button', { name: 'Unload' })).toBeEnabled();
  });

  it('polls local Fleet state into terminal ready and failed indicators without manual invalidation', async () => {
    vi.useFakeTimers();
    const terminalFleetState: FleetModelsResponse = {
      models: [
        {
          model: { id: 'fleet-poll-ready', image: 'ready:image' },
          status: { model_id: 'fleet-poll-ready', phase: 'ready', desired_state: 'ready', assigned_gpus: [0] },
        },
        {
          model: { id: 'fleet-poll-failed', image: 'failed:image' },
          status: { model_id: 'fleet-poll-failed', phase: 'failed', desired_state: 'ready', assigned_gpus: [1], last_error: 'boot failed' },
        },
      ],
    };
    const fleet = vi.spyOn(getConnection().client, 'fleet')
      .mockResolvedValueOnce({
        models: [
          {
            model: { id: 'fleet-poll-ready', image: 'ready:image' },
            status: { model_id: 'fleet-poll-ready', phase: 'loading', desired_state: 'ready', assigned_gpus: [0] },
          },
          {
            model: { id: 'fleet-poll-failed', image: 'failed:image' },
            status: { model_id: 'fleet-poll-failed', phase: 'loading', desired_state: 'ready', assigned_gpus: [1] },
          },
        ],
      })
      .mockResolvedValue(terminalFleetState);
    renderWithQuery(<ProvidersView />);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    const panel = screen.getByTestId('fleet-panel');
    const ready = within(panel).getByText('fleet-poll-ready');
    const failed = within(panel).getByText('fleet-poll-failed');
    const readyCard = ready.closest('[data-testid="fleet-model-card"]') as HTMLElement;
    const failedCard = failed.closest('[data-testid="fleet-model-card"]') as HTMLElement;
    expect(within(readyCard).getByRole('status', { name: 'model status: loading (ready)' })).toHaveAttribute('data-status', 'loading');
    expect(within(failedCard).getByRole('status', { name: 'model status: loading (ready)' })).toHaveAttribute('data-status', 'loading');

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5_000);
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    await vi.waitFor(() => {
      expect(fleet).toHaveBeenCalledTimes(2);
      const updatedReadyCard = within(panel).getByText('fleet-poll-ready').closest('[data-testid="fleet-model-card"]') as HTMLElement;
      const updatedFailedCard = within(panel).getByText('fleet-poll-failed').closest('[data-testid="fleet-model-card"]') as HTMLElement;
      expect(within(updatedReadyCard).getByRole('status', { name: 'model status: loaded' })).toHaveAttribute('data-status', 'loaded');
      expect(within(updatedFailedCard).getByRole('status', { name: 'model status: failed (failed)' })).toHaveAttribute('data-status', 'failed');
    });
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

  it('confirms and invokes model loading advertised by a downstream mesh worker', async () => {
    const client = getConnection().client;
    const mesh = await client.mesh();
    const endpoint = mesh.nodes.find((node) => node.model_switching)!.endpoint_id;
    vi.spyOn(client, 'providers').mockResolvedValue({ providers: [slot(`mesh:${endpoint}`, 'North lab', 'gpu-a')] });
    const loadModel = vi.spyOn(client, 'loadMeshModel');
    const unloadModel = vi.spyOn(client, 'unloadMeshModel');
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('mesh-admin');
    await waitFor(() => expect(panel).toHaveTextContent('remote model switching'));
    await waitFor(() => expect(within(screen.getByTestId('remote-model-switcher')).getByText('North lab')).toBeVisible());
    const model = within(panel).getByText('qwen3-32b').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    fireEvent.click(within(model).getByRole('button', { name: 'Load' }));
    const dialog = screen.getByRole('dialog');
    expect(dialog).toHaveTextContent('Every currently loaded model');
    fireEvent.click(within(dialog).getByRole('button', { name: 'Confirm load' }));

    await waitFor(() => expect(model).toHaveTextContent('loading'));
    expect(unloadModel).toHaveBeenCalledWith(endpoint, 'qwen3-8b-flash');
    expect(loadModel).toHaveBeenCalledWith(endpoint, 'qwen3-32b');
  });

  it('confirms remote model unloads before sending the mutation', async () => {
    const client = getConnection().client;
    const mesh = await client.mesh();
    const endpoint = mesh.nodes.find((node) => node.model_switching)!.endpoint_id;
    vi.spyOn(client, 'mesh').mockResolvedValue({
      ...mesh,
      nodes: mesh.nodes.map((node) => node.endpoint_id === endpoint && node.model_switching ? {
        ...node,
        model_switching: {
          ...node.model_switching,
          models: node.model_switching.models.map((model) => model.id === 'qwen3-8b-flash'
            ? { ...model, phase: 'ready', desired_state: 'ready', assigned_gpus: [0, 1, 6, 7] }
            : model),
        },
      } : node),
    });
    const unloadModel = vi.spyOn(client, 'unloadMeshModel');
    renderWithQuery(<ProvidersView />);

    const switcher = await screen.findByTestId('remote-model-switcher');
    const model = within(switcher).getByText('qwen3-8b-flash').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    fireEvent.click(within(model).getByRole('button', { name: 'Unload' }));
    expect(unloadModel).not.toHaveBeenCalled();
    fireEvent.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Confirm unload' }));

    await waitFor(() => expect(unloadModel).toHaveBeenCalledWith(endpoint, 'qwen3-8b-flash'));
  });

  it('shows remote switchable model status lights from advertised Fleet phases', async () => {
    const client = getConnection().client;
    const mesh = await client.mesh();
    const endpoint = mesh.nodes.find((node) => node.model_switching)!.endpoint_id;
    vi.spyOn(client, 'mesh').mockResolvedValue({
      ...mesh,
      nodes: mesh.nodes.map((node) => node.endpoint_id === endpoint ? {
        ...node,
        model_switching: {
          provider: 'fleet',
          revision: 7,
          models: [
            { id: 'remote-ready', phase: 'ready', desired_state: 'ready' },
            { id: 'remote-loading', phase: 'loading', desired_state: 'ready' },
            { id: 'remote-stopping', phase: 'stopping', desired_state: 'unloaded' },
            { id: 'remote-failed', phase: 'failed', desired_state: 'ready' },
            { id: 'remote-unhealthy', phase: 'unhealthy', desired_state: 'ready' },
            { id: 'remote-unloaded', phase: 'unloaded', desired_state: 'unloaded' },
          ],
        },
      } : node),
    });
    vi.spyOn(client, 'providers').mockResolvedValue({ providers: [slot(`mesh:${endpoint}`, 'North lab', 'gpu-a')] });
    renderWithQuery(<ProvidersView />);

    const switcher = await screen.findByTestId('remote-model-switcher');
    await waitFor(() => expect(within(switcher).getAllByTestId('remote-switch-model')).toHaveLength(6));

    const ready = within(switcher).getByText('remote-ready').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    const loading = within(switcher).getByText('remote-loading').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    const stopping = within(switcher).getByText('remote-stopping').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    const failed = within(switcher).getByText('remote-failed').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    const unhealthy = within(switcher).getByText('remote-unhealthy').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    const unloaded = within(switcher).getByText('remote-unloaded').closest('[data-testid="remote-switch-model"]') as HTMLElement;

    expect(within(ready).getByRole('status', { name: 'model status: loaded' })).toHaveAttribute('data-status', 'loaded');
    expect(within(loading).getByRole('status', { name: 'model status: loading (ready)' })).toHaveAttribute('data-status', 'loading');
    expect(within(stopping).getByRole('status', { name: 'model status: stopping (unloaded)' })).toHaveAttribute('data-status', 'loading');
    expect(within(failed).getByRole('status', { name: 'model status: failed (failed)' })).toHaveAttribute('data-status', 'failed');
    expect(within(unhealthy).getByRole('status', { name: 'model status: failed (unhealthy)' })).toHaveAttribute('data-status', 'failed');
    expect(within(unloaded).getByRole('status', { name: 'model status: unloaded' })).toHaveAttribute('data-status', 'unloaded');
  });
});
