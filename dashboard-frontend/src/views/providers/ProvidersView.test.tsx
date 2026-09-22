import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { getConnection } from '../../api/connection';
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
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('mesh-admin');
    await waitFor(() => expect(panel).toHaveTextContent('remote model switching'));
    const model = within(panel).getByText('qwen3-32b').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    fireEvent.click(within(model).getByRole('button', { name: 'Switch' }));

    await waitFor(() => expect(model).toHaveTextContent('loading'));
  });
});
