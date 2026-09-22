import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { cleanup, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { renderWithQuery, resetWorld } from '../../components/testHarness';
import { ProvidersView } from './ProvidersView';

beforeEach(() => resetWorld({ mock: true }));
afterEach(cleanup);

describe('ProvidersView', () => {
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
