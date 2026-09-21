import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { cleanup, screen, waitFor } from '@testing-library/react';
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
});
