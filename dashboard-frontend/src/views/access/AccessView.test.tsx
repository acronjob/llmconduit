import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { cleanup, fireEvent, screen, waitFor } from '@testing-library/react';
import { AccessView } from './AccessView';
import { renderWithQuery, resetWorld } from '../../components/testHarness';

beforeEach(() => resetWorld({ mock: true }));
afterEach(cleanup);

describe('AccessView', () => {
  it('renders all management surfaces and explicit deny/data-quality states', async () => {
    renderWithQuery(<AccessView />);
    await screen.findByTestId('access-view');

    expect(screen.getByText(/users and service accounts/i)).toBeInTheDocument();
    expect(screen.getByText(/^api keys$/i)).toBeInTheDocument();
    expect(screen.getByText(/groups and roles/i)).toBeInTheDocument();
    expect(screen.getByText(/policy editor/i)).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: /active sessions/i })).toBeInTheDocument();
    expect(screen.getByText(/audit log/i)).toBeInTheDocument();
    expect(screen.getAllByText('deny').length).toBeGreaterThan(0);
    expect(screen.getByText('unavailable')).toBeInTheDocument();
  });

  it('creates a key and reveals the raw secret only in a copy-once dialog', async () => {
    renderWithQuery(<AccessView />);
    await screen.findByTestId('access-view');

    fireEvent.change(screen.getByLabelText('Key name'), { target: { value: 'temporary key' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create key' }));

    const dialog = await screen.findByRole('dialog', { name: /copy api key/i });
    expect(dialog).toHaveTextContent(/will not be shown or returned/i);
    expect(screen.getByTestId('raw-api-key')).toHaveTextContent(/^llmc_/);
    fireEvent.click(screen.getByRole('button', { name: /i stored it/i }));
    await waitFor(() => expect(screen.queryByTestId('raw-api-key')).not.toBeInTheDocument());
    expect(screen.queryByText(/llmc_mock_.*copy_once/)).not.toBeInTheDocument();
  });
});
