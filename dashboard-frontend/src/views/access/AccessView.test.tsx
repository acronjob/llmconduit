import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { cleanup, fireEvent, screen, waitFor } from '@testing-library/react';
import { AccessView } from './AccessView';
import { validatePolicy } from './policyEditorModel';
import { renderWithQuery, resetWorld } from '../../components/testHarness';

beforeEach(() => resetWorld({ mock: true }));
afterEach(cleanup);

describe('AccessView', () => {
  it('validates policy subjects, UTC windows, and positive session limits', () => {
    expect(validatePolicy({
      name: '', effect: 'allow', subjects: [], endpoints: [], requested_models: [],
      served_models: [], providers: [], routes: [],
      time_windows: [{ days: [], start_utc: '25:00', end_utc: '25:00' }],
      max_concurrent_sessions: 0, daily_session_starts: 1.5,
    })).toEqual([
      'Policy name is required.', 'Select at least one real subject.',
      'Enter at least one endpoint.', 'Max concurrent sessions must be a positive integer.',
      'Daily session starts must be a positive integer.',
      'UTC window requires days and distinct HH:MM start/end times.',
    ]);
  });

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

  it('creates groups and roles and previews the exact non-hardcoded policy payload', async () => {
    renderWithQuery(<AccessView />);
    await screen.findByTestId('access-view');

    fireEvent.change(screen.getByLabelText('Group name'), { target: { value: 'Canary operators' } });
    const members = screen.getByLabelText('Group members') as HTMLSelectElement;
    members.options[0]!.selected = true;
    fireEvent.change(members);
    fireEvent.click(screen.getByRole('button', { name: 'Create group' }));
    await screen.findByText('Canary operators');

    fireEvent.change(screen.getByLabelText('Role name'), { target: { value: 'Policy reader' } });
    const permissions = screen.getByLabelText('Role permissions') as HTMLSelectElement;
    Array.from(permissions.options).find((option) => option.value === 'auth.policies.read')!.selected = true;
    fireEvent.change(permissions);
    fireEvent.click(screen.getByRole('button', { name: 'Create role' }));
    await screen.findByText('Policy reader');

    const subjects = screen.getByLabelText('Policy subjects') as HTMLSelectElement;
    Array.from(subjects.options).find((option) => option.value === 'principal:usr_ops')!.selected = true;
    fireEvent.change(subjects);
    fireEvent.change(screen.getByLabelText('Policy endpoints'), { target: { value: 'responses,chat' } });
    fireEvent.change(screen.getByLabelText('Policy requested models'), { target: { value: 'alias-*' } });
    fireEvent.change(screen.getByLabelText('Policy served models'), { target: { value: 'gpt-4.1' } });
    fireEvent.change(screen.getByLabelText('Policy providers'), { target: { value: 'openai' } });
    fireEvent.change(screen.getByLabelText('Policy routes'), { target: { value: 'cloud-primary' } });

    const preview = screen.getByTestId('policy-payload-preview');
    expect(preview).toHaveTextContent('"subjects": [');
    expect(preview).toHaveTextContent('"principal:usr_ops"');
    expect(preview).toHaveTextContent('"requested_models": [');
    expect(preview).toHaveTextContent('"served_models": [');
    expect(preview).toHaveTextContent('"routes": [');
    expect(preview).not.toHaveTextContent('grp_prod');
    expect(screen.getByRole('button', { name: 'Save reviewed policy' })).toBeEnabled();
  });
});
