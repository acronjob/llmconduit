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
      name: '', effect: 'allow', subjects: [], endpoints: [], models: [], requested_models: [],
      served_models: [], providers: [], routes: [],
      time_windows: [{ weekday_mask: 0, start_minute: 1500, end_minute: 1500, absolute_start_ms: null, absolute_end_ms: null }],
      max_concurrent_sessions: 0, max_daily_session_starts: 1.5, management_permissions: [],
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

    expect(screen.getByText(/direct access/i)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /close create access/i }));
    fireEvent.click(screen.getByRole('tab', { name: /people & groups/i }));
    expect(screen.getByText(/users and service accounts/i)).toBeInTheDocument();
    expect(screen.getByText(/groups and roles/i)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('tab', { name: /^api keys$/i }));
    expect(screen.getByRole('heading', { name: /^api keys$/i })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('tab', { name: /policies/i }));
    expect(screen.getByText(/policy editor/i)).toBeInTheDocument();
    expect(screen.getAllByText('deny').length).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole('tab', { name: /sessions/i }));
    expect(screen.getByRole('heading', { name: /active sessions/i })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('tab', { name: /audit & cost/i }));
    expect(screen.getByText(/audit log/i)).toBeInTheDocument();
    expect(screen.getByText('unavailable')).toBeInTheDocument();
  });

  it('creates a key and reveals the raw secret only in a copy-once dialog', async () => {
    renderWithQuery(<AccessView />);
    await screen.findByTestId('access-view');

    fireEvent.click(screen.getByRole('button', { name: /close create access/i }));
    fireEvent.click(screen.getByRole('tab', { name: /^api keys$/i }));
    fireEvent.change(screen.getByLabelText('Key name'), { target: { value: 'temporary key' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create key' }));

    const dialog = await screen.findByRole('dialog', { name: /copy api key/i });
    expect(dialog).toHaveTextContent(/will not be shown or returned/i);
    expect(screen.getByTestId('raw-api-key')).toHaveTextContent(/^llmc_/);
    fireEvent.click(screen.getByRole('button', { name: /i stored it/i }));
    await waitFor(() => expect(screen.queryByTestId('raw-api-key')).not.toBeInTheDocument());
    expect(screen.queryByText(/llmc_mock_.*copy_once/)).not.toBeInTheDocument();
  });

  it('guides a new service identity through policy, limits, review, and copy-once key creation', async () => {
    renderWithQuery(<AccessView />);
    await screen.findByTestId('access-view');

    fireEvent.click(screen.getByRole('button', { name: 'Service' }));
    fireEvent.change(screen.getByLabelText('Wizard display name'), { target: { value: 'Nightly evaluator' } });
    fireEvent.change(screen.getByLabelText('Wizard key name'), { target: { value: 'nightly runner' } });
    fireEvent.click(screen.getByRole('button', { name: /continue/i }));
    expect(screen.getByLabelText('Wizard models')).toHaveValue('gpt-*');
    fireEvent.click(screen.getByRole('button', { name: /continue/i }));
    expect(screen.getByLabelText('Wizard max concurrent sessions')).toHaveValue(4);
    fireEvent.click(screen.getByRole('button', { name: /continue/i }));
    expect(screen.getByText(/nightly evaluator \(service_account\)/i)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /^create access$/i }));

    const dialog = await screen.findByRole('dialog', { name: /copy api key/i });
    expect(dialog).toHaveTextContent(/^.*llmc_/s);
  });

  it('creates groups and roles and previews the exact non-hardcoded policy payload', async () => {
    renderWithQuery(<AccessView />);
    await screen.findByTestId('access-view');

    fireEvent.click(screen.getByRole('button', { name: /close create access/i }));
    fireEvent.click(screen.getByRole('tab', { name: /people & groups/i }));
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

    fireEvent.click(screen.getByRole('tab', { name: /policies/i }));
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
