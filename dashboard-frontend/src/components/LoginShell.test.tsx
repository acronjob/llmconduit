import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent, waitFor, act, cleanup } from '@testing-library/react';
import { LoginShell } from './LoginShell';
import { App } from '../App';
import { QueryClientProvider } from '@tanstack/react-query';
import { DashboardClient } from '../api/client';
import { authStore } from '../store/authStore';
import { getConnection, resetConnection } from '../api/connection';

describe('LoginShell', () => {
  beforeEach(() => {
    authStore.getState().setAuthenticated(false);
    authStore.getState().setAuthMode('token');
  });

  it('renders the token-entry form when only a token gates the dashboard', () => {
    const client = new DashboardClient();
    render(<LoginShell client={client} />);
    expect(screen.getByLabelText('Dashboard token')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /sign in/i })).toBeInTheDocument();
  });

  it('POSTs /dashboard/login and flips auth on success', async () => {
    const login = vi.fn().mockResolvedValue({ user: null });
    const client = { login } as unknown as DashboardClient;
    render(<LoginShell client={client} />);

    fireEvent.change(screen.getByLabelText('Dashboard token'), { target: { value: 'secret' } });
    fireEvent.click(screen.getByRole('button', { name: /sign in/i }));

    await waitFor(() => expect(login).toHaveBeenCalledWith({ token: 'secret' }));
    await waitFor(() => expect(authStore.getState().authenticated).toBe(true));
  });

  it('uses delegated key login for an llmc_ management key', async () => {
    const login = vi.fn();
    const keyLogin = vi.fn().mockResolvedValue(undefined);
    const client = { login, keyLogin } as unknown as DashboardClient;
    render(<LoginShell client={client} />);

    fireEvent.change(screen.getByLabelText('Dashboard token'), { target: { value: 'llmc_delegate' } });
    fireEvent.click(screen.getByRole('button', { name: /sign in/i }));

    await waitFor(() => expect(keyLogin).toHaveBeenCalledWith('llmc_delegate'));
    expect(login).not.toHaveBeenCalled();
  });

  it('shows an error and stays unauthenticated when login rejects', async () => {
    const login = vi.fn().mockRejectedValue(new Error('bad'));
    const client = { login } as unknown as DashboardClient;
    render(<LoginShell client={client} />);

    fireEvent.change(screen.getByLabelText('Dashboard token'), { target: { value: 'wrong' } });
    fireEvent.click(screen.getByRole('button', { name: /sign in/i }));

    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent(/invalid/i));
    expect(authStore.getState().authenticated).toBe(false);
  });

  it('defaults to username/password once accounts exist, remembers the user, and can switch to a token', async () => {
    authStore.getState().setAuthMode('users');
    const login = vi.fn().mockResolvedValue({ user: { id: 'u1', username: 'koen', is_admin: true } });
    const client = { login } as unknown as DashboardClient;
    render(<LoginShell client={client} />);
    expect(screen.getByTestId('login-form').getAttribute('data-mode')).toBe('users');
    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'koen' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'hunter22' } });
    fireEvent.click(screen.getByRole('button', { name: /sign in/i }));
    await waitFor(() => expect(login).toHaveBeenCalledWith({ username: 'koen', password: 'hunter22' }));
    await waitFor(() => expect(authStore.getState().authenticated).toBe(true));
    expect(authStore.getState().user?.username).toBe('koen');

    cleanup();
    authStore.getState().setAuthenticated(false);
    render(<LoginShell client={client} />);
    fireEvent.click(screen.getByTestId('login-toggle'));
    expect(screen.getByTestId('login-form').getAttribute('data-mode')).toBe('token');
    expect(screen.getByLabelText('Dashboard token')).toBeInTheDocument();
  });
});

describe('App auth gate — unauthed load renders login; 401 bounces back', () => {
  beforeEach(() => {
    resetConnection();
    authStore.getState().setAuthenticated(false);
  });
  afterEach(() => {
    // Unmount + tear down the mock socket so its deferred timers don't leak across tests.
    cleanup();
    resetConnection();
  });

  it('renders the login shell on an unauthenticated load', () => {
    const { queryClient } = getConnection();
    render(
      <QueryClientProvider client={queryClient}>
        <App />
      </QueryClientProvider>,
    );
    // The login shell's token field proves we did NOT mount the dashboard.
    expect(screen.getByLabelText('Dashboard token')).toBeInTheDocument();
  });

  it('a 401 (bounceToLogin) drops back to the login shell after auth', async () => {
    const { queryClient } = getConnection();
    authStore.getState().setAuthenticated(true);
    const { rerender } = render(
      <QueryClientProvider client={queryClient}>
        <App />
      </QueryClientProvider>,
    );
    // Authed: nav is present, login field is not.
    await waitFor(() => expect(screen.queryByLabelText('Dashboard token')).not.toBeInTheDocument());
    // Let the mock socket's deferred snapshot/connection updates flush inside act().
    await act(async () => {
      await new Promise((r) => setTimeout(r, 0));
    });

    // Simulate a 401 from any fetch → the client's onUnauthorized calls bounceToLogin.
    act(() => authStore.getState().bounceToLogin());
    rerender(
      <QueryClientProvider client={queryClient}>
        <App />
      </QueryClientProvider>,
    );
    await waitFor(() => expect(screen.getByLabelText('Dashboard token')).toBeInTheDocument());
  });
});

describe('CSRF read from bootstrap/cookie is sent on kill', () => {
  beforeEach(() => resetConnection());

  it('connection seeds csrfToken from the (mock) bootstrap and the client sends it', async () => {
    const { client } = getConnection();
    // The mock bootstrap exposes the mock CSRF token; getConnection seeded the auth store.
    expect(authStore.getState().csrfToken).toBeTruthy();
    const res = await client.kill('api_001');
    expect(res.killed).toBe(true);
  });
});
