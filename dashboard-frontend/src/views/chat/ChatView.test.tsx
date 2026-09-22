import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { cleanup, fireEvent, screen, waitFor } from '@testing-library/react';
import { renderWithQuery, resetWorld } from '../../components/testHarness';
import { authStore } from '../../store/authStore';
import { ChatView } from './ChatView';

beforeEach(() => resetWorld({ mock: true }));
afterEach(cleanup);

describe('ChatView', () => {
  it('selects a catalog model and renders the streamed response with terminal diagnostics', async () => {
    authStore.getState().setMutationsEnabled(false);
    renderWithQuery(<ChatView />);

    await waitFor(() => expect(screen.getByLabelText('Model')).toHaveValue('gpt-4o'));
    expect(screen.getByLabelText('Model').tagName).toBe('SELECT');
    expect(screen.getByLabelText('Thinking level')).toHaveValue('medium');
    expect(screen.getByLabelText('Temperature')).toHaveValue('1');
    expect(screen.getByLabelText('Top P')).toHaveValue('0.95');
    expect(screen.getByLabelText('Max context length')).toHaveValue('4096');
    expect(screen.getByRole('heading', { name: 'Chat' })).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('Message'), { target: { value: 'ping' } });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));

    await waitFor(() => expect(screen.getByTestId('chat-message-assistant')).toHaveTextContent('Mock response from gpt-4o: ping'));
    expect(screen.getByTestId('chat-thinking')).toHaveTextContent('Checking the request.');
    expect(screen.getByTestId('chat-thinking').querySelector('strong')).toHaveTextContent('Checking');
    expect(screen.getByTestId('chat-run-status')).toHaveTextContent('finish: stop');
    expect(screen.getByTestId('chat-run-status')).toHaveTextContent('20 tokens');
    expect(screen.getByTestId('chat-run-status')).toHaveTextContent('tg/s');
    expect(screen.getByTestId('chat-run-status')).toHaveTextContent('pp/s');
  });

  it('clears the local transcript without persisting it', async () => {
    renderWithQuery(<ChatView />);
    await waitFor(() => expect(screen.getByLabelText('Model')).toHaveValue('gpt-4o'));
    fireEvent.change(screen.getByLabelText('Message'), { target: { value: 'hello' } });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));
    await screen.findByTestId('chat-message-assistant');

    fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
    expect(screen.queryByTestId('chat-message-user')).toBeNull();
    expect(screen.getByTestId('chat-empty')).toBeInTheDocument();
  });
});
