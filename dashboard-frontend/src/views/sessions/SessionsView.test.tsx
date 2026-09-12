import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { cleanup, fireEvent, waitFor, within } from '@testing-library/react';
import { SessionsView } from './SessionsView';
import { renderWithQuery, resetWorld } from '../../components/testHarness';
import { flowFilterStore } from '../../store/flowFilterStore';

/** The Sessions view against the mock history API (seeded session tree). */
describe('SessionsView (mock history API)', () => {
  beforeEach(() => {
    resetWorld({ mock: true });
    window.location.hash = '#/sessions';
  });
  afterEach(cleanup);

  it('lists root sessions with harness + kind badges and request counts', async () => {
    const { getAllByTestId, getByTestId } = renderWithQuery(<SessionsView />);
    await waitFor(() => expect(getAllByTestId('session-row').length).toBe(2));
    const rows = getAllByTestId('session-row');
    // Roots only: the inferred sub-agent node is NOT listed at the top level.
    const harnesses = rows.map((r) => within(r).getByTestId('session-harness').textContent);
    expect(harnesses.sort()).toEqual(['claude-code', 'codex']);
    expect(rows.every((r) => within(r).getByTestId('session-kind').getAttribute('data-kind') === 'declared')).toBe(true);
    expect(getByTestId('sessions-count').textContent).toBe('2 roots');
    expect(getByTestId('session-detail-empty')).toBeTruthy();
  });

  it('selecting a session shows its sub-sessions and requests with lineage chips', async () => {
    const { getAllByTestId, getByTestId, findByTestId } = renderWithQuery(<SessionsView />);
    await waitFor(() => expect(getAllByTestId('session-row').length).toBe(2));
    const claude = getAllByTestId('session-row').find((r) => within(r).getByTestId('session-harness').textContent === 'claude-code')!;
    fireEvent.click(claude);
    await findByTestId('session-detail');
    // The inferred sub-agent appears under its parent.
    const children = within(getByTestId('session-children')).getAllByTestId('session-row');
    expect(children.length).toBe(1);
    expect(within(children[0]!).getByTestId('session-kind').getAttribute('data-kind')).toBe('inferred');
    // Requests: first turn = new chain, second = append; neither busts.
    const requests = within(getByTestId('session-requests')).getAllByTestId('session-request');
    expect(requests.length).toBe(2);
    const kinds = requests.map((r) => within(r).getByTestId('session-request-lineage').getAttribute('data-kind'));
    expect(kinds).toEqual(['new_chain', 'append']);
    expect(getByTestId('session-stat-busts').textContent).toContain('0');
    expect(getByTestId('session-stat-busts').getAttribute('data-quality')).toBe('derived');
    expect(getByTestId('session-stat-tokens').getAttribute('data-quality')).toBe('derived');

    // Descend into the sub-agent: breadcrumb shows the parent, requests belong to the child.
    fireEvent.click(children[0]!);
    await waitFor(() => expect(within(getByTestId('session-breadcrumb')).getAllByRole('button').length).toBe(1));
    await waitFor(() => expect(within(getByTestId('session-requests')).getAllByTestId('session-request').length).toBe(1));
  });

  it('flags cache busts on the codex session and cross-links into the flows table', async () => {
    const { getAllByTestId, getByTestId, findByTestId } = renderWithQuery(<SessionsView />);
    await waitFor(() => expect(getAllByTestId('session-row').length).toBe(2));
    const codex = getAllByTestId('session-row').find((r) => within(r).getByTestId('session-harness').textContent === 'codex')!;
    fireEvent.click(codex);
    await findByTestId('session-detail');
    const busted = within(getByTestId('session-requests')).getAllByTestId('session-request').filter((r) => r.getAttribute('data-cache-bust') === 'true');
    expect(busted.length).toBe(1);
    expect(within(busted[0]!).getByTestId('session-request-lineage').textContent).toBe('bust · tools');
    expect(getByTestId('session-stat-busts').textContent).toContain('1');

    fireEvent.click(getByTestId('session-show-in-flows'));
    expect(flowFilterStore.getState().filters.session).toBe('sess_codex');
    expect(window.location.hash).toBe('#/flows');
  });
});
