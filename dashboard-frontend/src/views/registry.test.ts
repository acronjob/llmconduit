import { describe, expect, it } from 'vitest';
import { AccessView } from './access/AccessView';
import { VIEW_BY_ROUTE } from './registry';

describe('view registry', () => {
  it('routes the access tab to the complete management console', () => {
    expect(VIEW_BY_ROUTE.access).toBe(AccessView);
  });
});
