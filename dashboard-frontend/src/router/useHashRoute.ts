/**
 * Minimal hash router. The views live at `#/flows`, `#/topology`, `#/sankey`, `#/theater` (D9),
 * and `#/overview` (gap 16 — the control-room overview that COMPOSES the other surfaces). No router
 * dependency — a `hashchange` listener bridged into React via useSyncExternalStore keeps it tear-free.
 */
import { useSyncExternalStore } from 'react';

export type RouteName = 'flows' | 'sessions' | 'topology' | 'sankey' | 'theater' | 'overview';

export const ROUTES: RouteName[] = ['flows', 'sessions', 'topology', 'sankey', 'theater', 'overview'];

const DEFAULT_ROUTE: RouteName = 'flows';

function parseHash(): RouteName {
  const raw = (typeof window !== 'undefined' ? window.location.hash : '').replace(/^#\/?/, '');
  const name = raw.split('/')[0] as RouteName;
  return ROUTES.includes(name) ? name : DEFAULT_ROUTE;
}

function subscribe(cb: () => void): () => void {
  window.addEventListener('hashchange', cb);
  return () => window.removeEventListener('hashchange', cb);
}

export function useHashRoute(): RouteName {
  return useSyncExternalStore(subscribe, parseHash, () => DEFAULT_ROUTE);
}

/** Imperatively navigate (used by the nav tabs). */
export function navigate(route: RouteName): void {
  window.location.hash = `#/${route}`;
}
