import { ROUTES, navigate, type RouteName } from '../router/useHashRoute';
import { Button } from './ui/Button';
import { cn } from '../lib/cn';

const LABELS: Record<RouteName, string> = {
  flows: 'Flows',
  sessions: 'Sessions',
  topology: 'Topology',
  sankey: 'Sankey',
  theater: 'Theater',
  overview: 'Overview',
};

/** The Argus eye — the hundred-eyed watchman's iris, the brand mark. Keeps a slow watch. */
function ArgusEye({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={className} aria-hidden="true">
      <path
        d="M1.6 12S5.2 5.6 12 5.6 22.4 12 22.4 12 18.8 18.4 12 18.4 1.6 12 1.6 12Z"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinejoin="round"
      />
      <circle cx="12" cy="12" r="3.4" stroke="currentColor" strokeWidth="1.5" />
      <circle cx="12" cy="12" r="1.25" fill="currentColor" />
    </svg>
  );
}

export function NavTabs({ active, onLogout }: { active: RouteName; onLogout: () => void }) {
  return (
    <nav className="flex items-center gap-6 border-b border-line bg-panel px-5 py-2.5">
      {/* Masthead: the Argus eye + tracked wordmark; llmconduit rides below as the eyebrow. */}
      <div className="flex items-center gap-2.5 pr-1">
        <ArgusEye className="argus-eye h-[18px] w-[18px] text-accent" />
        <div className="leading-none">
          <div className="font-ui text-sm font-bold tracking-[0.24em] text-text">ARGUS</div>
          <div className="mt-1 font-mono text-[9px] uppercase tracking-[0.22em] text-text-muted">
            llmconduit · watch
          </div>
        </div>
      </div>
      <div className="flex items-center gap-1">
        {ROUTES.map((r) => (
          <button
            key={r}
            onClick={() => navigate(r)}
            aria-current={r === active ? 'page' : undefined}
            className={cn(
              'rounded-md px-3 py-1.5 text-xs font-medium uppercase tracking-[0.14em] transition-colors',
              r === active
                ? 'bg-accent/12 text-accent'
                : 'text-text-muted hover:bg-line/40 hover:text-text',
            )}
          >
            {LABELS[r]}
          </button>
        ))}
      </div>
      <Button variant="ghost" className="ml-auto" onClick={onLogout}>
        Logout
      </Button>
    </nav>
  );
}
