import type { CreateAuthPolicyRequest } from '../../api/types';

export function validatePolicy(policy: CreateAuthPolicyRequest): string[] {
  const errors: string[] = [];
  if (!policy.name) errors.push('Policy name is required.');
  if (policy.subjects.length === 0) errors.push('Select at least one real subject.');
  if (policy.endpoints.length === 0) errors.push('Enter at least one endpoint.');
  for (const [label, value] of [
    ['Max concurrent sessions', policy.max_concurrent_sessions],
    ['Daily session starts', policy.daily_session_starts],
  ] as const) {
    if (value !== null && (!Number.isInteger(value) || value < 1)) {
      errors.push(`${label} must be a positive integer.`);
    }
  }
  for (const window of policy.time_windows) {
    if (!window.days.length || !/^([01]\d|2[0-3]):[0-5]\d$/.test(window.start_utc)
      || !/^([01]\d|2[0-3]):[0-5]\d$/.test(window.end_utc)
      || window.start_utc === window.end_utc) {
      errors.push('UTC window requires days and distinct HH:MM start/end times.');
    }
  }
  return errors;
}
