import type { CreateAuthPolicyRequest } from '../../api/types';

export function validatePolicy(policy: CreateAuthPolicyRequest): string[] {
  const errors: string[] = [];
  if (!policy.name) errors.push('Policy name is required.');
  if (policy.subjects.length === 0) errors.push('Select at least one real subject.');
  if (policy.endpoints.length === 0) errors.push('Enter at least one endpoint.');
  for (const [label, value] of [
    ['Max concurrent sessions', policy.max_concurrent_sessions],
    ['Daily session starts', policy.max_daily_session_starts],
  ] as const) {
    if (value !== null && (!Number.isInteger(value) || value < 1)) {
      errors.push(`${label} must be a positive integer.`);
    }
  }
  for (const window of policy.time_windows) {
    if (window.weekday_mask < 1 || window.weekday_mask > 0x7f
      || window.start_minute >= 24 * 60 || window.end_minute > 24 * 60
      || window.start_minute === window.end_minute) {
      errors.push('UTC window requires days and distinct HH:MM start/end times.');
    }
  }
  return errors;
}
