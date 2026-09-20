use super::{AuthError, AuthorizationScope};
use chrono::{DateTime, Datelike, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

#[derive(Debug, Clone, Default)]
pub struct SessionLimiter {
    state: Arc<Mutex<LimiterState>>,
}

#[derive(Debug, Default)]
struct LimiterState {
    by_key: HashMap<String, KeySessions>,
}

#[derive(Debug, Default)]
struct KeySessions {
    active: u32,
    day: i32,
    starts: u32,
}

impl SessionLimiter {
    pub fn acquire(
        &self,
        scope: &AuthorizationScope,
        now: DateTime<Utc>,
    ) -> Result<SessionLease, AuthError> {
        let key_id = scope.context().key_id.clone();
        let limits = scope.effective_limits();
        let day = now.date_naive().num_days_from_ce();
        let mut state = self.state.lock().map_err(|_| AuthError::PolicyUnavailable)?;
        let sessions = state.by_key.entry(key_id.clone()).or_default();
        if sessions.day != day {
            sessions.day = day;
            sessions.starts = 0;
        }
        if limits
            .max_concurrent_sessions
            .is_some_and(|maximum| sessions.active >= maximum)
            || limits
                .max_daily_session_starts
                .is_some_and(|maximum| sessions.starts >= maximum)
        {
            return Err(AuthError::QuotaExceeded);
        }
        sessions.active = sessions.active.saturating_add(1);
        sessions.starts = sessions.starts.saturating_add(1);
        drop(state);
        Ok(SessionLease {
            inner: Arc::new(LeaseInner {
                state: Arc::downgrade(&self.state),
                key_id,
            }),
        })
    }

    pub fn active_for_key(&self, key_id: &str) -> u32 {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.by_key.get(key_id).map(|sessions| sessions.active))
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub struct SessionLease {
    inner: Arc<LeaseInner>,
}

#[derive(Debug)]
struct LeaseInner {
    state: Weak<Mutex<LimiterState>>,
    key_id: String,
}

impl Drop for LeaseInner {
    fn drop(&mut self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let Ok(mut state) = state.lock() else {
            return;
        };
        if let Some(sessions) = state.by_key.get_mut(&self.key_id) {
            sessions.active = sessions.active.saturating_sub(1);
        }
    }
}

impl SessionLease {
    pub fn key_id(&self) -> &str {
        &self.inner.key_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authz::{
        AuthContext, AuthRequestId, Endpoint, LimitSet, PolicyBinding, PolicyEffect,
        PolicyMatcher, PolicyRule, PolicySnapshot, PolicySubject,
    };
    use std::collections::{HashMap, HashSet};

    fn scope(max_concurrent: u32, max_daily: u32) -> AuthorizationScope {
        let rule = PolicyRule {
            id: "pol_limit".into(),
            effect: PolicyEffect::Allow,
            binding: PolicyBinding { subject: PolicySubject::Key("key_a".into()) },
            matcher: PolicyMatcher::all(),
            windows: Vec::new(),
            limits: LimitSet {
                max_concurrent_sessions: Some(max_concurrent),
                max_daily_session_starts: Some(max_daily),
            },
            management_permissions: HashSet::new(),
        };
        let snapshot = Arc::new(PolicySnapshot::new(1, vec![rule], HashMap::new(), HashMap::new()));
        snapshot.authorize(
            &AuthContext {
                request_id: AuthRequestId::new(),
                key_id: "key_a".into(),
                key_prefix: "llmc_example".into(),
                principal_id: "usr_a".into(),
                policy_epoch: 1,
            },
            Endpoint::Responses,
            Some("model"),
            Utc::now(),
        ).unwrap()
    }

    #[test]
    fn cloned_lease_releases_once_after_last_owner() {
        let limiter = SessionLimiter::default();
        let scope = scope(1, 2);
        let now = Utc::now();
        let lease = limiter.acquire(&scope, now).unwrap();
        let clone = lease.clone();
        drop(lease);
        assert_eq!(limiter.active_for_key("key_a"), 1);
        assert_eq!(limiter.acquire(&scope, now), Err(AuthError::QuotaExceeded));
        drop(clone);
        assert_eq!(limiter.active_for_key("key_a"), 0);
        assert!(limiter.acquire(&scope, now).is_ok());
    }
}
