use std::sync::Arc;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct CapacityGate {
    inner: Arc<Mutex<CapacityGateState>>,
}

#[derive(Debug)]
struct CapacityGateState {
    limit: u32,
    active: u32,
}

#[derive(Debug)]
pub struct CapacityPermit {
    inner: Arc<Mutex<CapacityGateState>>,
}

impl CapacityGate {
    pub fn new(limit: u32) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CapacityGateState { limit, active: 0 })),
        }
    }

    pub fn set_limit(&self, limit: u32) {
        self.inner
            .lock()
            .expect("mesh capacity lock poisoned")
            .limit = limit;
    }

    pub fn set_limit_if_changed(&self, limit: u32) -> bool {
        let mut state = self.inner.lock().expect("mesh capacity lock poisoned");
        if state.limit == limit {
            return false;
        }
        state.limit = limit;
        true
    }

    pub fn snapshot(&self) -> CapacitySnapshot {
        let state = self.inner.lock().expect("mesh capacity lock poisoned");
        CapacitySnapshot {
            limit: state.limit,
            active: state.active,
            available: state.limit.saturating_sub(state.active),
        }
    }

    pub fn try_acquire(&self) -> Option<CapacityPermit> {
        let mut state = self.inner.lock().expect("mesh capacity lock poisoned");
        if state.active >= state.limit {
            return None;
        }
        state.active += 1;
        Some(CapacityPermit {
            inner: Arc::clone(&self.inner),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacitySnapshot {
    pub limit: u32,
    pub active: u32,
    pub available: u32,
}

impl Drop for CapacityPermit {
    fn drop(&mut self) {
        let mut state = self.inner.lock().expect("mesh capacity lock poisoned");
        state.active = state.active.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn capacity_shrinks_without_revoking_active_permits() {
        let gate = CapacityGate::new(3);
        let a = gate.try_acquire().expect("slot a");
        let b = gate.try_acquire().expect("slot b");
        gate.set_limit(1);

        assert!(gate.try_acquire().is_none());
        assert_eq!(
            gate.snapshot(),
            CapacitySnapshot {
                limit: 1,
                active: 2,
                available: 0,
            }
        );

        drop(a);
        assert!(gate.try_acquire().is_none());
        drop(b);
        let permit = gate.try_acquire();
        assert!(permit.is_some());
        drop(permit);
    }

    #[test]
    fn concurrent_acquire_never_exceeds_limit() {
        let gate = Arc::new(CapacityGate::new(8));
        let handles = (0..64)
            .map(|_| {
                let gate = Arc::clone(&gate);
                thread::spawn(move || gate.try_acquire())
            })
            .collect::<Vec<_>>();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().expect("join"))
            .collect::<Vec<_>>();
        let permits = results.into_iter().flatten().collect::<Vec<_>>();

        assert_eq!(permits.len(), 8);
        assert_eq!(
            gate.snapshot(),
            CapacitySnapshot {
                limit: 8,
                active: 8,
                available: 0,
            }
        );
        drop(permits);
        assert_eq!(
            gate.snapshot(),
            CapacitySnapshot {
                limit: 8,
                active: 0,
                available: 8,
            }
        );
    }
}
