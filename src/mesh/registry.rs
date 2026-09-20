#![allow(dead_code)]

use crate::mesh::capacity::{CapacityGate, CapacityPermit};
use crate::mesh::protocol::{
    Heartbeat, ModelAdvertisement, ResourceAdvertisement, WorkerAdvertisement,
};
use iroh::EndpointId;
use iroh::endpoint::Connection;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub(crate) struct MeshRegistry {
    inner: Arc<Mutex<RegistryState>>,
    heartbeat_timeout: Duration,
    next_generation: Arc<AtomicU64>,
}

#[derive(Debug, Default)]
struct RegistryState {
    workers: HashMap<EndpointId, Arc<WorkerSession>>,
    rr: u64,
}

#[derive(Debug)]
pub(crate) struct WorkerSession {
    pub(crate) endpoint_id: EndpointId,
    pub(crate) connection: Option<Connection>,
    pub(crate) generation: u64,
    connected_at: Instant,
    last_seen: Mutex<Instant>,
    resources: Mutex<HashMap<String, ResourceState>>,
}

#[derive(Debug, Clone)]
pub(crate) struct ResourceSnapshot {
    pub(crate) endpoint_id: EndpointId,
    pub(crate) generation: u64,
    pub(crate) resource_id: String,
    pub(crate) connection: Option<Connection>,
    pub(crate) effective_capacity: u32,
    pub(crate) active: u32,
}

#[derive(Debug)]
struct ResourceState {
    resource_id: String,
    models: Vec<ModelAdvertisement>,
    healthy: bool,
    accepting_requests: bool,
    revision: u64,
    gate: CapacityGate,
}

#[derive(Debug)]
pub(crate) struct MeshReservation {
    pub(crate) resource: ResourceSnapshot,
    _permit: CapacityPermit,
}

impl MeshRegistry {
    pub(crate) fn new(heartbeat_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryState::default())),
            heartbeat_timeout,
            next_generation: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(crate) fn register(
        &self,
        endpoint_id: EndpointId,
        connection: Connection,
        advertisement: WorkerAdvertisement,
    ) -> Arc<WorkerSession> {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let session = Arc::new(WorkerSession {
            endpoint_id,
            connection: Some(connection),
            generation,
            connected_at: Instant::now(),
            last_seen: Mutex::new(Instant::now()),
            resources: Mutex::new(resources_from_advertisement(advertisement)),
        });
        let old = {
            let mut state = self.inner.lock().expect("mesh registry lock poisoned");
            state.workers.insert(endpoint_id, Arc::clone(&session))
        };
        if let Some(old) = old
            && let Some(connection) = &old.connection
        {
            connection.close(0u32.into(), b"replaced by newer mesh session");
        }
        session
    }

    #[cfg(test)]
    pub(crate) fn register_test(
        &self,
        endpoint_id: EndpointId,
        advertisement: WorkerAdvertisement,
    ) -> Arc<WorkerSession> {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let session = Arc::new(WorkerSession {
            endpoint_id,
            connection: None,
            generation,
            connected_at: Instant::now(),
            last_seen: Mutex::new(Instant::now()),
            resources: Mutex::new(resources_from_advertisement(advertisement)),
        });
        let mut state = self.inner.lock().expect("mesh registry lock poisoned");
        state.workers.insert(endpoint_id, Arc::clone(&session));
        session
    }

    pub(crate) fn remove_generation(&self, endpoint_id: EndpointId, generation: u64) {
        let mut state = self.inner.lock().expect("mesh registry lock poisoned");
        let remove = state
            .workers
            .get(&endpoint_id)
            .is_some_and(|session| session.generation == generation);
        if remove {
            state.workers.remove(&endpoint_id);
        }
    }

    pub(crate) fn reserve(&self, model: &str) -> Option<MeshReservation> {
        self.reserve_excluding(model, &HashSet::new())
    }

    pub(crate) fn reserve_excluding(
        &self,
        model: &str,
        excluded: &HashSet<(EndpointId, String)>,
    ) -> Option<MeshReservation> {
        let mut candidates = self.candidates(model);
        candidates.retain(|candidate| {
            !excluded.contains(&(candidate.endpoint_id, candidate.resource_id.clone()))
        });
        candidates.sort_by(|a, b| {
            (a.active as u64 * b.effective_capacity as u64)
                .cmp(&(b.active as u64 * a.effective_capacity as u64))
                .then_with(|| a.endpoint_id.cmp(&b.endpoint_id))
                .then_with(|| a.resource_id.cmp(&b.resource_id))
        });
        let offset = {
            let mut state = self.inner.lock().expect("mesh registry lock poisoned");
            state.rr = state.rr.wrapping_add(1);
            state.rr as usize
        };
        let candidate_count = candidates.len();
        if candidate_count > 0 {
            candidates.rotate_left(offset % candidate_count);
        }
        for snapshot in candidates {
            let Some(session) = self.current_session(snapshot.endpoint_id, snapshot.generation)
            else {
                continue;
            };
            let resources = session
                .resources
                .lock()
                .expect("mesh worker resources lock poisoned");
            let Some(resource) = resources.get(&snapshot.resource_id) else {
                continue;
            };
            if let Some(permit) = resource.gate.try_acquire() {
                return Some(MeshReservation {
                    resource: snapshot,
                    _permit: permit,
                });
            }
        }
        None
    }

    pub(crate) fn model_catalog(&self) -> Vec<ModelAdvertisement> {
        let now = Instant::now();
        let sessions: Vec<_> = self
            .inner
            .lock()
            .expect("mesh registry lock poisoned")
            .workers
            .values()
            .cloned()
            .collect();
        let mut by_id: HashMap<String, Option<i64>> = HashMap::new();
        for session in sessions {
            if session.is_stale(now, self.heartbeat_timeout) {
                continue;
            }
            let resources = session
                .resources
                .lock()
                .expect("mesh worker resources lock poisoned");
            for resource in resources.values() {
                if !resource.healthy {
                    continue;
                }
                for model in &resource.models {
                    by_id.entry(model.id.clone()).or_insert(model.context_limit);
                }
            }
        }
        let mut models: Vec<_> = by_id
            .into_iter()
            .map(|(id, context_limit)| ModelAdvertisement { id, context_limit })
            .collect();
        models.sort_by(|a, b| a.id.cmp(&b.id));
        models
    }

    pub(crate) fn models_body(&self) -> Value {
        let data: Vec<Value> = self
            .model_catalog()
            .into_iter()
            .map(|model| {
                let mut object = serde_json::Map::new();
                object.insert("id".to_string(), Value::String(model.id));
                object.insert("object".to_string(), Value::String("model".to_string()));
                if let Some(limit) = model.context_limit {
                    object.insert("context_length".to_string(), Value::Number(limit.into()));
                }
                Value::Object(object)
            })
            .collect();
        serde_json::json!({ "object": "list", "data": data })
    }

    fn candidates(&self, model: &str) -> Vec<ResourceSnapshot> {
        let now = Instant::now();
        let sessions: Vec<_> = self
            .inner
            .lock()
            .expect("mesh registry lock poisoned")
            .workers
            .values()
            .cloned()
            .collect();
        let mut out = Vec::new();
        for session in sessions {
            if session.is_stale(now, self.heartbeat_timeout) {
                continue;
            }
            let resources = session
                .resources
                .lock()
                .expect("mesh worker resources lock poisoned");
            for resource in resources.values() {
                if !resource.healthy || !resource.accepting_requests {
                    continue;
                }
                if !resource
                    .models
                    .iter()
                    .any(|advertised| advertised.id.eq_ignore_ascii_case(model))
                {
                    continue;
                }
                let snapshot = resource.gate.snapshot();
                let effective_capacity = snapshot.limit;
                let active = snapshot.active;
                if effective_capacity == 0 || active >= effective_capacity {
                    continue;
                }
                out.push(ResourceSnapshot {
                    endpoint_id: session.endpoint_id,
                    generation: session.generation,
                    resource_id: resource.resource_id.clone(),
                    connection: session.connection.clone(),
                    effective_capacity,
                    active,
                });
            }
        }
        out
    }

    fn current_session(
        &self,
        endpoint_id: EndpointId,
        generation: u64,
    ) -> Option<Arc<WorkerSession>> {
        self.inner
            .lock()
            .expect("mesh registry lock poisoned")
            .workers
            .get(&endpoint_id)
            .filter(|session| session.generation == generation)
            .cloned()
    }
}

impl WorkerSession {
    pub(crate) fn touch(&self) {
        *self.last_seen.lock().expect("mesh last_seen lock poisoned") = Instant::now();
    }

    pub(crate) fn update_resource(&self, update: ResourceAdvertisement) {
        self.touch();
        let mut resources = self
            .resources
            .lock()
            .expect("mesh worker resources lock poisoned");
        match resources.get_mut(&update.resource_id) {
            Some(resource) if update.revision < resource.revision => {}
            Some(resource) => resource.apply(update),
            None => {
                resources.insert(update.resource_id.clone(), ResourceState::from(update));
            }
        }
    }

    pub(crate) fn apply_heartbeat(&self, heartbeat: Heartbeat) {
        self.touch();
        let mut resources = self
            .resources
            .lock()
            .expect("mesh worker resources lock poisoned");
        for runtime in heartbeat.resources {
            let Some(resource) = resources.get_mut(&runtime.resource_id) else {
                continue;
            };
            resource.healthy = runtime.healthy;
            resource.accepting_requests = runtime.accepting_requests;
            resource.gate.set_limit(runtime.effective_capacity);
        }
    }

    pub(crate) fn update_model_catalog(
        &self,
        resource_id: &str,
        models: Vec<ModelAdvertisement>,
        revision: u64,
    ) {
        self.touch();
        let mut resources = self
            .resources
            .lock()
            .expect("mesh worker resources lock poisoned");
        let Some(resource) = resources.get_mut(resource_id) else {
            return;
        };
        if revision < resource.revision {
            return;
        }
        resource.models = models;
        resource.revision = revision;
    }

    fn is_stale(&self, now: Instant, timeout: Duration) -> bool {
        let last_seen = *self.last_seen.lock().expect("mesh last_seen lock poisoned");
        now.duration_since(last_seen) > timeout
    }
}

fn resources_from_advertisement(
    advertisement: WorkerAdvertisement,
) -> HashMap<String, ResourceState> {
    advertisement
        .resources
        .into_iter()
        .map(|resource| (resource.resource_id.clone(), ResourceState::from(resource)))
        .collect()
}

impl From<ResourceAdvertisement> for ResourceState {
    fn from(value: ResourceAdvertisement) -> Self {
        Self {
            resource_id: value.resource_id,
            models: value.models,
            healthy: value.healthy,
            accepting_requests: value.accepting_requests,
            revision: value.revision,
            gate: CapacityGate::new(value.effective_capacity),
        }
    }
}

impl ResourceState {
    fn apply(&mut self, value: ResourceAdvertisement) {
        self.models = value.models;
        self.healthy = value.healthy;
        self.accepting_requests = value.accepting_requests;
        self.revision = value.revision;
        self.gate.set_limit(value.effective_capacity);
    }
}
