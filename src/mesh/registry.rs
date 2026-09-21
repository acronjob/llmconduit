#![allow(dead_code)]

use crate::mesh::capacity::{CapacityGate, CapacityPermit};
use crate::mesh::protocol::{
    Heartbeat, ModelAdvertisement, ResourceAdvertisement, WorkerAdvertisement,
};
use crate::upstream::ProviderInventoryEntry;
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
    availability: crate::config::AvailabilitySchedule,
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
        self.reserve_excluding_where(model, excluded, |_, _| true)
    }

    /// Reserve only after the caller's authorization predicate accepts the
    /// concrete worker/resource candidate. Filtering happens before capacity
    /// acquisition, so a denied candidate cannot consume a permit or perturb
    /// dispatch health/accounting.
    pub(crate) fn reserve_excluding_where<F>(
        &self,
        model: &str,
        excluded: &HashSet<(EndpointId, String)>,
        allows: F,
    ) -> Option<MeshReservation>
    where
        F: Fn(EndpointId, &str) -> bool,
    {
        let mut candidates = self.candidates(model);
        candidates.retain(|candidate| {
            !excluded.contains(&(candidate.endpoint_id, candidate.resource_id.clone()))
                && allows(candidate.endpoint_id, &candidate.resource_id)
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

    pub(crate) fn has_candidate_where<F>(&self, model: &str, allows: F) -> bool
    where
        F: Fn(EndpointId, &str) -> bool,
    {
        self.candidates(model)
            .into_iter()
            .any(|candidate| allows(candidate.endpoint_id, &candidate.resource_id))
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

    /// Snapshot every connected mesh resource for the administrative provider
    /// inventory. Stale sessions remain visible as unavailable so operators can
    /// distinguish a disconnected provider from one that was never configured.
    pub(crate) fn provider_inventory(&self) -> Vec<ProviderInventoryEntry> {
        let now = Instant::now();
        let sessions: Vec<_> = self
            .inner
            .lock()
            .expect("mesh registry lock poisoned")
            .workers
            .values()
            .cloned()
            .collect();
        let mut entries = Vec::new();
        for session in sessions {
            let stale = session.is_stale(now, self.heartbeat_timeout);
            let provider_id = format!("mesh:{}", session.endpoint_id);
            let resources = session
                .resources
                .lock()
                .expect("mesh worker resources lock poisoned");
            for resource in resources.values() {
                let capacity = resource.gate.snapshot();
                entries.push(ProviderInventoryEntry {
                    provider_id: provider_id.clone(),
                    provider_name: resource.resource_id.clone(),
                    resource_id: Some(resource.resource_id.clone()),
                    route: Some(resource.resource_id.clone()),
                    base_url: format!("mesh://{}/{}", session.endpoint_id, resource.resource_id),
                    models: resource
                        .models
                        .iter()
                        .map(|model| crate::upstream::UpstreamModelEntry {
                            id: model.id.clone(),
                            context_limit: model.context_limit,
                        })
                        .collect(),
                    availability: Some(resource.availability.clone()),
                    capacity_limit: Some(capacity.limit),
                    active_requests: Some(capacity.active),
                    accepting_requests: !stale
                        && resource.healthy
                        && resource.accepting_requests
                        && capacity.active < capacity.limit,
                    healthy: !stale && resource.healthy,
                });
            }
        }
        entries.sort_by(|left, right| {
            left.provider_id
                .cmp(&right.provider_id)
                .then_with(|| left.resource_id.cmp(&right.resource_id))
        });
        entries
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

    pub(crate) fn update_resource(&self, update: ResourceAdvertisement) -> bool {
        self.touch();
        let mut resources = self
            .resources
            .lock()
            .expect("mesh worker resources lock poisoned");
        match resources.get_mut(&update.resource_id) {
            Some(resource) if update.revision < resource.revision => true,
            Some(resource) => {
                resource.apply(update);
                true
            }
            None => false,
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
            availability: value.availability,
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
        self.availability = value.availability;
        self.healthy = value.healthy;
        self.accepting_requests = value.accepting_requests;
        self.revision = value.revision;
        self.gate.set_limit(value.effective_capacity);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AvailabilitySchedule;
    use crate::mesh::protocol::PROTOCOL_VERSION;
    use iroh::SecretKey;

    #[test]
    fn authorization_predicate_runs_before_mesh_capacity_reservation() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let endpoint = SecretKey::generate().public();
        registry.register_test(
            endpoint,
            WorkerAdvertisement {
                protocol_version: PROTOCOL_VERSION,
                node_name: None,
                agent_version: "test".into(),
                resources: vec![ResourceAdvertisement {
                    resource_id: "primary".into(),
                    models: vec![ModelAdvertisement {
                        id: "mesh-model".into(),
                        context_limit: None,
                    }],
                    availability: AvailabilitySchedule::default(),
                    effective_capacity: 1,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                }],
            },
        );

        let denied = registry.reserve_excluding_where(
            "mesh-model",
            &HashSet::new(),
            |candidate_endpoint, resource_id| {
                let candidate = registry
                    .candidates("mesh-model")
                    .into_iter()
                    .find(|candidate| {
                        candidate.endpoint_id == candidate_endpoint
                            && candidate.resource_id == resource_id
                    })
                    .expect("authorization sees a registered candidate");
                assert_eq!(candidate.active, 0, "capacity was reserved before authz");
                false
            },
        );
        assert!(denied.is_none());

        let reservation = registry
            .reserve("mesh-model")
            .expect("denial did not consume the only capacity permit");
        assert_eq!(reservation.resource.endpoint_id, endpoint);
    }

    #[test]
    fn provider_inventory_preserves_models_schedule_and_live_capacity() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let endpoint = SecretKey::generate().public();
        let availability = AvailabilitySchedule {
            timezone: "America/Chicago".into(),
            default_capacity: 2,
            weekly: Vec::new(),
            exceptions: Vec::new(),
        };
        registry.register_test(
            endpoint,
            WorkerAdvertisement {
                protocol_version: PROTOCOL_VERSION,
                node_name: Some("workstation".into()),
                agent_version: "test".into(),
                resources: vec![ResourceAdvertisement {
                    resource_id: "vllm".into(),
                    models: vec![ModelAdvertisement {
                        id: "local-model".into(),
                        context_limit: Some(32_768),
                    }],
                    availability: availability.clone(),
                    effective_capacity: 2,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                }],
            },
        );

        let reservation = registry.reserve("local-model").expect("resource available");
        let inventory = registry.provider_inventory();
        let provider = inventory.first().expect("provider inventory row");
        assert_eq!(provider.provider_id, format!("mesh:{endpoint}"));
        assert_eq!(provider.resource_id.as_deref(), Some("vllm"));
        assert_eq!(provider.models[0].id, "local-model");
        assert_eq!(provider.models[0].context_limit, Some(32_768));
        assert_eq!(provider.availability.as_ref(), Some(&availability));
        assert_eq!(provider.capacity_limit, Some(2));
        assert_eq!(provider.active_requests, Some(1));
        assert!(provider.accepting_requests);
        drop(reservation);
    }

    #[test]
    fn session_updates_cannot_add_unadvertised_resources() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let endpoint = SecretKey::generate().public();
        let session = registry.register_test(
            endpoint,
            WorkerAdvertisement {
                protocol_version: PROTOCOL_VERSION,
                node_name: None,
                agent_version: "test".into(),
                resources: vec![ResourceAdvertisement {
                    resource_id: "initial".into(),
                    models: vec![ModelAdvertisement {
                        id: "allowed-model".into(),
                        context_limit: None,
                    }],
                    availability: AvailabilitySchedule::default(),
                    effective_capacity: 1,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                }],
            },
        );

        assert!(!session.update_resource(ResourceAdvertisement {
            resource_id: "injected".into(),
            models: vec![ModelAdvertisement {
                id: "injected-model".into(),
                context_limit: None,
            }],
            availability: AvailabilitySchedule::default(),
            effective_capacity: 1,
            accepting_requests: true,
            healthy: true,
            revision: 2,
        }));
        assert!(registry.reserve("injected-model").is_none());
        assert!(registry.reserve("allowed-model").is_some());
        let catalog = registry.model_catalog();
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog[0].id, "allowed-model");
        assert_eq!(catalog[0].context_limit, None);
    }
}
