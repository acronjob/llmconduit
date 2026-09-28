#![allow(dead_code)]

use crate::mesh::capacity::{CapacityGate, CapacityPermit};
use crate::mesh::protocol::{
    Heartbeat, ModelAdvertisement, ModelLifecycleAction, ModelSwitchingAdvertisement,
    ResourceAdvertisement, StreamOpen, SwitchModelRequest, SwitchModelResponse,
    WorkerAdvertisement,
};
use crate::upstream::{ProviderInventoryEntry, RequestAffinity, canonical_model_key};
use iroh::EndpointId;
use iroh::endpoint::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const AFFINITY_PIN_LIMIT: usize = 16 * 1024;
const AFFINITY_PIN_IDLE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone)]
pub(crate) struct MeshRegistry {
    inner: Arc<Mutex<RegistryState>>,
    heartbeat_timeout: Duration,
    next_generation: Arc<AtomicU64>,
}

#[derive(Debug, Default)]
struct RegistryState {
    workers: HashMap<EndpointId, Arc<WorkerSession>>,
    disabled_models: HashSet<(EndpointId, String, String)>,
    affinity_pins: HashMap<AffinityPinKey, AffinityPin>,
    rr: u64,
}

#[derive(Debug)]
pub(crate) struct WorkerSession {
    pub(crate) endpoint_id: EndpointId,
    pub(crate) connection: Option<Connection>,
    pub(crate) generation: u64,
    worker_name: Option<String>,
    connected_at: Instant,
    last_seen: Mutex<Instant>,
    resources: Mutex<HashMap<String, ResourceState>>,
    model_switching: Mutex<Option<ModelSwitchingAdvertisement>>,
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
    affinity: Option<AffinityReservation>,
    _permit: CapacityPermit,
}

#[derive(Debug, Clone)]
pub(crate) struct MeshAffinityCommit {
    key: AffinityPinKey,
    observed: Option<AffinityPinTarget>,
    endpoint_id: EndpointId,
    resource_id: String,
}

#[derive(Debug)]
pub(crate) enum MeshReservationError {
    PinnedCapacityExhausted,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct AffinityPinKey {
    model: String,
    affinity_hash: [u8; 32],
}

#[derive(Debug, Clone)]
struct AffinityPin {
    endpoint_id: EndpointId,
    resource_id: String,
    last_seen: Instant,
}

#[derive(Debug, Clone)]
struct AffinityReservation {
    key: AffinityPinKey,
    observed: Option<AffinityPinTarget>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct AffinityPinTarget {
    endpoint_id: EndpointId,
    resource_id: String,
}

impl AffinityPin {
    fn target(&self) -> AffinityPinTarget {
        AffinityPinTarget {
            endpoint_id: self.endpoint_id,
            resource_id: self.resource_id.clone(),
        }
    }
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
        let worker_name = advertised_worker_name(advertisement.node_name.as_deref());
        let session = Arc::new(WorkerSession {
            endpoint_id,
            connection: Some(connection),
            generation,
            worker_name,
            connected_at: Instant::now(),
            last_seen: Mutex::new(Instant::now()),
            model_switching: Mutex::new(advertisement.model_switching.clone()),
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
        let worker_name = advertised_worker_name(advertisement.node_name.as_deref());
        let session = Arc::new(WorkerSession {
            endpoint_id,
            connection: None,
            generation,
            worker_name,
            connected_at: Instant::now(),
            last_seen: Mutex::new(Instant::now()),
            model_switching: Mutex::new(advertisement.model_switching.clone()),
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

    pub(crate) fn remove_endpoint(&self, endpoint_id: EndpointId, reason: &'static [u8]) -> bool {
        let removed = self
            .inner
            .lock()
            .expect("mesh registry lock poisoned")
            .workers
            .remove(&endpoint_id);
        if let Some(session) = removed {
            if let Some(connection) = &session.connection {
                connection.close(403u32.into(), reason);
            }
            true
        } else {
            false
        }
    }

    pub(crate) fn replace_disabled_models(
        &self,
        disabled: impl IntoIterator<Item = (EndpointId, String, String)>,
    ) {
        let mut state = self.inner.lock().expect("mesh registry lock poisoned");
        state.disabled_models = disabled
            .into_iter()
            .map(|(endpoint_id, resource_id, model)| {
                (endpoint_id, resource_id, model.to_ascii_lowercase())
            })
            .collect();
    }

    pub(crate) fn set_model_disabled(
        &self,
        endpoint_id: EndpointId,
        resource_id: &str,
        model: &str,
        disabled: bool,
    ) {
        let mut state = self.inner.lock().expect("mesh registry lock poisoned");
        let key = (
            endpoint_id,
            resource_id.to_string(),
            model.to_ascii_lowercase(),
        );
        if disabled {
            state.disabled_models.insert(key);
        } else {
            state.disabled_models.remove(&key);
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
        self.reserve_excluding_where_with_affinity(model, excluded, None, allows)
            .unwrap_or(None)
    }

    pub(crate) fn reserve_excluding_where_with_affinity<F>(
        &self,
        model: &str,
        excluded: &HashSet<(EndpointId, String)>,
        affinity: Option<&RequestAffinity>,
        allows: F,
    ) -> Result<Option<MeshReservation>, MeshReservationError>
    where
        F: Fn(EndpointId, &str) -> bool,
    {
        self.reserve_excluding_where_with_affinity_at(
            model,
            excluded,
            affinity,
            allows,
            Instant::now(),
        )
    }

    fn reserve_excluding_where_with_affinity_at<F>(
        &self,
        model: &str,
        excluded: &HashSet<(EndpointId, String)>,
        affinity: Option<&RequestAffinity>,
        allows: F,
        now: Instant,
    ) -> Result<Option<MeshReservation>, MeshReservationError>
    where
        F: Fn(EndpointId, &str) -> bool,
    {
        let affinity_context =
            affinity.map(|affinity| self.affinity_reservation_context(model, affinity, now));
        if let Some(affinity) = affinity
            && let Some(result) = self.reserve_pinned(model, excluded, affinity, &allows, now)
        {
            return result.map(Some);
        }

        let mut candidates = self.candidates_at(model, now);
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
                return Ok(Some(MeshReservation {
                    resource: snapshot,
                    affinity: affinity_context,
                    _permit: permit,
                }));
            }
        }
        Ok(None)
    }

    fn affinity_reservation_context(
        &self,
        model: &str,
        affinity: &RequestAffinity,
        now: Instant,
    ) -> AffinityReservation {
        let key = affinity_pin_key(model, affinity);
        let observed = {
            let mut state = self.inner.lock().expect("mesh registry lock poisoned");
            prune_affinity_pins(&mut state, now);
            state.affinity_pins.get_mut(&key).map(|pin| {
                pin.last_seen = now;
                pin.target()
            })
        };
        AffinityReservation { key, observed }
    }

    fn reserve_pinned<F>(
        &self,
        model: &str,
        excluded: &HashSet<(EndpointId, String)>,
        affinity: &RequestAffinity,
        allows: &F,
        now: Instant,
    ) -> Option<Result<MeshReservation, MeshReservationError>>
    where
        F: Fn(EndpointId, &str) -> bool,
    {
        let key = affinity_pin_key(model, affinity);
        let pin = {
            let mut state = self.inner.lock().expect("mesh registry lock poisoned");
            prune_affinity_pins(&mut state, now);
            state.affinity_pins.get_mut(&key).map(|pin| {
                pin.last_seen = now;
                pin.clone()
            })
        }?;
        let candidate_id = (pin.endpoint_id, pin.resource_id.clone());
        if excluded.contains(&candidate_id) {
            return None;
        }
        let snapshot = self.pinned_candidate(model, &pin, now)?;
        if !allows(snapshot.endpoint_id, &snapshot.resource_id) {
            return None;
        }
        let session = self.current_session(snapshot.endpoint_id, snapshot.generation)?;
        let resources = session
            .resources
            .lock()
            .expect("mesh worker resources lock poisoned");
        let resource = resources.get(&snapshot.resource_id)?;
        Some(match resource.gate.try_acquire() {
            Some(permit) => Ok(MeshReservation {
                resource: snapshot,
                affinity: Some(AffinityReservation {
                    key,
                    observed: Some(pin.target()),
                }),
                _permit: permit,
            }),
            None => Err(MeshReservationError::PinnedCapacityExhausted),
        })
    }

    pub(crate) fn commit_affinity(&self, commit: MeshAffinityCommit) {
        self.commit_affinity_at(commit, Instant::now());
    }

    fn commit_affinity_at(&self, commit: MeshAffinityCommit, now: Instant) {
        let endpoint_id = commit.endpoint_id;
        let resource_id = &commit.resource_id;
        let mut state = self.inner.lock().expect("mesh registry lock poisoned");
        prune_affinity_pins(&mut state, now);
        if let Some(existing) = state.affinity_pins.get(&commit.key) {
            let existing_target = existing.target();
            if existing_target.endpoint_id == endpoint_id
                && existing_target.resource_id == *resource_id
            {
                if let Some(existing) = state.affinity_pins.get_mut(&commit.key) {
                    existing.last_seen = now;
                }
                return;
            }
            if commit.observed.as_ref() != Some(&existing_target) {
                return;
            }
        }
        state.affinity_pins.insert(
            commit.key,
            AffinityPin {
                endpoint_id,
                resource_id: resource_id.clone(),
                last_seen: now,
            },
        );
        enforce_affinity_pin_limit(&mut state);
    }

    pub(crate) fn has_candidate_where<F>(&self, model: &str, allows: F) -> bool
    where
        F: Fn(EndpointId, &str) -> bool,
    {
        self.candidates(model)
            .into_iter()
            .any(|candidate| allows(candidate.endpoint_id, &candidate.resource_id))
    }

    pub(crate) fn has_resource_model(
        &self,
        endpoint_id: EndpointId,
        resource_id: &str,
        model: &str,
    ) -> bool {
        let Some(session) = self.current_session_any_generation(endpoint_id) else {
            return false;
        };
        let resources = session
            .resources
            .lock()
            .expect("mesh worker resources lock poisoned");
        resources.get(resource_id).is_some_and(|resource| {
            resource
                .models
                .iter()
                .any(|advertised| advertised.id.eq_ignore_ascii_case(model))
        })
    }

    pub(crate) fn model_switching(
        &self,
        endpoint_id: EndpointId,
    ) -> Option<ModelSwitchingAdvertisement> {
        let session = self.current_session_any_generation(endpoint_id)?;
        if session.is_stale(Instant::now(), self.heartbeat_timeout) {
            return None;
        }
        session
            .model_switching
            .lock()
            .expect("mesh model-switching lock poisoned")
            .clone()
    }

    pub(crate) async fn switch_model(
        &self,
        endpoint_id: EndpointId,
        model_id: String,
    ) -> crate::error::AppResult<SwitchModelResponse> {
        self.switch_model_instances(endpoint_id, model_id, None)
            .await
    }

    pub(crate) async fn switch_model_instances(
        &self,
        endpoint_id: EndpointId,
        model_id: String,
        instances: Option<u32>,
    ) -> crate::error::AppResult<SwitchModelResponse> {
        if let Some(instances) = instances {
            crate::mesh::protocol::validate_switch_model_request(&SwitchModelRequest {
                protocol_version: crate::mesh::protocol::REQUEST_PROTOCOL_VERSION,
                request_id: uuid::Uuid::nil(),
                model_id: model_id.clone(),
                instances: Some(instances),
            })
            .map_err(|err| crate::error::AppError::bad_request(err.to_string()))?;
        }
        self.set_model_state(endpoint_id, model_id, ModelLifecycleAction::Load, instances)
            .await
    }

    pub(crate) async fn unload_model(
        &self,
        endpoint_id: EndpointId,
        model_id: String,
    ) -> crate::error::AppResult<SwitchModelResponse> {
        self.set_model_state(endpoint_id, model_id, ModelLifecycleAction::Unload, None)
            .await
    }

    async fn set_model_state(
        &self,
        endpoint_id: EndpointId,
        model_id: String,
        action: ModelLifecycleAction,
        instances: Option<u32>,
    ) -> crate::error::AppResult<SwitchModelResponse> {
        let session = self
            .current_session_any_generation(endpoint_id)
            .ok_or_else(|| crate::error::AppError::upstream("mesh worker is not connected"))?;
        if session.is_stale(Instant::now(), self.heartbeat_timeout) {
            return Err(crate::error::AppError::upstream("mesh worker is stale"));
        }
        let advertised = session
            .model_switching
            .lock()
            .expect("mesh model-switching lock poisoned")
            .clone()
            .ok_or_else(|| {
                crate::error::AppError::bad_request(
                    "mesh worker does not advertise model switching",
                )
            })?;
        if !advertised.models.iter().any(|model| model.id == model_id) {
            return Err(crate::error::AppError::bad_request(
                "model is not advertised as switchable",
            ));
        }
        let request = SwitchModelRequest {
            protocol_version: crate::mesh::protocol::REQUEST_PROTOCOL_VERSION,
            request_id: uuid::Uuid::new_v4(),
            model_id,
            instances,
        };
        let connection = session.connection.as_ref().ok_or_else(|| {
            crate::error::AppError::upstream("mesh worker connection is unavailable")
        })?;
        let (mut send, mut recv) =
            tokio::time::timeout(Duration::from_secs(10), connection.open_bi())
                .await
                .map_err(|_| crate::error::AppError::upstream("mesh switch stream timed out"))?
                .map_err(|err| {
                    crate::error::AppError::upstream(format!(
                        "failed to open mesh switch stream: {err}"
                    ))
                })?;
        let stream_open = match action {
            ModelLifecycleAction::Load => StreamOpen::SwitchModel(request.clone()),
            ModelLifecycleAction::Unload => StreamOpen::UnloadModel(request.clone()),
        };
        crate::mesh::io::write_stream_open(&mut send, &stream_open).await?;
        let mut response = tokio::time::timeout(
            Duration::from_secs(30),
            crate::mesh::io::read_switch_response(&mut recv),
        )
        .await
        .map_err(|_| crate::error::AppError::upstream("mesh switch request timed out"))??;
        if response.request_id != request.request_id || response.model_id != request.model_id {
            return Err(crate::error::AppError::upstream(
                "mesh switch response did not match request",
            ));
        }
        if let Some(mut update) = response.model_switching.clone() {
            crate::mesh::protocol::validate_model_switching(&update).map_err(|err| {
                crate::error::AppError::upstream(format!(
                    "mesh worker returned invalid switching inventory: {err}"
                ))
            })?;
            // A switch response is worker-controlled and must not expand the
            // controller-filtered inventory that authorized this request.
            update.models.retain(|model| {
                advertised
                    .models
                    .iter()
                    .any(|allowed| allowed.id.eq_ignore_ascii_case(&model.id))
            });
            session.update_model_switching(update.clone());
            response.model_switching = Some(update);
        }
        Ok(response)
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
        let disabled = self.disabled_models_snapshot();
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
                    if disabled.contains(&(
                        session.endpoint_id,
                        resource.resource_id.clone(),
                        model.id.to_ascii_lowercase(),
                    )) {
                        continue;
                    }
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
        let disabled = self.disabled_models_snapshot();
        let mut entries = Vec::new();
        for session in sessions {
            let stale = session.is_stale(now, self.heartbeat_timeout);
            let provider_id = format!("mesh:{}", session.endpoint_id);
            let provider_name = session.provider_name(&provider_id);
            let resources = session
                .resources
                .lock()
                .expect("mesh worker resources lock poisoned");
            for resource in resources.values() {
                let capacity = resource.gate.snapshot();
                entries.push(ProviderInventoryEntry {
                    provider_id: provider_id.clone(),
                    provider_name: provider_name.clone(),
                    resource_id: Some(resource.resource_id.clone()),
                    route: Some(resource.resource_id.clone()),
                    base_url: format!("mesh://{}/{}", session.endpoint_id, resource.resource_id),
                    models: resource
                        .models
                        .iter()
                        .filter(|model| {
                            !disabled.contains(&(
                                session.endpoint_id,
                                resource.resource_id.clone(),
                                model.id.to_ascii_lowercase(),
                            ))
                        })
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
                        && resource.models.iter().any(|model| {
                            !disabled.contains(&(
                                session.endpoint_id,
                                resource.resource_id.clone(),
                                model.id.to_ascii_lowercase(),
                            ))
                        })
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
        self.candidates_at(model, Instant::now())
    }

    fn candidates_at(&self, model: &str, now: Instant) -> Vec<ResourceSnapshot> {
        let sessions: Vec<_> = self
            .inner
            .lock()
            .expect("mesh registry lock poisoned")
            .workers
            .values()
            .cloned()
            .collect();
        let disabled = self.disabled_models_snapshot();
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
                if disabled.contains(&(
                    session.endpoint_id,
                    resource.resource_id.clone(),
                    model.to_ascii_lowercase(),
                )) {
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

    fn pinned_candidate(
        &self,
        model: &str,
        pin: &AffinityPin,
        now: Instant,
    ) -> Option<ResourceSnapshot> {
        let state = self.inner.lock().expect("mesh registry lock poisoned");
        self.pinned_candidate_with_state(&state, model, pin, now)
    }

    fn pinned_candidate_with_state(
        &self,
        state: &RegistryState,
        model: &str,
        pin: &AffinityPin,
        now: Instant,
    ) -> Option<ResourceSnapshot> {
        let session = state.workers.get(&pin.endpoint_id)?;
        if session.is_stale(now, self.heartbeat_timeout) {
            return None;
        }
        let resources = session
            .resources
            .lock()
            .expect("mesh worker resources lock poisoned");
        let resource = resources.get(&pin.resource_id)?;
        if !resource.healthy || !resource.accepting_requests {
            return None;
        }
        if state.disabled_models.contains(&(
            session.endpoint_id,
            resource.resource_id.clone(),
            model.to_ascii_lowercase(),
        )) {
            return None;
        }
        if !resource
            .models
            .iter()
            .any(|advertised| advertised.id.eq_ignore_ascii_case(model))
        {
            return None;
        }
        let snapshot = resource.gate.snapshot();
        if snapshot.limit == 0 {
            return None;
        }
        Some(ResourceSnapshot {
            endpoint_id: session.endpoint_id,
            generation: session.generation,
            resource_id: resource.resource_id.clone(),
            connection: session.connection.clone(),
            effective_capacity: snapshot.limit,
            active: snapshot.active,
        })
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

    fn current_session_any_generation(
        &self,
        endpoint_id: EndpointId,
    ) -> Option<Arc<WorkerSession>> {
        self.inner
            .lock()
            .expect("mesh registry lock poisoned")
            .workers
            .get(&endpoint_id)
            .cloned()
    }

    fn disabled_models_snapshot(&self) -> HashSet<(EndpointId, String, String)> {
        self.inner
            .lock()
            .expect("mesh registry lock poisoned")
            .disabled_models
            .clone()
    }
}

impl MeshReservation {
    pub(crate) fn affinity_commit(&self) -> Option<MeshAffinityCommit> {
        self.affinity.as_ref().map(|affinity| MeshAffinityCommit {
            key: affinity.key.clone(),
            observed: affinity.observed.clone(),
            endpoint_id: self.resource.endpoint_id,
            resource_id: self.resource.resource_id.clone(),
        })
    }
}

fn affinity_pin_key(model: &str, affinity: &RequestAffinity) -> AffinityPinKey {
    AffinityPinKey {
        model: canonical_model_key(model),
        affinity_hash: Sha256::digest(affinity.0.as_bytes()).into(),
    }
}

fn prune_affinity_pins(state: &mut RegistryState, now: Instant) {
    state
        .affinity_pins
        .retain(|_, pin| now.duration_since(pin.last_seen) <= AFFINITY_PIN_IDLE_TTL);
    enforce_affinity_pin_limit(state);
}

fn enforce_affinity_pin_limit(state: &mut RegistryState) {
    if state.affinity_pins.len() <= AFFINITY_PIN_LIMIT {
        return;
    }
    let excess = state.affinity_pins.len() - AFFINITY_PIN_LIMIT;
    let mut keys_by_age: Vec<_> = state
        .affinity_pins
        .iter()
        .map(|(key, pin)| (key.clone(), pin.last_seen))
        .collect();
    keys_by_age.sort_by_key(|(_, last_seen)| *last_seen);
    for (key, _) in keys_by_age.into_iter().take(excess) {
        state.affinity_pins.remove(&key);
    }
}

impl WorkerSession {
    fn provider_name(&self, fallback_provider_id: &str) -> String {
        self.worker_name
            .clone()
            .unwrap_or_else(|| fallback_provider_id.to_string())
    }

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

    pub(crate) fn update_model_switching(&self, update: ModelSwitchingAdvertisement) {
        self.touch();
        let mut current = self
            .model_switching
            .lock()
            .expect("mesh model-switching lock poisoned");
        if current
            .as_ref()
            .is_some_and(|value| update.revision < value.revision)
        {
            return;
        }
        *current = Some(update);
    }

    fn is_stale(&self, now: Instant, timeout: Duration) -> bool {
        let last_seen = *self.last_seen.lock().expect("mesh last_seen lock poisoned");
        now.duration_since(last_seen) > timeout
    }
}

fn advertised_worker_name(node_name: Option<&str>) -> Option<String> {
    node_name.and_then(|name| {
        let trimmed = name.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
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
    use crate::mesh::protocol::{PROTOCOL_VERSION, SwitchableModelAdvertisement};
    use iroh::SecretKey;

    fn test_advertisement(resources: Vec<(&str, u32)>) -> WorkerAdvertisement {
        WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: None,
            agent_version: "test".into(),
            resources: resources
                .into_iter()
                .map(|(resource_id, capacity)| ResourceAdvertisement {
                    resource_id: resource_id.into(),
                    models: vec![ModelAdvertisement {
                        id: "mesh-model".into(),
                        context_limit: None,
                    }],
                    availability: AvailabilitySchedule::default(),
                    effective_capacity: capacity,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                })
                .collect(),
            model_switching: None,
        }
    }

    fn commit_new_affinity(
        registry: &MeshRegistry,
        model: &str,
        affinity: &RequestAffinity,
        resource_id: &str,
    ) {
        let reservation = registry
            .reserve_excluding_where_with_affinity(
                model,
                &HashSet::new(),
                Some(affinity),
                |_, id| id == resource_id,
            )
            .expect("reservation check")
            .expect("resource available");
        registry.commit_affinity(reservation.affinity_commit().expect("affinity commit"));
        drop(reservation);
    }

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
                model_switching: None,
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
    fn affinity_commit_pins_same_resource_and_refuses_capacity_migration() {
        let registry = MeshRegistry::new(
            AFFINITY_PIN_IDLE_TTL + Duration::from_secs(AFFINITY_PIN_LIMIT as u64 + 60),
        );
        let endpoint = SecretKey::generate().public();
        registry.register_test(
            endpoint,
            test_advertisement(vec![("primary", 1), ("spare", 1)]),
        );
        let affinity = RequestAffinity("session-a".to_string());

        let first = registry
            .reserve_excluding_where_with_affinity(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |_, _| true,
            )
            .expect("reservation check")
            .expect("first reservation");
        let pinned_resource = first.resource.resource_id.clone();
        registry.commit_affinity(first.affinity_commit().expect("affinity commit"));
        drop(first);

        let pinned = registry
            .reserve_excluding_where_with_affinity(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |_, _| true,
            )
            .expect("sticky reservation")
            .expect("pinned resource still available");
        assert_eq!(pinned.resource.resource_id, pinned_resource);

        let error = registry
            .reserve_excluding_where_with_affinity(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |_, _| true,
            )
            .expect_err("a full pinned resource must not migrate to spare capacity");
        assert!(matches!(
            error,
            MeshReservationError::PinnedCapacityExhausted
        ));

        let unpinned = registry
            .reserve_excluding_where_with_affinity("mesh-model", &HashSet::new(), None, |_, _| true)
            .expect("unpinned reservation")
            .expect("spare capacity is still available to unpinned sessions");
        assert_ne!(unpinned.resource.resource_id, pinned_resource);
    }

    #[test]
    fn affinity_authorization_denial_does_not_consume_capacity() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let endpoint = SecretKey::generate().public();
        registry.register_test(endpoint, test_advertisement(vec![("primary", 1)]));
        let affinity = RequestAffinity("session-a".to_string());
        commit_new_affinity(&registry, "mesh-model", &affinity, "primary");

        let denied = registry
            .reserve_excluding_where_with_affinity(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |candidate_endpoint, resource_id| {
                    let candidate = registry
                        .candidates("mesh-model")
                        .into_iter()
                        .find(|candidate| {
                            candidate.endpoint_id == candidate_endpoint
                                && candidate.resource_id == resource_id
                        })
                        .expect("authorization sees the pinned candidate");
                    assert_eq!(candidate.active, 0, "capacity was reserved before authz");
                    false
                },
            )
            .expect("denied sticky resource is skipped");
        assert!(denied.is_none());

        assert!(
            registry
                .reserve("mesh-model")
                .is_some_and(|reservation| reservation.resource.resource_id == "primary")
        );
    }

    #[test]
    fn affinity_rebinds_after_pinned_resource_is_disabled() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let endpoint = SecretKey::generate().public();
        registry.register_test(
            endpoint,
            test_advertisement(vec![("primary", 1), ("spare", 1)]),
        );
        let affinity = RequestAffinity("session-a".to_string());
        commit_new_affinity(&registry, "mesh-model", &affinity, "primary");
        registry.set_model_disabled(endpoint, "primary", "mesh-model", true);

        let rebound = registry
            .reserve_excluding_where_with_affinity(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |_, _| true,
            )
            .expect("sticky reservation can rebind")
            .expect("spare resource available");
        assert_eq!(rebound.resource.resource_id, "spare");
        registry.commit_affinity(rebound.affinity_commit().expect("affinity commit"));
        drop(rebound);
        registry.set_model_disabled(endpoint, "primary", "mesh-model", false);

        let pinned = registry
            .reserve_excluding_where_with_affinity(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |_, _| true,
            )
            .expect("sticky reservation")
            .expect("rebound resource remains pinned");
        assert_eq!(pinned.resource.resource_id, "spare");
    }

    #[test]
    fn affinity_rebinds_after_excluded_or_unauthorized_pinned_resource_succeeds_elsewhere() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let endpoint = SecretKey::generate().public();
        registry.register_test(
            endpoint,
            test_advertisement(vec![("primary", 1), ("spare", 1)]),
        );
        let affinity = RequestAffinity("session-a".to_string());
        commit_new_affinity(&registry, "mesh-model", &affinity, "primary");

        let excluded = HashSet::from([(endpoint, "primary".to_string())]);
        let rebound = registry
            .reserve_excluding_where_with_affinity(
                "mesh-model",
                &excluded,
                Some(&affinity),
                |_, _| true,
            )
            .expect("excluded pin falls back")
            .expect("spare is available");
        assert_eq!(rebound.resource.resource_id, "spare");
        registry.commit_affinity(rebound.affinity_commit().expect("affinity commit"));
        drop(rebound);

        let pinned = registry
            .reserve_excluding_where_with_affinity(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |_, _| true,
            )
            .expect("sticky reservation")
            .expect("rebound resource remains pinned");
        assert_eq!(pinned.resource.resource_id, "spare");
        drop(pinned);

        commit_new_affinity(&registry, "mesh-model", &affinity, "primary");
        let auth_rebound = registry
            .reserve_excluding_where_with_affinity(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |_, resource_id| resource_id != "primary",
            )
            .expect("unauthorized pin falls back")
            .expect("authorized spare is available");
        assert_eq!(auth_rebound.resource.resource_id, "spare");
        registry.commit_affinity(auth_rebound.affinity_commit().expect("affinity commit"));
        drop(auth_rebound);

        let pinned = registry
            .reserve_excluding_where_with_affinity(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |_, _| true,
            )
            .expect("sticky reservation")
            .expect("authorized rebound resource remains pinned");
        assert_eq!(pinned.resource.resource_id, "spare");
    }

    #[test]
    fn absent_affinity_keeps_existing_round_robin_selection() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let endpoint = SecretKey::generate().public();
        registry.register_test(endpoint, test_advertisement(vec![("a", 1), ("b", 1)]));

        let first = registry
            .reserve_excluding_where_with_affinity("mesh-model", &HashSet::new(), None, |_, _| true)
            .expect("first reservation")
            .expect("first resource");
        let first_resource = first.resource.resource_id.clone();
        drop(first);
        let second = registry
            .reserve_excluding_where_with_affinity("mesh-model", &HashSet::new(), None, |_, _| true)
            .expect("second reservation")
            .expect("second resource");
        assert_ne!(second.resource.resource_id, first_resource);
    }

    #[test]
    fn affinity_pins_are_bounded_ttl_pruned_and_first_writer_wins() {
        let registry = MeshRegistry::new(
            AFFINITY_PIN_IDLE_TTL + Duration::from_secs(AFFINITY_PIN_LIMIT as u64 + 60),
        );
        let endpoint = SecretKey::generate().public();
        registry.register_test(
            endpoint,
            test_advertisement(vec![("primary", 1), ("spare", 1)]),
        );
        let now = Instant::now();
        let affinity = RequestAffinity("session-a".to_string());

        let first = registry
            .reserve_excluding_where_with_affinity_at(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |_, resource_id| resource_id == "primary",
                now,
            )
            .expect("first reservation")
            .expect("primary reservation");
        let stale_later = registry
            .reserve_excluding_where_with_affinity_at(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |_, resource_id| resource_id == "spare",
                now + Duration::from_secs(1),
            )
            .expect("stale reservation")
            .expect("spare reservation");
        registry.commit_affinity_at(first.affinity_commit().expect("affinity commit"), now);
        drop(first);
        registry.commit_affinity_at(
            stale_later.affinity_commit().expect("affinity commit"),
            now + Duration::from_secs(1),
        );
        drop(stale_later);
        let pinned = registry
            .reserve_excluding_where_with_affinity_at(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |_, _| true,
                now + Duration::from_secs(2),
            )
            .expect("sticky reservation")
            .expect("pin exists");
        assert_eq!(pinned.resource.resource_id, "primary");
        drop(pinned);

        let after_ttl = now + AFFINITY_PIN_IDLE_TTL + Duration::from_secs(3);
        let _ = registry.reserve_excluding_where_with_affinity_at(
            "mesh-model",
            &HashSet::new(),
            Some(&affinity),
            |_, _| true,
            after_ttl,
        );
        assert!(
            !registry
                .inner
                .lock()
                .expect("mesh registry lock poisoned")
                .affinity_pins
                .contains_key(&affinity_pin_key("mesh-model", &affinity))
        );

        for index in 0..(AFFINITY_PIN_LIMIT + 1) {
            let loop_affinity = RequestAffinity(format!("session-{index}"));
            let reservation = registry
                .reserve_excluding_where_with_affinity_at(
                    "mesh-model",
                    &HashSet::new(),
                    Some(&loop_affinity),
                    |_, resource_id| resource_id == "primary",
                    after_ttl + Duration::from_secs(index as u64),
                )
                .expect("loop reservation")
                .unwrap_or_else(|| panic!("primary reservation at index {index}"));
            registry.commit_affinity_at(
                reservation.affinity_commit().expect("affinity commit"),
                after_ttl + Duration::from_secs(index as u64),
            );
            drop(reservation);
        }
        assert_eq!(
            registry
                .inner
                .lock()
                .expect("mesh registry lock poisoned")
                .affinity_pins
                .len(),
            AFFINITY_PIN_LIMIT
        );
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
                model_switching: None,
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
    fn provider_inventory_groups_resource_slots_under_advertised_worker_name() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let endpoint = SecretKey::generate().public();
        registry.register_test(
            endpoint,
            WorkerAdvertisement {
                protocol_version: PROTOCOL_VERSION,
                node_name: Some("  workstation-a  ".into()),
                agent_version: "test".into(),
                resources: vec![
                    ResourceAdvertisement {
                        resource_id: "slot-a".into(),
                        models: vec![ModelAdvertisement {
                            id: "model-a".into(),
                            context_limit: None,
                        }],
                        availability: AvailabilitySchedule::default(),
                        effective_capacity: 1,
                        accepting_requests: true,
                        healthy: true,
                        revision: 1,
                    },
                    ResourceAdvertisement {
                        resource_id: "slot-b".into(),
                        models: vec![ModelAdvertisement {
                            id: "model-b".into(),
                            context_limit: None,
                        }],
                        availability: AvailabilitySchedule::default(),
                        effective_capacity: 1,
                        accepting_requests: true,
                        healthy: true,
                        revision: 1,
                    },
                ],
                model_switching: None,
            },
        );

        let inventory = registry.provider_inventory();
        assert_eq!(inventory.len(), 2);
        assert!(
            inventory
                .iter()
                .all(|entry| entry.provider_id == format!("mesh:{endpoint}"))
        );
        assert!(
            inventory
                .iter()
                .all(|entry| entry.provider_name == "workstation-a")
        );
        assert_eq!(
            inventory
                .iter()
                .map(|entry| entry.resource_id.as_deref().unwrap_or_default())
                .collect::<Vec<_>>(),
            vec!["slot-a", "slot-b"]
        );
    }

    #[test]
    fn provider_inventory_falls_back_to_endpoint_provider_id_for_missing_worker_name() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let endpoint_without_name = SecretKey::generate().public();
        let endpoint_with_blank_name = SecretKey::generate().public();
        for (endpoint, node_name, resource_prefix) in [
            (endpoint_without_name, None, "none"),
            (endpoint_with_blank_name, Some(" \t "), "blank"),
        ] {
            registry.register_test(
                endpoint,
                WorkerAdvertisement {
                    protocol_version: PROTOCOL_VERSION,
                    node_name: node_name.map(str::to_string),
                    agent_version: "test".into(),
                    resources: ["a", "b"]
                        .into_iter()
                        .map(|slot| {
                            let resource_id = format!("slot-{resource_prefix}-{slot}");
                            ResourceAdvertisement {
                                resource_id: resource_id.clone(),
                                models: vec![ModelAdvertisement {
                                    id: format!("{resource_id}-model"),
                                    context_limit: None,
                                }],
                                availability: AvailabilitySchedule::default(),
                                effective_capacity: 1,
                                accepting_requests: true,
                                healthy: true,
                                revision: 1,
                            }
                        })
                        .collect(),
                    model_switching: None,
                },
            );
        }

        let inventory = registry.provider_inventory();
        assert_eq!(inventory.len(), 4);
        let mut rows_by_provider = HashMap::new();
        for entry in &inventory {
            assert_eq!(entry.provider_name, entry.provider_id);
            assert!(entry.provider_id.starts_with("mesh:"));
            *rows_by_provider
                .entry(entry.provider_id.clone())
                .or_insert(0) += 1;
        }
        assert_eq!(rows_by_provider.len(), 2);
        assert!(rows_by_provider.values().all(|rows| *rows == 2));
    }

    #[test]
    fn provider_inventory_uses_latest_worker_name_after_reregistration() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let endpoint = SecretKey::generate().public();
        for node_name in ["worker-old", "worker-new"] {
            registry.register_test(
                endpoint,
                WorkerAdvertisement {
                    protocol_version: PROTOCOL_VERSION,
                    node_name: Some(node_name.to_string()),
                    agent_version: "test".into(),
                    resources: vec![ResourceAdvertisement {
                        resource_id: "slot".into(),
                        models: vec![ModelAdvertisement {
                            id: "model".into(),
                            context_limit: None,
                        }],
                        availability: AvailabilitySchedule::default(),
                        effective_capacity: 1,
                        accepting_requests: true,
                        healthy: true,
                        revision: 1,
                    }],
                    model_switching: None,
                },
            );
        }

        let inventory = registry.provider_inventory();
        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].provider_name, "worker-new");
        assert_eq!(inventory[0].provider_id, format!("mesh:{endpoint}"));
        assert_eq!(inventory[0].resource_id.as_deref(), Some("slot"));
    }

    #[test]
    fn disabled_model_is_removed_from_routing_catalog_and_inventory() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let endpoint = SecretKey::generate().public();
        registry.register_test(
            endpoint,
            WorkerAdvertisement {
                protocol_version: PROTOCOL_VERSION,
                node_name: None,
                agent_version: "test".into(),
                resources: vec![ResourceAdvertisement {
                    resource_id: "gpu".into(),
                    models: vec![
                        ModelAdvertisement {
                            id: "qwen".into(),
                            context_limit: None,
                        },
                        ModelAdvertisement {
                            id: "llama".into(),
                            context_limit: None,
                        },
                    ],
                    availability: AvailabilitySchedule::default(),
                    effective_capacity: 1,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                }],
                model_switching: None,
            },
        );

        registry.set_model_disabled(endpoint, "gpu", "QWEN", true);

        assert!(registry.reserve("qwen").is_none());
        assert!(registry.reserve("llama").is_some());
        assert_eq!(
            registry
                .model_catalog()
                .into_iter()
                .map(|model| model.id)
                .collect::<Vec<_>>(),
            vec!["llama"]
        );
        assert_eq!(
            registry.provider_inventory()[0]
                .models
                .iter()
                .map(|model| model.id.as_str())
                .collect::<Vec<_>>(),
            vec!["llama"]
        );
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
                model_switching: None,
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

    #[test]
    fn switching_inventory_ignores_stale_revisions() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let endpoint = SecretKey::generate().public();
        let session = registry.register_test(
            endpoint,
            WorkerAdvertisement {
                protocol_version: PROTOCOL_VERSION,
                node_name: None,
                agent_version: "test".into(),
                resources: Vec::new(),
                model_switching: Some(ModelSwitchingAdvertisement {
                    provider: "lil-fleet".into(),
                    models: vec![SwitchableModelAdvertisement::legacy(
                        "qwen3-flash",
                        None,
                        "ready",
                        "loaded",
                        1,
                        vec![0],
                    )],
                    revision: 5,
                }),
            },
        );
        session.update_model_switching(ModelSwitchingAdvertisement {
            provider: "lil-fleet".into(),
            models: vec![SwitchableModelAdvertisement::legacy(
                "stale-model",
                None,
                "unloaded",
                "unloaded",
                1,
                Vec::new(),
            )],
            revision: 4,
        });
        let advertised = registry
            .model_switching(endpoint)
            .expect("switching inventory");
        assert_eq!(advertised.revision, 5);
        assert_eq!(advertised.models[0].id, "qwen3-flash");
    }
}
