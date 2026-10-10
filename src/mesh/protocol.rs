use crate::config::AvailabilitySchedule;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

pub const ENROLL_ALPN: &[u8] = b"llmconduit-mesh-enroll/2";
pub const WORKER_ALPN: &[u8] = b"llmconduit-mesh-worker/2";

pub const PROTOCOL_VERSION: u16 = 2;
pub const CONTROL_PROTOCOL_VERSION: u16 = 2;
pub const REQUEST_PROTOCOL_VERSION: u16 = 2;
pub const MAX_CONTROL_FRAME_BYTES: usize = 64 * 1024;
pub const MAX_REQUEST_PREFACE_BYTES: usize = 4 * 1024;
pub const MAX_NODE_NAME_BYTES: usize = 128;
pub const MAX_AGENT_VERSION_BYTES: usize = 128;
pub const MAX_RESOURCE_ID_BYTES: usize = 128;
pub const MAX_MODEL_ID_BYTES: usize = 512;
pub const MAX_RESOURCES_PER_NODE: usize = 32;
pub const MAX_MODELS_PER_RESOURCE: usize = 1024;
pub const MAX_SWITCHABLE_MODELS: usize = 256;
pub const MAX_SWITCHING_PROVIDER_BYTES: usize = 64;
pub const MAX_SWITCHING_DESCRIPTION_BYTES: usize = 1024;
pub const MAX_SWITCHING_STATE_BYTES: usize = 64;
pub const MAX_CAPACITY_PER_RESOURCE: u32 = 65_535;
pub const MAX_SWITCH_MODEL_INSTANCES: u32 = 64;
/// GPU inventory bounds. A node with more GPUs than this is not a single
/// Fleet host; the worker truncates and the hub rejects anything larger.
pub const MAX_GPUS_PER_NODE: usize = 64;
pub const MAX_GPU_NAME_BYTES: usize = 128;
pub const MAX_GPU_ASSIGNMENTS: usize = 16;
pub const MAX_GPU_INDEX: u32 = 255;
pub const MAX_GPU_GROUPS: usize = 64;
pub const MAX_GPU_SAMPLED_AT_BYTES: usize = 64;
/// Largest serialized inventory a worker will advertise. Realistic nodes
/// (8 GPUs) need ~2 KiB; the cap keeps a pathological Fleet answer from
/// pushing the hello past [`MAX_CONTROL_FRAME_BYTES`] and locking the worker
/// out of the mesh.
pub const MAX_GPU_INVENTORY_BYTES: usize = 16 * 1024;
/// A manual GPU pick names at most this many devices.
pub const MAX_GPU_SELECTION: usize = 64;
/// Fleet load warnings relayed to the dashboard.
pub const MAX_LOAD_WARNINGS: usize = 8;
pub const MAX_LOAD_WARNING_BYTES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollRequest {
    pub protocol_version: u16,
    pub join_key: String,
    pub node_name: Option<String>,
    pub agent_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollAccepted {
    pub protocol_version: u16,
    pub endpoint_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum EnrollResponse {
    Accepted { protocol_version: u16 },
    Rejected { reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerAdvertisement {
    pub protocol_version: u16,
    pub node_name: Option<String>,
    pub agent_version: String,
    pub resources: Vec<ResourceAdvertisement>,
    pub model_switching: Option<ModelSwitchingAdvertisement>,
    /// `Content-Encoding` values this worker can decode on inference request
    /// bodies. Optional on the wire in both directions: an older hub ignores
    /// the unknown field, and an older worker omits it so a newer hub keeps
    /// sending identity-encoded bodies.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub request_encodings: Vec<String>,
    /// Optional worker features beyond the base v2 protocol (for example
    /// [`CAPABILITY_FLEET_OPERATIONS`]). Additive in both directions: an older
    /// hub ignores the field and an older worker omits it, so a newer hub never
    /// sends that worker a stream type it cannot parse.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
}

/// The worker answers [`StreamOpen::FleetOperation`] lookups and reports the
/// Fleet operation in [`SwitchModelResponse::operation`].
pub const CAPABILITY_FLEET_OPERATIONS: &str = "fleet_operations";
/// The worker forwards [`SwitchModelRequest::gpus`] to Fleet. An older worker
/// would silently ignore the field and place automatically, so the hub refuses
/// a manual GPU pick for workers without this capability.
pub const CAPABILITY_GPU_PLACEMENT: &str = "gpu_placement";
pub const MAX_CAPABILITIES: usize = 16;
pub const MAX_CAPABILITY_BYTES: usize = 64;
pub const MAX_OPERATION_ID_BYTES: usize = 128;

/// Request-body encoding negotiated through
/// [`WorkerAdvertisement::request_encodings`].
pub const REQUEST_ENCODING_ZSTD: &str = "zstd";
pub const MAX_REQUEST_ENCODINGS: usize = 8;
pub const MAX_REQUEST_ENCODING_BYTES: usize = 32;

/// Largest context window a worker may advertise. Anything above this is
/// either a bug or hostile and would overflow downstream token budgeting.
pub const MAX_CONTEXT_LIMIT: i64 = 10_000_000;

impl WorkerAdvertisement {
    pub fn accepts_request_encoding(&self, encoding: &str) -> bool {
        self.request_encodings
            .iter()
            .any(|value| value.eq_ignore_ascii_case(encoding))
    }

    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities
            .iter()
            .any(|value| value.eq_ignore_ascii_case(capability))
    }
}

/// Drops a context limit outside `1..=MAX_CONTEXT_LIMIT` instead of
/// rejecting the whole advertisement: an unknown window only disables
/// pre-flight budgeting for that model, while a bogus one corrupts it.
pub fn sanitize_context_limit(limit: Option<i64>) -> Option<i64> {
    limit.filter(|limit| (1..=MAX_CONTEXT_LIMIT).contains(limit))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelSwitchingAdvertisement {
    pub provider: String,
    pub models: Vec<SwitchableModelAdvertisement>,
    pub revision: u64,
    /// Latest per-GPU VRAM sample from the worker's Fleet. Optional both
    /// ways: older workers omit it (no inventory) and older hubs ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_inventory: Option<GpuInventoryAdvertisement>,
}

/// A bounded snapshot of Fleet `GET /v1/gpus` as relayed by a worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuInventoryAdvertisement {
    /// Fleet's sample time (RFC3339, worker clock).
    pub sampled_at: String,
    /// Age of the sample in milliseconds when the worker sent it, measured on
    /// the worker's own clock so the hub can show staleness without trusting
    /// cross-host clock agreement.
    #[serde(default)]
    pub age_ms: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub stale: bool,
    /// PCIe groups (lists of GPU indices).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<Vec<u32>>,
    #[serde(default)]
    pub gpus: Vec<GpuAdvertisement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuAdvertisement {
    pub index: u32,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub memory_total_mib: u64,
    #[serde(default)]
    pub memory_used_mib: u64,
    #[serde(default)]
    pub memory_free_mib: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub utilization_percent: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<u32>,
    /// Fleet model ids occupying this GPU.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assigned: Vec<String>,
    /// Occupants the hub hid because its model allowlist does not expose
    /// them; set by the hub only, so the GPU still reads as occupied.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub hidden_assignments: u32,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

impl GpuInventoryAdvertisement {
    /// Builds the bounded wire form of a sanitized Fleet sample. Returns
    /// `None` if it would still exceed [`MAX_GPU_INVENTORY_BYTES`].
    pub(crate) fn from_fleet(
        value: crate::dashboard_fleet::FleetGpuInventory,
        age_ms: u64,
    ) -> Option<Self> {
        let inventory = Self {
            sampled_at: value.sampled_at,
            age_ms,
            stale: value.stale,
            groups: value.groups,
            gpus: value
                .gpus
                .into_iter()
                .map(|gpu| GpuAdvertisement {
                    index: gpu.index,
                    name: gpu.name,
                    memory_total_mib: gpu.memory_total_mib,
                    memory_used_mib: gpu.memory_used_mib,
                    memory_free_mib: gpu.memory_free_mib,
                    utilization_percent: gpu.utilization_percent,
                    group: gpu.group,
                    assigned: {
                        let mut models: Vec<String> = Vec::new();
                        for assignment in gpu.assigned {
                            if !models.contains(&assignment.model_id) {
                                models.push(assignment.model_id);
                            }
                        }
                        models.truncate(MAX_GPU_ASSIGNMENTS);
                        models
                    },
                    hidden_assignments: 0,
                })
                .collect(),
        };
        let size = serde_json::to_vec(&inventory).map_or(usize::MAX, |bytes| bytes.len());
        (size <= MAX_GPU_INVENTORY_BYTES && validate_gpu_inventory(&inventory).is_ok())
            .then_some(inventory)
    }

    /// Replaces occupants the caller may not see with a count, so a GPU used
    /// by a model outside the hub's allowlist still reads as occupied without
    /// naming that model.
    pub fn hide_assignments_except<F>(&mut self, visible: F)
    where
        F: Fn(&str) -> bool,
    {
        for gpu in &mut self.gpus {
            let before = gpu.assigned.len();
            gpu.assigned.retain(|model| visible(model));
            let hidden = (before - gpu.assigned.len()) as u32;
            gpu.hidden_assignments = gpu.hidden_assignments.saturating_add(hidden);
        }
    }

    pub fn has_gpu(&self, index: u32) -> bool {
        self.gpus.iter().any(|gpu| gpu.index == index)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwitchableModelAdvertisement {
    pub id: String,
    pub description: Option<String>,
    pub phase: String,
    pub desired_state: String,
    #[serde(default)]
    pub gpu_count: u32,
    #[serde(default)]
    pub assigned_gpus: Vec<u32>,
    #[serde(default = "default_max_instances")]
    pub max_instances: u32,
    #[serde(default)]
    pub desired_instances: u32,
    #[serde(default)]
    pub ready_instances: u32,
    #[serde(default)]
    pub instances: Vec<SwitchableModelInstanceAdvertisement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwitchableModelInstanceAdvertisement {
    pub instance_id: String,
    pub index: u32,
    pub port: u16,
    pub phase: String,
    #[serde(default)]
    pub assigned_gpus: Vec<u32>,
    pub container_status: Option<String>,
    pub last_error: Option<String>,
    pub health: Option<String>,
}

impl SwitchableModelAdvertisement {
    #[cfg(test)]
    pub(crate) fn legacy(
        id: impl Into<String>,
        description: Option<String>,
        phase: impl Into<String>,
        desired_state: impl Into<String>,
        gpu_count: u32,
        assigned_gpus: Vec<u32>,
    ) -> Self {
        Self {
            id: id.into(),
            description,
            phase: phase.into(),
            desired_state: desired_state.into(),
            gpu_count,
            assigned_gpus,
            max_instances: 1,
            desired_instances: 0,
            ready_instances: 0,
            instances: Vec::new(),
        }
    }
}

impl SwitchableModelInstanceAdvertisement {
    pub(crate) fn from_fleet(value: crate::dashboard_fleet::FleetInstanceStatus) -> Self {
        Self {
            instance_id: value.instance_id,
            index: value.index,
            port: value.port,
            phase: value.phase,
            assigned_gpus: value
                .assigned_gpus
                .into_iter()
                .filter_map(|gpu| u32::try_from(gpu).ok())
                .collect(),
            container_status: fleet_diagnostic_text(value.container_status),
            last_error: fleet_diagnostic_text(value.last_error),
            health: fleet_diagnostic_text(value.health),
        }
    }
}

fn fleet_diagnostic_text(value: Option<String>) -> Option<String> {
    // Docker errors contain newlines and can be arbitrarily long; they must not
    // invalidate the worker's entire advertisement or bypass its wire bounds.
    let value = value?;
    let mut normalized = String::new();
    for ch in value.trim().chars() {
        let ch = if ch.is_control() { ' ' } else { ch };
        if normalized.len() + ch.len_utf8() > MAX_SWITCHING_DESCRIPTION_BYTES {
            break;
        }
        normalized.push(ch);
    }
    let normalized = normalized.trim().to_owned();
    (!normalized.is_empty()).then_some(normalized)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceAdvertisement {
    pub resource_id: String,
    pub models: Vec<ModelAdvertisement>,
    pub availability: AvailabilitySchedule,
    pub effective_capacity: u32,
    pub accepting_requests: bool,
    pub healthy: bool,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelAdvertisement {
    pub id: String,
    pub context_limit: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Heartbeat {
    pub sequence: u64,
    pub resources: Vec<ResourceRuntimeState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRuntimeState {
    pub resource_id: String,
    pub effective_capacity: u32,
    pub worker_active_requests: u32,
    pub accepting_requests: bool,
    pub healthy: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum WorkerToHub {
    Hello(WorkerAdvertisement),
    Heartbeat(Heartbeat),
    ResourceUpdate(ResourceAdvertisement),
    ModelCatalogUpdate {
        resource_id: String,
        models: Vec<ModelAdvertisement>,
        revision: u64,
    },
    ModelSwitchingUpdate(ModelSwitchingAdvertisement),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum HubToWorker {
    HelloAck { protocol_version: u16 },
    Ping { nonce: u64 },
    Close { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestOpen {
    pub protocol_version: u16,
    pub request_id: Uuid,
    pub resource_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum StreamOpen {
    Inference(RequestOpen),
    SwitchModel(SwitchModelRequest),
    UnloadModel(SwitchModelRequest),
    /// Look up a Fleet lifecycle operation. Only sent to workers advertising
    /// [`CAPABILITY_FLEET_OPERATIONS`]; an older worker cannot parse it.
    FleetOperation(FleetOperationRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FleetOperationRequest {
    pub protocol_version: u16,
    pub request_id: Uuid,
    pub operation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FleetOperationResponse {
    pub request_id: Uuid,
    pub operation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<LifecycleOperation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Status Fleet (or the worker) answered with when the lookup failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_status: Option<u16>,
}

/// A Fleet lifecycle operation as relayed by a worker. `id`, `state` and
/// `error` are the stable contract; the remaining fields are informational and
/// optional on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct LifecycleOperation {
    pub id: String,
    pub state: String,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instances: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
}

impl LifecycleOperation {
    pub(crate) fn from_fleet(value: crate::dashboard_fleet::FleetOperation) -> Self {
        Self {
            id: value.id,
            state: value.state,
            error: value.error,
            kind: Some(value.kind),
            model_id: Some(value.model_id),
            instances: value.instances,
            created_at: Some(value.created_at),
            started_at: value.started_at,
            finished_at: value.finished_at,
        }
    }

    /// Bounds every worker-controlled field. Returns `None` when the id or
    /// state is unusable; free text is truncated and stripped of control
    /// characters rather than rejected.
    pub fn sanitized(self) -> Option<Self> {
        if validate_operation_id(&self.id).is_err() {
            return None;
        }
        let state = bounded_text(Some(self.state), MAX_SWITCHING_STATE_BYTES)?;
        Some(Self {
            id: self.id,
            state,
            error: bounded_text(self.error, MAX_SWITCHING_DESCRIPTION_BYTES),
            kind: bounded_text(self.kind, MAX_SWITCHING_STATE_BYTES),
            model_id: self
                .model_id
                .filter(|model| validate_model_id(model).is_ok()),
            instances: self.instances,
            created_at: bounded_text(self.created_at, MAX_SWITCHING_STATE_BYTES),
            started_at: bounded_text(self.started_at, MAX_SWITCHING_STATE_BYTES),
            finished_at: bounded_text(self.finished_at, MAX_SWITCHING_STATE_BYTES),
        })
    }
}

/// Operation ids are opaque Fleet tokens (hex today): 1-128 bytes of
/// `[A-Za-z0-9._:-]`, so they can travel in a URL path segment unescaped.
pub fn validate_operation_id(operation_id: &str) -> Result<(), ProtocolError> {
    if operation_id.is_empty()
        || operation_id.len() > MAX_OPERATION_ID_BYTES
        || !operation_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(ProtocolError::InvalidOperationId);
    }
    Ok(())
}

fn bounded_text(value: Option<String>, max: usize) -> Option<String> {
    let value = value?;
    let mut normalized = String::new();
    for ch in value.trim().chars() {
        let ch = if ch.is_control() { ' ' } else { ch };
        if normalized.len() + ch.len_utf8() > max {
            break;
        }
        normalized.push(ch);
    }
    let normalized = normalized.trim().to_owned();
    (!normalized.is_empty()).then_some(normalized)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwitchModelRequest {
    pub protocol_version: u16,
    pub request_id: Uuid,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instances: Option<u32>,
    /// Manual GPU pick for the newly started instances, in assignment order.
    /// Empty means Fleet places automatically. Only sent to workers that
    /// advertise [`CAPABILITY_GPU_PLACEMENT`]; older workers ignore it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gpus: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelLifecycleAction {
    Load,
    Unload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwitchModelResponse {
    pub request_id: Uuid,
    pub model_id: String,
    pub accepted: bool,
    pub changed: bool,
    pub error: Option<String>,
    pub model_switching: Option<ModelSwitchingAdvertisement>,
    /// HTTP status the worker's Fleet answered with when it refused the
    /// operation, so the hub can report e.g. a 400 instead of a blanket 409.
    /// Optional both ways: older peers omit or ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_status: Option<u16>,
    /// Fleet's machine-readable error code (e.g. `operation_in_progress`,
    /// `insufficient_resources`) when it refused. Optional both ways.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    /// The Fleet operation this request started or joined. Optional both
    /// ways: older workers omit it and older hubs ignore it. `None` also for
    /// an already-satisfied no-op.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<LifecycleOperation>,
    /// Fleet's advisory warnings for an accepted load (e.g. a GPU pick that
    /// spans PCIe groups). Optional both ways; bounded by
    /// [`sanitize_warnings`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Keeps at most [`MAX_LOAD_WARNINGS`] non-blank warnings, each truncated to
/// [`MAX_LOAD_WARNING_BYTES`] with control characters replaced.
pub fn sanitize_warnings(warnings: Vec<String>) -> Vec<String> {
    warnings
        .into_iter()
        .filter_map(|warning| bounded_text(Some(warning), MAX_LOAD_WARNING_BYTES))
        .take(MAX_LOAD_WARNINGS)
        .collect()
}

/// A manual GPU pick: at most [`MAX_GPU_SELECTION`] distinct indices in
/// `0..=MAX_GPU_INDEX`. Whether the count fits the model is Fleet's call.
pub fn validate_gpu_selection(gpus: &[u32]) -> Result<(), ProtocolError> {
    if gpus.len() > MAX_GPU_SELECTION {
        return Err(ProtocolError::InvalidGpuSelection(
            "gpus must list at most 64 GPU indices",
        ));
    }
    for (position, gpu) in gpus.iter().enumerate() {
        if *gpu > MAX_GPU_INDEX {
            return Err(ProtocolError::InvalidGpuSelection(
                "gpus indices must be between 0 and 255",
            ));
        }
        if gpus[..position].contains(gpu) {
            return Err(ProtocolError::InvalidGpuSelection(
                "gpus must not repeat an index",
            ));
        }
    }
    Ok(())
}

impl SwitchModelResponse {
    /// Status for the hub's dashboard response when the worker refused:
    /// Fleet's request-level 4xx (and gateway 502/504) pass through,
    /// anything else keeps the historical 409.
    pub fn rejection_status(&self) -> http::StatusCode {
        self.error_status
            .filter(|status| (400..=499).contains(status) || matches!(status, 502 | 504))
            .and_then(|status| http::StatusCode::from_u16(status).ok())
            .unwrap_or(http::StatusCode::CONFLICT)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum Admission {
    Accepted,
    Rejected { code: AdmissionRejectCode },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionRejectCode {
    CapacityExhausted,
    UnknownResource,
    ResourceUnhealthy,
    LocalConnectFailed,
    ProtocolError,
}

pub fn validate_enroll_request(request: &EnrollRequest) -> Result<(), ProtocolError> {
    validate_protocol_version(request.protocol_version)?;
    validate_optional_name(request.node_name.as_deref())?;
    validate_agent_version(&request.agent_version)?;
    if request.join_key.trim().is_empty() {
        return Err(ProtocolError::BlankJoinKey);
    }
    Ok(())
}

pub fn validate_worker_advertisement(
    advertisement: &WorkerAdvertisement,
) -> Result<(), ProtocolError> {
    validate_protocol_version(advertisement.protocol_version)?;
    validate_optional_name(advertisement.node_name.as_deref())?;
    validate_agent_version(&advertisement.agent_version)?;
    if advertisement.resources.len() > MAX_RESOURCES_PER_NODE {
        return Err(ProtocolError::TooManyResources {
            count: advertisement.resources.len(),
            max: MAX_RESOURCES_PER_NODE,
        });
    }
    for resource in &advertisement.resources {
        validate_resource_advertisement(resource)?;
    }
    if let Some(switching) = &advertisement.model_switching {
        validate_model_switching(switching)?;
    }
    if advertisement.request_encodings.len() > MAX_REQUEST_ENCODINGS {
        return Err(ProtocolError::TooManyRequestEncodings {
            count: advertisement.request_encodings.len(),
            max: MAX_REQUEST_ENCODINGS,
        });
    }
    if advertisement.capabilities.len() > MAX_CAPABILITIES {
        return Err(ProtocolError::TooManyCapabilities {
            count: advertisement.capabilities.len(),
            max: MAX_CAPABILITIES,
        });
    }
    for capability in &advertisement.capabilities {
        validate_bounded_string(
            capability,
            MAX_CAPABILITY_BYTES,
            ProtocolError::BlankSwitchingState {
                field: "worker capability",
            },
            |len, max| ProtocolError::SwitchingStateTooLong {
                field: "worker capability",
                len,
                max,
            },
        )?;
        reject_control_characters(capability, "worker capability")?;
    }
    for encoding in &advertisement.request_encodings {
        validate_bounded_string(
            encoding,
            MAX_REQUEST_ENCODING_BYTES,
            ProtocolError::BlankSwitchingState {
                field: "request encoding",
            },
            |len, max| ProtocolError::SwitchingStateTooLong {
                field: "request encoding",
                len,
                max,
            },
        )?;
        reject_control_characters(encoding, "request encoding")?;
    }
    Ok(())
}

pub fn validate_model_switching(
    switching: &ModelSwitchingAdvertisement,
) -> Result<(), ProtocolError> {
    validate_bounded_string(
        &switching.provider,
        MAX_SWITCHING_PROVIDER_BYTES,
        ProtocolError::BlankSwitchingProvider,
        |len, max| ProtocolError::SwitchingProviderTooLong { len, max },
    )?;
    reject_control_characters(&switching.provider, "switching provider")?;
    if let Some(inventory) = &switching.gpu_inventory {
        validate_gpu_inventory(inventory)?;
    }
    if switching.models.len() > MAX_SWITCHABLE_MODELS {
        return Err(ProtocolError::TooManyModels {
            count: switching.models.len(),
            max: MAX_SWITCHABLE_MODELS,
        });
    }
    for model in &switching.models {
        validate_model_id(&model.id)?;
        if model.gpu_count > 64 || model.assigned_gpus.len() > 64 {
            return Err(ProtocolError::CapacityTooLarge {
                capacity: model.gpu_count.max(model.assigned_gpus.len() as u32),
                max: 64,
            });
        }
        for (field, count) in [
            ("switching model max_instances", model.max_instances),
            ("switching model desired_instances", model.desired_instances),
            ("switching model ready_instances", model.ready_instances),
        ] {
            if count > MAX_SWITCH_MODEL_INSTANCES {
                return Err(ProtocolError::InstanceCountOutOfRange {
                    field,
                    count,
                    max: MAX_SWITCH_MODEL_INSTANCES,
                });
            }
        }
        if model.instances.len() > MAX_SWITCH_MODEL_INSTANCES as usize {
            return Err(ProtocolError::TooManyModelInstances {
                count: model.instances.len(),
                max: MAX_SWITCH_MODEL_INSTANCES as usize,
            });
        }
        if let Some(description) = &model.description {
            if description.len() > MAX_SWITCHING_DESCRIPTION_BYTES {
                return Err(ProtocolError::SwitchingDescriptionTooLong {
                    len: description.len(),
                    max: MAX_SWITCHING_DESCRIPTION_BYTES,
                });
            }
            reject_control_characters(description, "switching model description")?;
        }
        for (field, value) in [
            ("switching model phase", model.phase.as_str()),
            (
                "switching model desired state",
                model.desired_state.as_str(),
            ),
        ] {
            if value.trim().is_empty() {
                return Err(ProtocolError::BlankSwitchingState { field });
            }
            if value.len() > MAX_SWITCHING_STATE_BYTES {
                return Err(ProtocolError::SwitchingStateTooLong {
                    field,
                    len: value.len(),
                    max: MAX_SWITCHING_STATE_BYTES,
                });
            }
            reject_control_characters(value, field)?;
        }
        for instance in &model.instances {
            validate_bounded_string(
                &instance.instance_id,
                MAX_RESOURCE_ID_BYTES,
                ProtocolError::BlankSwitchingState {
                    field: "switching model instance id",
                },
                |len, max| ProtocolError::SwitchingStateTooLong {
                    field: "switching model instance id",
                    len,
                    max,
                },
            )?;
            reject_control_characters(&instance.instance_id, "switching model instance id")?;
            if instance.assigned_gpus.len() > 64 {
                return Err(ProtocolError::CapacityTooLarge {
                    capacity: instance.assigned_gpus.len() as u32,
                    max: 64,
                });
            }
            for (field, value) in [
                (
                    "switching model instance phase",
                    Some(instance.phase.as_str()),
                ),
                (
                    "switching model instance container status",
                    instance.container_status.as_deref(),
                ),
                (
                    "switching model instance last error",
                    instance.last_error.as_deref(),
                ),
                (
                    "switching model instance health",
                    instance.health.as_deref(),
                ),
            ] {
                if let Some(value) = value {
                    if value.trim().is_empty() {
                        return Err(ProtocolError::BlankSwitchingState { field });
                    }
                    if value.len() > MAX_SWITCHING_DESCRIPTION_BYTES {
                        return Err(ProtocolError::SwitchingDescriptionTooLong {
                            len: value.len(),
                            max: MAX_SWITCHING_DESCRIPTION_BYTES,
                        });
                    }
                    reject_control_characters(value, field)?;
                }
            }
        }
    }
    Ok(())
}

pub fn validate_gpu_inventory(inventory: &GpuInventoryAdvertisement) -> Result<(), ProtocolError> {
    let invalid = ProtocolError::InvalidGpuInventory;
    validate_bounded_string(
        &inventory.sampled_at,
        MAX_GPU_SAMPLED_AT_BYTES,
        invalid("sampled_at must not be blank"),
        |_, _| invalid("sampled_at is too long"),
    )?;
    reject_control_characters(&inventory.sampled_at, "gpu sampled_at")?;
    if inventory.gpus.len() > MAX_GPUS_PER_NODE {
        return Err(invalid("too many GPUs"));
    }
    if inventory.groups.len() > MAX_GPU_GROUPS {
        return Err(invalid("too many GPU groups"));
    }
    for group in &inventory.groups {
        if group.len() > MAX_GPUS_PER_NODE || group.iter().any(|gpu| *gpu > MAX_GPU_INDEX) {
            return Err(invalid("GPU group is out of range"));
        }
    }
    for (position, gpu) in inventory.gpus.iter().enumerate() {
        if gpu.index > MAX_GPU_INDEX {
            return Err(invalid("GPU index is out of range"));
        }
        if inventory.gpus[..position]
            .iter()
            .any(|other| other.index == gpu.index)
        {
            return Err(invalid("GPU index is repeated"));
        }
        if gpu.name.len() > MAX_GPU_NAME_BYTES {
            return Err(invalid("GPU name is too long"));
        }
        reject_control_characters(&gpu.name, "gpu name")?;
        if gpu.utilization_percent.is_some_and(|value| value > 100) {
            return Err(invalid("GPU utilization is out of range"));
        }
        if gpu
            .group
            .is_some_and(|group| group as usize >= MAX_GPU_GROUPS)
        {
            return Err(invalid("GPU group is out of range"));
        }
        if gpu.assigned.len() > MAX_GPU_ASSIGNMENTS
            || gpu.hidden_assignments as usize > MAX_GPU_ASSIGNMENTS
        {
            return Err(invalid("too many GPU assignments"));
        }
        for model in &gpu.assigned {
            validate_model_id(model)?;
        }
    }
    Ok(())
}

pub fn validate_heartbeat(heartbeat: &Heartbeat) -> Result<(), ProtocolError> {
    if heartbeat.resources.len() > MAX_RESOURCES_PER_NODE {
        return Err(ProtocolError::TooManyResources {
            count: heartbeat.resources.len(),
            max: MAX_RESOURCES_PER_NODE,
        });
    }
    for resource in &heartbeat.resources {
        validate_resource_id(&resource.resource_id)?;
        if resource.effective_capacity > MAX_CAPACITY_PER_RESOURCE {
            return Err(ProtocolError::CapacityTooLarge {
                capacity: resource.effective_capacity,
                max: MAX_CAPACITY_PER_RESOURCE,
            });
        }
    }
    Ok(())
}

pub fn validate_model_catalog(
    resource_id: &str,
    models: &[ModelAdvertisement],
) -> Result<(), ProtocolError> {
    validate_resource_id(resource_id)?;
    if models.len() > MAX_MODELS_PER_RESOURCE {
        return Err(ProtocolError::TooManyModels {
            count: models.len(),
            max: MAX_MODELS_PER_RESOURCE,
        });
    }
    for model in models {
        validate_model_id(&model.id)?;
    }
    Ok(())
}

pub fn validate_resource_advertisement(
    resource: &ResourceAdvertisement,
) -> Result<(), ProtocolError> {
    validate_resource_id(&resource.resource_id)?;
    if resource.effective_capacity > MAX_CAPACITY_PER_RESOURCE {
        return Err(ProtocolError::CapacityTooLarge {
            capacity: resource.effective_capacity,
            max: MAX_CAPACITY_PER_RESOURCE,
        });
    }
    if resource.models.len() > MAX_MODELS_PER_RESOURCE {
        return Err(ProtocolError::TooManyModels {
            count: resource.models.len(),
            max: MAX_MODELS_PER_RESOURCE,
        });
    }
    for model in &resource.models {
        validate_model_id(&model.id)?;
    }
    Ok(())
}

pub fn validate_request_open(request: &RequestOpen) -> Result<(), ProtocolError> {
    validate_protocol_version(request.protocol_version)?;
    validate_resource_id(&request.resource_id)
}

pub fn validate_switch_model_request(request: &SwitchModelRequest) -> Result<(), ProtocolError> {
    validate_protocol_version(request.protocol_version)?;
    validate_model_id(&request.model_id)?;
    if let Some(instances) = request.instances {
        validate_instance_count("instances", instances)?;
    }
    validate_gpu_selection(&request.gpus)
}

pub fn validate_fleet_operation_request(
    request: &FleetOperationRequest,
) -> Result<(), ProtocolError> {
    validate_protocol_version(request.protocol_version)?;
    validate_operation_id(&request.operation_id)
}

pub fn validate_protocol_version(version: u16) -> Result<(), ProtocolError> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedProtocolVersion(version))
    }
}

pub fn validate_resource_id(resource_id: &str) -> Result<(), ProtocolError> {
    validate_bounded_string(
        resource_id,
        MAX_RESOURCE_ID_BYTES,
        ProtocolError::BlankResourceId,
        |len, max| ProtocolError::ResourceIdTooLong { len, max },
    )?;
    reject_control_characters(resource_id, "resource id")
}

pub(crate) fn validate_model_id(model_id: &str) -> Result<(), ProtocolError> {
    validate_bounded_string(
        model_id,
        MAX_MODEL_ID_BYTES,
        ProtocolError::BlankModelId,
        |len, max| ProtocolError::ModelIdTooLong { len, max },
    )?;
    reject_control_characters(model_id, "model id")
}

fn validate_agent_version(agent_version: &str) -> Result<(), ProtocolError> {
    validate_bounded_string(
        agent_version,
        MAX_AGENT_VERSION_BYTES,
        ProtocolError::BlankAgentVersion,
        |len, max| ProtocolError::AgentVersionTooLong { len, max },
    )?;
    reject_control_characters(agent_version, "agent version")
}

fn validate_optional_name(name: Option<&str>) -> Result<(), ProtocolError> {
    if let Some(name) = name {
        validate_bounded_string(
            name,
            MAX_NODE_NAME_BYTES,
            ProtocolError::BlankNodeName,
            |len, max| ProtocolError::NodeNameTooLong { len, max },
        )?;
        reject_control_characters(name, "node name")?;
    }
    Ok(())
}

fn reject_control_characters(value: &str, field: &'static str) -> Result<(), ProtocolError> {
    if value.chars().any(char::is_control) {
        return Err(ProtocolError::ControlCharacter { field });
    }
    Ok(())
}

fn validate_instance_count(field: &'static str, count: u32) -> Result<(), ProtocolError> {
    if !(1..=MAX_SWITCH_MODEL_INSTANCES).contains(&count) {
        return Err(ProtocolError::InstanceCountOutOfRange {
            field,
            count,
            max: MAX_SWITCH_MODEL_INSTANCES,
        });
    }
    Ok(())
}

fn default_max_instances() -> u32 {
    1
}

fn validate_bounded_string<F>(
    value: &str,
    max: usize,
    blank_error: ProtocolError,
    too_long: F,
) -> Result<(), ProtocolError>
where
    F: FnOnce(usize, usize) -> ProtocolError,
{
    if value.trim().is_empty() {
        return Err(blank_error);
    }
    let len = value.len();
    if len > max {
        return Err(too_long(len, max));
    }
    Ok(())
}

pub async fn read_control<R, T>(reader: &mut R) -> crate::error::AppResult<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    read_json_frame(reader, MAX_CONTROL_FRAME_BYTES)
        .await
        .map_err(|err| crate::error::AppError::upstream(format!("mesh control frame error: {err}")))
}

pub async fn write_control<W, T>(writer: &mut W, message: &T) -> crate::error::AppResult<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    write_json_frame(writer, message, MAX_CONTROL_FRAME_BYTES)
        .await
        .map_err(|err| crate::error::AppError::upstream(format!("mesh control frame error: {err}")))
}

pub fn encode_json_frame<T: Serialize>(message: &T, max_len: usize) -> Result<Vec<u8>, FrameError> {
    let payload = serde_json::to_vec(message)?;
    if payload.len() > max_len {
        return Err(FrameError::FrameTooLarge {
            len: payload.len(),
            max: max_len,
        });
    }
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub fn decode_json_frame<T: DeserializeOwned>(
    frame: &[u8],
    max_len: usize,
) -> Result<T, FrameError> {
    if frame.len() < 4 {
        return Err(FrameError::ShortLengthPrefix);
    }
    let len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
    if len > max_len {
        return Err(FrameError::FrameTooLarge { len, max: max_len });
    }
    if frame.len() - 4 != len {
        return Err(FrameError::LengthMismatch {
            declared: len,
            actual: frame.len() - 4,
        });
    }
    Ok(serde_json::from_slice(&frame[4..])?)
}

pub async fn read_json_frame<R, T>(reader: &mut R, max_len: usize) -> Result<T, FrameError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut len_buf = [0; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_len {
        return Err(FrameError::FrameTooLarge { len, max: max_len });
    }
    let mut payload = vec![0; len];
    reader.read_exact(&mut payload).await?;
    Ok(serde_json::from_slice(&payload)?)
}

pub async fn write_json_frame<W, T>(
    writer: &mut W,
    message: &T,
    max_len: usize,
) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let frame = encode_json_frame(message, max_len)?;
    writer.write_all(&frame).await?;
    Ok(())
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("unsupported mesh protocol version {0}")]
    UnsupportedProtocolVersion(u16),
    #[error("node name must not be blank")]
    BlankNodeName,
    #[error("node name is {len} bytes, maximum is {max}")]
    NodeNameTooLong { len: usize, max: usize },
    #[error("agent version must not be blank")]
    BlankAgentVersion,
    #[error("agent version is {len} bytes, maximum is {max}")]
    AgentVersionTooLong { len: usize, max: usize },
    #[error("resource id must not be blank")]
    BlankResourceId,
    #[error("resource id is {len} bytes, maximum is {max}")]
    ResourceIdTooLong { len: usize, max: usize },
    #[error("capacity {capacity} exceeds maximum {max}")]
    CapacityTooLarge { capacity: u32, max: u32 },
    #[error("worker advertised {count} resources, maximum is {max}")]
    TooManyResources { count: usize, max: usize },
    #[error("resource advertised {count} models, maximum is {max}")]
    TooManyModels { count: usize, max: usize },
    #[error("model advertised {count} instances, maximum is {max}")]
    TooManyModelInstances { count: usize, max: usize },
    #[error("model id must not be blank")]
    BlankModelId,
    #[error("model id is {len} bytes, maximum is {max}")]
    ModelIdTooLong { len: usize, max: usize },
    #[error("switching provider must not be blank")]
    BlankSwitchingProvider,
    #[error("switching provider is {len} bytes, maximum is {max}")]
    SwitchingProviderTooLong { len: usize, max: usize },
    #[error("switching model description is {len} bytes, maximum is {max}")]
    SwitchingDescriptionTooLong { len: usize, max: usize },
    #[error("{field} must not be blank")]
    BlankSwitchingState { field: &'static str },
    #[error("{field} is {len} bytes, maximum is {max}")]
    SwitchingStateTooLong {
        field: &'static str,
        len: usize,
        max: usize,
    },
    #[error("{field} must not contain control characters")]
    ControlCharacter { field: &'static str },
    #[error("{field} instance count {count} must be between 1 and {max}")]
    InstanceCountOutOfRange {
        field: &'static str,
        count: u32,
        max: u32,
    },
    #[error("join key must not be blank")]
    BlankJoinKey,
    #[error("worker advertised {count} request encodings, maximum is {max}")]
    TooManyRequestEncodings { count: usize, max: usize },
    #[error("worker advertised {count} capabilities, maximum is {max}")]
    TooManyCapabilities { count: usize, max: usize },
    #[error("invalid Fleet operation id")]
    InvalidOperationId,
    #[error("invalid GPU inventory: {0}")]
    InvalidGpuInventory(&'static str),
    #[error("{0}")]
    InvalidGpuSelection(&'static str),
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("frame length prefix was incomplete")]
    ShortLengthPrefix,
    #[error("frame is {len} bytes, maximum is {max}")]
    FrameTooLarge { len: usize, max: usize },
    #[error("frame declared {declared} bytes but had {actual}")]
    LengthMismatch { declared: usize, actual: usize },
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[test]
    fn framed_json_round_trips_and_enforces_limit() {
        let request = RequestOpen {
            protocol_version: PROTOCOL_VERSION,
            request_id: Uuid::nil(),
            resource_id: "primary".to_string(),
        };
        let frame = encode_json_frame(&request, MAX_REQUEST_PREFACE_BYTES).unwrap();
        let decoded: RequestOpen = decode_json_frame(&frame, MAX_REQUEST_PREFACE_BYTES).unwrap();
        assert_eq!(decoded, request);
        assert!(matches!(
            encode_json_frame(&request, 4),
            Err(FrameError::FrameTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn async_frame_helpers_round_trip() {
        let (mut client, mut server) = duplex(1024);
        let writer = tokio::spawn(async move {
            write_control(&mut client, &HubToWorker::Ping { nonce: 42 })
                .await
                .unwrap();
        });
        let decoded: HubToWorker = read_control(&mut server).await.unwrap();
        writer.await.unwrap();
        assert_eq!(decoded, HubToWorker::Ping { nonce: 42 });
    }

    #[test]
    fn request_open_validation_rejects_unknown_version_and_blank_resource() {
        let request = RequestOpen {
            protocol_version: PROTOCOL_VERSION + 1,
            request_id: Uuid::nil(),
            resource_id: "primary".to_string(),
        };
        assert_eq!(
            validate_request_open(&request),
            Err(ProtocolError::UnsupportedProtocolVersion(
                PROTOCOL_VERSION + 1
            ))
        );

        let request = RequestOpen {
            protocol_version: PROTOCOL_VERSION,
            request_id: Uuid::nil(),
            resource_id: " ".to_string(),
        };
        assert_eq!(
            validate_request_open(&request),
            Err(ProtocolError::BlankResourceId)
        );
    }

    #[test]
    fn advertisement_validation_enforces_limits() {
        let mut advertisement = WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: Some("node-a".to_string()),
            agent_version: "test".to_string(),
            resources: vec![ResourceAdvertisement {
                resource_id: "primary".to_string(),
                models: vec![ModelAdvertisement {
                    id: "qwen".to_string(),
                    context_limit: Some(32_768),
                }],
                availability: AvailabilitySchedule::default(),
                effective_capacity: 4,
                accepting_requests: true,
                healthy: true,
                revision: 1,
            }],
            model_switching: None,
            request_encodings: Vec::new(),
            capabilities: Vec::new(),
        };
        validate_worker_advertisement(&advertisement).unwrap();
        advertisement.resources[0].models = vec![
            ModelAdvertisement {
                id: "m".to_string(),
                context_limit: None,
            };
            MAX_MODELS_PER_RESOURCE + 1
        ];
        assert!(matches!(
            validate_worker_advertisement(&advertisement),
            Err(ProtocolError::TooManyModels { .. })
        ));
    }

    #[test]
    fn model_switching_validation_bounds_all_operator_visible_fields() {
        let mut switching = ModelSwitchingAdvertisement {
            provider: "lil-fleet".to_string(),
            models: vec![SwitchableModelAdvertisement {
                id: "qwen3-flash".to_string(),
                description: Some("fast lane".to_string()),
                phase: "ready".to_string(),
                desired_state: "loaded".to_string(),
                gpu_count: 1,
                assigned_gpus: vec![0],
                max_instances: 4,
                desired_instances: 2,
                ready_instances: 1,
                instances: vec![SwitchableModelInstanceAdvertisement {
                    instance_id: "qwen3-flash-0".to_string(),
                    index: 0,
                    port: 8114,
                    phase: "ready".to_string(),
                    assigned_gpus: vec![0],
                    container_status: Some("running".to_string()),
                    last_error: None,
                    health: Some("healthy".to_string()),
                }],
            }],
            revision: 1,
            gpu_inventory: None,
        };
        validate_model_switching(&switching).unwrap();
        switching.models[0].description = Some("x".repeat(MAX_SWITCHING_DESCRIPTION_BYTES + 1));
        assert!(matches!(
            validate_model_switching(&switching),
            Err(ProtocolError::SwitchingDescriptionTooLong { .. })
        ));
        switching.models[0].description = None;
        switching.models[0].phase = "ready\nforged".to_string();
        assert!(matches!(
            validate_model_switching(&switching),
            Err(ProtocolError::ControlCharacter { .. })
        ));
        switching.models[0].phase = "ready".to_string();
        switching.models[0].instances[0].phase = "ready\nforged".to_string();
        assert!(matches!(
            validate_model_switching(&switching),
            Err(ProtocolError::ControlCharacter { .. })
        ));
        switching.models[0].instances[0].phase = "ready".to_string();
        switching.models[0].desired_instances = MAX_SWITCH_MODEL_INSTANCES + 1;
        assert!(matches!(
            validate_model_switching(&switching),
            Err(ProtocolError::InstanceCountOutOfRange { .. })
        ));
    }

    #[test]
    fn fleet_instance_diagnostics_do_not_invalidate_worker_inventory() {
        for (input, expected) in [
            (
                "container failed\nGPU unavailable\t\0".to_string(),
                Some("container failed GPU unavailable".to_string()),
            ),
            ("\n\t\0".to_string(), None),
            (
                "界".repeat(MAX_SWITCHING_DESCRIPTION_BYTES),
                Some("界".repeat(MAX_SWITCHING_DESCRIPTION_BYTES / 3)),
            ),
        ] {
            let instance = SwitchableModelInstanceAdvertisement::from_fleet(
                serde_json::from_value(serde_json::json!({
                    "instance_id": "qwen--2",
                    "index": 2,
                    "port": 8115,
                    "phase": "failed",
                    "last_error": input,
                    "container_status": "\n",
                    "health": "unhealthy\n"
                }))
                .unwrap(),
            );
            assert_eq!(instance.last_error, expected);
            assert_eq!(instance.container_status, None);
            assert_eq!(instance.health.as_deref(), Some("unhealthy"));
            let mut model =
                SwitchableModelAdvertisement::legacy("qwen", None, "failed", "ready", 1, vec![]);
            model.instances = vec![instance];
            validate_model_switching(&ModelSwitchingAdvertisement {
                provider: "lil-fleet".to_string(),
                models: vec![model],
                revision: 1,
                gpu_inventory: None,
            })
            .unwrap();
        }
    }

    #[test]
    fn unload_model_uses_a_distinct_stream_variant() {
        let request = SwitchModelRequest {
            protocol_version: REQUEST_PROTOCOL_VERSION,
            request_id: Uuid::nil(),
            model_id: "qwen3-flash".to_string(),
            instances: Some(2),
            gpus: Vec::new(),
        };
        let value = serde_json::to_value(StreamOpen::UnloadModel(request.clone()))
            .expect("serialize unload request");
        assert_eq!(value["type"], "unload_model");
        assert_eq!(value["payload"]["instances"], serde_json::json!(2));
        assert_eq!(
            serde_json::from_value::<StreamOpen>(value).expect("deserialize unload request"),
            StreamOpen::UnloadModel(request)
        );
    }

    #[test]
    fn switch_model_request_is_backward_compatible_and_bounds_instances() {
        let request: SwitchModelRequest = serde_json::from_value(serde_json::json!({
            "protocol_version": REQUEST_PROTOCOL_VERSION,
            "request_id": Uuid::nil(),
            "model_id": "qwen3-flash"
        }))
        .expect("legacy request");
        assert_eq!(request.instances, None);
        validate_switch_model_request(&request).unwrap();

        let mut request = request;
        request.instances = Some(0);
        assert!(matches!(
            validate_switch_model_request(&request),
            Err(ProtocolError::InstanceCountOutOfRange { .. })
        ));
        request.instances = Some(MAX_SWITCH_MODEL_INSTANCES);
        validate_switch_model_request(&request).unwrap();
    }

    #[test]
    fn request_encodings_round_trip_and_stay_backward_compatible() {
        let advertisement = WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: None,
            agent_version: "test".to_string(),
            resources: Vec::new(),
            model_switching: None,
            request_encodings: vec![REQUEST_ENCODING_ZSTD.to_string()],
            capabilities: Vec::new(),
        };
        let hello = WorkerToHub::Hello(advertisement.clone());
        let value = serde_json::to_value(&hello).unwrap();
        assert_eq!(
            value["payload"]["request_encodings"],
            serde_json::json!(["zstd"])
        );
        let decoded: WorkerToHub = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(decoded, hello);
        let WorkerToHub::Hello(decoded) = decoded else {
            unreachable!()
        };
        assert!(decoded.accepts_request_encoding("ZSTD"));

        // An older worker omits the field: a newer hub must parse it and not
        // compress.
        let mut legacy = value["payload"].clone();
        legacy.as_object_mut().unwrap().remove("request_encodings");
        let legacy: WorkerAdvertisement = serde_json::from_value(legacy).unwrap();
        assert!(legacy.request_encodings.is_empty());
        assert!(!legacy.accepts_request_encoding(REQUEST_ENCODING_ZSTD));
        // ...and it serializes without the field so an older peer sees the
        // exact legacy shape.
        assert!(
            serde_json::to_value(&legacy).unwrap()["request_encodings"].is_null(),
            "empty encodings must be omitted on the wire"
        );

        // An older hub's struct (no field) tolerates a newer worker's hello.
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct LegacyAdvertisement {
            protocol_version: u16,
            node_name: Option<String>,
            agent_version: String,
            resources: Vec<ResourceAdvertisement>,
            model_switching: Option<ModelSwitchingAdvertisement>,
        }
        serde_json::from_value::<LegacyAdvertisement>(value["payload"].clone())
            .expect("older hub ignores the new field");
    }

    #[test]
    fn switch_response_error_status_round_trips_and_is_optional() {
        let response = SwitchModelResponse {
            request_id: Uuid::nil(),
            model_id: "qwen3-flash".to_string(),
            accepted: false,
            changed: false,
            error: Some("Fleet rejected the request (overrides_not_allowed)".to_string()),
            model_switching: None,
            error_status: Some(400),
            error_code: None,
            operation: None,
            warnings: Vec::new(),
        };
        let value = serde_json::to_value(&response).unwrap();
        assert_eq!(value["error_status"], serde_json::json!(400));
        assert_eq!(
            serde_json::from_value::<SwitchModelResponse>(value.clone()).unwrap(),
            response
        );
        assert_eq!(response.rejection_status(), http::StatusCode::BAD_REQUEST);

        let mut legacy = value;
        legacy.as_object_mut().unwrap().remove("error_status");
        let legacy: SwitchModelResponse = serde_json::from_value(legacy).unwrap();
        assert_eq!(legacy.error_status, None);
        assert_eq!(legacy.rejection_status(), http::StatusCode::CONFLICT);
        assert!(
            serde_json::to_value(&legacy).unwrap()["error_status"].is_null(),
            "absent status stays off the wire"
        );
        for (status, expected) in [
            (404, http::StatusCode::NOT_FOUND),
            (502, http::StatusCode::BAD_GATEWAY),
            (500, http::StatusCode::CONFLICT),
            (200, http::StatusCode::CONFLICT),
        ] {
            let response = SwitchModelResponse {
                error_status: Some(status),
                ..legacy.clone()
            };
            assert_eq!(response.rejection_status(), expected, "{status}");
        }
    }

    #[test]
    fn request_encodings_are_bounded() {
        let mut advertisement = WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: None,
            agent_version: "test".to_string(),
            resources: Vec::new(),
            model_switching: None,
            request_encodings: vec!["zstd".to_string(); MAX_REQUEST_ENCODINGS + 1],
            capabilities: Vec::new(),
        };
        assert!(matches!(
            validate_worker_advertisement(&advertisement),
            Err(ProtocolError::TooManyRequestEncodings { .. })
        ));
        advertisement.request_encodings = vec!["zstd\nforged".to_string()];
        assert!(matches!(
            validate_worker_advertisement(&advertisement),
            Err(ProtocolError::ControlCharacter { .. })
        ));
        advertisement.request_encodings = vec!["zstd".to_string()];
        validate_worker_advertisement(&advertisement).unwrap();
    }

    #[test]
    fn context_limits_outside_the_sane_range_are_dropped() {
        assert_eq!(sanitize_context_limit(Some(32_768)), Some(32_768));
        assert_eq!(sanitize_context_limit(Some(1)), Some(1));
        assert_eq!(
            sanitize_context_limit(Some(MAX_CONTEXT_LIMIT)),
            Some(MAX_CONTEXT_LIMIT)
        );
        for invalid in [0, -1, i64::MIN, MAX_CONTEXT_LIMIT + 1, i64::MAX] {
            assert_eq!(sanitize_context_limit(Some(invalid)), None, "{invalid}");
        }
        assert_eq!(sanitize_context_limit(None), None);
    }

    #[test]
    fn advertisement_validation_rejects_log_injection_and_oversized_model_ids() {
        let mut advertisement = WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: Some("node-a\nforged-log".to_string()),
            agent_version: "test".to_string(),
            resources: Vec::new(),
            model_switching: None,
            request_encodings: Vec::new(),
            capabilities: Vec::new(),
        };
        assert!(matches!(
            validate_worker_advertisement(&advertisement),
            Err(ProtocolError::ControlCharacter { field: "node name" })
        ));

        advertisement.node_name = None;
        advertisement.resources.push(ResourceAdvertisement {
            resource_id: "primary".to_string(),
            models: vec![ModelAdvertisement {
                id: "m".repeat(MAX_MODEL_ID_BYTES + 1),
                context_limit: None,
            }],
            availability: AvailabilitySchedule::default(),
            effective_capacity: 1,
            accepting_requests: true,
            healthy: true,
            revision: 1,
        });
        assert!(matches!(
            validate_worker_advertisement(&advertisement),
            Err(ProtocolError::ModelIdTooLong { .. })
        ));
    }

    #[test]
    fn switch_response_operation_is_optional_in_both_directions() {
        // An old worker's response (no `operation`) still parses.
        let legacy: SwitchModelResponse = serde_json::from_value(serde_json::json!({
            "request_id": Uuid::nil(),
            "model_id": "qwen",
            "accepted": true,
            "changed": true,
            "error": null,
            "model_switching": null
        }))
        .expect("legacy response");
        assert_eq!(legacy.operation, None);
        // A new response omits the field when there is no operation, so an
        // old hub sees exactly the legacy shape.
        let encoded = serde_json::to_value(&legacy).unwrap();
        assert!(encoded.get("operation").is_none());

        let mut current = legacy;
        current.operation = Some(LifecycleOperation {
            id: "0123abcd".into(),
            state: "pending".into(),
            error: None,
            kind: Some("activate".into()),
            model_id: Some("qwen".into()),
            instances: Some(2),
            created_at: Some("2026-10-01T00:00:00Z".into()),
            started_at: None,
            finished_at: None,
        });
        let encoded = serde_json::to_value(&current).unwrap();
        assert_eq!(encoded["operation"]["id"], "0123abcd");
        assert_eq!(
            serde_json::from_value::<SwitchModelResponse>(encoded).unwrap(),
            current
        );
    }

    #[test]
    fn worker_capabilities_are_additive_and_bounded() {
        let legacy: WorkerAdvertisement = serde_json::from_value(serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "node_name": null,
            "agent_version": "old",
            "resources": [],
            "model_switching": null
        }))
        .expect("legacy advertisement");
        assert!(legacy.capabilities.is_empty());
        assert!(!legacy.has_capability(CAPABILITY_FLEET_OPERATIONS));
        assert!(
            serde_json::to_value(&legacy)
                .unwrap()
                .get("capabilities")
                .is_none()
        );
        let mut advertisement = legacy;
        advertisement.capabilities = vec![CAPABILITY_FLEET_OPERATIONS.to_string()];
        validate_worker_advertisement(&advertisement).unwrap();
        assert!(advertisement.has_capability(CAPABILITY_FLEET_OPERATIONS));
        advertisement.capabilities = vec!["x".to_string(); MAX_CAPABILITIES + 1];
        assert!(matches!(
            validate_worker_advertisement(&advertisement),
            Err(ProtocolError::TooManyCapabilities { .. })
        ));
        advertisement.capabilities = vec!["bad\ncap".to_string()];
        assert!(validate_worker_advertisement(&advertisement).is_err());
    }

    #[test]
    fn fleet_operation_stream_round_trips_and_validates_ids() {
        let request = FleetOperationRequest {
            protocol_version: REQUEST_PROTOCOL_VERSION,
            request_id: Uuid::nil(),
            operation_id: "0123abcd".into(),
        };
        let value = serde_json::to_value(StreamOpen::FleetOperation(request.clone())).unwrap();
        assert_eq!(value["type"], "fleet_operation");
        assert_eq!(
            serde_json::from_value::<StreamOpen>(value).unwrap(),
            StreamOpen::FleetOperation(request.clone())
        );
        validate_fleet_operation_request(&request).unwrap();
        for bad in ["", "a/b", "a b", &"x".repeat(MAX_OPERATION_ID_BYTES + 1)] {
            assert!(validate_operation_id(bad).is_err(), "{bad:?}");
        }
        validate_operation_id("op_1.2:3-4").unwrap();
    }

    #[test]
    fn lifecycle_operation_sanitization_bounds_worker_text() {
        let operation = LifecycleOperation {
            id: "0123abcd".into(),
            state: " running ".into(),
            error: Some(format!("line1\nline2{}", "e".repeat(4096))),
            kind: Some("unload".into()),
            model_id: Some("bad\nmodel".into()),
            instances: None,
            created_at: None,
            started_at: None,
            finished_at: None,
        }
        .sanitized()
        .expect("valid operation");
        assert_eq!(operation.state, "running");
        let error = operation.error.unwrap();
        assert!(error.starts_with("line1 line2"));
        assert!(error.len() <= MAX_SWITCHING_DESCRIPTION_BYTES);
        assert_eq!(operation.model_id, None);

        let invalid = LifecycleOperation {
            id: "bad id".into(),
            state: "running".into(),
            error: None,
            kind: None,
            model_id: None,
            instances: None,
            created_at: None,
            started_at: None,
            finished_at: None,
        };
        assert_eq!(invalid.sanitized(), None);
    }

    fn sample_gpu_inventory() -> GpuInventoryAdvertisement {
        GpuInventoryAdvertisement {
            sampled_at: "2026-10-10T00:00:00Z".into(),
            age_ms: 1500,
            stale: false,
            groups: vec![vec![0, 1, 6, 7], vec![2, 3, 4, 5]],
            gpus: vec![
                GpuAdvertisement {
                    index: 0,
                    name: "NVIDIA RTX PRO 6000".into(),
                    memory_total_mib: 97887,
                    memory_used_mib: 1234,
                    memory_free_mib: 96000,
                    utilization_percent: Some(0),
                    group: Some(0),
                    assigned: vec!["ds41-flash-tp4-ssd".into()],
                    hidden_assignments: 0,
                },
                GpuAdvertisement {
                    index: 2,
                    name: "NVIDIA RTX PRO 6000".into(),
                    memory_total_mib: 97887,
                    memory_used_mib: 0,
                    memory_free_mib: 97887,
                    utilization_percent: None,
                    group: None,
                    assigned: Vec::new(),
                    hidden_assignments: 0,
                },
            ],
        }
    }

    #[test]
    fn gpu_inventory_round_trips_and_is_optional_both_ways() {
        let switching = ModelSwitchingAdvertisement {
            provider: "lil-fleet".into(),
            models: Vec::new(),
            revision: 3,
            gpu_inventory: Some(sample_gpu_inventory()),
        };
        validate_model_switching(&switching).unwrap();
        let update = WorkerToHub::ModelSwitchingUpdate(switching.clone());
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(
            value["payload"]["gpu_inventory"]["gpus"][0]["assigned"],
            serde_json::json!(["ds41-flash-tp4-ssd"])
        );
        // Unknown utilization and group stay off the wire instead of 0.
        let second = &value["payload"]["gpu_inventory"]["gpus"][1];
        assert!(second.get("utilization_percent").is_none());
        assert!(second.get("group").is_none());
        assert!(second.get("hidden_assignments").is_none());
        assert_eq!(
            serde_json::from_value::<WorkerToHub>(value.clone()).unwrap(),
            update
        );

        // An older worker omits the inventory: a newer hub reads "none".
        let mut legacy = value["payload"].clone();
        legacy.as_object_mut().unwrap().remove("gpu_inventory");
        let legacy: ModelSwitchingAdvertisement = serde_json::from_value(legacy).unwrap();
        assert_eq!(legacy.gpu_inventory, None);
        assert!(
            serde_json::to_value(&legacy)
                .unwrap()
                .get("gpu_inventory")
                .is_none()
        );

        // An older hub's struct ignores the new field.
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct LegacySwitching {
            provider: String,
            models: Vec<SwitchableModelAdvertisement>,
            revision: u64,
        }
        serde_json::from_value::<LegacySwitching>(value["payload"].clone())
            .expect("older hub ignores the inventory");

        // Minimal sample: every optional per-GPU field may be absent.
        let minimal: GpuInventoryAdvertisement = serde_json::from_value(serde_json::json!({
            "sampled_at": "2026-10-10T00:00:00Z",
            "gpus": [{"index": 1}]
        }))
        .unwrap();
        assert_eq!(minimal.age_ms, 0);
        assert!(minimal.groups.is_empty());
        validate_gpu_inventory(&minimal).unwrap();
    }

    #[test]
    fn gpu_inventory_validation_enforces_bounds() {
        let check = |mutate: &dyn Fn(&mut GpuInventoryAdvertisement)| {
            let mut inventory = sample_gpu_inventory();
            mutate(&mut inventory);
            validate_gpu_inventory(&inventory)
        };
        assert!(check(&|_| {}).is_ok());
        assert!(
            check(&|inv| inv.gpus = (0..65)
                .map(|index| {
                    let mut gpu = inv.gpus[1].clone();
                    gpu.index = index;
                    gpu
                })
                .collect())
            .is_err()
        );
        assert!(check(&|inv| inv.gpus[0].name = "x".repeat(MAX_GPU_NAME_BYTES + 1)).is_err());
        assert!(check(&|inv| inv.gpus[0].name = "GPU\nforged".into()).is_err());
        assert!(
            check(&|inv| inv.gpus[0].assigned = vec!["m".into(); MAX_GPU_ASSIGNMENTS + 1]).is_err()
        );
        assert!(check(&|inv| inv.gpus[0].assigned = vec!["bad\nmodel".into()]).is_err());
        assert!(check(&|inv| inv.gpus[1].index = 0).is_err());
        assert!(check(&|inv| inv.gpus[0].index = MAX_GPU_INDEX + 1).is_err());
        assert!(check(&|inv| inv.gpus[0].utilization_percent = Some(101)).is_err());
        assert!(check(&|inv| inv.groups = vec![vec![0]; MAX_GPU_GROUPS + 1]).is_err());
        assert!(check(&|inv| inv.groups = vec![vec![MAX_GPU_INDEX + 1]]).is_err());
        assert!(check(&|inv| inv.sampled_at = " ".into()).is_err());
        assert!(check(&|inv| inv.sampled_at = "x".repeat(MAX_GPU_SAMPLED_AT_BYTES + 1)).is_err());
        // A bad inventory invalidates the switching update it rides on.
        let switching = ModelSwitchingAdvertisement {
            provider: "lil-fleet".into(),
            models: Vec::new(),
            revision: 1,
            gpu_inventory: Some({
                let mut inventory = sample_gpu_inventory();
                inventory.gpus[0].name = "GPU\nforged".into();
                inventory
            }),
        };
        assert!(validate_model_switching(&switching).is_err());
    }

    #[test]
    fn hidden_gpu_occupants_stay_counted_but_unnamed() {
        let mut inventory = sample_gpu_inventory();
        inventory.gpus[1].assigned = vec!["secret-model".into(), "visible".into()];
        inventory.hide_assignments_except(|model| model == "visible");
        assert!(inventory.gpus[0].assigned.is_empty());
        assert_eq!(inventory.gpus[0].hidden_assignments, 1);
        assert_eq!(inventory.gpus[1].assigned, vec!["visible".to_string()]);
        assert_eq!(inventory.gpus[1].hidden_assignments, 1);
        let encoded = serde_json::to_value(&inventory).unwrap();
        assert_eq!(encoded["gpus"][0]["hidden_assignments"], 1);
        assert!(!encoded.to_string().contains("secret-model"));
    }

    #[test]
    fn worker_side_inventory_is_bounded_and_deduplicated() {
        let fleet: crate::dashboard_fleet::FleetGpuInventory =
            serde_json::from_value(serde_json::json!({
                "sampled_at": "2026-10-10T00:00:00Z",
                "gpus": [{"index": 0, "assigned": [
                    {"model_id": "tp4", "instance_id": "tp4--0"},
                    {"model_id": "tp4", "instance_id": "tp4--1"}
                ]}]
            }))
            .unwrap();
        let wire = GpuInventoryAdvertisement::from_fleet(fleet, 42).expect("bounded");
        assert_eq!(wire.gpus[0].assigned, vec!["tp4".to_string()]);
        assert_eq!(wire.age_ms, 42);
        // Anything that would not fit the frame budget is dropped whole.
        let huge: crate::dashboard_fleet::FleetGpuInventory =
            serde_json::from_value(serde_json::json!({
                "sampled_at": "2026-10-10T00:00:00Z",
                "gpus": (0..64).map(|index| serde_json::json!({
                    "index": index,
                    "name": "n".repeat(MAX_GPU_NAME_BYTES),
                    "assigned": (0..16).map(|n| serde_json::json!({"model_id": format!("{}-{n}", "m".repeat(60))})).collect::<Vec<_>>()
                })).collect::<Vec<_>>()
            }))
            .unwrap();
        assert_eq!(GpuInventoryAdvertisement::from_fleet(huge, 0), None);
    }

    #[test]
    fn switch_request_gpus_and_response_warnings_round_trip_and_stay_optional() {
        let request = SwitchModelRequest {
            protocol_version: REQUEST_PROTOCOL_VERSION,
            request_id: Uuid::nil(),
            model_id: "big".into(),
            instances: Some(1),
            gpus: vec![3, 1],
        };
        let value = serde_json::to_value(StreamOpen::SwitchModel(request.clone())).unwrap();
        assert_eq!(value["payload"]["gpus"], serde_json::json!([3, 1]));
        assert_eq!(
            serde_json::from_value::<StreamOpen>(value.clone()).unwrap(),
            StreamOpen::SwitchModel(request.clone())
        );
        validate_switch_model_request(&request).unwrap();
        // Automatic placement keeps the exact legacy request shape.
        let auto = SwitchModelRequest {
            gpus: Vec::new(),
            ..request.clone()
        };
        assert!(serde_json::to_value(&auto).unwrap().get("gpus").is_none());
        // An older worker's struct ignores `gpus` (hence the capability gate).
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct LegacyRequest {
            protocol_version: u16,
            request_id: Uuid,
            model_id: String,
            instances: Option<u32>,
        }
        serde_json::from_value::<LegacyRequest>(value["payload"].clone()).unwrap();
        for bad in [vec![1, 1], vec![MAX_GPU_INDEX + 1], (0..65).collect()] {
            let request = SwitchModelRequest {
                gpus: bad,
                ..request.clone()
            };
            assert!(matches!(
                validate_switch_model_request(&request),
                Err(ProtocolError::InvalidGpuSelection(_))
            ));
        }

        let legacy: SwitchModelResponse = serde_json::from_value(serde_json::json!({
            "request_id": Uuid::nil(),
            "model_id": "big",
            "accepted": true,
            "changed": true,
            "error": null,
            "model_switching": null
        }))
        .unwrap();
        assert!(legacy.warnings.is_empty());
        assert!(
            serde_json::to_value(&legacy)
                .unwrap()
                .get("warnings")
                .is_none()
        );
        let current = SwitchModelResponse {
            warnings: vec!["spans PCIe groups".into()],
            ..legacy
        };
        let encoded = serde_json::to_value(&current).unwrap();
        assert_eq!(
            encoded["warnings"],
            serde_json::json!(["spans PCIe groups"])
        );
        assert_eq!(
            serde_json::from_value::<SwitchModelResponse>(encoded).unwrap(),
            current
        );
    }

    #[test]
    fn warnings_are_capped_and_single_line() {
        let warnings = sanitize_warnings(
            std::iter::once("line1\nline2".to_string())
                .chain(std::iter::once(" ".to_string()))
                .chain(std::iter::once("w".repeat(MAX_LOAD_WARNING_BYTES * 2)))
                .chain((0..20).map(|n| format!("w{n}")))
                .collect(),
        );
        assert_eq!(warnings.len(), MAX_LOAD_WARNINGS);
        assert_eq!(warnings[0], "line1 line2");
        assert_eq!(warnings[1].len(), MAX_LOAD_WARNING_BYTES);
    }
}
