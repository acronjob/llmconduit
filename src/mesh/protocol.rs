use crate::config::AvailabilitySchedule;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

pub const ENROLL_ALPN: &[u8] = b"llmconduit-mesh-enroll/1";
pub const WORKER_ALPN: &[u8] = b"llmconduit-mesh-worker/1";

pub const PROTOCOL_VERSION: u16 = 1;
pub const CONTROL_PROTOCOL_VERSION: u16 = 1;
pub const REQUEST_PROTOCOL_VERSION: u16 = 1;
pub const MAX_CONTROL_FRAME_BYTES: usize = 64 * 1024;
pub const MAX_REQUEST_PREFACE_BYTES: usize = 4 * 1024;
pub const MAX_NODE_NAME_BYTES: usize = 128;
pub const MAX_AGENT_VERSION_BYTES: usize = 128;
pub const MAX_RESOURCE_ID_BYTES: usize = 128;
pub const MAX_MODEL_ID_BYTES: usize = 512;
pub const MAX_RESOURCES_PER_NODE: usize = 32;
pub const MAX_MODELS_PER_RESOURCE: usize = 1024;
pub const MAX_CAPACITY_PER_RESOURCE: u32 = 65_535;

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

fn validate_model_id(model_id: &str) -> Result<(), ProtocolError> {
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
    #[error("model id must not be blank")]
    BlankModelId,
    #[error("model id is {len} bytes, maximum is {max}")]
    ModelIdTooLong { len: usize, max: usize },
    #[error("{field} must not contain control characters")]
    ControlCharacter { field: &'static str },
    #[error("join key must not be blank")]
    BlankJoinKey,
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
    fn advertisement_validation_rejects_log_injection_and_oversized_model_ids() {
        let mut advertisement = WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: Some("node-a\nforged-log".to_string()),
            agent_version: "test".to_string(),
            resources: Vec::new(),
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
}
