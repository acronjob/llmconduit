//! Local Fleet GPU/model lifecycle controls exposed through the authenticated dashboard.
//!
//! The first integration is deliberately loopback-only. Fleet credentials are read from
//! the environment or an env-selected file and never cross the dashboard API boundary.

use crate::dashboard_access::{ManagementActor, ManagementPermission};
use crate::dashboard_api::json_no_store;
use crate::engine::Gateway;
use axum::Extension;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use futures::StreamExt;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use utoipa::ToSchema;

const FLEET_URL_ENV: &str = "LLMCONDUIT_FLEET_URL";
const FLEET_TOKEN_ENV: &str = "LLMCONDUIT_FLEET_TOKEN";
const FLEET_TOKEN_FILE_ENV: &str = "LLMCONDUIT_FLEET_TOKEN_FILE";
const FLEET_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_TOKEN_BYTES: u64 = 16 * 1024;
const MAX_RESPONSE_BODY_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_FLEET_MODEL_INSTANCES: u32 = 64;

#[derive(Clone)]
enum FleetTokenSource {
    Env(String),
    File(PathBuf),
}

#[derive(Clone)]
pub struct FleetClient {
    client: Client,
    base_url: Url,
    token: FleetTokenSource,
}

impl std::fmt::Debug for FleetClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FleetClient")
            .field("base_url", &self.base_url)
            .field("token", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct FleetModelsResponse {
    pub models: Vec<FleetModelEntry>,
    /// Per-GPU VRAM from Fleet `GET /v1/gpus`, fetched alongside the models.
    /// Absent when the Fleet predates the route or could not sample its GPUs:
    /// GPU info is then unavailable, never zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_inventory: Option<FleetGpuInventory>,
}

/// Fleet `GET /v1/gpus`, sanitized, and the dashboard's per-node GPU view.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct FleetGpuInventory {
    /// Fleet's sample time (RFC3339).
    pub sampled_at: String,
    /// Fleet flagged the sample as stale.
    #[serde(default)]
    pub stale: bool,
    /// Age of the sample in milliseconds when the dashboard answered.
    /// Absent when it cannot be computed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age_ms: Option<u64>,
    /// PCIe groups as lists of GPU indices.
    #[serde(default)]
    pub groups: Vec<Vec<u32>>,
    #[serde(default)]
    pub gpus: Vec<FleetGpu>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct FleetGpu {
    pub index: u32,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub memory_total_mib: u64,
    #[serde(default)]
    pub memory_used_mib: u64,
    #[serde(default)]
    pub memory_free_mib: u64,
    /// Absent when Fleet could not read it (never reported as 0).
    #[serde(
        default,
        deserialize_with = "deserialize_percent",
        skip_serializing_if = "Option::is_none"
    )]
    pub utilization_percent: Option<u32>,
    /// Index into `groups`; absent when the GPU is in no known PCIe group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<u32>,
    #[serde(default)]
    pub assigned: Vec<FleetGpuAssignment>,
    /// Mesh only: occupants hidden by the hub's model allowlist.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub hidden_assignments: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct FleetGpuAssignment {
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

/// Fleet reports utilization as an integer today; accept any JSON number (or
/// null) and clamp so a float or out-of-range value cannot fail the sample.
fn deserialize_percent<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Number>::deserialize(deserializer)?;
    Ok(value
        .and_then(|number| number.as_f64())
        .filter(|value| value.is_finite())
        .map(|value| value.round().clamp(0.0, 100.0) as u32))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct FleetModelEntry {
    pub model: FleetModel,
    pub status: FleetDeploymentStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct FleetModel {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_count: Option<u32>,
    #[serde(default = "default_max_instances")]
    pub max_instances: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct FleetDeploymentStatus {
    pub model_id: String,
    pub phase: String,
    pub desired_state: String,
    #[serde(default)]
    pub desired_instances: u32,
    #[serde(default)]
    pub ready_instances: u32,
    #[serde(default)]
    pub instances: Vec<FleetInstanceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oom_killed: Option<bool>,
    #[serde(default)]
    pub assigned_gpus: Vec<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_checked: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct FleetInstanceStatus {
    pub instance_id: String,
    pub index: u32,
    pub port: u16,
    pub phase: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_status: Option<String>,
    #[serde(default)]
    pub assigned_gpus: Vec<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_checked: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct FleetOperationResponse {
    pub changed: bool,
    /// Fleet omits this for an already-satisfied no-op.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<FleetOperation>,
    /// Fleet's advisory warnings (e.g. a GPU pick spanning PCIe groups),
    /// capped and sanitized.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct FleetOperation {
    pub id: String,
    pub kind: String,
    pub model_id: String,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instances: Option<u32>,
}

/// Dashboard load body: at least one of `instances` and `gpus`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FleetLoadModelRequest {
    /// Desired replica count, 1..=64.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instances: Option<u32>,
    /// Manual GPU pick for the newly started instances, in assignment order
    /// (`gpu_count` x new instances). Empty or omitted: Fleet places
    /// automatically on the GPUs with the most free VRAM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpus: Option<Vec<u32>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct FleetLoadModelPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    instances: Option<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    gpus: Vec<u32>,
}

/// Parsed dashboard load options.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FleetLoadOptions {
    pub(crate) instances: Option<u32>,
    pub(crate) gpus: Vec<u32>,
}

impl FleetClient {
    pub fn from_env(client: Client) -> Result<Option<Self>, String> {
        let Some(url) = read_nonempty_env(FLEET_URL_ENV) else {
            if read_nonempty_env(FLEET_TOKEN_ENV).is_some()
                || read_nonempty_env(FLEET_TOKEN_FILE_ENV).is_some()
            {
                return Err(format!(
                    "{FLEET_URL_ENV} is required when a Fleet token is set"
                ));
            }
            return Ok(None);
        };
        let base_url = parse_loopback_url(&url)?;
        let token = match (
            read_nonempty_env(FLEET_TOKEN_ENV),
            read_nonempty_env(FLEET_TOKEN_FILE_ENV),
        ) {
            (Some(token), _) if token.len() as u64 <= MAX_TOKEN_BYTES => {
                FleetTokenSource::Env(token)
            }
            (Some(_), _) => return Err(format!("{FLEET_TOKEN_ENV} is too large")),
            (None, Some(path)) => FleetTokenSource::File(PathBuf::from(path)),
            (None, None) => {
                return Err(format!(
                    "{FLEET_URL_ENV} is set but no Fleet token source is set"
                ));
            }
        };
        Ok(Some(Self {
            client,
            base_url,
            token,
        }))
    }

    pub async fn list_models(&self) -> Result<FleetModelsResponse, FleetProxyError> {
        self.request(reqwest::Method::GET, "/v1/models").await
    }

    /// `GET /v1/gpus`: per-GPU VRAM and placement. `Ok(None)` when this
    /// Fleet predates the route (404), so an older Fleet never breaks the
    /// model inventory. The answer is bounded and sanitized.
    pub async fn list_gpus(&self) -> Result<Option<FleetGpuInventory>, FleetProxyError> {
        match self
            .request::<FleetGpuInventory>(reqwest::Method::GET, "/v1/gpus")
            .await
        {
            Ok(inventory) => Ok(Some(sanitize_gpu_inventory(inventory))),
            Err(error) if error.status == StatusCode::NOT_FOUND => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn load_model(
        &self,
        model_id: &str,
    ) -> Result<FleetOperationResponse, FleetProxyError> {
        self.load_model_instances(model_id, None, None).await
    }

    pub async fn load_model_instances(
        &self,
        model_id: &str,
        instances: Option<u32>,
        gpus: Option<Vec<u32>>,
    ) -> Result<FleetOperationResponse, FleetProxyError> {
        validate_model_id(model_id)?;
        validate_optional_instances(instances)?;
        let gpus = gpus.unwrap_or_default();
        validate_gpus(&gpus)?;
        // An explicit count, including 1, must reach Fleet: an empty load body
        // means "keep the current replica count", so dropping `{"instances":1}`
        // would make scaling a model back down to one replica impossible.
        // An empty GPU list means automatic placement and stays off the wire.
        let body = (instances.is_some() || !gpus.is_empty())
            .then_some(FleetLoadModelPayload { instances, gpus });
        let mut response: FleetOperationResponse = self
            .request_json(
                reqwest::Method::POST,
                &format!("/v1/models/{model_id}/load"),
                body.as_ref(),
            )
            .await?;
        response.warnings =
            crate::mesh::protocol::sanitize_warnings(std::mem::take(&mut response.warnings));
        Ok(response)
    }

    pub async fn unload_model(
        &self,
        model_id: &str,
    ) -> Result<FleetOperationResponse, FleetProxyError> {
        validate_model_id(model_id)?;
        self.request(
            reqwest::Method::POST,
            &format!("/v1/models/{model_id}/unload"),
        )
        .await
    }

    /// `GET /v1/operations/{id}`: the state of one lifecycle operation.
    pub async fn operation(&self, operation_id: &str) -> Result<FleetOperation, FleetProxyError> {
        crate::mesh::protocol::validate_operation_id(operation_id)
            .map_err(|_| FleetProxyError::new(StatusCode::BAD_REQUEST, "invalid operation id"))?;
        self.request(
            reqwest::Method::GET,
            &format!("/v1/operations/{operation_id}"),
        )
        .await
    }

    /// Test seam: a client for a loopback Fleet mock with an inline token.
    #[cfg(test)]
    pub(crate) fn for_test(client: Client, base_url: &str, token: &str) -> Self {
        Self {
            client,
            base_url: parse_loopback_url(base_url).expect("loopback Fleet URL"),
            token: FleetTokenSource::Env(token.to_string()),
        }
    }

    async fn request<T>(&self, method: reqwest::Method, path: &str) -> Result<T, FleetProxyError>
    where
        T: DeserializeOwned,
    {
        self.request_json::<T, serde_json::Value>(method, path, None)
            .await
    }

    async fn request_json<T, B>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<T, FleetProxyError>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let token = self.token().await?;
        let mut url = self.base_url.clone();
        url.set_path(path);
        let mut request = self
            .client
            .request(method, url)
            .bearer_auth(token)
            .timeout(FLEET_REQUEST_TIMEOUT);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.map_err(|error| {
            if error.is_timeout() {
                FleetProxyError::new(StatusCode::GATEWAY_TIMEOUT, "Fleet request timed out")
            } else {
                FleetProxyError::new(StatusCode::BAD_GATEWAY, "Fleet request failed")
            }
        })?;
        let upstream_status = response.status();
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BODY_BYTES as u64)
        {
            return Err(FleetProxyError::new(
                StatusCode::BAD_GATEWAY,
                "Fleet returned an oversized response",
            ));
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| {
                FleetProxyError::new(StatusCode::BAD_GATEWAY, "Fleet response failed")
            })?;
            if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BODY_BYTES {
                return Err(FleetProxyError::new(
                    StatusCode::BAD_GATEWAY,
                    "Fleet returned an oversized response",
                ));
            }
            body.extend_from_slice(&chunk);
        }
        if !upstream_status.is_success() {
            return Err(fleet_status_error(upstream_status.as_u16(), &body));
        }
        serde_json::from_slice(&body).map_err(|_| {
            FleetProxyError::new(StatusCode::BAD_GATEWAY, "Fleet returned invalid JSON")
        })
    }

    async fn token(&self) -> Result<String, FleetProxyError> {
        match &self.token {
            FleetTokenSource::Env(token) => Ok(token.clone()),
            FleetTokenSource::File(path) => {
                let metadata = tokio::fs::metadata(path)
                    .await
                    .map_err(|_| token_unavailable())?;
                if !metadata.is_file() || metadata.len() > MAX_TOKEN_BYTES {
                    return Err(token_unavailable());
                }
                let token = tokio::fs::read_to_string(path)
                    .await
                    .map_err(|_| token_unavailable())?;
                let token = token.trim();
                if token.is_empty() || token.len() as u64 > MAX_TOKEN_BYTES {
                    return Err(token_unavailable());
                }
                Ok(token.to_owned())
            }
        }
    }
}

#[derive(Debug)]
pub struct FleetProxyError {
    status: StatusCode,
    message: &'static str,
    /// Fleet's own machine-readable error code, sanitized, when it sent one.
    code: Option<String>,
}

impl FleetProxyError {
    fn new(status: StatusCode, message: &'static str) -> Self {
        Self {
            status,
            message,
            code: None,
        }
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Fleet's sanitized machine-readable error code, when it sent one.
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    fn into_response(self) -> Response {
        let mut body = serde_json::json!({ "error": self.to_string() });
        if let Some(code) = &self.code {
            body["code"] = serde_json::Value::String(code.clone());
        }
        json_no_store(self.status, &body)
    }
}

impl std::fmt::Display for FleetProxyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)?;
        if let Some(code) = &self.code {
            write!(formatter, " ({code})")?;
        }
        Ok(())
    }
}

/// Maps a non-2xx Fleet response. Fleet's 4xx answers describe the request
/// (unknown model, busy GPUs, rejected body), so they stay 4xx for the
/// operator instead of collapsing into a generic gateway error; only Fleet
/// failures and credential problems on our side are 502s.
fn fleet_status_error(status: u16, body: &[u8]) -> FleetProxyError {
    let (status, message) = match status {
        400 | 422 => (StatusCode::BAD_REQUEST, "Fleet rejected the request"),
        404 => (StatusCode::NOT_FOUND, "Fleet model was not found"),
        409 | 423 => (
            StatusCode::CONFLICT,
            "Fleet is busy or has insufficient GPU capacity",
        ),
        429 => (
            StatusCode::TOO_MANY_REQUESTS,
            "Fleet is rate limiting requests",
        ),
        401 | 403 => (
            StatusCode::BAD_GATEWAY,
            "Fleet rejected the configured credentials",
        ),
        400..=499 => (StatusCode::BAD_REQUEST, "Fleet rejected the request"),
        _ => (StatusCode::BAD_GATEWAY, "Fleet failed the request"),
    };
    let code = fleet_error_code(body);
    // Fleet's code distinguishes a transient lifecycle race from a capacity
    // shortfall; operators act differently on each.
    let message = match (status, code.as_deref()) {
        (StatusCode::CONFLICT, Some("operation_in_progress")) => {
            "Fleet is already changing this model"
        }
        (StatusCode::CONFLICT, Some("insufficient_resources")) => {
            "Fleet has insufficient GPU capacity"
        }
        (StatusCode::CONFLICT, Some("gpus_unavailable")) => "Requested GPUs are not available",
        (StatusCode::BAD_REQUEST, Some("gpus_not_supported")) => {
            "Fleet does not support GPU selection for this model"
        }
        (StatusCode::BAD_REQUEST, Some("gpus_not_applicable")) => {
            "GPU selection does not apply to this load"
        }
        (StatusCode::BAD_REQUEST, Some("invalid_gpus")) => "Fleet rejected the GPU selection",
        _ => message,
    };
    FleetProxyError {
        status,
        message,
        code,
    }
}

/// Extracts Fleet's error code (`{"error":"code"}`, `{"error":{"code":..}}`
/// or `{"code":..}`) and keeps it only if it is a short identifier, so free
/// text from Fleet (paths, docker output) never reaches the client.
fn fleet_error_code(body: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let code = value
        .get("error")
        .and_then(|error| error.as_str().or_else(|| error.get("code")?.as_str()))
        .or_else(|| value.get("code")?.as_str())?;
    let valid = !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'));
    valid.then(|| code.to_ascii_lowercase())
}

impl std::error::Error for FleetProxyError {}

#[utoipa::path(
    get,
    path = "/dashboard/api/fleet",
    tag = "dashboard",
    responses(
        (status = 200, body = FleetModelsResponse),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError),
        (status = 502, body = crate::openapi::DashboardError),
        (status = 504, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn fleet_models(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
) -> Response {
    if !actor.allows(ManagementPermission::FleetModelsRead) {
        return fleet_error(StatusCode::FORBIDDEN, "management permission denied");
    }
    let Some(fleet) = gateway.fleet() else {
        return fleet_error(StatusCode::NOT_FOUND, "Fleet is not configured");
    };
    let (models, gpus) = tokio::join!(fleet.list_models(), fleet.list_gpus());
    match models {
        Ok(mut models) => {
            models.gpu_inventory = match gpus {
                Ok(inventory) => inventory.map(|mut inventory| {
                    inventory.age_ms = sample_age_ms(&inventory.sampled_at);
                    inventory
                }),
                Err(error) => {
                    // nvidia-smi failures (503 gpu_query_failed) degrade to
                    // "GPU info unavailable"; the model inventory still works.
                    tracing::debug!(error = %error, "Fleet GPU inventory unavailable");
                    None
                }
            };
            json_no_store(StatusCode::OK, &models)
        }
        Err(error) => error.into_response(),
    }
}

#[utoipa::path(
    post,
    path = "/dashboard/api/fleet/models/{id}/load",
    tag = "dashboard",
    params(("id" = String, Path, description = "Fleet model id.")),
    request_body(content = Option<FleetLoadModelRequest>),
    responses(
        (status = 200, body = FleetOperationResponse),
        (status = 202, body = FleetOperationResponse),
        (status = 400, body = crate::openapi::DashboardError, description = "Invalid body, or Fleet refused the GPU pick (`invalid_gpus`, `gpus_not_supported`, `gpus_not_applicable`)."),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError),
        (status = 409, body = crate::openapi::DashboardError, description = "Fleet is busy, short on GPUs, or `gpus_unavailable`."),
        (status = 429, body = crate::openapi::DashboardError),
        (status = 502, body = crate::openapi::DashboardError),
        (status = 504, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn fleet_load_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let options = match parse_fleet_load_body(&body) {
        Ok(options) => options,
        Err(error) => return error.into_response(),
    };
    fleet_model_action(gateway, actor, id, Some(options)).await
}

#[utoipa::path(
    post,
    path = "/dashboard/api/fleet/models/{id}/unload",
    tag = "dashboard",
    params(("id" = String, Path, description = "Fleet model id.")),
    responses(
        (status = 200, body = FleetOperationResponse),
        (status = 202, body = FleetOperationResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError),
        (status = 409, body = crate::openapi::DashboardError),
        (status = 429, body = crate::openapi::DashboardError),
        (status = 502, body = crate::openapi::DashboardError),
        (status = 504, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn fleet_unload_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
    Path(id): Path<String>,
) -> Response {
    fleet_model_action(gateway, actor, id, None).await
}

/// `load` carries the load options; `None` is an unload.
async fn fleet_model_action(
    gateway: Arc<Gateway>,
    actor: ManagementActor,
    model_id: String,
    load: Option<FleetLoadOptions>,
) -> Response {
    let permission = fleet_action_permission(load.is_some());
    if !actor.allows(permission) {
        return fleet_error(StatusCode::FORBIDDEN, "management permission denied");
    }
    let Some(fleet) = gateway.fleet() else {
        return fleet_error(StatusCode::NOT_FOUND, "Fleet is not configured");
    };
    let result = match load {
        Some(options) => {
            fleet
                .load_model_instances(&model_id, options.instances, Some(options.gpus))
                .await
        }
        None => fleet.unload_model(&model_id).await,
    };
    match result {
        Ok(operation) => json_no_store(
            if operation.changed {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            },
            &operation,
        ),
        Err(error) => error.into_response(),
    }
}

fn parse_fleet_load_body(body: &[u8]) -> Result<FleetLoadOptions, FleetProxyError> {
    if body.is_empty() {
        return Ok(FleetLoadOptions::default());
    }
    let shape_error = || {
        FleetProxyError::new(
            StatusCode::BAD_REQUEST,
            "load body must be empty or JSON with instances and/or gpus",
        )
    };
    let request: FleetLoadModelRequest = serde_json::from_slice(body).map_err(|_| shape_error())?;
    if request.instances.is_none() && request.gpus.is_none() {
        return Err(shape_error());
    }
    validate_optional_instances(request.instances)?;
    let gpus = request.gpus.unwrap_or_default();
    validate_gpus(&gpus)?;
    Ok(FleetLoadOptions {
        instances: request.instances,
        gpus,
    })
}

fn validate_gpus(gpus: &[u32]) -> Result<(), FleetProxyError> {
    crate::mesh::protocol::validate_gpu_selection(gpus).map_err(|_| FleetProxyError {
        status: StatusCode::BAD_REQUEST,
        message: "gpus must list at most 64 distinct GPU indices between 0 and 255",
        code: Some("invalid_gpus".to_string()),
    })
}

/// Milliseconds since an RFC3339 sample time, clamped at zero for clock
/// skew; `None` if Fleet sent an unparseable time.
pub(crate) fn sample_age_ms(sampled_at: &str) -> Option<u64> {
    let sampled = chrono::DateTime::parse_from_rfc3339(sampled_at).ok()?;
    let age = chrono::Utc::now().signed_duration_since(sampled);
    Some(age.num_milliseconds().max(0) as u64)
}

/// Bounds a Fleet GPU answer the same way the mesh wire is bounded, so the
/// local dashboard and the hub see identical limits: truncates instead of
/// rejecting (a long GPU name must not hide the whole inventory).
fn sanitize_gpu_inventory(inventory: FleetGpuInventory) -> FleetGpuInventory {
    use crate::mesh::protocol::{
        MAX_GPU_ASSIGNMENTS, MAX_GPU_GROUPS, MAX_GPU_INDEX, MAX_GPU_NAME_BYTES,
        MAX_GPU_SAMPLED_AT_BYTES, MAX_GPUS_PER_NODE,
    };
    let mut gpus: Vec<FleetGpu> = Vec::new();
    for gpu in inventory.gpus {
        if gpus.len() == MAX_GPUS_PER_NODE {
            break;
        }
        if gpu.index > MAX_GPU_INDEX || gpus.iter().any(|seen| seen.index == gpu.index) {
            continue;
        }
        let mut assigned: Vec<FleetGpuAssignment> = Vec::new();
        for assignment in gpu.assigned {
            if assigned.len() == MAX_GPU_ASSIGNMENTS {
                break;
            }
            if validate_model_id(&assignment.model_id).is_err() {
                continue;
            }
            let instance_id = assignment.instance_id.filter(|id| {
                id.len() <= crate::mesh::protocol::MAX_RESOURCE_ID_BYTES
                    && !id.trim().is_empty()
                    && !id.chars().any(char::is_control)
            });
            assigned.push(FleetGpuAssignment {
                model_id: assignment.model_id,
                instance_id,
            });
        }
        gpus.push(FleetGpu {
            index: gpu.index,
            name: sanitize_text(&gpu.name, MAX_GPU_NAME_BYTES),
            memory_total_mib: gpu.memory_total_mib,
            memory_used_mib: gpu.memory_used_mib,
            memory_free_mib: gpu.memory_free_mib,
            utilization_percent: gpu.utilization_percent.map(|value| value.min(100)),
            group: gpu.group.filter(|group| (*group as usize) < MAX_GPU_GROUPS),
            assigned,
            hidden_assignments: 0,
        });
    }
    let groups = inventory
        .groups
        .into_iter()
        .take(MAX_GPU_GROUPS)
        .map(|group| {
            group
                .into_iter()
                .filter(|gpu| *gpu <= MAX_GPU_INDEX)
                .take(MAX_GPUS_PER_NODE)
                .collect()
        })
        .collect();
    let sampled_at = Some(sanitize_text(
        &inventory.sampled_at,
        MAX_GPU_SAMPLED_AT_BYTES,
    ))
    .filter(|value| !value.is_empty())
    .unwrap_or_else(|| "unknown".to_string());
    FleetGpuInventory {
        sampled_at,
        stale: inventory.stale,
        age_ms: None,
        groups,
        gpus,
    }
}

fn sanitize_text(value: &str, max: usize) -> String {
    let mut normalized = String::new();
    for ch in value.trim().chars() {
        let ch = if ch.is_control() { ' ' } else { ch };
        if normalized.len() + ch.len_utf8() > max {
            break;
        }
        normalized.push(ch);
    }
    normalized.trim().to_owned()
}

fn validate_optional_instances(instances: Option<u32>) -> Result<(), FleetProxyError> {
    if instances.is_some_and(|instances| !(1..=MAX_FLEET_MODEL_INSTANCES).contains(&instances)) {
        return Err(FleetProxyError::new(
            StatusCode::BAD_REQUEST,
            "instances must be between 1 and 64",
        ));
    }
    Ok(())
}

fn default_max_instances() -> u32 {
    1
}

fn fleet_action_permission(load: bool) -> ManagementPermission {
    if load {
        ManagementPermission::FleetModelsLoad
    } else {
        ManagementPermission::FleetModelsUnload
    }
}

fn parse_loopback_url(raw: &str) -> Result<Url, String> {
    let url = Url::parse(raw.trim()).map_err(|_| format!("invalid {FLEET_URL_ENV}"))?;
    let loopback = match url.host_str() {
        Some(host) if host.eq_ignore_ascii_case("localhost") => true,
        Some(host) => host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback()),
        None => false,
    };
    if !matches!(url.scheme(), "http" | "https") || !loopback {
        return Err(format!(
            "{FLEET_URL_ENV} must use http(s) and point at a loopback host"
        ));
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(format!(
            "{FLEET_URL_ENV} must be an origin without credentials, path, query, or fragment"
        ));
    }
    Ok(url)
}

fn validate_model_id(model_id: &str) -> Result<(), FleetProxyError> {
    let bytes = model_id.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 63
        || !(bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        || !bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(FleetProxyError::new(
            StatusCode::BAD_REQUEST,
            "invalid Fleet model id",
        ));
    }
    Ok(())
}

fn token_unavailable() -> FleetProxyError {
    FleetProxyError::new(StatusCode::INTERNAL_SERVER_ERROR, "Fleet token unavailable")
}

fn read_nonempty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn fleet_error(status: StatusCode, message: &str) -> Response {
    json_no_store(status, &serde_json::json!({ "error": message }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard_access::ManagementActor;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_client(base_url: &str) -> FleetClient {
        FleetClient {
            client: Client::new(),
            base_url: parse_loopback_url(base_url).expect("loopback mock URL"),
            token: FleetTokenSource::Env("secret-token".to_owned()),
        }
    }

    #[test]
    fn fleet_actions_require_distinct_management_permissions() {
        assert_eq!(
            fleet_action_permission(true),
            ManagementPermission::FleetModelsLoad
        );
        assert_eq!(
            fleet_action_permission(false),
            ManagementPermission::FleetModelsUnload
        );

        let read_only = ManagementActor::Delegated {
            session_id: "sess_fleet_read".into(),
            principal_id: "usr_fleet_read".into(),
            key_id: "key_fleet_read".into(),
            permissions: Arc::from([ManagementPermission::FleetModelsRead]),
        };
        assert!(read_only.allows(ManagementPermission::FleetModelsRead));
        assert!(!read_only.allows(ManagementPermission::FleetModelsLoad));
        assert!(!read_only.allows(ManagementPermission::FleetModelsUnload));
    }

    #[test]
    fn fleet_url_and_model_id_validation_match_local_fleet_contract() {
        assert!(parse_loopback_url("http://127.0.0.1:8090").is_ok());
        assert!(parse_loopback_url("http://[::1]:8090").is_ok());
        assert!(parse_loopback_url("https://localhost:8090").is_ok());
        assert!(parse_loopback_url("http://10.0.0.10:8090").is_err());
        assert!(parse_loopback_url("http://user@127.0.0.1:8090").is_err());
        assert!(parse_loopback_url("http://127.0.0.1:8090/admin").is_err());
        assert!(validate_model_id("qwen3-flash").is_ok());
        assert!(validate_model_id("../secret").is_err());
        assert!(validate_model_id("Qwen").is_err());
    }

    #[test]
    fn fleet_wire_shapes_round_trip_including_noop() {
        let value = serde_json::json!({
            "models": [{
                "model": {"id": "qwen3-flash", "description": "fast local", "image": "qwen:flash", "max_instances": 4},
                "status": {
                    "model_id": "qwen3-flash",
                    "phase": "ready",
                    "desired_state": "loaded",
                    "desired_instances": 2,
                    "ready_instances": 1,
                    "instances": [{
                        "instance_id": "qwen3-flash-0",
                        "index": 0,
                        "port": 8114,
                        "phase": "ready",
                        "container_status": "running",
                        "assigned_gpus": [0],
                        "health": "healthy"
                    }],
                    "container_status": "running",
                    "health": "healthy",
                    "assigned_gpus": [0, 1],
                    "last_checked": "2026-09-21T00:00:00Z"
                }
            }]
        });
        let decoded: FleetModelsResponse = serde_json::from_value(value).expect("Fleet models");
        assert_eq!(decoded.models[0].status.assigned_gpus, vec![0, 1]);
        assert_eq!(decoded.models[0].model.max_instances, 4);
        assert_eq!(decoded.models[0].status.desired_instances, 2);
        assert_eq!(decoded.models[0].status.ready_instances, 1);
        assert_eq!(decoded.models[0].status.instances[0].port, 8114);
        let legacy: FleetModelsResponse = serde_json::from_value(serde_json::json!({
            "models": [{
                "model": {"id": "legacy", "image": "legacy:image"},
                "status": {"model_id": "legacy", "phase": "unloaded", "desired_state": "unloaded"}
            }]
        }))
        .expect("legacy Fleet models");
        assert_eq!(legacy.models[0].model.max_instances, 1);
        assert_eq!(legacy.models[0].status.desired_instances, 0);
        let noop: FleetOperationResponse =
            serde_json::from_value(serde_json::json!({"changed": false})).expect("Fleet no-op");
        assert!(noop.operation.is_none());
    }

    #[test]
    fn fleet_load_body_is_empty_legacy_or_strict_instances() {
        assert_eq!(
            parse_fleet_load_body(b"").unwrap(),
            FleetLoadOptions::default()
        );
        assert_eq!(
            parse_fleet_load_body(br#"{"instances":2}"#).unwrap(),
            FleetLoadOptions {
                instances: Some(2),
                gpus: Vec::new()
            }
        );
        assert_eq!(
            parse_fleet_load_body(br#"{"instances":2,"gpus":[3,1]}"#).unwrap(),
            FleetLoadOptions {
                instances: Some(2),
                gpus: vec![3, 1]
            }
        );
        assert_eq!(
            parse_fleet_load_body(br#"{"gpus":[]}"#).unwrap(),
            FleetLoadOptions::default()
        );
        for invalid in [
            &br#"{"gpus":[1,1]}"#[..],
            br#"{"gpus":[256]}"#,
            br#"{"gpus":[-1]}"#,
            br#"{"gpus":"0,1"}"#,
        ] {
            assert!(parse_fleet_load_body(invalid).is_err());
        }
        let too_many = serde_json::to_vec(&serde_json::json!({
            "gpus": (0..65).collect::<Vec<u32>>()
        }))
        .unwrap();
        let error = parse_fleet_load_body(&too_many).expect_err("65 GPUs");
        assert_eq!(error.code(), Some("invalid_gpus"));
        assert!(parse_fleet_load_body(br#"{"instances":0}"#).is_err());
        assert!(parse_fleet_load_body(br#"{"instances":65}"#).is_err());
        assert!(parse_fleet_load_body(br#"{"instances":1,"token":"secret"}"#).is_err());
        assert!(parse_fleet_load_body(br#"{}"#).is_err());
    }

    #[tokio::test]
    async fn fleet_client_keeps_bearer_server_side_and_decodes_models() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .and(header("authorization", "Bearer secret-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [{
                    "model": {"id": "qwen3-flash", "image": "qwen:flash"},
                    "status": {"model_id": "qwen3-flash", "phase": "unloaded", "desired_state": "unloaded", "last_checked": "2026-09-21T00:00:00Z"}
                }]
            })))
            .mount(&server)
            .await;
        let models = test_client(&server.uri())
            .list_models()
            .await
            .expect("models response");
        assert_eq!(models.models[0].model.id, "qwen3-flash");
    }

    #[tokio::test]
    async fn fleet_client_forwards_instance_count_without_exposing_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/models/qwen3-flash/load"))
            .and(header("authorization", "Bearer secret-token"))
            .respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({
                "changed": true,
                "operation": {
                    "id": "op-1",
                    "kind": "load",
                    "model_id": "qwen3-flash",
                    "state": "queued",
                    "created_at": "2026-09-28T00:00:00Z",
                    "instances": 2
                }
            })))
            .mount(&server)
            .await;

        let response = test_client(&server.uri())
            .load_model_instances("qwen3-flash", Some(2), None)
            .await
            .expect("load operation");
        assert_eq!(
            response.operation.as_ref().and_then(|op| op.instances),
            Some(2)
        );

        let requests = server.received_requests().await.expect("recorded requests");
        assert_eq!(requests.len(), 1);
        let body: serde_json::Value =
            serde_json::from_slice(&requests[0].body).expect("request body");
        assert_eq!(body, serde_json::json!({"instances": 2}));
        assert!(!String::from_utf8_lossy(&requests[0].body).contains("secret-token"));
    }

    #[tokio::test]
    async fn explicit_instance_counts_are_always_sent() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/models/qwen3-flash/load"))
            .respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({
                "changed": true
            })))
            .mount(&server)
            .await;
        let client = test_client(&server.uri());
        for instances in [None, Some(1)] {
            client
                .load_model_instances("qwen3-flash", instances, Some(Vec::new()))
                .await
                .expect("load operation");
        }
        let requests = server.received_requests().await.expect("recorded requests");
        assert_eq!(requests.len(), 2);
        assert!(requests[0].body.is_empty(), "{:?}", requests[0].body);
        let scale_down: serde_json::Value =
            serde_json::from_slice(&requests[1].body).expect("json load body");
        assert_eq!(scale_down, serde_json::json!({"instances": 1}));
    }

    #[tokio::test]
    async fn fleet_client_errors_keep_request_level_statuses_and_sanitized_codes() {
        let server = MockServer::start().await;
        for (model, status, body) in [
            (
                "rejects-body",
                400,
                serde_json::json!({"error": "overrides_not_allowed"}),
            ),
            (
                "busy",
                409,
                serde_json::json!({"error": {"code": "gpu_busy", "message": "GPU 0 in use by /secret/path"}}),
            ),
            (
                "missing",
                404,
                serde_json::json!({"code": "model_not_found"}),
            ),
            (
                "free-text",
                422,
                serde_json::json!({"error": "docker: failed at /var/lib/secret"}),
            ),
            (
                "bad-token",
                401,
                serde_json::json!({"error": "unauthorized"}),
            ),
            ("broken", 500, serde_json::json!({"error": "internal"})),
        ] {
            Mock::given(method("POST"))
                .and(path(format!("/v1/models/{model}/load")))
                .respond_with(ResponseTemplate::new(status).set_body_json(body))
                .mount(&server)
                .await;
        }
        let client = test_client(&server.uri());
        for (model, expected_status, expected_code) in [
            (
                "rejects-body",
                StatusCode::BAD_REQUEST,
                Some("overrides_not_allowed"),
            ),
            ("busy", StatusCode::CONFLICT, Some("gpu_busy")),
            ("missing", StatusCode::NOT_FOUND, Some("model_not_found")),
            ("free-text", StatusCode::BAD_REQUEST, None),
            ("bad-token", StatusCode::BAD_GATEWAY, Some("unauthorized")),
            ("broken", StatusCode::BAD_GATEWAY, Some("internal")),
        ] {
            let error = client
                .load_model(model)
                .await
                .expect_err("Fleet error surfaces");
            assert_eq!(error.status(), expected_status, "{model}");
            assert_eq!(error.code.as_deref(), expected_code, "{model}");
            let rendered = error.to_string();
            assert!(!rendered.contains("secret"), "{rendered}");
            if let Some(code) = expected_code {
                assert!(rendered.contains(code), "{rendered}");
            }
        }
    }

    #[tokio::test]
    async fn fleet_client_bounds_success_responses() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![
                b'x';
                MAX_RESPONSE_BODY_BYTES
                    + 1
            ]))
            .mount(&server)
            .await;
        let error = test_client(&server.uri())
            .list_models()
            .await
            .expect_err("oversized response rejected");
        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn load_request_round_trips_with_optional_gpus() {
        for value in [
            serde_json::json!({"instances": 2}),
            serde_json::json!({"instances": 2, "gpus": [4, 5]}),
            serde_json::json!({"gpus": [0]}),
        ] {
            let decoded: FleetLoadModelRequest = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(serde_json::to_value(&decoded).unwrap(), value);
        }
    }

    fn gpu_inventory_json() -> serde_json::Value {
        serde_json::json!({
            "sampled_at": "2026-10-10T00:00:00Z",
            "groups": [[0, 1, 6, 7], [2, 3, 4, 5]],
            "gpus": [{
                "index": 0,
                "name": "NVIDIA RTX PRO 6000 Blackwell",
                "memory_total_mib": 97887,
                "memory_used_mib": 1234,
                "memory_free_mib": 96000,
                "utilization_percent": 0,
                "group": 0,
                "assigned": [{"model_id": "ds41-flash-tp4-ssd", "instance_id": "ds41-flash-tp4-ssd"}]
            }, {
                "index": 2,
                "name": "NVIDIA RTX PRO 6000 Blackwell",
                "memory_total_mib": 97887,
                "memory_used_mib": 0,
                "memory_free_mib": 97887,
                "utilization_percent": null,
                "group": null,
                "assigned": []
            }]
        })
    }

    #[test]
    fn gpu_inventory_round_trips_and_tolerates_missing_optionals() {
        let decoded: FleetGpuInventory = serde_json::from_value(gpu_inventory_json()).unwrap();
        assert!(!decoded.stale);
        assert_eq!(decoded.gpus[0].utilization_percent, Some(0));
        assert_eq!(decoded.gpus[1].utilization_percent, None);
        assert_eq!(decoded.gpus[1].group, None);
        assert_eq!(
            decoded.gpus[0].assigned[0].instance_id.as_deref(),
            Some("ds41-flash-tp4-ssd")
        );
        let encoded = serde_json::to_value(&decoded).unwrap();
        assert_eq!(
            serde_json::from_value::<FleetGpuInventory>(encoded.clone()).unwrap(),
            decoded
        );
        // Unknown utilization stays absent on the dashboard wire, never 0.
        assert!(encoded["gpus"][1].get("utilization_percent").is_none());
        let minimal: FleetGpuInventory = serde_json::from_value(serde_json::json!({
            "sampled_at": "2026-10-10T00:00:00Z",
            "gpus": [{"index": 3, "utilization_percent": 37.6}]
        }))
        .unwrap();
        assert!(minimal.groups.is_empty());
        assert_eq!(minimal.gpus[0].utilization_percent, Some(38));
        assert!(minimal.gpus[0].assigned.is_empty());
        // The models response stays parseable without an inventory.
        let models: FleetModelsResponse =
            serde_json::from_value(serde_json::json!({"models": []})).unwrap();
        assert_eq!(models.gpu_inventory, None);
        assert!(
            serde_json::to_value(&models)
                .unwrap()
                .get("gpu_inventory")
                .is_none()
        );
    }

    #[test]
    fn gpu_inventory_sanitization_bounds_fleet_text() {
        let mut gpus = Vec::new();
        for index in 0..70u32 {
            gpus.push(serde_json::json!({
                "index": index,
                "name": format!("GPU\n{}", "x".repeat(300)),
                "assigned": (0..20).map(|n| serde_json::json!({"model_id": format!("m{n}")}))
                    .chain([serde_json::json!({"model_id": "../bad"})])
                    .collect::<Vec<_>>(),
                "group": 999
            }));
        }
        gpus.push(serde_json::json!({"index": 1}));
        let raw: FleetGpuInventory = serde_json::from_value(serde_json::json!({
            "sampled_at": "2026-10-10T00:00:00Z\n",
            "gpus": gpus,
            "groups": [[0, 300]]
        }))
        .unwrap();
        let clean = sanitize_gpu_inventory(raw);
        assert_eq!(clean.gpus.len(), crate::mesh::protocol::MAX_GPUS_PER_NODE);
        assert_eq!(clean.sampled_at, "2026-10-10T00:00:00Z");
        assert_eq!(clean.groups, vec![vec![0]]);
        let gpu = &clean.gpus[0];
        assert!(gpu.name.len() <= crate::mesh::protocol::MAX_GPU_NAME_BYTES);
        assert!(!gpu.name.contains('\n'));
        assert_eq!(gpu.group, None);
        assert_eq!(
            gpu.assigned.len(),
            crate::mesh::protocol::MAX_GPU_ASSIGNMENTS
        );
        assert!(gpu.assigned.iter().all(|a| a.model_id != "../bad"));
    }

    #[tokio::test]
    async fn list_gpus_treats_missing_route_as_unavailable_and_surfaces_failures() {
        let server = MockServer::start().await;
        let client = test_client(&server.uri());
        // An older Fleet without the route.
        assert_eq!(client.list_gpus().await.expect("404 is not an error"), None);
        Mock::given(method("GET"))
            .and(path("/v1/gpus"))
            .and(header("authorization", "Bearer secret-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(gpu_inventory_json()))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        let inventory = client.list_gpus().await.unwrap().expect("inventory");
        assert_eq!(inventory.gpus.len(), 2);
        assert_eq!(inventory.groups.len(), 2);
        Mock::given(method("GET"))
            .and(path("/v1/gpus"))
            .respond_with(
                ResponseTemplate::new(503)
                    .set_body_json(serde_json::json!({"error": "gpu_query_failed"})),
            )
            .mount(&server)
            .await;
        let error = client.list_gpus().await.expect_err("503 surfaces");
        assert_eq!(error.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(error.code(), Some("gpu_query_failed"));
    }

    #[tokio::test]
    async fn gpu_pick_is_forwarded_only_when_non_empty_and_warnings_are_sanitized() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/models/qwen3-flash/load"))
            .respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({
                "changed": true,
                "warnings": [
                    "selected GPUs span PCIe groups\n0 and 1",
                    "",
                    "w".repeat(1000),
                    "a", "b", "c", "d", "e", "f", "g", "h"
                ]
            })))
            .mount(&server)
            .await;
        let client = test_client(&server.uri());
        let response = client
            .load_model_instances("qwen3-flash", Some(2), Some(vec![2, 3, 4, 5]))
            .await
            .expect("load");
        assert_eq!(
            response.warnings.len(),
            crate::mesh::protocol::MAX_LOAD_WARNINGS
        );
        assert_eq!(
            response.warnings[0],
            "selected GPUs span PCIe groups 0 and 1"
        );
        assert!(
            response
                .warnings
                .iter()
                .all(|w| w.len() <= crate::mesh::protocol::MAX_LOAD_WARNING_BYTES)
        );
        client
            .load_model_instances("qwen3-flash", None, Some(Vec::new()))
            .await
            .expect("auto placement");
        client
            .load_model_instances("qwen3-flash", None, Some(vec![7]))
            .await
            .expect("gpus without instances");
        assert!(
            client
                .load_model_instances("qwen3-flash", None, Some(vec![1, 1]))
                .await
                .is_err(),
            "duplicates are rejected before Fleet"
        );
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 3);
        let body = |index: usize| -> serde_json::Value {
            serde_json::from_slice(&requests[index].body).unwrap()
        };
        assert_eq!(
            body(0),
            serde_json::json!({"instances": 2, "gpus": [2, 3, 4, 5]})
        );
        assert!(
            requests[1].body.is_empty(),
            "empty pick means automatic placement"
        );
        assert_eq!(body(2), serde_json::json!({"gpus": [7]}));
    }

    #[tokio::test]
    async fn gpu_placement_errors_map_to_sanitized_codes() {
        let server = MockServer::start().await;
        for (model, status, code) in [
            ("unavailable", 409, "gpus_unavailable"),
            ("unsupported", 400, "gpus_not_supported"),
            ("not-applicable", 400, "gpus_not_applicable"),
            ("invalid", 400, "invalid_gpus"),
        ] {
            Mock::given(method("POST"))
                .and(path(format!("/v1/models/{model}/load")))
                .respond_with(
                    ResponseTemplate::new(status).set_body_json(serde_json::json!({
                        "error": {"code": code, "message": "GPU 3 used by /secret"}
                    })),
                )
                .mount(&server)
                .await;
        }
        let client = test_client(&server.uri());
        for (model, status, code) in [
            ("unavailable", StatusCode::CONFLICT, "gpus_unavailable"),
            ("unsupported", StatusCode::BAD_REQUEST, "gpus_not_supported"),
            (
                "not-applicable",
                StatusCode::BAD_REQUEST,
                "gpus_not_applicable",
            ),
            ("invalid", StatusCode::BAD_REQUEST, "invalid_gpus"),
        ] {
            let error = client
                .load_model_instances(model, Some(1), Some(vec![3]))
                .await
                .expect_err("Fleet refusal");
            assert_eq!(error.status(), status, "{model}");
            assert_eq!(error.code(), Some(code), "{model}");
            assert!(!error.to_string().contains("secret"));
        }
        let error = client
            .load_model_instances("unavailable", Some(1), Some(vec![3]))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("Requested GPUs are not available")
        );
    }
}
