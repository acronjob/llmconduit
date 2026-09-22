//! Local Fleet GPU/model lifecycle controls exposed through the authenticated dashboard.
//!
//! The first integration is deliberately loopback-only. Fleet credentials are read from
//! the environment or an env-selected file and never cross the dashboard API boundary.

use crate::dashboard_api::json_no_store;
use crate::dashboard_auth::{AuthSession, DashboardAuth};
use crate::dashboard_mesh::{authorize_mesh_admin_read, authorize_mesh_mutation};
use crate::engine::Gateway;
use axum::Extension;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct FleetDeploymentStatus {
    pub model_id: String,
    pub phase: String,
    pub desired_state: String,
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
pub struct FleetOperationResponse {
    pub changed: bool,
    /// Fleet omits this for an already-satisfied no-op.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<FleetOperation>,
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

    pub async fn load_model(
        &self,
        model_id: &str,
    ) -> Result<FleetOperationResponse, FleetProxyError> {
        validate_model_id(model_id)?;
        self.request(
            reqwest::Method::POST,
            &format!("/v1/models/{model_id}/load"),
        )
        .await
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

    async fn request<T>(&self, method: reqwest::Method, path: &str) -> Result<T, FleetProxyError>
    where
        T: DeserializeOwned,
    {
        let token = self.token().await?;
        let mut url = self.base_url.clone();
        url.set_path(path);
        let response = self
            .client
            .request(method, url)
            .bearer_auth(token)
            .timeout(FLEET_REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|error| {
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
            let (status, message) = match upstream_status {
                reqwest::StatusCode::NOT_FOUND => {
                    (StatusCode::NOT_FOUND, "Fleet model was not found")
                }
                reqwest::StatusCode::CONFLICT => (
                    StatusCode::CONFLICT,
                    "Fleet is busy or has insufficient GPU capacity",
                ),
                _ => (StatusCode::BAD_GATEWAY, "Fleet rejected the request"),
            };
            return Err(FleetProxyError::new(status, message));
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
}

impl FleetProxyError {
    fn new(status: StatusCode, message: &'static str) -> Self {
        Self { status, message }
    }

    fn into_response(self) -> Response {
        fleet_error(self.status, self.message)
    }
}

impl std::fmt::Display for FleetProxyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
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
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = authorize_mesh_admin_read(auth.as_ref(), &session, &headers) {
        return response;
    }
    let Some(fleet) = gateway.fleet() else {
        return fleet_error(StatusCode::NOT_FOUND, "Fleet is not configured");
    };
    match fleet.list_models().await {
        Ok(models) => json_no_store(StatusCode::OK, &models),
        Err(error) => error.into_response(),
    }
}

#[utoipa::path(
    post,
    path = "/dashboard/api/fleet/models/{id}/load",
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
        (status = 502, body = crate::openapi::DashboardError),
        (status = 504, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn fleet_load_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    fleet_model_action(gateway, auth, session, headers, id, true).await
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
        (status = 502, body = crate::openapi::DashboardError),
        (status = 504, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn fleet_unload_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    fleet_model_action(gateway, auth, session, headers, id, false).await
}

async fn fleet_model_action(
    gateway: Arc<Gateway>,
    auth: Arc<DashboardAuth>,
    session: AuthSession,
    headers: HeaderMap,
    model_id: String,
    load: bool,
) -> Response {
    if let Some(response) = authorize_mesh_mutation(auth.as_ref(), &session, &headers).await {
        return response;
    }
    let Some(fleet) = gateway.fleet() else {
        return fleet_error(StatusCode::NOT_FOUND, "Fleet is not configured");
    };
    let result = if load {
        fleet.load_model(&model_id).await
    } else {
        fleet.unload_model(&model_id).await
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
                "model": {"id": "qwen3-flash", "description": "fast local", "image": "qwen:flash"},
                "status": {
                    "model_id": "qwen3-flash",
                    "phase": "ready",
                    "desired_state": "loaded",
                    "container_status": "running",
                    "health": "healthy",
                    "assigned_gpus": [0, 1],
                    "last_checked": "2026-09-21T00:00:00Z"
                }
            }]
        });
        let decoded: FleetModelsResponse = serde_json::from_value(value).expect("Fleet models");
        assert_eq!(decoded.models[0].status.assigned_gpus, vec![0, 1]);
        let noop: FleetOperationResponse =
            serde_json::from_value(serde_json::json!({"changed": false})).expect("Fleet no-op");
        assert!(noop.operation.is_none());
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
}
