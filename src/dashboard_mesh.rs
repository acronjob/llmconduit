use crate::dashboard_access::{ManagementActor, ManagementChannel, ManagementPermission};
use crate::dashboard_api::json_no_store;
use crate::dashboard_auth::AuthSession;
use crate::dashboard_auth::DashboardAuth;
use crate::dashboard_auth::MutationPolicy;
use crate::engine::Gateway;
use crate::mesh::protocol::{
    LifecycleOperation, MAX_SWITCH_MODEL_INSTANCES, ModelSwitchingAdvertisement,
    SwitchableModelAdvertisement, SwitchableModelInstanceAdvertisement, validate_model_id,
    validate_operation_id, validate_resource_id,
};
use crate::mesh::registry::FleetOperationLookupError;
use crate::mesh::store::CreateHoldOutcome;
use crate::mesh::store::CreatedJoinKey;
use crate::mesh::store::DisabledMeshModelRecord;
use crate::mesh::store::JoinKeyRecord;
use crate::mesh::store::MeshNodeRecord;
use crate::mesh::store::ModelHoldRecord;
use crate::mesh::store::now_ms;
use axum::Extension;
use axum::Json;
use axum::body::Bytes;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::Response;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use utoipa::ToSchema;

const MAX_JOIN_KEY_LABEL_BYTES: usize = 128;
/// Hold leases are bounded so a crashed coordinator cannot pin a GPU forever.
pub(crate) const MIN_HOLD_TTL_SECS: u64 = 60;
pub(crate) const MAX_HOLD_TTL_SECS: u64 = 24 * 60 * 60;
const MAX_HOLDER_BYTES: usize = 128;
const MAX_HOLD_ID_BYTES: usize = 64;
const MAX_RELEASE_HOLDS: usize = 32;

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshAdminState {
    pub join_keys: Vec<MeshJoinKey>,
    pub nodes: Vec<MeshNode>,
    pub disabled_models: Vec<MeshDisabledModel>,
    /// Every active model hold, including holds on models a disconnected
    /// worker is not currently advertising.
    pub model_holds: Vec<MeshModelHold>,
}

/// An active lease on a mesh Fleet profile. While one exists, unloads (and
/// loads that would stop the held profile on an exclusive-switching worker)
/// are refused with `409 model_held` unless the caller releases its own hold.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct MeshModelHold {
    pub hold_id: String,
    pub holder: String,
    pub expires_at_ms: i64,
    pub endpoint_id: String,
    pub model_id: String,
    pub created_at_ms: i64,
}

/// Per-model hold summary embedded in `GET /dashboard/api/mesh` model entries.
#[derive(Debug, Clone, Serialize, ToSchema, PartialEq, Eq)]
pub struct MeshModelHoldSummary {
    pub hold_id: String,
    pub holder: String,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateMeshModelHoldRequest {
    /// 1-128 characters of `[A-Za-z0-9._:@/-]`, e.g. `harbor/run-42`.
    pub holder: String,
    /// Lease length, 60..=86400 seconds.
    pub ttl_secs: u64,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RenewMeshModelHoldRequest {
    /// New lease length from now, 60..=86400 seconds.
    pub ttl_secs: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshModelHoldsResponse {
    pub endpoint_id: String,
    pub model_id: String,
    pub holds: Vec<MeshModelHold>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ReleaseMeshModelHoldResponse {
    pub hold_id: String,
    pub released: bool,
}

/// `409` body when active holds block a lifecycle change.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshModelHeldError {
    pub error: String,
    /// Always `model_held`.
    pub code: String,
    pub holders: Vec<MeshModelHold>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshOperationResponse {
    pub endpoint_id: String,
    pub operation: LifecycleOperation,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshJoinKey {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub enabled: bool,
    pub created_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_uses: Option<i64>,
    pub use_count: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshNode {
    pub endpoint_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub enabled: bool,
    pub joined_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join_key_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_switching: Option<MeshModelSwitching>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshModelSwitching {
    pub provider: String,
    pub models: Vec<MeshSwitchableModel>,
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshSwitchableModel {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub phase: String,
    pub desired_state: String,
    pub gpu_count: u32,
    pub assigned_gpus: Vec<u32>,
    pub max_instances: u32,
    pub desired_instances: u32,
    pub ready_instances: u32,
    pub instances: Vec<MeshSwitchableModelInstance>,
    /// Active holds on this profile (empty when none).
    pub holds: Vec<MeshModelHoldSummary>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshSwitchableModelInstance {
    pub instance_id: String,
    pub index: u32,
    pub port: u16,
    pub phase: String,
    pub assigned_gpus: Vec<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshDisabledModel {
    pub endpoint_id: String,
    pub resource_id: String,
    pub model: String,
    pub disabled_at_ms: i64,
}

/// Omitted `max_uses` / `expires_in_secs` default to a single-use key valid
/// for 24 hours: a forgotten join key is a standing credential to add GPU
/// workers. An explicit JSON `null` still requests no limit.
#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
pub struct CreateMeshJoinKeyRequest {
    pub label: Option<String>,
    #[serde(default, deserialize_with = "present_option")]
    #[schema(value_type = Option<i64>, nullable = true)]
    pub max_uses: Option<Option<i64>>,
    #[serde(default, deserialize_with = "present_option")]
    #[schema(value_type = Option<i64>, nullable = true)]
    pub expires_in_secs: Option<Option<i64>>,
}

const DEFAULT_JOIN_KEY_MAX_USES: i64 = 1;
const DEFAULT_JOIN_KEY_TTL_SECS: i64 = 24 * 60 * 60;

/// Distinguishes an absent field (`None`) from an explicit `null`
/// (`Some(None)`).
fn present_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CreateMeshJoinKeyResponse {
    pub join_key: MeshJoinKey,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RevokeMeshJoinKeyResponse {
    pub updated: bool,
    pub disabled_endpoint_ids: Vec<String>,
    pub evicted_endpoint_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SetMeshNodeResponse {
    pub updated: bool,
    pub endpoint_id: String,
    pub enabled: bool,
    pub evicted: bool,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct MeshModelOverrideRequest {
    pub endpoint_id: String,
    pub resource_id: String,
    pub model: String,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct MeshModelLoadRequest {
    pub instances: u32,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SetMeshModelResponse {
    pub updated: bool,
    pub endpoint_id: String,
    pub resource_id: String,
    pub model: String,
    pub disabled: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SwitchMeshModelResponse {
    pub endpoint_id: String,
    pub model_id: String,
    pub accepted: bool,
    pub changed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The Fleet operation started or joined (`{id, state, error}`), when the
    /// worker reports it. Absent for no-ops and for workers that predate
    /// operation reporting; poll `GET .../operations/{id}` for progress.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<LifecycleOperation>,
    /// Holds released because the caller passed `release_hold`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub released_holds: Vec<String>,
}

#[utoipa::path(
    get,
    path = "/dashboard/api/mesh",
    tag = "dashboard",
    responses(
        (status = 200, body = MeshAdminState),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn mesh_state(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
) -> Response {
    if !actor.allows(ManagementPermission::FleetModelsRead) {
        return mesh_error(StatusCode::FORBIDDEN, "management permission denied");
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    let store = admin.store();
    let (join_keys, nodes, disabled_models, holds) = match futures::try_join!(
        store.list_join_keys(),
        store.list_nodes(),
        store.list_disabled_models(),
        store.list_active_holds(None, None, now_ms())
    ) {
        Ok(records) => records,
        Err(err) => {
            tracing::error!(error = %err, "failed to read mesh admin state");
            return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
        }
    };
    let registry = admin.registry();
    let mut holds_by_model: BTreeMap<(String, String), Vec<MeshModelHoldSummary>> = BTreeMap::new();
    for hold in &holds {
        holds_by_model
            .entry((hold.endpoint_id.clone(), hold.model_id.clone()))
            .or_default()
            .push(MeshModelHoldSummary {
                hold_id: hold.hold_id.clone(),
                holder: hold.holder.clone(),
                expires_at_ms: hold.expires_at_ms,
            });
    }
    let nodes = nodes
        .into_iter()
        .map(|record| {
            let switching = record
                .endpoint_id
                .parse()
                .ok()
                .and_then(|endpoint| registry.model_switching(endpoint))
                .map(MeshModelSwitching::from)
                .map(|mut switching| {
                    for model in &mut switching.models {
                        if let Some(holds) =
                            holds_by_model.get(&(record.endpoint_id.clone(), model.id.clone()))
                        {
                            model.holds = holds.clone();
                        }
                    }
                    switching
                });
            let mut node = MeshNode::from(record);
            node.model_switching = switching;
            node
        })
        .collect();
    json_no_store(
        StatusCode::OK,
        &MeshAdminState {
            join_keys: join_keys.into_iter().map(MeshJoinKey::from).collect(),
            nodes,
            disabled_models: disabled_models
                .into_iter()
                .map(MeshDisabledModel::from)
                .collect(),
            model_holds: holds.into_iter().map(MeshModelHold::from).collect(),
        },
    )
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/join-keys",
    tag = "dashboard",
    request_body = CreateMeshJoinKeyRequest,
    responses(
        (status = 200, body = CreateMeshJoinKeyResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn create_join_key(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    headers: HeaderMap,
    Json(body): Json<CreateMeshJoinKeyRequest>,
) -> Response {
    if let Some(response) = authorize_mesh_mutation(auth.as_ref(), &session, &headers).await {
        return response;
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    let Some((expires_at_ms, max_uses)) = validate_join_key_request(&body) else {
        return mesh_error(
            StatusCode::BAD_REQUEST,
            "max_uses and expires_in_secs must be greater than zero when provided",
        );
    };
    if body
        .label
        .as_deref()
        .is_some_and(|label| label.len() > MAX_JOIN_KEY_LABEL_BYTES)
    {
        return mesh_error(StatusCode::BAD_REQUEST, "label must be at most 128 bytes");
    }
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    match admin
        .store()
        .create_join_key(body.label, expires_at_ms, max_uses)
        .await
    {
        Ok(created) => json_no_store(StatusCode::OK, &CreateMeshJoinKeyResponse::from(created)),
        Err(err) => {
            tracing::error!(error = %err, "failed to create mesh join key");
            mesh_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create join key",
            )
        }
    }
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/join-keys/{id}/revoke",
    tag = "dashboard",
    params(("id" = String, Path, description = "Join key id.")),
    responses(
        (status = 200, body = RevokeMeshJoinKeyResponse),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn revoke_join_key(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = authorize_mesh_mutation(auth.as_ref(), &session, &headers).await {
        return response;
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    match admin.store().revoke_join_key(&id).await {
        Ok(revoked) => {
            let mut evicted_endpoint_ids = Vec::new();
            for endpoint_id in &revoked.disabled_endpoint_ids {
                if let Ok(endpoint) = endpoint_id.parse()
                    && admin.disable_live_node(endpoint)
                {
                    evicted_endpoint_ids.push(endpoint_id.clone());
                }
            }
            json_no_store(
                StatusCode::OK,
                &RevokeMeshJoinKeyResponse {
                    updated: revoked.updated,
                    disabled_endpoint_ids: revoked.disabled_endpoint_ids,
                    evicted_endpoint_ids,
                },
            )
        }
        Err(err) => {
            tracing::error!(error = %err, "failed to revoke mesh join key");
            mesh_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to revoke join key",
            )
        }
    }
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/disable",
    tag = "dashboard",
    params(("endpoint_id" = String, Path, description = "Iroh endpoint id.")),
    responses(
        (status = 200, body = SetMeshNodeResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn disable_node(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    Path(endpoint_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    set_node_enabled(gateway, auth, session, headers, endpoint_id, false).await
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/enable",
    tag = "dashboard",
    params(("endpoint_id" = String, Path, description = "Iroh endpoint id.")),
    responses(
        (status = 200, body = SetMeshNodeResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn enable_node(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    Path(endpoint_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    set_node_enabled(gateway, auth, session, headers, endpoint_id, true).await
}

/// Query options shared by switch/load/unload.
///
/// - `release_hold=<hold_id>` (repeatable or comma-separated): the caller's
///   own holds that may be released by this change. Holds owned by another
///   credential are refused (`403 hold_not_owned`).
/// - `force=true`: administrator dashboard sessions only; overrides every
///   blocking hold and is recorded in the hold audit log.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct LifecycleOptions {
    release_holds: Vec<String>,
    force: bool,
}

fn parse_lifecycle_options(pairs: &[(String, String)]) -> Result<LifecycleOptions, &'static str> {
    let mut options = LifecycleOptions::default();
    for (key, value) in pairs {
        match key.as_str() {
            "release_hold" => {
                for id in value.split(',').map(str::trim).filter(|id| !id.is_empty()) {
                    if !valid_hold_id(id) {
                        return Err("invalid release_hold");
                    }
                    if !options.release_holds.iter().any(|known| known == id) {
                        options.release_holds.push(id.to_string());
                    }
                }
                if options.release_holds.len() > MAX_RELEASE_HOLDS {
                    return Err("too many release_hold values");
                }
            }
            "force" => {
                options.force = match value.trim() {
                    "true" | "1" => true,
                    "false" | "0" | "" => false,
                    _ => return Err("force must be true or false"),
                };
            }
            _ => {}
        }
    }
    Ok(options)
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/models/{model_id}/switch",
    tag = "dashboard",
    params(
        ("endpoint_id" = String, Path, description = "Iroh endpoint id."),
        ("model_id" = String, Path, description = "Advertised Fleet model id."),
        ("release_hold" = Option<String>, Query, description = "Caller-owned hold id(s) this load may stop and release (repeatable or comma-separated)."),
        ("force" = Option<bool>, Query, description = "Administrator dashboard sessions only: override blocking holds (audited).")
    ),
    request_body(content = Option<MeshModelLoadRequest>),
    responses(
        (status = 200, body = SwitchMeshModelResponse),
        (status = 202, body = SwitchMeshModelResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError),
        (status = 409, body = MeshModelHeldError, description = "`model_held`: an active hold would be stopped, or Fleet refused (busy / insufficient GPUs)."),
        (status = 502, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn switch_node_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
    channel: Option<Extension<ManagementChannel>>,
    Path((endpoint_id, model_id)): Path<(String, String)>,
    Query(query): Query<Vec<(String, String)>>,
    body: Bytes,
) -> Response {
    let instances = match parse_mesh_model_load_body(&body) {
        Ok(instances) => instances,
        Err(error) => return error.into_response(),
    };
    let options = match parse_lifecycle_options(&query) {
        Ok(options) => options,
        Err(message) => return mesh_error(StatusCode::BAD_REQUEST, message),
    };
    set_node_model_state(
        gateway,
        actor,
        channel.map(|Extension(channel)| channel),
        endpoint_id,
        model_id,
        false,
        instances,
        options,
    )
    .await
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/models/{model_id}/load",
    tag = "dashboard",
    params(
        ("endpoint_id" = String, Path, description = "Iroh endpoint id."),
        ("model_id" = String, Path, description = "Advertised Fleet model id."),
        ("release_hold" = Option<String>, Query, description = "Caller-owned hold id(s) this load may stop and release (repeatable or comma-separated)."),
        ("force" = Option<bool>, Query, description = "Administrator dashboard sessions only: override blocking holds (audited).")
    ),
    request_body(content = Option<MeshModelLoadRequest>),
    responses(
        (status = 200, body = SwitchMeshModelResponse),
        (status = 202, body = SwitchMeshModelResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError),
        (status = 409, body = MeshModelHeldError, description = "`model_held`: an active hold would be stopped, or Fleet refused (busy / insufficient GPUs)."),
        (status = 502, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn load_node_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
    channel: Option<Extension<ManagementChannel>>,
    Path((endpoint_id, model_id)): Path<(String, String)>,
    Query(query): Query<Vec<(String, String)>>,
    body: Bytes,
) -> Response {
    let instances = match parse_mesh_model_load_body(&body) {
        Ok(instances) => instances,
        Err(error) => return error.into_response(),
    };
    let options = match parse_lifecycle_options(&query) {
        Ok(options) => options,
        Err(message) => return mesh_error(StatusCode::BAD_REQUEST, message),
    };
    set_node_model_state(
        gateway,
        actor,
        channel.map(|Extension(channel)| channel),
        endpoint_id,
        model_id,
        false,
        instances,
        options,
    )
    .await
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/models/{model_id}/unload",
    tag = "dashboard",
    params(
        ("endpoint_id" = String, Path, description = "Iroh endpoint id."),
        ("model_id" = String, Path, description = "Advertised Fleet model id."),
        ("release_hold" = Option<String>, Query, description = "Caller-owned hold id(s) on this model to release with the unload (repeatable or comma-separated)."),
        ("force" = Option<bool>, Query, description = "Administrator dashboard sessions only: override blocking holds (audited).")
    ),
    responses(
        (status = 200, body = SwitchMeshModelResponse),
        (status = 202, body = SwitchMeshModelResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError),
        (status = 409, body = MeshModelHeldError, description = "`model_held`: another holder's hold is active, or Fleet refused."),
        (status = 502, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn unload_node_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
    channel: Option<Extension<ManagementChannel>>,
    Path((endpoint_id, model_id)): Path<(String, String)>,
    Query(query): Query<Vec<(String, String)>>,
) -> Response {
    let options = match parse_lifecycle_options(&query) {
        Ok(options) => options,
        Err(message) => return mesh_error(StatusCode::BAD_REQUEST, message),
    };
    set_node_model_state(
        gateway,
        actor,
        channel.map(|Extension(channel)| channel),
        endpoint_id,
        model_id,
        true,
        None,
        options,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn set_node_model_state(
    gateway: Arc<Gateway>,
    actor: ManagementActor,
    channel: Option<ManagementChannel>,
    endpoint_id: String,
    model_id: String,
    unload: bool,
    instances: Option<u32>,
    options: LifecycleOptions,
) -> Response {
    let permission = fleet_model_permission(unload);
    if !actor.allows(permission) {
        return mesh_error(StatusCode::FORBIDDEN, "management permission denied");
    }
    if options.force && channel != Some(ManagementChannel::AdminSession) {
        return mesh_error_code(
            StatusCode::FORBIDDEN,
            "force requires an administrator dashboard session",
            "force_not_allowed",
        );
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    let endpoint_id = endpoint_id.trim().to_string();
    let model_id = model_id.trim().to_string();
    let endpoint = match endpoint_id.parse() {
        Ok(endpoint) => endpoint,
        Err(_) => return mesh_error(StatusCode::BAD_REQUEST, "invalid mesh endpoint id"),
    };
    if validate_model_id(&model_id).is_err() {
        return mesh_error(StatusCode::BAD_REQUEST, "invalid model id");
    }
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    let action = if unload {
        HoldAction::Unload
    } else {
        HoldAction::Load
    };
    let active = match admin
        .store()
        .list_active_holds(Some(&endpoint_id), None, now_ms())
        .await
    {
        Ok(active) => active,
        Err(err) => {
            tracing::error!(error = %err, "failed to read mesh model holds");
            return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
        }
    };
    let switching = admin.registry().model_switching(endpoint);
    let decision = match evaluate_holds(
        &active,
        &model_id,
        action,
        switching.as_ref(),
        &options,
        &actor.owner_id(),
    ) {
        HoldEvaluation::Proceed { release } => (release, Vec::new()),
        HoldEvaluation::NotOwned => {
            return mesh_error_code(
                StatusCode::FORBIDDEN,
                "release_hold names a hold owned by another credential",
                "hold_not_owned",
            );
        }
        HoldEvaluation::Blocked { release, blocking } if options.force => (release, blocking),
        HoldEvaluation::Blocked { blocking, .. } => return model_held_response(blocking),
    };
    let (release, overridden) = decision;
    if !overridden.is_empty() {
        tracing::warn!(
            endpoint_id,
            model_id,
            action = action.as_str(),
            holds = ?overridden.iter().map(|hold| hold.hold_id.as_str()).collect::<Vec<_>>(),
            "administrator forced a mesh lifecycle change over active model holds"
        );
        if let Err(err) = admin
            .store()
            .audit_hold_override(overridden, &actor.owner_id(), action.as_str(), now_ms())
            .await
        {
            tracing::error!(error = %err, "failed to audit forced hold override");
            return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
        }
    }
    let result = if unload {
        admin
            .registry()
            .unload_model(endpoint, model_id.clone())
            .await
    } else {
        admin
            .registry()
            .switch_model_instances(endpoint, model_id.clone(), instances)
            .await
    };
    match result {
        Ok(result) if result.accepted => {
            let mut released_holds = Vec::new();
            for hold_id in release {
                match admin
                    .store()
                    .release_hold(
                        &hold_id,
                        &actor.owner_id(),
                        action.release_reason(),
                        now_ms(),
                    )
                    .await
                {
                    Ok(Some(_)) => released_holds.push(hold_id),
                    Ok(None) => {}
                    Err(err) => {
                        tracing::error!(error = %err, hold_id, "failed to release mesh model hold");
                    }
                }
            }
            json_no_store(
                if result.changed {
                    StatusCode::ACCEPTED
                } else {
                    StatusCode::OK
                },
                &SwitchMeshModelResponse {
                    endpoint_id,
                    model_id,
                    accepted: true,
                    changed: result.changed,
                    error: None,
                    operation: result.operation,
                    released_holds,
                },
            )
        }
        Ok(result) => {
            let status = result.rejection_status();
            let code = rejection_code(&result, status);
            mesh_error_code(
                status,
                result
                    .error
                    .as_deref()
                    .unwrap_or("downstream provider rejected the model switch"),
                code,
            )
        }
        Err(err) => {
            tracing::warn!(error = %err, endpoint_id, model_id, "mesh model switch failed");
            mesh_error_code(
                err.status_code(),
                &err.client_message,
                mesh_failure_code(&err.client_message),
            )
        }
    }
}

/// Machine-readable code for a worker/Fleet refusal. Fleet's own code wins
/// when the worker relayed one (newer workers); older workers only send text,
/// so the code is derived from the message and status.
fn rejection_code(
    result: &crate::mesh::protocol::SwitchModelResponse,
    status: StatusCode,
) -> &'static str {
    const FLEET_CODES: &[&str] = &[
        "operation_in_progress",
        "insufficient_resources",
        "model_not_found",
        "unknown_model",
        "invalid_request",
        "backend_unavailable",
        "switching_unsupported",
        "model_not_switchable",
    ];
    let message = result.error.as_deref().unwrap_or_default();
    let relayed = result.error_code.as_deref().or_else(|| {
        // Older workers fold Fleet's code into the text: "... (code)".
        message
            .rsplit_once('(')
            .and_then(|(_, rest)| rest.strip_suffix(')'))
    });
    if let Some(code) = relayed.and_then(|code| FLEET_CODES.iter().find(|known| **known == code)) {
        return code;
    }
    if message.contains("already changing") {
        return "operation_in_progress";
    }
    // "Fleet is busy or has insufficient GPU capacity" (no Fleet code) is
    // ambiguous and stays the generic conflict below.
    if message.contains("insufficient GPU") && !message.contains("busy or") {
        return "insufficient_resources";
    }
    if message.contains("not configured") {
        return "switching_unsupported";
    }
    if message.contains("not advertised as switchable") {
        return "model_not_switchable";
    }
    match status {
        StatusCode::CONFLICT => "fleet_conflict",
        StatusCode::NOT_FOUND => "model_not_found",
        StatusCode::TOO_MANY_REQUESTS => "fleet_rate_limited",
        StatusCode::BAD_GATEWAY => "fleet_unavailable",
        StatusCode::GATEWAY_TIMEOUT => "fleet_timeout",
        status if status.is_client_error() => "fleet_rejected",
        _ => "fleet_error",
    }
}

/// Machine-readable code for a hub-side mesh failure (`AppError`).
fn mesh_failure_code(message: &str) -> &'static str {
    match message {
        "mesh worker is not connected" | "mesh worker connection is unavailable" => {
            "worker_disconnected"
        }
        "mesh worker is stale" => "worker_stale",
        "mesh worker does not advertise model switching" => "switching_unsupported",
        "model is not advertised as switchable" => "model_not_switchable",
        message if message.contains("timed out") => "worker_timeout",
        message if message.starts_with("failed to open mesh") => "worker_unreachable",
        message if message.starts_with("instances") || message.contains("instance") => {
            "invalid_request"
        }
        _ => "mesh_error",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HoldAction {
    Load,
    Unload,
}

impl HoldAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Load => "load",
            Self::Unload => "unload",
        }
    }

    fn release_reason(self) -> &'static str {
        match self {
            Self::Load => "released_by_load",
            Self::Unload => "released_by_unload",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum HoldEvaluation {
    /// No conflicting hold remains; `release` lists the caller's own holds to
    /// delete once the worker accepts the change.
    Proceed { release: Vec<String> },
    /// `release_hold` named a conflicting hold owned by someone else.
    NotOwned,
    Blocked {
        release: Vec<String>,
        blocking: Vec<ModelHoldRecord>,
    },
}

/// Decides whether active holds on one endpoint allow a lifecycle change.
///
/// An unload conflicts with every hold on the target model. A load of model
/// X conflicts with a hold on another model H only when Fleet would stop H to
/// start X: H is currently active and the node may be switching models
/// exclusively (see [`may_switch_exclusively`]). Loading or rescaling the held
/// model itself never conflicts. Without a live inventory nothing else can be
/// stopped, so only same-model unloads are checked.
fn evaluate_holds(
    active: &[ModelHoldRecord],
    model_id: &str,
    action: HoldAction,
    switching: Option<&ModelSwitchingAdvertisement>,
    options: &LifecycleOptions,
    owner: &str,
) -> HoldEvaluation {
    let load_may_stop_others = switching.is_some_and(may_switch_exclusively);
    let conflicting: Vec<&ModelHoldRecord> = active
        .iter()
        .filter(|hold| match action {
            HoldAction::Unload => hold.model_id == model_id,
            HoldAction::Load => {
                hold.model_id != model_id
                    && load_may_stop_others
                    && switching.is_some_and(|switching| {
                        switching
                            .models
                            .iter()
                            .any(|model| model.id == hold.model_id && model_is_active(model))
                    })
            }
        })
        .collect();
    let mut release = Vec::new();
    let mut blocking = Vec::new();
    for hold in conflicting {
        if options.release_holds.iter().any(|id| id == &hold.hold_id) {
            if hold.owner != owner {
                return HoldEvaluation::NotOwned;
            }
            release.push(hold.hold_id.clone());
        } else {
            blocking.push(hold.clone());
        }
    }
    // An own hold on the unloaded model that was not conflicting cannot
    // exist, so only conflicting holds are ever released.
    if blocking.is_empty() {
        HoldEvaluation::Proceed { release }
    } else {
        HoldEvaluation::Blocked { release, blocking }
    }
}

/// Whether loading one model on this node may make Fleet stop the others.
///
/// lil-fleet only stops other models on activation when it runs without
/// `runtime.concurrent_deployments` ("exclusive switching"). With concurrent
/// deployments it places the new instances on GPUs no other model holds, or
/// refuses the load as insufficient capacity; it never evicts a running model
/// to make room. Fleet accepts per-model `placement.gpu_count` only together
/// with concurrent deployments, so any advertised `gpu_count > 0` proves the
/// node is not switching exclusively, as does seeing two models active at
/// once. Without either signal (an older Fleet, static GPU lists, another
/// provider), every load is conservatively treated as exclusive.
fn may_switch_exclusively(switching: &ModelSwitchingAdvertisement) -> bool {
    if switching.provider != "lil-fleet" {
        return true;
    }
    let places_by_gpu_count = switching.models.iter().any(|model| model.gpu_count > 0);
    let visibly_concurrent = switching
        .models
        .iter()
        .filter(|model| model_is_active(model))
        .count()
        >= 2;
    !(places_by_gpu_count || visibly_concurrent)
}

/// Fleet runs (or is starting) at least one instance of this profile.
fn model_is_active(model: &SwitchableModelAdvertisement) -> bool {
    !model.phase.eq_ignore_ascii_case("unloaded")
        || model.desired_state.eq_ignore_ascii_case("ready")
        || model.ready_instances > 0
        || !model.instances.is_empty()
}

fn model_held_response(blocking: Vec<ModelHoldRecord>) -> Response {
    json_no_store(
        StatusCode::CONFLICT,
        &MeshModelHeldError {
            error: "model is held by an active lease".to_string(),
            code: "model_held".to_string(),
            holders: blocking.into_iter().map(MeshModelHold::from).collect(),
        },
    )
}

fn valid_hold_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_HOLD_ID_BYTES
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn valid_holder(holder: &str) -> bool {
    !holder.is_empty()
        && holder.len() <= MAX_HOLDER_BYTES
        && holder.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'@' | b'/' | b'-')
        })
}

fn valid_hold_ttl(ttl_secs: u64) -> bool {
    (MIN_HOLD_TTL_SECS..=MAX_HOLD_TTL_SECS).contains(&ttl_secs)
}

/// Validated `(endpoint, model)` path pair shared by the hold routes.
fn hold_target(
    endpoint_id: &str,
    model_id: &str,
) -> Result<(iroh::EndpointId, String, String), &'static str> {
    let endpoint_id = endpoint_id.trim().to_string();
    let model_id = model_id.trim().to_string();
    let endpoint = endpoint_id
        .parse()
        .map_err(|_| "invalid mesh endpoint id")?;
    if validate_model_id(&model_id).is_err() {
        return Err("invalid model id");
    }
    Ok((endpoint, endpoint_id, model_id))
}

#[utoipa::path(
    get,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/models/{model_id}/holds",
    tag = "dashboard",
    params(
        ("endpoint_id" = String, Path, description = "Iroh endpoint id."),
        ("model_id" = String, Path, description = "Fleet model (profile) id.")
    ),
    responses(
        (status = 200, body = MeshModelHoldsResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn list_model_holds(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
    Path((endpoint_id, model_id)): Path<(String, String)>,
) -> Response {
    if !actor.allows(ManagementPermission::FleetModelsRead) {
        return mesh_error(StatusCode::FORBIDDEN, "management permission denied");
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    let (_, endpoint_id, model_id) = match hold_target(&endpoint_id, &model_id) {
        Ok(target) => target,
        Err(message) => return mesh_error(StatusCode::BAD_REQUEST, message),
    };
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    match admin
        .store()
        .list_active_holds(Some(&endpoint_id), Some(&model_id), now_ms())
        .await
    {
        Ok(holds) => json_no_store(
            StatusCode::OK,
            &MeshModelHoldsResponse {
                endpoint_id,
                model_id,
                holds: holds.into_iter().map(MeshModelHold::from).collect(),
            },
        ),
        Err(err) => {
            tracing::error!(error = %err, "failed to read mesh model holds");
            mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable")
        }
    }
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/models/{model_id}/holds",
    tag = "dashboard",
    params(
        ("endpoint_id" = String, Path, description = "Iroh endpoint id."),
        ("model_id" = String, Path, description = "Fleet model (profile) id.")
    ),
    request_body = CreateMeshModelHoldRequest,
    responses(
        (status = 201, body = MeshModelHold),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError),
        (status = 409, body = crate::openapi::DashboardError, description = "`too_many_holds`: the per-model active-hold cap was reached.")
    ),
    security(("session" = []))
)]
pub async fn create_model_hold(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
    Path((endpoint_id, model_id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    if !actor.allows(ManagementPermission::FleetModelsHold) {
        return mesh_error(StatusCode::FORBIDDEN, "management permission denied");
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    let (endpoint, endpoint_id, model_id) = match hold_target(&endpoint_id, &model_id) {
        Ok(target) => target,
        Err(message) => return mesh_error(StatusCode::BAD_REQUEST, message),
    };
    let Ok(request) = serde_json::from_slice::<CreateMeshModelHoldRequest>(&body) else {
        return mesh_error(
            StatusCode::BAD_REQUEST,
            "hold body must be JSON with holder and ttl_secs",
        );
    };
    if !valid_holder(&request.holder) {
        return mesh_error(
            StatusCode::BAD_REQUEST,
            "holder must be 1-128 characters of [A-Za-z0-9._:@/-]",
        );
    }
    if !valid_hold_ttl(request.ttl_secs) {
        return mesh_error(
            StatusCode::BAD_REQUEST,
            "ttl_secs must be between 60 and 86400",
        );
    }
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    match admin.store().node_authorization(&endpoint_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return mesh_error(StatusCode::NOT_FOUND, "mesh node not found"),
        Err(err) => {
            tracing::error!(error = %err, "failed to read mesh node");
            return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
        }
    }
    // A connected worker's inventory is authoritative; an offline worker's
    // profile may still be held (e.g. across a controller restart).
    if let Some(switching) = admin.registry().model_switching(endpoint)
        && !switching.models.iter().any(|model| model.id == model_id)
    {
        return mesh_error(
            StatusCode::NOT_FOUND,
            "model is not advertised as switchable",
        );
    }
    let ttl_ms = i64::try_from(request.ttl_secs * 1000).unwrap_or(i64::MAX);
    match admin
        .store()
        .create_hold(
            &endpoint_id,
            &model_id,
            &request.holder,
            &actor.owner_id(),
            ttl_ms,
            now_ms(),
        )
        .await
    {
        Ok(CreateHoldOutcome::Created(hold)) => {
            tracing::info!(
                endpoint_id,
                model_id,
                hold_id = %hold.hold_id,
                holder = %hold.holder,
                expires_at_ms = hold.expires_at_ms,
                "mesh model hold created"
            );
            json_no_store(StatusCode::CREATED, &MeshModelHold::from(hold))
        }
        Ok(CreateHoldOutcome::TooMany) => mesh_error_code(
            StatusCode::CONFLICT,
            "too many active holds on this model",
            "too_many_holds",
        ),
        Err(err) => {
            tracing::error!(error = %err, "failed to create mesh model hold");
            mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create hold")
        }
    }
}

/// Loads an active hold addressed by the full route and checks ownership.
async fn owned_hold(
    admin: &crate::mesh::controller::MeshAdmin,
    endpoint_id: &str,
    model_id: &str,
    hold_id: &str,
    actor: &ManagementActor,
    admin_override: bool,
) -> Result<ModelHoldRecord, Response> {
    if !valid_hold_id(hold_id) {
        return Err(mesh_error(StatusCode::BAD_REQUEST, "invalid hold id"));
    }
    let hold = match admin.store().active_hold(hold_id, now_ms()).await {
        Ok(Some(hold)) if hold.endpoint_id == endpoint_id && hold.model_id == model_id => hold,
        Ok(_) => return Err(mesh_error(StatusCode::NOT_FOUND, "hold not found")),
        Err(err) => {
            tracing::error!(error = %err, "failed to read mesh model hold");
            return Err(mesh_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "mesh state unavailable",
            ));
        }
    };
    if hold.owner != actor.owner_id() && !admin_override {
        return Err(mesh_error_code(
            StatusCode::FORBIDDEN,
            "hold is owned by another credential",
            "hold_not_owned",
        ));
    }
    Ok(hold)
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/models/{model_id}/holds/{hold_id}/renew",
    tag = "dashboard",
    params(
        ("endpoint_id" = String, Path, description = "Iroh endpoint id."),
        ("model_id" = String, Path, description = "Fleet model (profile) id."),
        ("hold_id" = String, Path, description = "Hold id returned at creation.")
    ),
    request_body = RenewMeshModelHoldRequest,
    responses(
        (status = 200, body = MeshModelHold),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError, description = "Unknown or already expired hold.")
    ),
    security(("session" = []))
)]
pub async fn renew_model_hold(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
    Path((endpoint_id, model_id, hold_id)): Path<(String, String, String)>,
    body: Bytes,
) -> Response {
    if !actor.allows(ManagementPermission::FleetModelsHold) {
        return mesh_error(StatusCode::FORBIDDEN, "management permission denied");
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    let (_, endpoint_id, model_id) = match hold_target(&endpoint_id, &model_id) {
        Ok(target) => target,
        Err(message) => return mesh_error(StatusCode::BAD_REQUEST, message),
    };
    let Ok(request) = serde_json::from_slice::<RenewMeshModelHoldRequest>(&body) else {
        return mesh_error(
            StatusCode::BAD_REQUEST,
            "renew body must be JSON with ttl_secs",
        );
    };
    if !valid_hold_ttl(request.ttl_secs) {
        return mesh_error(
            StatusCode::BAD_REQUEST,
            "ttl_secs must be between 60 and 86400",
        );
    }
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    if let Err(response) =
        owned_hold(&admin, &endpoint_id, &model_id, &hold_id, &actor, false).await
    {
        return response;
    }
    let ttl_ms = i64::try_from(request.ttl_secs * 1000).unwrap_or(i64::MAX);
    match admin
        .store()
        .renew_hold(&hold_id, &actor.owner_id(), ttl_ms, now_ms())
        .await
    {
        Ok(Some(hold)) => json_no_store(StatusCode::OK, &MeshModelHold::from(hold)),
        Ok(None) => mesh_error(StatusCode::NOT_FOUND, "hold not found"),
        Err(err) => {
            tracing::error!(error = %err, "failed to renew mesh model hold");
            mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "failed to renew hold")
        }
    }
}

#[utoipa::path(
    delete,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/models/{model_id}/holds/{hold_id}",
    tag = "dashboard",
    params(
        ("endpoint_id" = String, Path, description = "Iroh endpoint id."),
        ("model_id" = String, Path, description = "Fleet model (profile) id."),
        ("hold_id" = String, Path, description = "Hold id returned at creation."),
        ("force" = Option<bool>, Query, description = "Administrator dashboard sessions only: release another credential's hold (audited).")
    ),
    responses(
        (status = 200, body = ReleaseMeshModelHoldResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError, description = "Unknown or already expired hold.")
    ),
    security(("session" = []))
)]
pub async fn release_model_hold(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
    channel: Option<Extension<ManagementChannel>>,
    Path((endpoint_id, model_id, hold_id)): Path<(String, String, String)>,
    Query(query): Query<Vec<(String, String)>>,
) -> Response {
    if !actor.allows(ManagementPermission::FleetModelsHold) {
        return mesh_error(StatusCode::FORBIDDEN, "management permission denied");
    }
    let options = match parse_lifecycle_options(&query) {
        Ok(options) => options,
        Err(message) => return mesh_error(StatusCode::BAD_REQUEST, message),
    };
    let channel = channel.map(|Extension(channel)| channel);
    if options.force && channel != Some(ManagementChannel::AdminSession) {
        return mesh_error_code(
            StatusCode::FORBIDDEN,
            "force requires an administrator dashboard session",
            "force_not_allowed",
        );
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    let (_, endpoint_id, model_id) = match hold_target(&endpoint_id, &model_id) {
        Ok(target) => target,
        Err(message) => return mesh_error(StatusCode::BAD_REQUEST, message),
    };
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    let hold = match owned_hold(
        &admin,
        &endpoint_id,
        &model_id,
        &hold_id,
        &actor,
        options.force,
    )
    .await
    {
        Ok(hold) => hold,
        Err(response) => return response,
    };
    let reason = if hold.owner == actor.owner_id() {
        "released"
    } else {
        tracing::warn!(
            endpoint_id,
            model_id,
            hold_id,
            "administrator force-released a mesh model hold"
        );
        "force_released"
    };
    match admin
        .store()
        .release_hold(&hold_id, &actor.owner_id(), reason, now_ms())
        .await
    {
        Ok(Some(_)) => json_no_store(
            StatusCode::OK,
            &ReleaseMeshModelHoldResponse {
                hold_id,
                released: true,
            },
        ),
        Ok(None) => mesh_error(StatusCode::NOT_FOUND, "hold not found"),
        Err(err) => {
            tracing::error!(error = %err, "failed to release mesh model hold");
            mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "failed to release hold")
        }
    }
}

#[utoipa::path(
    get,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/operations/{operation_id}",
    tag = "dashboard",
    params(
        ("endpoint_id" = String, Path, description = "Iroh endpoint id."),
        ("operation_id" = String, Path, description = "Fleet operation id from a load/unload response.")
    ),
    responses(
        (status = 200, body = MeshOperationResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError, description = "`operation_not_found`: Fleet no longer knows the operation (finished operations expire)."),
        (status = 501, body = crate::openapi::DashboardError, description = "`worker_unsupported`: the worker predates operation lookups."),
        (status = 502, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn get_node_operation(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
    Path((endpoint_id, operation_id)): Path<(String, String)>,
) -> Response {
    if !actor.allows(ManagementPermission::FleetModelsRead) {
        return mesh_error(StatusCode::FORBIDDEN, "management permission denied");
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    let endpoint_id = endpoint_id.trim().to_string();
    let endpoint = match endpoint_id.parse() {
        Ok(endpoint) => endpoint,
        Err(_) => return mesh_error(StatusCode::BAD_REQUEST, "invalid mesh endpoint id"),
    };
    if validate_operation_id(&operation_id).is_err() {
        return mesh_error(StatusCode::BAD_REQUEST, "invalid operation id");
    }
    match admin
        .registry()
        .fleet_operation(endpoint, operation_id.clone())
        .await
    {
        Ok(response) => match response.operation {
            Some(operation) => json_no_store(
                StatusCode::OK,
                &MeshOperationResponse {
                    endpoint_id,
                    operation,
                },
            ),
            None => match response.error_status {
                Some(404) => mesh_error_code(
                    StatusCode::NOT_FOUND,
                    "Fleet operation was not found",
                    "operation_not_found",
                ),
                Some(400) => mesh_error(StatusCode::BAD_REQUEST, "invalid operation id"),
                _ => mesh_error_code(
                    StatusCode::BAD_GATEWAY,
                    response
                        .error
                        .as_deref()
                        .unwrap_or("mesh worker did not return the operation"),
                    "fleet_unavailable",
                ),
            },
        },
        Err(FleetOperationLookupError::Unsupported) => mesh_error_code(
            StatusCode::NOT_IMPLEMENTED,
            "mesh worker does not support operation lookups; upgrade the worker",
            "worker_unsupported",
        ),
        Err(FleetOperationLookupError::Mesh(err)) => {
            tracing::warn!(error = %err, endpoint_id, operation_id, "mesh operation lookup failed");
            mesh_error_code(
                err.status_code(),
                &err.client_message,
                mesh_failure_code(&err.client_message),
            )
        }
    }
}

fn fleet_model_permission(unload: bool) -> ManagementPermission {
    if unload {
        ManagementPermission::FleetModelsUnload
    } else {
        ManagementPermission::FleetModelsLoad
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MeshLoadBodyError {
    InvalidShape,
    InvalidInstances,
}

impl MeshLoadBodyError {
    fn into_response(self) -> Response {
        match self {
            Self::InvalidShape => mesh_error(
                StatusCode::BAD_REQUEST,
                "load body must be empty or JSON with only instances",
            ),
            Self::InvalidInstances => mesh_error(
                StatusCode::BAD_REQUEST,
                "instances must be between 1 and 64",
            ),
        }
    }
}

fn parse_mesh_model_load_body(body: &[u8]) -> Result<Option<u32>, MeshLoadBodyError> {
    if body.is_empty() {
        return Ok(None);
    }
    let request: MeshModelLoadRequest =
        serde_json::from_slice(body).map_err(|_| MeshLoadBodyError::InvalidShape)?;
    if !(1..=MAX_SWITCH_MODEL_INSTANCES).contains(&request.instances) {
        return Err(MeshLoadBodyError::InvalidInstances);
    }
    Ok(Some(request.instances))
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/models/disable",
    tag = "dashboard",
    request_body = MeshModelOverrideRequest,
    responses(
        (status = 200, body = SetMeshModelResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn disable_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    headers: HeaderMap,
    Json(body): Json<MeshModelOverrideRequest>,
) -> Response {
    set_model_disabled(gateway, auth, session, headers, body, true).await
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/models/enable",
    tag = "dashboard",
    request_body = MeshModelOverrideRequest,
    responses(
        (status = 200, body = SetMeshModelResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn enable_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    headers: HeaderMap,
    Json(body): Json<MeshModelOverrideRequest>,
) -> Response {
    set_model_disabled(gateway, auth, session, headers, body, false).await
}

async fn set_node_enabled(
    gateway: Arc<Gateway>,
    auth: Arc<DashboardAuth>,
    session: AuthSession,
    headers: HeaderMap,
    endpoint_id: String,
    enabled: bool,
) -> Response {
    if let Some(response) = authorize_mesh_mutation(auth.as_ref(), &session, &headers).await {
        return response;
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    let endpoint_id = endpoint_id.trim().to_string();
    let endpoint = match endpoint_id.parse() {
        Ok(endpoint) => endpoint,
        Err(_) => return mesh_error(StatusCode::BAD_REQUEST, "invalid mesh endpoint id"),
    };
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    match admin.store().set_node_enabled(&endpoint_id, enabled).await {
        Ok(updated) => {
            if !updated {
                return mesh_error(StatusCode::NOT_FOUND, "mesh node not found");
            }
            let evicted = if updated && !enabled {
                admin.disable_live_node(endpoint)
            } else {
                false
            };
            json_no_store(
                StatusCode::OK,
                &SetMeshNodeResponse {
                    updated,
                    endpoint_id,
                    enabled,
                    evicted,
                },
            )
        }
        Err(err) => {
            tracing::error!(error = %err, "failed to update mesh node");
            mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update node")
        }
    }
}

async fn set_model_disabled(
    gateway: Arc<Gateway>,
    auth: Arc<DashboardAuth>,
    session: AuthSession,
    headers: HeaderMap,
    body: MeshModelOverrideRequest,
    disabled: bool,
) -> Response {
    if let Some(response) = authorize_mesh_mutation(auth.as_ref(), &session, &headers).await {
        return response;
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    let target = match ModelOverrideTarget::try_from(body) {
        Ok(target) => target,
        Err(message) => return mesh_error(StatusCode::BAD_REQUEST, message),
    };
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    if disabled
        && !admin
            .registry()
            .has_resource_model(target.endpoint, &target.resource_id, &target.model)
    {
        return mesh_error(
            StatusCode::NOT_FOUND,
            "mesh endpoint/resource/model is not currently advertised",
        );
    }
    if !disabled {
        let exists = match admin.store().list_disabled_models().await {
            Ok(records) => records.into_iter().any(|record| {
                record.endpoint_id == target.endpoint_id
                    && record.resource_id == target.resource_id
                    && record.model.eq_ignore_ascii_case(&target.model)
            }),
            Err(err) => {
                tracing::error!(error = %err, "failed to read mesh model overrides");
                return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
            }
        };
        if !exists {
            return mesh_error(StatusCode::NOT_FOUND, "mesh model override not found");
        }
    }
    let changed = admin
        .store()
        .set_model_disabled(
            &target.endpoint_id,
            &target.resource_id,
            &target.model,
            disabled,
            now_ms(),
        )
        .await;
    match changed {
        Ok(updated) => {
            admin.apply_model_disabled(
                target.endpoint,
                &target.resource_id,
                &target.model,
                disabled,
            );
            json_no_store(
                StatusCode::OK,
                &SetMeshModelResponse {
                    updated,
                    endpoint_id: target.endpoint_id,
                    resource_id: target.resource_id,
                    model: target.model,
                    disabled,
                },
            )
        }
        Err(err) => {
            tracing::error!(error = %err, "failed to update mesh model override");
            mesh_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to update model override",
            )
        }
    }
}

pub(crate) async fn authorize_mesh_mutation(
    auth: &DashboardAuth,
    session: &AuthSession,
    headers: &HeaderMap,
) -> Option<Response> {
    if let Some(response) = authorize_mesh_admin_read(auth, session, headers) {
        return Some(response);
    }
    if let Err(denied) = auth.authorize_mutation(headers) {
        return Some(mesh_error(denied.status(), denied.message()));
    }
    None
}

pub(crate) fn authorize_mesh_admin_read(
    auth: &DashboardAuth,
    session: &AuthSession,
    headers: &HeaderMap,
) -> Option<Response> {
    if auth.delegated_session(headers).is_some() {
        return Some(mesh_error(
            StatusCode::FORBIDDEN,
            "administrator role required",
        ));
    }
    if session
        .user
        .as_ref()
        .map_or_else(|| session.bootstrap_admin(), |user| user.is_admin)
    {
        None
    } else {
        Some(mesh_error(
            StatusCode::FORBIDDEN,
            "administrator role required",
        ))
    }
}

fn validate_join_key_request(
    body: &CreateMeshJoinKeyRequest,
) -> Option<(Option<i64>, Option<i64>)> {
    let max_uses = body.max_uses.unwrap_or(Some(DEFAULT_JOIN_KEY_MAX_USES));
    let expires_in_secs = body
        .expires_in_secs
        .unwrap_or(Some(DEFAULT_JOIN_KEY_TTL_SECS));
    if max_uses.is_some_and(|uses| uses <= 0) || expires_in_secs.is_some_and(|seconds| seconds <= 0)
    {
        return None;
    }
    let expires_at_ms =
        expires_in_secs.map(|seconds| now_ms().saturating_add(seconds.saturating_mul(1000)));
    Some((expires_at_ms, max_uses))
}

#[derive(Debug)]
struct ModelOverrideTarget {
    endpoint: iroh::EndpointId,
    endpoint_id: String,
    resource_id: String,
    model: String,
}

impl TryFrom<MeshModelOverrideRequest> for ModelOverrideTarget {
    type Error = &'static str;

    fn try_from(body: MeshModelOverrideRequest) -> Result<Self, Self::Error> {
        let endpoint_id = body.endpoint_id.trim().to_string();
        let resource_id = body.resource_id.trim().to_string();
        let model = body.model.trim().to_ascii_lowercase();
        if endpoint_id.is_empty() || resource_id.is_empty() || model.is_empty() {
            return Err("endpoint_id, resource_id, and model are required");
        }
        validate_resource_id(&resource_id).map_err(|_| "invalid resource_id")?;
        validate_model_id(&model).map_err(|_| "invalid model")?;
        let endpoint = endpoint_id
            .parse()
            .map_err(|_| "invalid mesh endpoint id")?;
        Ok(Self {
            endpoint,
            endpoint_id,
            resource_id,
            model,
        })
    }
}

fn mesh_error(status: StatusCode, message: &str) -> Response {
    json_no_store(status, &serde_json::json!({ "error": message }))
}

fn mesh_error_code(status: StatusCode, message: &str, code: &str) -> Response {
    json_no_store(
        status,
        &serde_json::json!({ "error": message, "code": code }),
    )
}

impl From<ModelHoldRecord> for MeshModelHold {
    fn from(value: ModelHoldRecord) -> Self {
        Self {
            hold_id: value.hold_id,
            holder: value.holder,
            expires_at_ms: value.expires_at_ms,
            endpoint_id: value.endpoint_id,
            model_id: value.model_id,
            created_at_ms: value.created_at_ms,
        }
    }
}

impl From<JoinKeyRecord> for MeshJoinKey {
    fn from(value: JoinKeyRecord) -> Self {
        Self {
            id: value.id,
            label: value.label,
            enabled: value.enabled,
            created_at_ms: value.created_at_ms,
            expires_at_ms: value.expires_at_ms,
            max_uses: value.max_uses,
            use_count: value.use_count,
        }
    }
}

impl From<CreatedJoinKey> for CreateMeshJoinKeyResponse {
    fn from(value: CreatedJoinKey) -> Self {
        let token = value.token;
        Self {
            join_key: MeshJoinKey {
                id: value.id,
                label: value.label,
                enabled: true,
                created_at_ms: value.created_at_ms,
                expires_at_ms: value.expires_at_ms,
                max_uses: value.max_uses,
                use_count: 0,
            },
            token,
        }
    }
}

impl From<MeshNodeRecord> for MeshNode {
    fn from(value: MeshNodeRecord) -> Self {
        Self {
            endpoint_id: value.endpoint_id,
            label: value.label,
            enabled: value.enabled,
            joined_at_ms: value.joined_at_ms,
            join_key_id: value.join_key_id,
            last_seen_at_ms: value.last_seen_at_ms,
            model_switching: None,
        }
    }
}

impl From<ModelSwitchingAdvertisement> for MeshModelSwitching {
    fn from(value: ModelSwitchingAdvertisement) -> Self {
        Self {
            provider: value.provider,
            models: value
                .models
                .into_iter()
                .map(|model| MeshSwitchableModel {
                    id: model.id,
                    description: model.description,
                    phase: model.phase,
                    desired_state: model.desired_state,
                    gpu_count: model.gpu_count,
                    assigned_gpus: model.assigned_gpus,
                    max_instances: model.max_instances,
                    desired_instances: model.desired_instances,
                    ready_instances: model.ready_instances,
                    instances: model
                        .instances
                        .into_iter()
                        .map(MeshSwitchableModelInstance::from)
                        .collect(),
                    holds: Vec::new(),
                })
                .collect(),
            revision: value.revision,
        }
    }
}

impl From<SwitchableModelInstanceAdvertisement> for MeshSwitchableModelInstance {
    fn from(value: SwitchableModelInstanceAdvertisement) -> Self {
        Self {
            instance_id: value.instance_id,
            index: value.index,
            port: value.port,
            phase: value.phase,
            assigned_gpus: value.assigned_gpus,
            container_status: value.container_status,
            last_error: value.last_error,
            health: value.health,
        }
    }
}

impl From<DisabledMeshModelRecord> for MeshDisabledModel {
    fn from(value: DisabledMeshModelRecord) -> Self {
        Self {
            endpoint_id: value.endpoint_id,
            resource_id: value.resource_id,
            model: value.model,
            disabled_at_ms: value.disabled_at_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard_auth::CSRF_COOKIE;
    use crate::dashboard_auth::CSRF_HEADER;
    use crate::dashboard_auth::DashboardEnv;
    use crate::dashboard_auth::SESSION_COOKIE;
    use axum::http::HeaderValue;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use iroh::SecretKey;
    use std::net::SocketAddr;

    fn test_auth() -> Arc<DashboardAuth> {
        DashboardAuth::from_env(
            "127.0.0.1:0".parse::<SocketAddr>().expect("socket addr"),
            &DashboardEnv {
                token: Some("dashboard-token".to_string()),
                session_key_b64: Some(STANDARD.encode([7u8; 32])),
                public_origin: None,
                allow_insecure: false,
                allow_mutations: true,
                ..Default::default()
            },
        )
        .expect("auth")
        .auth
    }

    fn user_session(is_admin: bool) -> AuthSession {
        AuthSession {
            exp: u64::MAX,
            user: Some(crate::accounts::SessionUser {
                id: "user-id".to_string(),
                username: "operator".to_string(),
                is_admin,
            }),
            kind: crate::dashboard_auth::AuthSessionKind::User,
        }
    }

    fn bootstrap_session() -> AuthSession {
        AuthSession {
            exp: u64::MAX,
            user: None,
            kind: crate::dashboard_auth::AuthSessionKind::DashboardToken,
        }
    }

    fn csrf_headers(auth: &DashboardAuth) -> HeaderMap {
        let csrf = auth.issue_csrf_token();
        let mut headers = HeaderMap::new();
        headers.insert(CSRF_HEADER, HeaderValue::from_str(&csrf).expect("csrf"));
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str(&format!("{CSRF_COOKIE}={csrf}")).expect("cookie"),
        );
        headers
    }

    #[test]
    fn remote_fleet_actions_require_distinct_management_permissions() {
        assert_eq!(
            fleet_model_permission(false),
            ManagementPermission::FleetModelsLoad
        );
        assert_eq!(
            fleet_model_permission(true),
            ManagementPermission::FleetModelsUnload
        );

        let read_only = ManagementActor::Delegated {
            session_id: "sess_mesh_fleet_read".into(),
            principal_id: "usr_mesh_fleet_read".into(),
            key_id: "key_mesh_fleet_read".into(),
            permissions: Arc::from([ManagementPermission::FleetModelsRead]),
        };
        assert!(read_only.allows(ManagementPermission::FleetModelsRead));
        assert!(!read_only.allows(ManagementPermission::FleetModelsLoad));
        assert!(!read_only.allows(ManagementPermission::FleetModelsUnload));
    }

    #[test]
    fn mesh_load_body_is_empty_legacy_or_strict_instances() {
        assert_eq!(parse_mesh_model_load_body(b"").unwrap(), None);
        assert_eq!(
            parse_mesh_model_load_body(br#"{"instances":2}"#).unwrap(),
            Some(2)
        );
        assert!(parse_mesh_model_load_body(br#"{"instances":0}"#).is_err());
        assert!(parse_mesh_model_load_body(br#"{"instances":65}"#).is_err());
        assert!(parse_mesh_model_load_body(br#"{"instances":1,"extra":true}"#).is_err());
        assert!(parse_mesh_model_load_body(br#"{}"#).is_err());
    }

    #[test]
    fn model_override_target_trims_and_canonicalizes_model() {
        let endpoint_id = SecretKey::generate().public().to_string();

        let target = ModelOverrideTarget::try_from(MeshModelOverrideRequest {
            endpoint_id: format!(" {endpoint_id} "),
            resource_id: " gpu-a ".to_string(),
            model: " QWEN-Flash ".to_string(),
        })
        .expect("valid target");

        assert_eq!(target.endpoint_id, endpoint_id);
        assert_eq!(target.resource_id, "gpu-a");
        assert_eq!(target.model, "qwen-flash");
    }

    #[test]
    fn model_override_target_rejects_blank_and_invalid_endpoint() {
        assert_eq!(
            ModelOverrideTarget::try_from(MeshModelOverrideRequest {
                endpoint_id: " ".to_string(),
                resource_id: "gpu".to_string(),
                model: "qwen".to_string(),
            })
            .expect_err("blank endpoint rejected"),
            "endpoint_id, resource_id, and model are required"
        );
        assert_eq!(
            ModelOverrideTarget::try_from(MeshModelOverrideRequest {
                endpoint_id: "not-an-endpoint".to_string(),
                resource_id: "gpu".to_string(),
                model: "qwen".to_string(),
            })
            .expect_err("invalid endpoint rejected"),
            "invalid mesh endpoint id"
        );
    }

    #[test]
    fn join_key_request_rejects_non_positive_limits() {
        assert!(
            validate_join_key_request(&CreateMeshJoinKeyRequest {
                label: None,
                max_uses: Some(Some(0)),
                expires_in_secs: None,
            })
            .is_none()
        );
        assert!(
            validate_join_key_request(&CreateMeshJoinKeyRequest {
                label: None,
                max_uses: None,
                expires_in_secs: Some(Some(-1)),
            })
            .is_none()
        );
    }

    #[test]
    fn join_keys_default_to_single_use_and_a_day_but_allow_explicit_unlimited() {
        let parse = |json: serde_json::Value| -> CreateMeshJoinKeyRequest {
            serde_json::from_value(json).expect("request")
        };

        let before = now_ms();
        let (expires_at_ms, max_uses) =
            validate_join_key_request(&parse(serde_json::json!({}))).expect("defaults");
        assert_eq!(max_uses, Some(DEFAULT_JOIN_KEY_MAX_USES));
        let expires_at_ms = expires_at_ms.expect("default expiry");
        assert!(expires_at_ms >= before + DEFAULT_JOIN_KEY_TTL_SECS * 1000);
        assert!(expires_at_ms <= now_ms() + DEFAULT_JOIN_KEY_TTL_SECS * 1000);

        let explicit = parse(serde_json::json!({"max_uses": 5, "expires_in_secs": 60}));
        let (expires_at_ms, max_uses) = validate_join_key_request(&explicit).expect("explicit");
        assert_eq!(max_uses, Some(5));
        assert!(expires_at_ms.unwrap() <= now_ms() + 60_000);

        let unlimited = parse(serde_json::json!({"max_uses": null, "expires_in_secs": null}));
        assert_eq!(unlimited.max_uses, Some(None));
        assert_eq!(
            validate_join_key_request(&unlimited).expect("unlimited"),
            (None, None)
        );
    }

    #[test]
    fn model_override_target_rejects_oversized_identifiers() {
        let endpoint_id = SecretKey::generate().public().to_string();
        assert_eq!(
            ModelOverrideTarget::try_from(MeshModelOverrideRequest {
                endpoint_id: endpoint_id.clone(),
                resource_id: "r".repeat(crate::mesh::protocol::MAX_RESOURCE_ID_BYTES + 1),
                model: "qwen".to_string(),
            })
            .expect_err("oversized resource id rejected"),
            "invalid resource_id"
        );
        assert_eq!(
            ModelOverrideTarget::try_from(MeshModelOverrideRequest {
                endpoint_id,
                resource_id: "gpu".to_string(),
                model: "m".repeat(crate::mesh::protocol::MAX_MODEL_ID_BYTES + 1),
            })
            .expect_err("oversized model id rejected"),
            "invalid model"
        );
    }

    #[test]
    fn mesh_admin_read_rejects_non_admin_user() {
        let auth = test_auth();
        let headers = HeaderMap::new();

        let response = authorize_mesh_admin_read(&auth, &user_session(false), &headers)
            .expect("non-admin rejected");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn mesh_admin_read_allows_admin_user_and_bootstrap_session() {
        let auth = test_auth();
        let headers = HeaderMap::new();

        assert!(authorize_mesh_admin_read(&auth, &user_session(true), &headers).is_none());
        assert!(authorize_mesh_admin_read(&auth, &bootstrap_session(), &headers).is_none());
    }

    #[test]
    fn mesh_admin_read_rejects_delegated_management_session() {
        let auth = test_auth();
        let cookie = auth.issue_delegated_session("delegated-session", u64::MAX - 1);
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str(&format!("{SESSION_COOKIE}={cookie}")).expect("cookie"),
        );

        let response = authorize_mesh_admin_read(&auth, &bootstrap_session(), &headers)
            .expect("delegated session rejected");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn mesh_mutation_keeps_csrf_gate_for_bootstrap_session() {
        let auth = test_auth();

        assert!(
            authorize_mesh_mutation(&auth, &bootstrap_session(), &csrf_headers(&auth))
                .await
                .is_none()
        );
        assert_eq!(
            authorize_mesh_mutation(&auth, &bootstrap_session(), &HeaderMap::new())
                .await
                .expect("missing csrf rejected")
                .status(),
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn mesh_mutation_rejects_delegated_session_even_with_csrf() {
        let auth = test_auth();
        let mut headers = csrf_headers(&auth);
        let cookie = auth.issue_delegated_session("delegated-session", u64::MAX - 1);
        let csrf_cookie = headers
            .get(axum::http::header::COOKIE)
            .and_then(|value| value.to_str().ok())
            .expect("csrf cookie");
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str(&format!("{SESSION_COOKIE}={cookie}; {csrf_cookie}"))
                .expect("cookie"),
        );

        let response = authorize_mesh_mutation(&auth, &bootstrap_session(), &headers)
            .await
            .expect("delegated rejected");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    // ---- Model holds / lifecycle operations -------------------------------

    struct MeshFixture {
        gateway: Arc<Gateway>,
        dir: std::path::PathBuf,
        endpoint: iroh::EndpointId,
        endpoint_id: String,
    }

    impl Drop for MeshFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// A profile on an exclusive-switching Fleet (no per-model GPU placement).
    fn switchable(id: &str, phase: &str, desired: &str) -> SwitchableModelAdvertisement {
        SwitchableModelAdvertisement::legacy(id, None, phase, desired, 0, Vec::new())
    }

    /// A profile Fleet places on `gpu_count` free GPUs (concurrent deployments).
    fn placed(
        id: &str,
        phase: &str,
        desired: &str,
        gpu_count: u32,
        assigned_gpus: Vec<u32>,
    ) -> SwitchableModelAdvertisement {
        SwitchableModelAdvertisement::legacy(id, None, phase, desired, gpu_count, assigned_gpus)
    }

    fn fleet_inventory(models: Vec<SwitchableModelAdvertisement>) -> ModelSwitchingAdvertisement {
        ModelSwitchingAdvertisement {
            provider: "lil-fleet".into(),
            models,
            revision: 1,
        }
    }

    fn hold_on(model: &str, hold_id: &str, owner: &str) -> ModelHoldRecord {
        ModelHoldRecord {
            hold_id: hold_id.into(),
            endpoint_id: "endpoint".into(),
            model_id: model.into(),
            holder: "harbor/run-1".into(),
            owner: owner.into(),
            created_at_ms: 1,
            expires_at_ms: i64::MAX,
        }
    }

    /// The incident shape: an 8-GPU node with a held 4-GPU profile on GPUs
    /// 0,1,6,7, a second 4-GPU profile just unloaded, and a third to load.
    fn split_node() -> ModelSwitchingAdvertisement {
        fleet_inventory(vec![
            placed("qwen", "ready", "ready", 4, vec![0, 1, 6, 7]),
            placed("deepseek", "unloaded", "unloaded", 4, Vec::new()),
            placed("mimo", "unloaded", "unloaded", 4, Vec::new()),
        ])
    }

    fn evaluate_load(
        holds: &[ModelHoldRecord],
        model: &str,
        switching: Option<&ModelSwitchingAdvertisement>,
        release: &[&str],
    ) -> HoldEvaluation {
        evaluate_holds(
            holds,
            model,
            HoldAction::Load,
            switching,
            &LifecycleOptions {
                release_holds: release.iter().map(|id| id.to_string()).collect(),
                force: false,
            },
            "key_coordinator",
        )
    }

    #[test]
    fn placed_loads_beside_a_held_profile_do_not_conflict() {
        let holds = [hold_on("qwen", "hold_qwen", "key_coordinator")];
        let switching = split_node();
        assert_eq!(
            evaluate_load(&holds, "mimo", Some(&switching), &[]),
            HoldEvaluation::Proceed {
                release: Vec::new()
            }
        );
    }

    #[test]
    fn placed_loads_that_exceed_free_gpus_are_left_to_fleet() {
        // Both halves of the node are busy and one is held. Fleet refuses the
        // load as insufficient capacity instead of evicting, so the hold is
        // never at risk and Fleet's own 409 explains the refusal.
        let holds = [hold_on("qwen", "hold_qwen", "key_coordinator")];
        let switching = fleet_inventory(vec![
            placed("qwen", "ready", "ready", 4, vec![0, 1, 6, 7]),
            placed("deepseek", "ready", "ready", 4, vec![2, 3, 4, 5]),
            placed("mimo", "unloaded", "unloaded", 4, Vec::new()),
        ]);
        assert_eq!(
            evaluate_load(&holds, "mimo", Some(&switching), &[]),
            HoldEvaluation::Proceed {
                release: Vec::new()
            }
        );
    }

    #[test]
    fn rescaling_and_multiple_holds_on_a_placed_node_do_not_conflict() {
        let holds = [
            hold_on("qwen", "hold_qwen", "key_coordinator"),
            hold_on("qwen", "hold_other", "key_operator"),
            hold_on("deepseek", "hold_deepseek", "key_operator"),
        ];
        let switching = fleet_inventory(vec![
            placed("qwen", "ready", "ready", 4, vec![0, 1, 6, 7]),
            placed("deepseek", "ready", "ready", 2, vec![2, 3]),
            placed("mimo", "ready", "ready", 1, vec![4]),
        ]);
        // Rescaling mimo onto the remaining GPUs stops nothing.
        assert_eq!(
            evaluate_load(&holds, "mimo", Some(&switching), &[]),
            HoldEvaluation::Proceed {
                release: Vec::new()
            }
        );
        // Another credential's hold cannot be named because nothing conflicts.
        assert_eq!(
            evaluate_load(&holds, "mimo", Some(&switching), &["hold_deepseek"]),
            HoldEvaluation::Proceed {
                release: Vec::new()
            }
        );
        // Rescaling a held profile never conflicts with any hold.
        assert_eq!(
            evaluate_load(&holds, "qwen", Some(&switching), &[]),
            HoldEvaluation::Proceed {
                release: Vec::new()
            }
        );
    }

    #[test]
    fn unloading_a_held_placed_profile_is_still_blocked() {
        let holds = [
            hold_on("qwen", "hold_qwen", "key_coordinator"),
            hold_on("mimo", "hold_mimo", "key_coordinator"),
        ];
        let switching = split_node();
        let options = LifecycleOptions::default();
        let HoldEvaluation::Blocked { release, blocking } = evaluate_holds(
            &holds,
            "qwen",
            HoldAction::Unload,
            Some(&switching),
            &options,
            "key_coordinator",
        ) else {
            panic!("unload of a held profile must be blocked");
        };
        assert!(release.is_empty());
        assert_eq!(blocking, vec![holds[0].clone()]);
        // The owner may still release its own hold through the unload.
        assert_eq!(
            evaluate_holds(
                &holds,
                "qwen",
                HoldAction::Unload,
                Some(&switching),
                &LifecycleOptions {
                    release_holds: vec!["hold_qwen".into()],
                    force: false,
                },
                "key_coordinator",
            ),
            HoldEvaluation::Proceed {
                release: vec!["hold_qwen".into()]
            }
        );
    }

    #[test]
    fn exclusive_switching_loads_still_conflict_with_every_active_hold() {
        let holds = [
            hold_on("qwen", "hold_qwen", "key_coordinator"),
            hold_on("idle", "hold_idle", "key_operator"),
        ];
        let switching = fleet_inventory(vec![
            switchable("qwen", "ready", "ready"),
            switchable("idle", "unloaded", "unloaded"),
            switchable("mimo", "unloaded", "unloaded"),
        ]);
        assert_eq!(
            evaluate_load(&holds, "mimo", Some(&switching), &[]),
            HoldEvaluation::Blocked {
                release: Vec::new(),
                blocking: vec![holds[0].clone()],
            }
        );
        assert_eq!(
            evaluate_load(&holds, "mimo", Some(&switching), &["hold_qwen"]),
            HoldEvaluation::Proceed {
                release: vec!["hold_qwen".into()]
            }
        );
    }

    #[test]
    fn unknown_placement_signals_keep_the_conservative_rule() {
        let holds = [hold_on("qwen", "hold_qwen", "key_coordinator")];
        // Another provider's gpu_count says nothing about Fleet's eviction rule.
        let mut other_provider = split_node();
        other_provider.provider = "other".into();
        assert!(matches!(
            evaluate_load(&holds, "mimo", Some(&other_provider), &[]),
            HoldEvaluation::Blocked { .. }
        ));
        // Without a live inventory nothing is known to be running.
        assert_eq!(
            evaluate_load(&holds, "mimo", None, &[]),
            HoldEvaluation::Proceed {
                release: Vec::new()
            }
        );
    }

    #[tokio::test]
    async fn loads_beside_a_held_profile_on_free_gpus_are_forwarded() {
        let fixture = mesh_fixture(split_node().models).await;
        new_hold(&fixture, coordinator(), "qwen").await;
        // The hold does not refuse the load; the test worker is unreachable,
        // so the forwarded request fails with 502 rather than 409.
        assert_eq!(
            load(&fixture, operator(), "mimo", &[]).await.status(),
            StatusCode::BAD_GATEWAY
        );
        let refused = unload(&fixture, operator(), ManagementChannel::ApiKey, "qwen", &[]).await;
        assert_eq!(refused.status(), StatusCode::CONFLICT);
        assert_eq!(body_json(refused).await["code"], "model_held");
    }

    async fn mesh_fixture(models: Vec<SwitchableModelAdvertisement>) -> MeshFixture {
        mesh_fixture_with_capabilities(models, Vec::new()).await
    }

    /// A gateway with the mesh controller enabled and `endpoint` enrolled.
    async fn mesh_gateway(endpoint: iroh::EndpointId) -> (Arc<Gateway>, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "llmconduit-mesh-holds-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let persisted: crate::config::PersistedConfig =
            serde_yaml::from_str("upstream_base_url: \"http://127.0.0.1:9/v1\"\n").expect("yaml");
        let mut config = crate::config::Config::from_persisted(&persisted).expect("config");
        config.mesh.controller.enabled = true;
        config.mesh.controller.bind_addr = "127.0.0.1:0".parse().expect("bind");
        config.mesh.controller.state_path = Some(dir.join("mesh.sqlite3"));
        config.mesh.controller.identity_path = Some(dir.join("controller.key"));
        let (_router, gateway) = crate::build_app_with_gateway(config);
        let admin = gateway.mesh_admin().expect("mesh admin");
        admin.initialize().await.expect("init");
        let key = admin
            .store()
            .create_join_key(None, None, None)
            .await
            .expect("join key");
        admin
            .store()
            .validate_join_key_for_endpoint(&key.token, &endpoint.to_string(), None, now_ms())
            .await
            .expect("enroll");
        (gateway, dir)
    }

    async fn mesh_fixture_with_capabilities(
        models: Vec<SwitchableModelAdvertisement>,
        capabilities: Vec<String>,
    ) -> MeshFixture {
        let endpoint = SecretKey::generate().public();
        let endpoint_id = endpoint.to_string();
        let (gateway, dir) = mesh_gateway(endpoint).await;
        let admin = gateway.mesh_admin().expect("mesh admin");
        admin.registry().register_test(
            endpoint,
            crate::mesh::protocol::WorkerAdvertisement {
                protocol_version: crate::mesh::protocol::PROTOCOL_VERSION,
                node_name: None,
                agent_version: "test".into(),
                resources: Vec::new(),
                model_switching: Some(ModelSwitchingAdvertisement {
                    provider: "lil-fleet".into(),
                    models,
                    revision: 1,
                }),
                request_encodings: Vec::new(),
                capabilities,
            },
        );
        MeshFixture {
            gateway,
            dir,
            endpoint,
            endpoint_id,
        }
    }

    fn key_actor(key: &str, permissions: &[ManagementPermission]) -> ManagementActor {
        ManagementActor::Delegated {
            session_id: format!("authreq_{key}"),
            principal_id: "usr_eval".into(),
            key_id: key.into(),
            permissions: permissions.to_vec().into(),
        }
    }

    fn coordinator() -> ManagementActor {
        key_actor(
            "key_coordinator",
            &[
                ManagementPermission::FleetModelsRead,
                ManagementPermission::FleetModelsLoad,
                ManagementPermission::FleetModelsUnload,
                ManagementPermission::FleetModelsHold,
            ],
        )
    }

    fn operator() -> ManagementActor {
        key_actor(
            "key_operator",
            &[
                ManagementPermission::FleetModelsRead,
                ManagementPermission::FleetModelsLoad,
                ManagementPermission::FleetModelsUnload,
                ManagementPermission::FleetModelsHold,
            ],
        )
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .expect("body");
        serde_json::from_slice(&bytes).expect("json body")
    }

    async fn create_hold(
        fixture: &MeshFixture,
        actor: ManagementActor,
        model: &str,
        body: serde_json::Value,
    ) -> Response {
        create_model_hold(
            State(Arc::clone(&fixture.gateway)),
            Extension(actor),
            Path((fixture.endpoint_id.clone(), model.to_string())),
            Bytes::from(body.to_string()),
        )
        .await
    }

    async fn unload(
        fixture: &MeshFixture,
        actor: ManagementActor,
        channel: ManagementChannel,
        model: &str,
        query: &[(&str, &str)],
    ) -> Response {
        unload_node_model(
            State(Arc::clone(&fixture.gateway)),
            Extension(actor),
            Some(Extension(channel)),
            Path((fixture.endpoint_id.clone(), model.to_string())),
            Query(
                query
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
        )
        .await
    }

    async fn load(
        fixture: &MeshFixture,
        actor: ManagementActor,
        model: &str,
        query: &[(&str, &str)],
    ) -> Response {
        load_node_model(
            State(Arc::clone(&fixture.gateway)),
            Extension(actor),
            Some(Extension(ManagementChannel::ApiKey)),
            Path((fixture.endpoint_id.clone(), model.to_string())),
            Query(
                query
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            Bytes::new(),
        )
        .await
    }

    async fn new_hold(fixture: &MeshFixture, actor: ManagementActor, model: &str) -> String {
        let response = create_hold(
            fixture,
            actor,
            model,
            serde_json::json!({"holder": "harbor/run-1", "ttl_secs": 600}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = body_json(response).await;
        assert_eq!(body["holder"], "harbor/run-1");
        assert!(body["expires_at_ms"].as_i64().unwrap() > now_ms() + 590_000);
        body["hold_id"].as_str().expect("hold id").to_string()
    }

    #[tokio::test]
    async fn hold_blocks_other_unloads_but_owner_can_release_its_own() {
        let fixture = mesh_fixture(vec![switchable("qwen", "ready", "ready")]).await;
        let hold_id = new_hold(&fixture, coordinator(), "qwen").await;

        // Another credential, even with fleet.models.unload, is refused.
        let refused = unload(&fixture, operator(), ManagementChannel::ApiKey, "qwen", &[]).await;
        assert_eq!(refused.status(), StatusCode::CONFLICT);
        let body = body_json(refused).await;
        assert_eq!(body["code"], "model_held");
        assert_eq!(body["holders"][0]["hold_id"], hold_id.as_str());
        assert_eq!(body["holders"][0]["holder"], "harbor/run-1");

        // Presenting someone else's hold id does not help.
        let stolen = unload(
            &fixture,
            operator(),
            ManagementChannel::ApiKey,
            "qwen",
            &[("release_hold", &hold_id)],
        )
        .await;
        assert_eq!(stolen.status(), StatusCode::FORBIDDEN);
        assert_eq!(body_json(stolen).await["code"], "hold_not_owned");

        // The holder passes the gate (the test worker has no live
        // connection, so the dispatch itself then fails with a 502).
        let own = unload(
            &fixture,
            coordinator(),
            ManagementChannel::ApiKey,
            "qwen",
            &[("release_hold", &hold_id)],
        )
        .await;
        assert_eq!(own.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(body_json(own).await["code"], "worker_disconnected");
        // A failed dispatch keeps the hold.
        let store = fixture.gateway.mesh_admin().unwrap().store().clone();
        assert!(
            store
                .active_hold(&hold_id, now_ms())
                .await
                .unwrap()
                .is_some()
        );

        // Holds on other models never block.
        let other = unload(
            &fixture,
            operator(),
            ManagementChannel::ApiKey,
            "other",
            &[],
        )
        .await;
        assert_eq!(other.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn force_is_reserved_for_admin_sessions_and_audited() {
        let fixture = mesh_fixture(vec![switchable("qwen", "ready", "ready")]).await;
        new_hold(&fixture, coordinator(), "qwen").await;

        let api_key_force = unload(
            &fixture,
            ManagementActor::Bootstrap,
            ManagementChannel::ApiKey,
            "qwen",
            &[("force", "true")],
        )
        .await;
        assert_eq!(api_key_force.status(), StatusCode::FORBIDDEN);
        assert_eq!(body_json(api_key_force).await["code"], "force_not_allowed");

        let admin_without_force = unload(
            &fixture,
            ManagementActor::Bootstrap,
            ManagementChannel::AdminSession,
            "qwen",
            &[],
        )
        .await;
        assert_eq!(admin_without_force.status(), StatusCode::CONFLICT);

        let forced = unload(
            &fixture,
            ManagementActor::Bootstrap,
            ManagementChannel::AdminSession,
            "qwen",
            &[("force", "true")],
        )
        .await;
        assert_eq!(forced.status(), StatusCode::BAD_GATEWAY, "gate passed");
        let events = fixture
            .gateway
            .mesh_admin()
            .unwrap()
            .store()
            .list_hold_events(1)
            .await
            .unwrap();
        assert_eq!(events[0].action, "force_override");
        assert_eq!(events[0].actor, "bootstrap");
        assert_eq!(events[0].detail.as_deref(), Some("unload"));
    }

    #[tokio::test]
    async fn expired_holds_do_not_block_and_holds_survive_store_reopen() {
        let fixture = mesh_fixture(vec![switchable("qwen", "ready", "ready")]).await;
        let store = fixture.gateway.mesh_admin().unwrap().store().clone();
        let CreateHoldOutcome::Created(_) = store
            .create_hold(
                &fixture.endpoint_id,
                "qwen",
                "stale",
                "key:key_coordinator",
                60_000,
                now_ms() - 120_000,
            )
            .await
            .unwrap()
        else {
            panic!("created");
        };
        let passes = unload(&fixture, operator(), ManagementChannel::ApiKey, "qwen", &[]).await;
        assert_eq!(
            passes.status(),
            StatusCode::BAD_GATEWAY,
            "expired hold ignored"
        );

        let hold_id = new_hold(&fixture, coordinator(), "qwen").await;
        // A controller restart opens a fresh connection to the same file.
        let reopened = crate::mesh::store::MeshStore::open(fixture.dir.join("mesh.sqlite3"))
            .await
            .expect("reopen");
        let holds = reopened
            .list_active_holds(Some(&fixture.endpoint_id), Some("qwen"), now_ms())
            .await
            .unwrap();
        assert_eq!(holds.len(), 1);
        assert_eq!(holds[0].hold_id, hold_id);
    }

    #[tokio::test]
    async fn loads_that_would_stop_a_held_profile_are_refused() {
        let fixture = mesh_fixture(vec![
            switchable("held", "ready", "ready"),
            switchable("next", "unloaded", "unloaded"),
        ])
        .await;
        let hold_id = new_hold(&fixture, coordinator(), "held").await;

        // Exclusive switching would stop the held profile.
        let refused = load(&fixture, operator(), "next", &[]).await;
        assert_eq!(refused.status(), StatusCode::CONFLICT);
        assert_eq!(body_json(refused).await["code"], "model_held");
        // Loading or rescaling the held profile itself never conflicts.
        assert_eq!(
            load(&fixture, operator(), "held", &[]).await.status(),
            StatusCode::BAD_GATEWAY
        );
        // The holder may switch away from its own profile.
        assert_eq!(
            load(
                &fixture,
                coordinator(),
                "next",
                &[("release_hold", &hold_id)]
            )
            .await
            .status(),
            StatusCode::BAD_GATEWAY
        );
    }

    #[tokio::test]
    async fn loads_on_visibly_concurrent_workers_do_not_conflict() {
        let fixture = mesh_fixture(vec![
            switchable("held", "ready", "ready"),
            switchable("busy", "ready", "ready"),
            switchable("next", "unloaded", "unloaded"),
        ])
        .await;
        new_hold(&fixture, coordinator(), "held").await;
        assert_eq!(
            load(&fixture, operator(), "next", &[]).await.status(),
            StatusCode::BAD_GATEWAY
        );
    }

    #[tokio::test]
    async fn hold_routes_validate_permissions_inputs_and_ownership() {
        let fixture = mesh_fixture(vec![switchable("qwen", "ready", "ready")]).await;
        let read_only = key_actor("key_reader", &[ManagementPermission::FleetModelsRead]);
        let denied = create_hold(
            &fixture,
            read_only.clone(),
            "qwen",
            serde_json::json!({"holder": "h", "ttl_secs": 60}),
        )
        .await;
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        for body in [
            serde_json::json!({"holder": "", "ttl_secs": 60}),
            serde_json::json!({"holder": "has space", "ttl_secs": 60}),
            serde_json::json!({"holder": "x".repeat(129), "ttl_secs": 60}),
            serde_json::json!({"holder": "h", "ttl_secs": 59}),
            serde_json::json!({"holder": "h", "ttl_secs": 86_401}),
            serde_json::json!({"holder": "h", "ttl_secs": 60, "extra": 1}),
        ] {
            let response = create_hold(&fixture, coordinator(), "qwen", body.clone()).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
        }
        let unadvertised = create_hold(
            &fixture,
            coordinator(),
            "missing",
            serde_json::json!({"holder": "h", "ttl_secs": 60}),
        )
        .await;
        assert_eq!(unadvertised.status(), StatusCode::NOT_FOUND);

        let hold_id = new_hold(&fixture, coordinator(), "qwen").await;
        // Reads need only fleet.models.read.
        let listed = list_model_holds(
            State(Arc::clone(&fixture.gateway)),
            Extension(read_only),
            Path((fixture.endpoint_id.clone(), "qwen".to_string())),
        )
        .await;
        assert_eq!(listed.status(), StatusCode::OK);
        let listed = body_json(listed).await;
        assert_eq!(listed["holds"][0]["hold_id"], hold_id.as_str());
        assert!(listed["holds"][0].get("owner").is_none());

        // Renewal is owner-only and extends the lease.
        let renew = |actor: ManagementActor, ttl: u64| {
            let gateway = Arc::clone(&fixture.gateway);
            let endpoint_id = fixture.endpoint_id.clone();
            let hold_id = hold_id.clone();
            async move {
                renew_model_hold(
                    State(gateway),
                    Extension(actor),
                    Path((endpoint_id, "qwen".to_string(), hold_id)),
                    Bytes::from(serde_json::json!({"ttl_secs": ttl}).to_string()),
                )
                .await
            }
        };
        let foreign = renew(operator(), 600).await;
        assert_eq!(foreign.status(), StatusCode::FORBIDDEN);
        assert_eq!(body_json(foreign).await["code"], "hold_not_owned");
        let renewed = renew(coordinator(), 7_200).await;
        assert_eq!(renewed.status(), StatusCode::OK);
        assert!(body_json(renewed).await["expires_at_ms"].as_i64().unwrap() > now_ms() + 7_000_000);

        // The mesh state exposes the hold on the model entry.
        let state = mesh_state(
            State(Arc::clone(&fixture.gateway)),
            Extension(key_actor(
                "key_reader",
                &[ManagementPermission::FleetModelsRead],
            )),
        )
        .await;
        let state = body_json(state).await;
        let node = state["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|node| node["endpoint_id"] == fixture.endpoint_id.as_str())
            .expect("node");
        assert_eq!(
            node["model_switching"]["models"][0]["holds"][0]["hold_id"],
            hold_id.as_str()
        );
        assert_eq!(state["model_holds"][0]["model_id"], "qwen");

        // Release: non-owner refused, owner succeeds, then it is gone.
        let release = |actor: ManagementActor, channel: ManagementChannel, force: bool| {
            let gateway = Arc::clone(&fixture.gateway);
            let endpoint_id = fixture.endpoint_id.clone();
            let hold_id = hold_id.clone();
            async move {
                release_model_hold(
                    State(gateway),
                    Extension(actor),
                    Some(Extension(channel)),
                    Path((endpoint_id, "qwen".to_string(), hold_id)),
                    Query(if force {
                        vec![("force".to_string(), "true".to_string())]
                    } else {
                        Vec::new()
                    }),
                )
                .await
            }
        };
        assert_eq!(
            release(operator(), ManagementChannel::ApiKey, false)
                .await
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            release(operator(), ManagementChannel::ApiKey, true)
                .await
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            release(coordinator(), ManagementChannel::ApiKey, false)
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            release(coordinator(), ManagementChannel::ApiKey, false)
                .await
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            unload(&fixture, operator(), ManagementChannel::ApiKey, "qwen", &[])
                .await
                .status(),
            StatusCode::BAD_GATEWAY,
            "released hold no longer blocks"
        );
    }

    #[tokio::test]
    async fn admin_session_can_force_release_a_foreign_hold() {
        let fixture = mesh_fixture(vec![switchable("qwen", "ready", "ready")]).await;
        let hold_id = new_hold(&fixture, coordinator(), "qwen").await;
        let response = release_model_hold(
            State(Arc::clone(&fixture.gateway)),
            Extension(ManagementActor::Bootstrap),
            Some(Extension(ManagementChannel::AdminSession)),
            Path((fixture.endpoint_id.clone(), "qwen".to_string(), hold_id)),
            Query(vec![("force".to_string(), "true".to_string())]),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let events = fixture
            .gateway
            .mesh_admin()
            .unwrap()
            .store()
            .list_hold_events(1)
            .await
            .unwrap();
        assert_eq!(events[0].action, "force_released");
    }

    #[tokio::test]
    async fn operation_lookup_requires_read_and_a_capable_worker() {
        let fixture = mesh_fixture(vec![switchable("qwen", "ready", "ready")]).await;
        let lookup = |actor: ManagementActor, operation_id: &str| {
            get_node_operation(
                State(Arc::clone(&fixture.gateway)),
                Extension(actor),
                Path((fixture.endpoint_id.clone(), operation_id.to_string())),
            )
        };
        assert_eq!(
            lookup(key_actor("key_none", &[]), "abc").await.status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            lookup(coordinator(), "bad/id").await.status(),
            StatusCode::BAD_REQUEST
        );
        // An old worker (no `fleet_operations` capability) is never sent a
        // stream type it cannot parse.
        let unsupported = lookup(coordinator(), "abc123").await;
        assert_eq!(unsupported.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(body_json(unsupported).await["code"], "worker_unsupported");
        let _ = fixture.endpoint;
    }

    #[tokio::test]
    async fn capable_worker_without_connection_is_a_gateway_error() {
        let fixture = mesh_fixture_with_capabilities(
            vec![switchable("qwen", "ready", "ready")],
            vec![crate::mesh::protocol::CAPABILITY_FLEET_OPERATIONS.to_string()],
        )
        .await;
        let response = get_node_operation(
            State(Arc::clone(&fixture.gateway)),
            Extension(coordinator()),
            Path((fixture.endpoint_id.clone(), "abc123".to_string())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn lifecycle_options_parse_repeated_and_comma_separated_holds() {
        let pairs = vec![
            ("release_hold".to_string(), "hold_a, hold_b".to_string()),
            ("release_hold".to_string(), "hold_a".to_string()),
            ("force".to_string(), "1".to_string()),
            ("unrelated".to_string(), "x".to_string()),
        ];
        assert_eq!(
            parse_lifecycle_options(&pairs).unwrap(),
            LifecycleOptions {
                release_holds: vec!["hold_a".into(), "hold_b".into()],
                force: true,
            }
        );
        assert!(parse_lifecycle_options(&[("release_hold".into(), "bad id".into())]).is_err());
        assert!(parse_lifecycle_options(&[("force".into(), "yes".into())]).is_err());
    }

    #[test]
    fn hold_permission_is_distinct_from_load_and_unload() {
        let loader = key_actor(
            "key_loader",
            &[
                ManagementPermission::FleetModelsLoad,
                ManagementPermission::FleetModelsUnload,
            ],
        );
        assert!(!loader.allows(ManagementPermission::FleetModelsHold));
        assert_eq!(loader.owner_id(), "key:key_loader");
        assert_eq!(ManagementActor::Bootstrap.owner_id(), "bootstrap");
    }

    async fn fleet_mock() -> wiremock::MockServer {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, ResponseTemplate};
        let server = wiremock::MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .and(header("authorization", "Bearer test-fleet-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [{
                    "model": {"id": "qwen", "image": "vllm:latest", "gpu_count": 1, "max_instances": 1},
                    "status": {"model_id": "qwen", "phase": "ready", "desired_state": "ready",
                               "desired_instances": 1, "ready_instances": 1}
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/models/qwen/unload"))
            .respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({
                "changed": true,
                "operation": {"id": "0123abcd", "kind": "unload", "model_id": "qwen",
                              "state": "pending", "created_at": "2026-10-01T00:00:00Z"}
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/models/qwen/load"))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "error": {"code": "operation_in_progress", "message": "busy"}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/operations/0123abcd"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "0123abcd", "kind": "unload", "model_id": "qwen", "state": "failed",
                "error": "container stop timed out\nafter 30s",
                "created_at": "2026-10-01T00:00:00Z", "finished_at": "2026-10-01T00:00:31Z"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/operations/missing"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error": {"code": "operation_not_found", "message": "operation not found"}
            })))
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn unload_reports_fleet_operation_and_releases_the_callers_hold_end_to_end() {
        let fleet = fleet_mock().await;
        // The worker joins the registry owned by the gateway's mesh controller;
        // `endpoint` only bootstraps the gateway, the worker enrolls below.
        let endpoint = SecretKey::generate().public();
        let (gateway, dir) = mesh_gateway(endpoint).await;
        let registry = gateway.mesh_admin().unwrap().registry();
        let worker = crate::mesh::spawn_test_fleet_worker(&registry, &fleet.uri(), true).await;
        let admin = gateway.mesh_admin().unwrap();
        let key = admin
            .store()
            .create_join_key(None, None, None)
            .await
            .unwrap();
        admin
            .store()
            .validate_join_key_for_endpoint(
                &key.token,
                &worker.endpoint_id.to_string(),
                None,
                now_ms(),
            )
            .await
            .unwrap();
        let fixture = MeshFixture {
            gateway,
            dir,
            endpoint: worker.endpoint_id,
            endpoint_id: worker.endpoint_id.to_string(),
        };
        let hold_id = new_hold(&fixture, coordinator(), "qwen").await;

        let refused = unload(&fixture, operator(), ManagementChannel::ApiKey, "qwen", &[]).await;
        assert_eq!(refused.status(), StatusCode::CONFLICT);

        // Fleet refusals carry Fleet's code next to the unchanged message.
        let busy = load(&fixture, operator(), "qwen", &[]).await;
        assert_eq!(busy.status(), StatusCode::CONFLICT);
        let busy = body_json(busy).await;
        assert_eq!(busy["code"], "operation_in_progress");
        assert_eq!(
            busy["error"],
            "Fleet is already changing this model (operation_in_progress)"
        );

        let accepted = unload(
            &fixture,
            coordinator(),
            ManagementChannel::ApiKey,
            "qwen",
            &[("release_hold", &hold_id)],
        )
        .await;
        assert_eq!(accepted.status(), StatusCode::ACCEPTED);
        let body = body_json(accepted).await;
        assert_eq!(body["accepted"], true);
        assert_eq!(body["changed"], true);
        assert_eq!(body["operation"]["id"], "0123abcd");
        assert_eq!(body["operation"]["state"], "pending");
        assert_eq!(body["operation"]["error"], serde_json::Value::Null);
        assert_eq!(body["released_holds"][0], hold_id.as_str());
        let store = fixture.gateway.mesh_admin().unwrap().store().clone();
        assert!(
            store
                .active_hold(&hold_id, now_ms())
                .await
                .unwrap()
                .is_none()
        );

        let lookup = |operation_id: &'static str| {
            get_node_operation(
                State(Arc::clone(&fixture.gateway)),
                Extension(key_actor(
                    "key_reader",
                    &[ManagementPermission::FleetModelsRead],
                )),
                Path((fixture.endpoint_id.clone(), operation_id.to_string())),
            )
        };
        let found = lookup("0123abcd").await;
        assert_eq!(found.status(), StatusCode::OK);
        let found = body_json(found).await;
        assert_eq!(found["operation"]["state"], "failed");
        assert_eq!(found["operation"]["kind"], "unload");
        // Worker-controlled free text is single-line and bounded.
        assert_eq!(
            found["operation"]["error"],
            "container stop timed out after 30s"
        );
        let missing = lookup("missing").await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(missing).await["code"], "operation_not_found");
        let _ = fixture.endpoint;
        drop(worker);
    }

    #[tokio::test]
    async fn old_workers_without_operation_support_still_load_and_unload() {
        let fleet = fleet_mock().await;
        let endpoint = SecretKey::generate().public();
        let (gateway, dir) = mesh_gateway(endpoint).await;
        let registry = gateway.mesh_admin().unwrap().registry();
        let worker = crate::mesh::spawn_test_fleet_worker(&registry, &fleet.uri(), false).await;
        let admin = gateway.mesh_admin().unwrap();
        let key = admin
            .store()
            .create_join_key(None, None, None)
            .await
            .unwrap();
        admin
            .store()
            .validate_join_key_for_endpoint(
                &key.token,
                &worker.endpoint_id.to_string(),
                None,
                now_ms(),
            )
            .await
            .unwrap();
        let fixture = MeshFixture {
            gateway,
            dir,
            endpoint: worker.endpoint_id,
            endpoint_id: worker.endpoint_id.to_string(),
        };
        // The unload itself is unaffected by the missing capability.
        let accepted = unload(&fixture, operator(), ManagementChannel::ApiKey, "qwen", &[]).await;
        assert_eq!(accepted.status(), StatusCode::ACCEPTED);
        let lookup = get_node_operation(
            State(Arc::clone(&fixture.gateway)),
            Extension(coordinator()),
            Path((fixture.endpoint_id.clone(), "0123abcd".to_string())),
        )
        .await;
        assert_eq!(lookup.status(), StatusCode::NOT_IMPLEMENTED);
        drop(worker);
    }

    #[test]
    fn rejection_codes_prefer_fleet_codes_and_fall_back_for_old_workers() {
        let response =
            |error: &str, code: Option<&str>| crate::mesh::protocol::SwitchModelResponse {
                request_id: uuid::Uuid::nil(),
                model_id: "qwen".into(),
                accepted: false,
                changed: false,
                error: Some(error.into()),
                model_switching: None,
                error_status: Some(409),
                error_code: code.map(str::to_owned),
                operation: None,
            };
        assert_eq!(
            rejection_code(
                &response(
                    "Fleet has insufficient GPU capacity",
                    Some("insufficient_resources")
                ),
                StatusCode::CONFLICT
            ),
            "insufficient_resources"
        );
        // Old worker: the code only exists in the message text.
        assert_eq!(
            rejection_code(
                &response(
                    "Fleet is already changing this model (operation_in_progress)",
                    None
                ),
                StatusCode::CONFLICT
            ),
            "operation_in_progress"
        );
        assert_eq!(
            rejection_code(
                &response("Fleet is busy or has insufficient GPU capacity", None),
                StatusCode::CONFLICT
            ),
            "fleet_conflict"
        );
        assert_eq!(
            rejection_code(
                &response("something else", Some("weird")),
                StatusCode::CONFLICT
            ),
            "fleet_conflict"
        );
        assert_eq!(mesh_failure_code("mesh worker is stale"), "worker_stale");
        assert_eq!(
            mesh_failure_code("mesh worker is not connected"),
            "worker_disconnected"
        );
        assert_eq!(
            mesh_failure_code("mesh switch request timed out"),
            "worker_timeout"
        );
    }
}
