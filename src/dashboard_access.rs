//! Wire contract and isolated router for the dashboard Access console.
//!
//! Authentication/session lookup and CSRF/origin enforcement stay in `http.rs` and
//! `dashboard_auth`; those layers insert a [`ManagementActor`] before these handlers run.
//! The authz implementation owns [`AccessBackend`] and therefore remains free to transact,
//! rebuild its immutable snapshot, and revoke delegated sessions atomically.

use axum::Router;
use axum::extract::{Extension, Json, Path};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagementPermission {
    #[serde(rename = "auth.keys.read")]
    KeysRead,
    #[serde(rename = "auth.keys.create")]
    KeysCreate,
    #[serde(rename = "auth.keys.revoke")]
    KeysRevoke,
    #[serde(rename = "auth.keys.rotate")]
    KeysRotate,
    #[serde(rename = "auth.principals.read")]
    PrincipalsRead,
    #[serde(rename = "auth.principals.write")]
    PrincipalsWrite,
    #[serde(rename = "auth.groups.read")]
    GroupsRead,
    #[serde(rename = "auth.groups.write")]
    GroupsWrite,
    #[serde(rename = "auth.roles.read")]
    RolesRead,
    #[serde(rename = "auth.roles.write")]
    RolesWrite,
    #[serde(rename = "auth.policies.read")]
    PoliciesRead,
    #[serde(rename = "auth.policies.write")]
    PoliciesWrite,
    #[serde(rename = "auth.usage.read")]
    UsageRead,
    #[serde(rename = "auth.audit.read")]
    AuditRead,
    #[serde(rename = "auth.pricing.read")]
    PricingRead,
    #[serde(rename = "auth.pricing.sync")]
    PricingSync,
    #[serde(rename = "auth.pricing.write")]
    PricingWrite,
    #[serde(rename = "auth.sessions.read")]
    SessionsRead,
    #[serde(rename = "auth.sessions.terminate")]
    SessionsTerminate,
}

#[derive(Debug, Clone)]
pub enum ManagementActor {
    Bootstrap,
    Delegated {
        session_id: String,
        principal_id: String,
        key_id: String,
        permissions: Arc<[ManagementPermission]>,
    },
}

impl ManagementActor {
    fn allows(&self, permission: ManagementPermission) -> bool {
        matches!(self, Self::Bootstrap)
            || matches!(self, Self::Delegated { permissions, .. } if permissions.contains(&permission))
    }

    fn has_management_access(&self) -> bool {
        matches!(self, Self::Bootstrap)
            || matches!(self, Self::Delegated { permissions, .. } if !permissions.is_empty())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessSummary {
    pub policy_epoch: u64,
    pub actor: ActorSummary,
    pub counts: AccessCounts,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActorSummary {
    pub kind: String,
    pub principal_id: Option<String>,
    pub display_name: String,
    pub permissions: Vec<ManagementPermission>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AccessCounts {
    pub users: usize,
    pub groups: usize,
    pub roles: usize,
    pub policies: usize,
    pub api_keys: usize,
    pub active_sessions: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessUser {
    pub id: String,
    pub kind: String,
    pub display_name: String,
    pub enabled: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessGroup {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub member_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessRole {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub permissions: Vec<ManagementPermission>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessPolicy {
    pub id: String,
    pub name: String,
    pub effect: String,
    pub enabled: bool,
    pub subjects: Vec<String>,
    pub endpoints: Vec<String>,
    pub models: Vec<String>,
    pub providers: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessApiKey {
    pub id: String,
    pub principal_id: String,
    pub name: String,
    pub prefix: String,
    pub enabled: bool,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub last_used_at: Option<String>,
}

/// Only the create/rotate response carries `raw_key`; list responses cannot recover it.
#[derive(Serialize)]
pub struct CreatedAccessApiKey {
    #[serde(flatten)]
    pub api_key: AccessApiKey,
    pub raw_key: String,
}

impl std::fmt::Debug for CreatedAccessApiKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CreatedAccessApiKey")
            .field("api_key", &self.api_key)
            .field("raw_key", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessSession {
    pub id: String,
    pub kind: String,
    pub principal_id: String,
    pub key_id: String,
    pub endpoint: Option<String>,
    pub requested_model: Option<String>,
    pub started_at: String,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessUsageRow {
    pub dimension: String,
    pub value: String,
    pub requests: u64,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub cost: Option<f64>,
    pub cost_confidence: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessAuditEvent {
    pub id: String,
    pub timestamp: String,
    pub actor: String,
    pub action: String,
    pub target: String,
    pub outcome: String,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessPricingRow {
    pub model: String,
    pub provider: String,
    pub source: String,
    pub fetched_at: String,
    pub input_per_1k: String,
    pub output_per_1k: String,
    pub confidence: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub display_name: String,
    pub kind: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub principal_id: String,
    pub name: String,
    pub expires_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreatePolicyRequest {
    pub name: String,
    pub effect: String,
    #[serde(default)]
    pub subjects: Vec<String>,
    #[serde(default)]
    pub endpoints: Vec<String>,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default)]
    pub providers: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateGroupRequest {
    pub name: String,
    #[serde(default)]
    pub members: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateRoleRequest {
    pub name: String,
    #[serde(default)]
    pub permissions: Vec<ManagementPermission>,
}

#[derive(Debug, Deserialize)]
pub struct WritePricingRequest {
    pub pricing: Vec<AccessPricingInput>,
}

#[derive(Debug, Deserialize)]
pub struct AccessPricingInput {
    pub model: String,
    pub provider: String,
    pub input_per_1k: String,
    pub output_per_1k: String,
}

#[derive(Debug)]
pub enum AccessOperation {
    Summary,
    ListUsers,
    CreateUser(CreateUserRequest),
    ListGroups,
    CreateGroup(CreateGroupRequest),
    ListRoles,
    CreateRole(CreateRoleRequest),
    ListPolicies,
    CreatePolicy(CreatePolicyRequest),
    ListApiKeys,
    CreateApiKey(CreateApiKeyRequest),
    RevokeApiKey(String),
    RotateApiKey(String),
    ListSessions,
    RevokeSession(String),
    Usage,
    Audit,
    Pricing,
    WritePricing(WritePricingRequest),
}

#[derive(Debug)]
pub enum AccessResult {
    Summary(AccessSummary),
    Users(Vec<AccessUser>),
    Groups(Vec<AccessGroup>),
    Roles(Vec<AccessRole>),
    Policies(Vec<AccessPolicy>),
    ApiKeys(Vec<AccessApiKey>),
    CreatedApiKey(CreatedAccessApiKey),
    Sessions(Vec<AccessSession>),
    Usage(Vec<AccessUsageRow>),
    Audit(Vec<AccessAuditEvent>),
    Pricing(Vec<AccessPricingRow>),
}

pub type AccessFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AccessResult, AccessError>> + Send + 'a>>;

/// Authz/store adapter. Mutations must enforce anti-escalation and atomically publish a fresh
/// policy snapshot before their future resolves successfully.
pub trait AccessBackend: Send + Sync + 'static {
    fn dispatch<'a>(
        &'a self,
        actor: &'a ManagementActor,
        operation: AccessOperation,
    ) -> AccessFuture<'a>;
}

#[derive(Debug)]
pub struct AccessError {
    status: StatusCode,
    message: String,
}

impl AccessError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn forbidden() -> Self {
        Self::new(StatusCode::FORBIDDEN, "management permission denied")
    }

    fn contract() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "access backend contract mismatch",
        )
    }
}

impl IntoResponse for AccessError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

/// Routes are generic over the application's missing axum state and carry the backend as an
/// extension, so the main router only needs a narrow merge plus its existing session/CSRF layers.
pub fn routes<S>(backend: Arc<dyn AccessBackend>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/dashboard/api/auth/summary", get(summary))
        .route("/dashboard/api/auth/users", get(users).post(create_user))
        .route("/dashboard/api/auth/groups", get(groups).post(create_group))
        .route("/dashboard/api/auth/roles", get(roles).post(create_role))
        .route(
            "/dashboard/api/auth/policies",
            get(policies).post(create_policy),
        )
        .route(
            "/dashboard/api/auth/api-keys",
            get(api_keys).post(create_api_key),
        )
        .route(
            "/dashboard/api/auth/api-keys/{id}/revoke",
            post(revoke_api_key),
        )
        .route(
            "/dashboard/api/auth/api-keys/{id}/rotate",
            post(rotate_api_key),
        )
        .route("/dashboard/api/auth/sessions", get(sessions))
        .route(
            "/dashboard/api/auth/sessions/{id}/revoke",
            post(revoke_session),
        )
        .route("/dashboard/api/auth/usage", get(usage))
        .route("/dashboard/api/auth/audit", get(audit))
        .route(
            "/dashboard/api/auth/pricing",
            get(pricing).post(write_pricing),
        )
        .layer(Extension(backend))
}

type Ctx = (
    Extension<Arc<dyn AccessBackend>>,
    Extension<ManagementActor>,
);

async fn execute(
    ctx: Ctx,
    permission: ManagementPermission,
    operation: AccessOperation,
) -> Result<AccessResult, AccessError> {
    let (Extension(backend), Extension(actor)) = ctx;
    if !actor.allows(permission) {
        return Err(AccessError::forbidden());
    }
    backend.dispatch(&actor, operation).await
}

macro_rules! read_handler {
    ($name:ident, $permission:expr, $operation:expr, $variant:ident, $key:literal) => {
        async fn $name(backend: Extension<Arc<dyn AccessBackend>>, actor: Extension<ManagementActor>) -> Result<Json<Value>, AccessError> {
            match execute((backend, actor), $permission, $operation).await? {
                AccessResult::$variant(value) => Ok(Json(json!({ $key: value }))),
                _ => Err(AccessError::contract()),
            }
        }
    };
}

async fn summary(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
) -> Result<Json<AccessSummary>, AccessError> {
    let Extension(backend) = backend;
    let Extension(actor) = actor;
    if !actor.has_management_access() {
        return Err(AccessError::forbidden());
    }
    match backend.dispatch(&actor, AccessOperation::Summary).await? {
        AccessResult::Summary(value) => Ok(Json(value)),
        _ => Err(AccessError::contract()),
    }
}

async fn create_group(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Json(body): Json<CreateGroupRequest>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::GroupsWrite,
        AccessOperation::CreateGroup(body),
    )
    .await?
    {
        AccessResult::Groups(value) => Ok(Json(json!({ "groups": value }))),
        _ => Err(AccessError::contract()),
    }
}

async fn create_role(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Json(body): Json<CreateRoleRequest>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::RolesWrite,
        AccessOperation::CreateRole(body),
    )
    .await?
    {
        AccessResult::Roles(value) => Ok(Json(json!({ "roles": value }))),
        _ => Err(AccessError::contract()),
    }
}

read_handler!(
    users,
    ManagementPermission::PrincipalsRead,
    AccessOperation::ListUsers,
    Users,
    "users"
);
read_handler!(
    groups,
    ManagementPermission::GroupsRead,
    AccessOperation::ListGroups,
    Groups,
    "groups"
);
read_handler!(
    roles,
    ManagementPermission::RolesRead,
    AccessOperation::ListRoles,
    Roles,
    "roles"
);
read_handler!(
    policies,
    ManagementPermission::PoliciesRead,
    AccessOperation::ListPolicies,
    Policies,
    "policies"
);
read_handler!(
    api_keys,
    ManagementPermission::KeysRead,
    AccessOperation::ListApiKeys,
    ApiKeys,
    "api_keys"
);
read_handler!(
    sessions,
    ManagementPermission::SessionsRead,
    AccessOperation::ListSessions,
    Sessions,
    "sessions"
);
read_handler!(
    usage,
    ManagementPermission::UsageRead,
    AccessOperation::Usage,
    Usage,
    "usage"
);
read_handler!(
    audit,
    ManagementPermission::AuditRead,
    AccessOperation::Audit,
    Audit,
    "events"
);
read_handler!(
    pricing,
    ManagementPermission::PricingRead,
    AccessOperation::Pricing,
    Pricing,
    "pricing"
);

async fn create_user(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Json(body): Json<CreateUserRequest>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::PrincipalsWrite,
        AccessOperation::CreateUser(body),
    )
    .await?
    {
        AccessResult::Users(value) => Ok(Json(json!({ "users": value }))),
        _ => Err(AccessError::contract()),
    }
}

async fn create_policy(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Json(body): Json<CreatePolicyRequest>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::PoliciesWrite,
        AccessOperation::CreatePolicy(body),
    )
    .await?
    {
        AccessResult::Policies(value) => Ok(Json(json!({ "policies": value }))),
        _ => Err(AccessError::contract()),
    }
}

async fn create_api_key(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Json(body): Json<CreateApiKeyRequest>,
) -> Result<Json<CreatedAccessApiKey>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::KeysCreate,
        AccessOperation::CreateApiKey(body),
    )
    .await?
    {
        AccessResult::CreatedApiKey(value) => Ok(Json(value)),
        _ => Err(AccessError::contract()),
    }
}

async fn revoke_api_key(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::KeysRevoke,
        AccessOperation::RevokeApiKey(id),
    )
    .await?
    {
        AccessResult::ApiKeys(value) => Ok(Json(json!({ "api_keys": value }))),
        _ => Err(AccessError::contract()),
    }
}

async fn rotate_api_key(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Path(id): Path<String>,
) -> Result<Json<CreatedAccessApiKey>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::KeysRotate,
        AccessOperation::RotateApiKey(id),
    )
    .await?
    {
        AccessResult::CreatedApiKey(value) => Ok(Json(value)),
        _ => Err(AccessError::contract()),
    }
}

async fn revoke_session(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::SessionsTerminate,
        AccessOperation::RevokeSession(id),
    )
    .await?
    {
        AccessResult::Sessions(value) => Ok(Json(json!({ "sessions": value }))),
        _ => Err(AccessError::contract()),
    }
}

async fn write_pricing(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Json(body): Json<WritePricingRequest>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::PricingWrite,
        AccessOperation::WritePricing(body),
    )
    .await?
    {
        AccessResult::Pricing(value) => Ok(Json(json!({ "pricing": value }))),
        _ => Err(AccessError::contract()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn created_key_debug_never_contains_secret() {
        let created = CreatedAccessApiKey {
            api_key: AccessApiKey {
                id: "key_1".into(),
                principal_id: "usr_1".into(),
                name: "test".into(),
                prefix: "llmc_abcd".into(),
                enabled: true,
                created_at: "now".into(),
                expires_at: None,
                last_used_at: None,
            },
            raw_key: "llmc_extremely_secret".into(),
        };
        let debug = format!("{created:?}");
        assert!(!debug.contains("extremely_secret"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn delegated_permissions_are_exact() {
        let actor = ManagementActor::Delegated {
            session_id: "sess_1".into(),
            principal_id: "usr_1".into(),
            key_id: "key_1".into(),
            permissions: Arc::from([ManagementPermission::KeysRead]),
        };
        assert!(actor.allows(ManagementPermission::KeysRead));
        assert!(!actor.allows(ManagementPermission::KeysRevoke));
        assert!(ManagementActor::Bootstrap.allows(ManagementPermission::KeysRevoke));
    }
}
