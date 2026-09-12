//! Dashboard account and key management routes (`/dashboard/api/me`,
//! `/dashboard/api/users*`, `/dashboard/api/keys*`).
//!
//! All routes sit behind `require_session`. Mutations additionally require the
//! double-submit CSRF token. Authorization: a session without a user (token
//! login / dev-open) is treated as admin; a user session may manage its own
//! keys; user management and other users' keys need `is_admin`.

// Helpers return the ready-to-send axum `Response` as their error so handlers
// can `return denied;` directly; boxing it would only add noise.
#![allow(clippy::result_large_err)]

use crate::accounts::{self, SessionUser};
use crate::control_plane_store::{ApiKeyRecord, PersistenceStore, UserRecord};
use crate::dashboard_auth::{AuthSession, DashboardAuth, no_store};
use crate::engine::Gateway;
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const MAX_LABEL_LEN: usize = 128;
const MAX_ALLOWED_MODELS: usize = 64;

fn json<T: Serialize>(status: StatusCode, body: &T) -> Response {
    no_store((status, Json(body)).into_response())
}

fn error(status: StatusCode, message: &str) -> Response {
    json(status, &serde_json::json!({ "error": message }))
}

fn store_or_503(gateway: &Gateway) -> Result<Arc<dyn PersistenceStore>, Response> {
    gateway.persistence_store().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "user and key management need a SQL store (control_plane.storage sqlite or postgres)",
        )
    })
}

fn is_admin(session: &AuthSession) -> bool {
    session.user.as_ref().is_none_or(|user| user.is_admin)
}

fn actor(session: &AuthSession) -> String {
    session
        .user
        .as_ref()
        .map(|user| user.username.clone())
        .unwrap_or_else(|| "dashboard-token".to_string())
}

fn require_admin(session: &AuthSession) -> Result<(), Response> {
    if is_admin(session) {
        Ok(())
    } else {
        Err(error(StatusCode::FORBIDDEN, "administrator role required"))
    }
}

fn require_csrf(auth: &DashboardAuth, headers: &HeaderMap) -> Result<(), Response> {
    if auth.verify_csrf(headers) {
        Ok(())
    } else {
        Err(error(
            StatusCode::FORBIDDEN,
            "missing or invalid CSRF token",
        ))
    }
}

// ---------------------------------------------------------------------------
// /me
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct MeBody {
    /// `None` for a token/dev-open session.
    user: Option<SessionUser>,
    is_admin: bool,
    /// `users` when at least one account exists, `token` when only the env
    /// token gates the dashboard, `open` when nothing does (loopback dev).
    auth_mode: &'static str,
    /// Whether users/keys can be managed (a SQL store is configured).
    accounts_enabled: bool,
}

/// `GET /dashboard/api/me`
pub async fn me(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
) -> Response {
    let body = MeBody {
        is_admin: is_admin(&session),
        user: session.user,
        auth_mode: auth_mode(&gateway, &auth).await,
        accounts_enabled: gateway.persistence_store().is_some(),
    };
    json(StatusCode::OK, &body)
}

/// The dashboard's authentication mode, for the login shell and `/me`. Users
/// created out of band (the CLI, another replica) are noticed here: while the
/// in-memory flag is off, a cheap `count_users` refreshes it.
pub async fn auth_mode(gateway: &Gateway, auth: &DashboardAuth) -> &'static str {
    if !gateway.users_configured()
        && let Some(store) = gateway.persistence_store()
        && let Ok(count) = store.count_users().await
        && count > 0
    {
        gateway.set_users_configured(true);
    }
    if gateway.users_configured() {
        "users"
    } else if auth.dev_open() {
        "open"
    } else {
        "token"
    }
}

// ---------------------------------------------------------------------------
// /users
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub is_admin: bool,
}

#[derive(Debug, Deserialize)]
pub struct UpdateUserRequest {
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub is_admin: Option<bool>,
}

#[derive(Debug, Serialize)]
struct UsersBody {
    users: Vec<UserRecord>,
}

/// `GET /dashboard/api/users` (admin)
pub async fn list_users(State(gateway): State<Arc<Gateway>>, session: AuthSession) -> Response {
    if let Err(denied) = require_admin(&session) {
        return denied;
    }
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    match store.list_users().await {
        Ok(users) => json(StatusCode::OK, &UsersBody { users }),
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("list users: {err}"),
        ),
    }
}

/// `POST /dashboard/api/users` (admin, CSRF)
pub async fn create_user(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    headers: HeaderMap,
    payload: Result<Json<CreateUserRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Err(denied) = require_admin(&session).and_then(|()| require_csrf(&auth, &headers)) {
        return denied;
    }
    let Ok(Json(body)) = payload else {
        return error(
            StatusCode::BAD_REQUEST,
            "expected {username, password, is_admin?}",
        );
    };
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    let username = match accounts::validate_username(&body.username) {
        Ok(username) => username.to_string(),
        Err(err) => return error(StatusCode::BAD_REQUEST, &err),
    };
    let hash = match accounts::hash_password(&body.password) {
        Ok(hash) => hash,
        Err(err) => return error(StatusCode::BAD_REQUEST, &err),
    };
    match store.get_user_auth(&username).await {
        Ok(Some(_)) => return error(StatusCode::CONFLICT, "username already exists"),
        Ok(None) => {}
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("lookup user: {err}"),
            );
        }
    }
    match store
        .create_user(&username, &hash, body.is_admin, &actor(&session))
        .await
    {
        Ok(user) => {
            gateway.set_users_configured(true);
            json(StatusCode::CREATED, &user)
        }
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("create user: {err}"),
        ),
    }
}

/// `PATCH /dashboard/api/users/{id}` (admin, CSRF): reset password and/or role.
pub async fn update_user(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    Path(id): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<UpdateUserRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Err(denied) = require_admin(&session).and_then(|()| require_csrf(&auth, &headers)) {
        return denied;
    }
    let Ok(Json(body)) = payload else {
        return error(StatusCode::BAD_REQUEST, "expected {password?, is_admin?}");
    };
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    match store.get_user(&id).await {
        Ok(Some(_)) => {}
        Ok(None) => return error(StatusCode::NOT_FOUND, "user not found"),
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("lookup user: {err}"),
            );
        }
    }
    // An admin cannot demote themselves (no lock-out through the UI).
    if body.is_admin == Some(false) && session.user.as_ref().is_some_and(|user| user.id == id) {
        return error(
            StatusCode::BAD_REQUEST,
            "you cannot remove your own administrator role",
        );
    }
    let hash = match body.password.as_deref() {
        Some(password) => match accounts::hash_password(password) {
            Ok(hash) => Some(hash),
            Err(err) => return error(StatusCode::BAD_REQUEST, &err),
        },
        None => None,
    };
    match store
        .update_user(&id, hash.as_deref(), body.is_admin, &actor(&session))
        .await
    {
        Ok(()) => json(
            StatusCode::OK,
            &serde_json::json!({ "id": id, "updated": true }),
        ),
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("update user: {err}"),
        ),
    }
}

/// `DELETE /dashboard/api/users/{id}` (admin, CSRF): soft-deletes the user and
/// revokes their keys (the registry reloads immediately).
pub async fn delete_user(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(denied) = require_admin(&session).and_then(|()| require_csrf(&auth, &headers)) {
        return denied;
    }
    if session.user.as_ref().is_some_and(|user| user.id == id) {
        return error(
            StatusCode::BAD_REQUEST,
            "you cannot delete your own account",
        );
    }
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    match store.get_user(&id).await {
        Ok(Some(_)) => {}
        Ok(None) => return error(StatusCode::NOT_FOUND, "user not found"),
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("lookup user: {err}"),
            );
        }
    }
    let who = actor(&session);
    let keys = match store.list_api_keys_for_user(&id).await {
        Ok(keys) => keys,
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("list keys: {err}"),
            );
        }
    };
    for key in &keys {
        if let Err(err) = store.delete_api_key(&key.id, &who).await {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("revoke key: {err}"),
            );
        }
    }
    if let Err(err) = store.delete_user(&id, &who).await {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("delete user: {err}"),
        );
    }
    if let Err(err) = accounts::reload_client_keys(&gateway).await {
        tracing::warn!(error = %err, "key registry reload after user delete failed");
    }
    json(
        StatusCode::OK,
        &serde_json::json!({ "id": id, "deleted": true, "keys_revoked": keys.len() }),
    )
}

// ---------------------------------------------------------------------------
// /keys
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
pub struct KeysQuery {
    /// Admin only: another user's keys, or `all`.
    pub user_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateKeyRequest {
    #[serde(default)]
    pub label: Option<String>,
    /// Client-facing model/alias names this key may request; empty = any.
    #[serde(default)]
    pub allowed_models: Vec<String>,
    /// Admin only: create the key for another user (default: the caller).
    #[serde(default)]
    pub user_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct KeysBody {
    keys: Vec<ApiKeyRecord>,
}

#[derive(Debug, Serialize)]
struct CreatedKeyBody {
    key: ApiKeyRecord,
    /// The plaintext, shown exactly once.
    secret: String,
    registered_keys: usize,
}

/// `GET /dashboard/api/keys[?user_id=<id>|all]`
pub async fn list_keys(
    State(gateway): State<Arc<Gateway>>,
    session: AuthSession,
    Query(query): Query<KeysQuery>,
) -> Response {
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    let scope = match (&session.user, query.user_id.as_deref()) {
        (Some(user), None) => Some(user.id.clone()),
        (Some(user), Some(requested)) if !user.is_admin => {
            if requested != user.id {
                return error(StatusCode::FORBIDDEN, "administrator role required");
            }
            Some(user.id.clone())
        }
        (_, Some("all")) | (None, None) => None,
        (_, Some(requested)) => Some(requested.to_string()),
    };
    let result = match scope {
        Some(user_id) => store.list_api_keys_for_user(&user_id).await,
        None => store.list_api_keys().await,
    };
    match result {
        Ok(keys) => json(StatusCode::OK, &KeysBody { keys }),
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("list keys: {err}"),
        ),
    }
}

/// `POST /dashboard/api/keys` (CSRF): mints a key, stores its digest, reloads
/// the live registry, and returns the plaintext once.
pub async fn create_key(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    headers: HeaderMap,
    payload: Result<Json<CreateKeyRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Err(denied) = require_csrf(&auth, &headers) {
        return denied;
    }
    let body = match payload {
        Ok(Json(body)) => body,
        Err(_) => CreateKeyRequest {
            label: None,
            allowed_models: Vec::new(),
            user_id: None,
        },
    };
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    let owner = match (&session.user, body.user_id.as_deref()) {
        (Some(user), Some(requested)) if requested != user.id => {
            if !user.is_admin {
                return error(StatusCode::FORBIDDEN, "administrator role required");
            }
            Some(requested.to_string())
        }
        (Some(user), _) => Some(user.id.clone()),
        (None, requested) => requested.map(str::to_string),
    };
    if let Some(owner) = owner.as_deref() {
        match store.get_user(owner).await {
            Ok(Some(_)) => {}
            Ok(None) => return error(StatusCode::NOT_FOUND, "user not found"),
            Err(err) => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("lookup user: {err}"),
                );
            }
        }
    }
    let label = body
        .label
        .as_deref()
        .map(str::trim)
        .filter(|label| !label.is_empty())
        .map(|label| label.chars().take(MAX_LABEL_LEN).collect::<String>());
    let allowed: Vec<String> = body
        .allowed_models
        .iter()
        .map(|model| model.trim().to_string())
        .filter(|model| !model.is_empty())
        .take(MAX_ALLOWED_MODELS)
        .collect();
    let secret = accounts::generate_api_key();
    let id = uuid::Uuid::new_v4().to_string();
    let record = match store
        .put_api_key(
            &id,
            &secret,
            label.as_deref(),
            owner.as_deref(),
            &allowed,
            &actor(&session),
        )
        .await
    {
        Ok(record) => record,
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("store key: {err}"),
            );
        }
    };
    let registered_keys = match accounts::reload_client_keys(&gateway).await {
        Ok(count) => count,
        Err(err) => {
            // The digest is stored; the registry will pick it up on the next
            // successful reload. Report it rather than hand out a dead key.
            tracing::warn!(error = %err, "key registry reload after key create failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("key stored but not activated: {err}"),
            );
        }
    };
    json(
        StatusCode::CREATED,
        &CreatedKeyBody {
            key: record,
            secret,
            registered_keys,
        },
    )
}

/// `DELETE /dashboard/api/keys/{id}` (CSRF): revokes a key (own, or any as admin).
pub async fn delete_key(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(denied) = require_csrf(&auth, &headers) {
        return denied;
    }
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    let keys = match store.list_api_keys().await {
        Ok(keys) => keys,
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("list keys: {err}"),
            );
        }
    };
    let Some(key) = keys.into_iter().find(|key| key.id == id) else {
        return error(StatusCode::NOT_FOUND, "key not found");
    };
    if let Some(user) = &session.user
        && !user.is_admin
        && key.user_id.as_deref() != Some(user.id.as_str())
    {
        return error(StatusCode::FORBIDDEN, "not your key");
    }
    if let Err(err) = store.delete_api_key(&id, &actor(&session)).await {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("revoke key: {err}"),
        );
    }
    let registered_keys = accounts::reload_client_keys(&gateway)
        .await
        .unwrap_or_else(|err| {
            tracing::warn!(error = %err, "key registry reload after key revoke failed");
            0
        });
    json(
        StatusCode::OK,
        &serde_json::json!({ "id": id, "revoked": true, "registered_keys": registered_keys }),
    )
}
