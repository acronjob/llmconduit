mod access;
pub mod keys;
pub mod policy;
pub mod session;
pub mod store;

use crate::config::{AuthConfig, AuthMode};
use axum::http::HeaderMap;
use std::sync::{Arc, RwLock};

use keys::{AuthPepper, digest_matches, generate_api_key, key_prefix};
pub use policy::{
    AuthError, AuthRequestId, Endpoint, LimitSet, ManagementPermission, PolicyBinding,
    PolicyDecision, PolicyEffect, PolicyIdentity, PolicyMatcher, PolicyRule, PolicyScope,
    PolicySnapshot, PolicySubject, UtcWindow,
};
pub use session::{SessionLease, SessionLimiter};
pub use store::{ApiKeySummary, CreatedApiKey};

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub auth_request_id: String,
    pub key_id: String,
    pub principal_id: String,
    identity: PolicyIdentity,
    policy: Arc<PolicySnapshot>,
}

impl AuthContext {
    pub fn management_actor(&self) -> crate::dashboard_access::ManagementActor {
        if self.key_id == "key_bootstrap" {
            return crate::dashboard_access::ManagementActor::Bootstrap;
        }
        let permissions = self
            .policy
            .management_permissions(&self.identity, chrono::Utc::now())
            .into_iter()
            .filter_map(access::wire_permission)
            .collect::<Vec<_>>();
        crate::dashboard_access::ManagementActor::Delegated {
            session_id: self.auth_request_id.clone(),
            principal_id: self.principal_id.clone(),
            key_id: self.key_id.clone(),
            permissions: permissions.into(),
        }
    }

    pub fn allows_endpoint(&self, endpoint: &str) -> bool {
        Endpoint::parse(endpoint).is_some_and(|endpoint| {
            self.policy
                .permits_endpoint(&self.identity, endpoint, chrono::Utc::now())
        })
    }

    pub fn allows_model(&self, endpoint: &str, model: &str) -> bool {
        Endpoint::parse(endpoint).is_some_and(|endpoint| {
            self.policy
                .authorize(&self.identity, endpoint, Some(model), chrono::Utc::now())
                .is_ok()
        })
    }

    pub fn authorization_scope(&self) -> crate::upstream::AuthorizationScope {
        let identity = self.identity.clone();
        let policy = Arc::clone(&self.policy);
        crate::upstream::AuthorizationScope::restricted(
            move |provider_id, route_id, served_model, endpoint| {
                let endpoint = inference_endpoint(endpoint);
                policy
                    .authorize(&identity, endpoint, Some(served_model), chrono::Utc::now())
                    .is_ok_and(|scope| {
                        scope.allows_candidate(Some(provider_id), route_id, Some(served_model))
                    })
            },
        )
    }

    pub fn effective_limits(&self) -> LimitSet {
        self.policy
            .effective_limits(&self.identity, chrono::Utc::now())
    }
}

struct Inner {
    pepper: AuthPepper,
    store: std::sync::Mutex<store::AuthStore>,
    authority: RwLock<Arc<store::StoredAuthority>>,
    session_limiter: SessionLimiter,
}

#[derive(Clone, Default)]
pub struct AuthzService {
    inner: Option<Arc<Inner>>,
}

impl std::fmt::Debug for AuthzService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthzService")
            .field("enabled", &self.inner.is_some())
            .finish()
    }
}

impl AuthzService {
    pub fn from_config(config: &AuthConfig) -> Result<Self, String> {
        if config.mode == AuthMode::Disabled {
            return Ok(Self::default());
        }
        let pepper = std::env::var("LLMCONDUIT_AUTH_PEPPER")
            .map_err(|_| "auth.mode=enforce requires LLMCONDUIT_AUTH_PEPPER".to_string())?;
        let bootstrap = std::env::var("LLMCONDUIT_AUTH_BOOTSTRAP_KEY").ok();
        Self::open_enforced(config, pepper.into_bytes(), bootstrap.as_deref())
    }

    fn open_enforced(
        config: &AuthConfig,
        pepper: Vec<u8>,
        bootstrap: Option<&str>,
    ) -> Result<Self, String> {
        if config.store_path.as_os_str().is_empty() {
            return Err("auth.store_path must not be blank in enforce mode".into());
        }
        let pepper = AuthPepper::new(pepper).map_err(|err| err.to_string())?;
        let mut store = store::AuthStore::open(&config.store_path)?;
        if store.key_count()? == 0 {
            let raw = bootstrap
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    "auth store has no keys; set LLMCONDUIT_AUTH_BOOTSTRAP_KEY for first startup"
                        .to_string()
                })?;
            key_prefix(raw).map_err(|_| {
                "LLMCONDUIT_AUTH_BOOTSTRAP_KEY must be a valid llmc_ API key".to_string()
            })?;
            store.insert_bootstrap_key(raw, &pepper.digest(raw))?;
        }
        let authority = Arc::new(store.load_authority()?);
        Ok(Self {
            inner: Some(Arc::new(Inner {
                pepper,
                store: std::sync::Mutex::new(store),
                authority: RwLock::new(authority),
                session_limiter: SessionLimiter::default(),
            })),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub fn enabled(&self) -> bool {
        self.is_enabled()
    }

    pub fn access_backend(self: &Arc<Self>) -> Arc<dyn crate::dashboard_access::AccessBackend> {
        Arc::clone(self) as Arc<dyn crate::dashboard_access::AccessBackend>
    }

    pub fn authenticate(&self, headers: &HeaderMap) -> Result<Option<AuthContext>, AuthFailure> {
        let Some(inner) = &self.inner else {
            return Ok(None);
        };
        let raw = presented_key(headers).ok_or(AuthFailure::Missing)?;
        key_prefix(raw).map_err(|_| AuthFailure::Invalid)?;
        let digest = inner.pepper.digest(raw);
        let authority = inner
            .authority
            .read()
            .map_err(|_| AuthFailure::Unavailable)?
            .clone();
        let mut matched = None;
        for credential in &authority.credentials {
            if digest_matches(&credential.digest, &digest) {
                matched = Some(credential);
            }
        }
        let credential = matched.ok_or(AuthFailure::Invalid)?;
        let request_id = AuthRequestId::new();
        let identity = PolicyIdentity {
            request_id: request_id.clone(),
            key_id: credential.id.clone(),
            key_prefix: credential.prefix.clone(),
            principal_id: credential.principal_id.clone(),
            policy_epoch: authority.epoch,
        };
        Ok(Some(AuthContext {
            auth_request_id: request_id.as_str().to_string(),
            key_id: identity.key_id.clone(),
            principal_id: identity.principal_id.clone(),
            identity,
            policy: Arc::clone(&authority.policy),
        }))
    }

    pub fn acquire_session(
        &self,
        context: &AuthContext,
    ) -> Result<Option<SessionLease>, AuthError> {
        let Some(inner) = &self.inner else {
            return Ok(None);
        };
        let session_id = format!("ses_{}", uuid::Uuid::new_v4().simple());
        let weak = Arc::downgrade(inner);
        let on_release: Arc<dyn Fn(&str) + Send + Sync> = Arc::new(move |session_id| {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            if let Ok(mut store) = inner.store.lock() {
                let _ = store.delete_session(session_id);
            }
        });
        let lease = inner.session_limiter.acquire_for_key_with_release(
            &context.key_id,
            context.effective_limits(),
            chrono::Utc::now(),
            session_id.clone(),
            Some(on_release),
        )?;
        if inner
            .store
            .lock()
            .map_err(|_| AuthError::PolicyUnavailable)?
            .insert_session(&session_id, &context.key_id)
            .is_err()
        {
            drop(lease);
            return Err(AuthError::PolicyUnavailable);
        }
        Ok(Some(lease))
    }

    pub fn create_delegated_session(
        &self,
        context: &AuthContext,
        csrf_digest: &[u8],
        expires_at: i64,
    ) -> Result<crate::dashboard_access::ManagementActor, AuthError> {
        let inner = self.inner.as_ref().ok_or(AuthError::PolicyUnavailable)?;
        let permissions = context
            .policy
            .management_permissions(&context.identity, chrono::Utc::now())
            .into_iter()
            .filter_map(access::wire_permission)
            .collect::<Vec<_>>();
        if permissions.is_empty() {
            return Err(AuthError::Forbidden);
        }
        let session = inner
            .store
            .lock()
            .map_err(|_| AuthError::PolicyUnavailable)?
            .create_dashboard_session(
                &context.principal_id,
                &context.key_id,
                csrf_digest,
                expires_at,
                context.identity.policy_epoch,
            )
            .map_err(|_| AuthError::PolicyUnavailable)?;
        Ok(crate::dashboard_access::ManagementActor::Delegated {
            session_id: session.session_id,
            principal_id: session.principal_id,
            key_id: session.key_id,
            permissions: permissions.into(),
        })
    }

    pub fn authenticate_delegated_session(
        &self,
        session_id: &str,
    ) -> Result<Option<crate::dashboard_access::ManagementActor>, AuthError> {
        let Some(inner) = &self.inner else {
            return Ok(None);
        };
        let session = inner
            .store
            .lock()
            .map_err(|_| AuthError::PolicyUnavailable)?
            .load_dashboard_session(session_id)
            .map_err(|_| AuthError::PolicyUnavailable)?;
        let Some(session) = session else {
            return Ok(None);
        };
        let authority = inner
            .authority
            .read()
            .map_err(|_| AuthError::PolicyUnavailable)?
            .clone();
        let identity = PolicyIdentity {
            request_id: AuthRequestId::new(),
            key_id: session.key_id.clone(),
            key_prefix: session.key_prefix,
            principal_id: session.principal_id.clone(),
            policy_epoch: authority.epoch,
        };
        let permissions = authority
            .policy
            .management_permissions(&identity, chrono::Utc::now())
            .into_iter()
            .filter_map(access::wire_permission)
            .collect::<Vec<_>>();
        if permissions.is_empty() {
            let _ = inner
                .store
                .lock()
                .map_err(|_| AuthError::PolicyUnavailable)?
                .revoke_dashboard_session(session_id, "permission_removed");
            return Ok(None);
        }
        Ok(Some(crate::dashboard_access::ManagementActor::Delegated {
            session_id: session.session_id,
            principal_id: session.principal_id,
            key_id: session.key_id,
            permissions: permissions.into(),
        }))
    }

    pub fn revoke_delegated_session(&self, session_id: &str) -> Result<bool, String> {
        let inner = self.inner()?;
        inner
            .store
            .lock()
            .map_err(|_| "auth store lock poisoned".to_string())?
            .revoke_dashboard_session(session_id, "logout")
    }

    pub fn create_key(
        &self,
        principal_name: &str,
        key_name: &str,
        endpoints: &[String],
        models: &[String],
    ) -> Result<CreatedApiKey, String> {
        let inner = self.inner()?;
        let generated = generate_api_key(&inner.pepper);
        let raw = generated.expose_once();
        let digest = inner.pepper.digest(&raw);
        let mut store = inner.store.lock().map_err(|_| "auth store lock poisoned")?;
        let created =
            store.create_key(principal_name, key_name, &raw, &digest, endpoints, models)?;
        self.reload_locked(inner, &store)?;
        Ok(created.with_raw(raw))
    }

    pub fn revoke_key(&self, key_id: &str) -> Result<bool, String> {
        let inner = self.inner()?;
        let mut store = inner.store.lock().map_err(|_| "auth store lock poisoned")?;
        let changed = store.revoke_key(key_id)?;
        if changed {
            self.reload_locked(inner, &store)?;
        }
        Ok(changed)
    }

    pub fn list_keys(&self) -> Result<Vec<ApiKeySummary>, String> {
        let inner = self.inner()?;
        inner
            .store
            .lock()
            .map_err(|_| "auth store lock poisoned".to_string())?
            .list_keys()
    }

    pub fn record_usage_once(
        &self,
        event: &crate::usage_accounting::UsageEvent,
    ) -> Result<bool, String> {
        let Some(inner) = &self.inner else {
            return Ok(false);
        };
        inner
            .store
            .lock()
            .map_err(|_| "auth store lock poisoned".to_string())?
            .record_usage_once(event)
    }

    fn inner(&self) -> Result<&Arc<Inner>, String> {
        self.inner
            .as_ref()
            .ok_or_else(|| "inference auth is disabled".to_string())
    }

    fn reload_locked(&self, inner: &Inner, store: &store::AuthStore) -> Result<(), String> {
        let fresh = Arc::new(store.load_authority()?);
        *inner
            .authority
            .write()
            .map_err(|_| "auth snapshot lock poisoned")? = fresh;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthFailure {
    Missing,
    Invalid,
    Forbidden,
    Unavailable,
}

fn presented_key(headers: &HeaderMap) -> Option<&str> {
    let authorization = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(value) = authorization {
        if value.len() >= 7 && value[..7].eq_ignore_ascii_case("bearer ") {
            return Some(value[7..].trim()).filter(|value| !value.is_empty());
        }
        return Some(value);
    }
    headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn inference_endpoint(endpoint: crate::upstream::InferenceEndpoint) -> Endpoint {
    match endpoint {
        crate::upstream::InferenceEndpoint::Responses => Endpoint::Responses,
        crate::upstream::InferenceEndpoint::ChatCompletions => Endpoint::ChatCompletions,
        crate::upstream::InferenceEndpoint::Messages => Endpoint::Messages,
        crate::upstream::InferenceEndpoint::CountTokens => Endpoint::CountTokens,
        crate::upstream::InferenceEndpoint::Completions => Endpoint::Completions,
    }
}

#[cfg(test)]
mod tests {
    use super::AuthzService;
    use crate::config::{AuthConfig, AuthMode};
    use axum::http::{HeaderMap, HeaderValue};

    #[test]
    fn key_lifecycle_uses_rich_snapshot_and_revokes_immediately() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-rich-auth-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let config = AuthConfig {
            mode: AuthMode::Enforce,
            store_path: path.clone(),
        };
        let bootstrap = format!("llmc_{}", uuid::Uuid::new_v4().simple());
        let service =
            AuthzService::open_enforced(&config, b"unit-test-pepper".to_vec(), Some(&bootstrap))
                .unwrap();
        let created = service
            .create_key(
                "reporting",
                "reporting",
                &["chat".into()],
                &["public-*".into()],
            )
            .unwrap();
        let raw = created.raw_key.as_deref().unwrap().to_string();
        assert!(!format!("{created:?}").contains(&raw));
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_str(&raw).unwrap());
        let context = service.authenticate(&headers).unwrap().unwrap();
        assert!(context.allows_model("chat", "public-v1"));
        assert!(!context.allows_model("chat", "secret-v1"));
        assert!(!context.allows_endpoint("responses"));
        assert!(service.acquire_session(&context).unwrap().is_some());
        assert!(service.revoke_key(&created.summary.id).unwrap());
        assert!(service.authenticate(&headers).is_err());
        drop(service);
        let _ = std::fs::remove_file(path);
    }
}
