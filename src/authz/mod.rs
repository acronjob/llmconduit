pub mod keys;
pub mod policy;
pub mod session;
pub mod store;

use crate::config::{AuthConfig, AuthMode};
use axum::http::HeaderMap;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::{Arc, RwLock};
use subtle::ConstantTimeEq;

pub use policy::{
    AuthError, AuthRequestId, Endpoint, LimitSet, ManagementPermission, PolicyBinding,
    PolicyDecision, PolicyEffect, PolicyIdentity, PolicyMatcher, PolicyRule, PolicyScope,
    PolicySnapshot, PolicySubject, UtcWindow,
};
pub use store::{ApiKeySummary, CreatedApiKey};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub auth_request_id: String,
    pub key_id: String,
    pub principal_id: String,
    grants: Arc<Vec<PolicyGrant>>,
}

impl AuthContext {
    pub fn allows_endpoint(&self, endpoint: &str) -> bool {
        decide(&self.grants, endpoint, None)
    }

    pub fn allows_model(&self, endpoint: &str, model: &str) -> bool {
        decide(&self.grants, endpoint, Some(model))
    }

    /// Produces the transport-neutral candidate predicate carried by routing,
    /// failover, mesh, completions, and token-count dispatch. The closure owns
    /// only immutable grants and never captures the presented raw credential.
    pub fn authorization_scope(&self) -> crate::upstream::AuthorizationScope {
        let grants = Arc::clone(&self.grants);
        crate::upstream::AuthorizationScope::restricted(
            move |_provider_id, _route_id, served_model, endpoint| {
                decide(
                    &grants,
                    inference_endpoint_name(endpoint),
                    Some(served_model),
                )
            },
        )
    }
}

#[derive(Debug, Clone)]
struct PolicyGrant {
    effect: Effect,
    endpoint: String,
    model_pattern: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Effect {
    Allow,
    Deny,
}

#[derive(Debug, Clone)]
struct KeyRecord {
    id: String,
    principal_id: String,
    digest: [u8; 32],
    grants: Arc<Vec<PolicyGrant>>,
}

#[derive(Debug, Default)]
struct Snapshot {
    keys: Vec<KeyRecord>,
}

struct Inner {
    pepper: Vec<u8>,
    store: std::sync::Mutex<store::AuthStore>,
    snapshot: RwLock<Arc<Snapshot>>,
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
        if pepper.trim().is_empty() {
            return Err("LLMCONDUIT_AUTH_PEPPER must not be blank".to_string());
        }
        let bootstrap = std::env::var("LLMCONDUIT_AUTH_BOOTSTRAP_KEY").ok();
        Self::open_enforced(config, pepper.into_bytes(), bootstrap.as_deref())
    }

    fn open_enforced(
        config: &AuthConfig,
        pepper: Vec<u8>,
        bootstrap: Option<&str>,
    ) -> Result<Self, String> {
        if config.store_path.as_os_str().is_empty() {
            return Err("auth.store_path must not be blank in enforce mode".to_string());
        }
        let mut store = store::AuthStore::open(&config.store_path)?;
        if store.key_count()? == 0 {
            let raw = bootstrap
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    "auth store has no keys; set LLMCONDUIT_AUTH_BOOTSTRAP_KEY for first startup"
                        .to_string()
                })?;
            if !raw.starts_with("llmc_") || raw.len() < 32 {
                return Err(
                    "LLMCONDUIT_AUTH_BOOTSTRAP_KEY must be an llmc_ key with at least 32 characters"
                        .to_string(),
                );
            }
            let digest = hmac_digest(&pepper, raw)?;
            store.insert_bootstrap_key(raw, &digest)?;
        }
        let snapshot = Arc::new(load_snapshot(&mut store)?);
        Ok(Self {
            inner: Some(Arc::new(Inner {
                pepper,
                store: std::sync::Mutex::new(store),
                snapshot: RwLock::new(snapshot),
            })),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub fn authenticate(&self, headers: &HeaderMap) -> Result<Option<AuthContext>, AuthFailure> {
        let Some(inner) = &self.inner else {
            return Ok(None);
        };
        let raw = presented_key(headers).ok_or(AuthFailure::Missing)?;
        if !raw.starts_with("llmc_") || raw.len() < 32 {
            return Err(AuthFailure::Invalid);
        }
        let digest = hmac_digest(&inner.pepper, raw).map_err(|_| AuthFailure::Unavailable)?;
        let snapshot = inner
            .snapshot
            .read()
            .map_err(|_| AuthFailure::Unavailable)?
            .clone();
        // Scan every record rather than stopping at the first match. Each
        // verifier comparison is fixed-width and constant-time; avoiding an
        // early exit also keeps lookup work independent of the matching row's
        // position in the snapshot.
        let mut matched = None;
        for record in &snapshot.keys {
            if bool::from(record.digest.ct_eq(&digest)) {
                matched = Some(record);
            }
        }
        let record = matched.ok_or(AuthFailure::Invalid)?;
        Ok(Some(AuthContext {
            auth_request_id: format!("areq_{}", uuid::Uuid::new_v4().simple()),
            key_id: record.id.clone(),
            principal_id: record.principal_id.clone(),
            grants: Arc::clone(&record.grants),
        }))
    }

    pub fn create_key(
        &self,
        principal_name: &str,
        key_name: &str,
        endpoints: &[String],
        models: &[String],
    ) -> Result<CreatedApiKey, String> {
        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| "inference auth is disabled".to_string())?;
        let raw = format!(
            "llmc_{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let digest = hmac_digest(&inner.pepper, &raw)?;
        let created = {
            let mut store = inner.store.lock().map_err(|_| "auth store lock poisoned")?;
            let created =
                store.create_key(principal_name, key_name, &raw, &digest, endpoints, models)?;
            self.reload_locked(inner, &mut store)?;
            created
        };
        Ok(created.with_raw(raw))
    }

    pub fn revoke_key(&self, key_id: &str) -> Result<bool, String> {
        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| "inference auth is disabled".to_string())?;
        let mut store = inner.store.lock().map_err(|_| "auth store lock poisoned")?;
        let changed = store.revoke_key(key_id)?;
        if changed {
            self.reload_locked(inner, &mut store)?;
        }
        Ok(changed)
    }

    pub fn list_keys(&self) -> Result<Vec<ApiKeySummary>, String> {
        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| "inference auth is disabled".to_string())?;
        inner
            .store
            .lock()
            .map_err(|_| "auth store lock poisoned".to_string())?
            .list_keys()
    }

    fn reload_locked(&self, inner: &Inner, store: &mut store::AuthStore) -> Result<(), String> {
        let fresh = Arc::new(load_snapshot(store)?);
        *inner
            .snapshot
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

fn load_snapshot(store: &mut store::AuthStore) -> Result<Snapshot, String> {
    Ok(Snapshot {
        keys: store
            .load_active_keys()?
            .into_iter()
            .map(|row| KeyRecord {
                id: row.id,
                principal_id: row.principal_id,
                digest: row.digest,
                grants: Arc::new(
                    row.grants
                        .into_iter()
                        .map(|grant| PolicyGrant {
                            effect: if grant.effect == "deny" {
                                Effect::Deny
                            } else {
                                Effect::Allow
                            },
                            endpoint: grant.endpoint,
                            model_pattern: grant.model_pattern,
                        })
                        .collect(),
                ),
            })
            .collect(),
    })
}

fn hmac_digest(pepper: &[u8], raw: &str) -> Result<[u8; 32], String> {
    let mut mac = HmacSha256::new_from_slice(pepper)
        .map_err(|_| "failed to initialize API-key verifier".to_string())?;
    mac.update(raw.as_bytes());
    Ok(mac.finalize().into_bytes().into())
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

fn decide(grants: &[PolicyGrant], endpoint: &str, model: Option<&str>) -> bool {
    let mut allowed = false;
    for grant in grants {
        if !matches_pattern(&grant.endpoint, endpoint)
            || model.is_some_and(|model| !matches_pattern(&grant.model_pattern, model))
        {
            continue;
        }
        if grant.effect == Effect::Deny {
            return false;
        }
        allowed = true;
    }
    allowed
}

fn matches_pattern(pattern: &str, value: &str) -> bool {
    let pattern = pattern.trim();
    if pattern == "*" {
        return true;
    }
    let (mut p, mut v) = (0usize, 0usize);
    let bytes = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut star, mut retry) = (None, 0usize);
    while v < value.len() {
        if p < bytes.len() && (bytes[p] == b'?' || bytes[p].eq_ignore_ascii_case(&value[v])) {
            p += 1;
            v += 1;
        } else if p < bytes.len() && bytes[p] == b'*' {
            star = Some(p);
            retry = v;
            p += 1;
        } else if let Some(index) = star {
            p = index + 1;
            retry += 1;
            v = retry;
        } else {
            return false;
        }
    }
    while p < bytes.len() && bytes[p] == b'*' {
        p += 1;
    }
    p == bytes.len()
}

fn inference_endpoint_name(endpoint: crate::upstream::InferenceEndpoint) -> &'static str {
    match endpoint {
        crate::upstream::InferenceEndpoint::Responses => "responses",
        crate::upstream::InferenceEndpoint::ChatCompletions => "chat",
        crate::upstream::InferenceEndpoint::Messages => "messages",
        crate::upstream::InferenceEndpoint::CountTokens => "count_tokens",
        crate::upstream::InferenceEndpoint::Completions => "completions",
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthContext, AuthzService, Effect, PolicyGrant, decide, matches_pattern};
    use crate::config::{AuthConfig, AuthMode};
    use crate::upstream::InferenceEndpoint;
    use axum::http::{HeaderMap, HeaderValue};
    use std::sync::Arc;

    #[test]
    fn explicit_deny_overrides_allow() {
        let grants = vec![
            PolicyGrant {
                effect: Effect::Allow,
                endpoint: "*".into(),
                model_pattern: "*".into(),
            },
            PolicyGrant {
                effect: Effect::Deny,
                endpoint: "chat".into(),
                model_pattern: "secret-*".into(),
            },
        ];
        assert!(decide(&grants, "chat", Some("public-model")));
        assert!(!decide(&grants, "chat", Some("secret-v1")));
        assert!(!decide(&[], "chat", Some("public-model")));
    }

    #[test]
    fn glob_matching_is_case_insensitive() {
        assert!(matches_pattern("Claude-*-Sonnet", "claude-4-sonnet"));
        assert!(!matches_pattern("Claude-*-Sonnet", "claude-4-opus"));
    }

    #[test]
    fn dispatch_scope_reuses_grants_without_raw_credentials() {
        let context = AuthContext {
            auth_request_id: "areq_test".into(),
            key_id: "key_test".into(),
            principal_id: "usr_test".into(),
            grants: Arc::new(vec![PolicyGrant {
                effect: Effect::Allow,
                endpoint: "responses".into(),
                model_pattern: "public-*".into(),
            }]),
        };
        let scope = context.authorization_scope();
        assert!(scope.allows_candidate(
            "primary",
            None,
            "public-model",
            InferenceEndpoint::Responses
        ));
        assert!(!scope.allows_candidate(
            "primary",
            None,
            "secret-model",
            InferenceEndpoint::Responses
        ));
        assert!(!scope.allows_candidate(
            "primary",
            None,
            "public-model",
            InferenceEndpoint::Messages
        ));
    }

    #[test]
    fn key_lifecycle_is_scoped_redacted_and_immediately_revoked() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-auth-test-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let config = AuthConfig {
            mode: AuthMode::Enforce,
            store_path: path.clone(),
        };
        let bootstrap = format!("llmc_{}", uuid::Uuid::new_v4().simple());
        let service = AuthzService::open_enforced(
            &config,
            b"unit-test-pepper-never-log".to_vec(),
            Some(&bootstrap),
        )
        .unwrap();

        let created = service
            .create_key(
                "reporting service",
                "reporting",
                &["chat".to_string()],
                &["public-*".to_string()],
            )
            .unwrap();
        let raw = created.raw_key.as_deref().unwrap().to_string();
        assert!(raw.starts_with("llmc_"));
        assert!(!format!("{created:?}").contains(&raw));

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(&raw).expect("generated key is a valid header"),
        );
        let context = service.authenticate(&headers).unwrap().unwrap();
        assert!(context.allows_endpoint("chat"));
        assert!(!context.allows_endpoint("responses"));
        assert!(context.allows_model("chat", "public-v1"));
        assert!(!context.allows_model("chat", "secret-v1"));
        assert!(context.auth_request_id.starts_with("areq_"));

        let listed = service.list_keys().unwrap();
        assert!(listed.iter().all(|key| !key.prefix.contains(&raw)));
        assert!(service.revoke_key(&created.summary.id).unwrap());
        assert!(service.authenticate(&headers).is_err());

        drop(service);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }
}
