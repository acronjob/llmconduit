//! Authentication for client-facing inference APIs.
//!
//! Dashboard authentication deliberately remains a separate concern. This
//! module handles virtual API keys presented to `/v1/*`, keeps only fixed-size
//! SHA-256 digests in memory, and attaches a stable key id for authorization and
//! durable attribution.

use axum::http::HeaderMap;
use axum::http::header;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use subtle::ConstantTimeEq;

pub const KEY_HASH_PREFIX: &str = "sha256:";

#[derive(Clone)]
pub struct VirtualKeySpec {
    pub id: String,
    pub label: Option<String>,
    pub owner_id: Option<String>,
    /// Lowercase `sha256:<hex>`; plaintext keys are rejected at this boundary.
    pub secret_hash: String,
    /// Client-facing model/alias names this key may request. Empty means any.
    pub allowed_models: Vec<String>,
}

impl std::fmt::Debug for VirtualKeySpec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VirtualKeySpec")
            .field("id", &self.id)
            .field("label", &self.label)
            .field("owner_id", &self.owner_id)
            .field("secret_hash", &"[redacted]")
            .field("allowed_models", &self.allowed_models)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdentity {
    pub key_id: String,
    pub label: Option<String>,
    pub owner_id: Option<String>,
    allowed_models: Arc<[String]>,
}

impl ClientIdentity {
    pub fn allows_model(&self, model: &str) -> bool {
        self.allowed_models.is_empty()
            || self
                .allowed_models
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(model.trim()))
    }

    pub fn allowed_models(&self) -> &[String] {
        &self.allowed_models
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientAuthOutcome {
    Authenticated(ClientIdentity),
    Open,
    Rejected,
}

#[derive(Clone)]
pub struct ClientAuth {
    state: Arc<RwLock<AuthState>>,
}

#[derive(Default)]
struct AuthState {
    require: bool,
    by_digest: HashMap<[u8; 32], ClientIdentity>,
}

impl std::fmt::Debug for ClientAuth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.read().expect("client auth lock poisoned");
        formatter
            .debug_struct("ClientAuth")
            .field("require", &state.require)
            .field("keys", &state.by_digest.len())
            .finish()
    }
}

impl Default for ClientAuth {
    fn default() -> Self {
        Self::open()
    }
}

impl ClientAuth {
    pub fn open() -> Self {
        Self {
            state: Arc::new(RwLock::new(AuthState::default())),
        }
    }

    pub fn from_specs(
        require: bool,
        specs: impl IntoIterator<Item = VirtualKeySpec>,
    ) -> Result<Self, String> {
        let auth = Self::open();
        auth.replace(require, specs)?;
        Ok(auth)
    }

    /// Atomically replace the live key registry. Invalid input leaves the old
    /// registry untouched, which makes a failed control-plane edit harmless to
    /// already authenticated callers.
    pub fn replace(
        &self,
        require: bool,
        specs: impl IntoIterator<Item = VirtualKeySpec>,
    ) -> Result<(), String> {
        let mut next = HashMap::new();
        let mut ids = std::collections::HashSet::new();
        for spec in specs {
            let digest = parse_secret_hash(&spec.secret_hash)?;
            let id = spec.id.trim();
            if id.is_empty() {
                return Err("virtual API key id must not be blank".to_string());
            }
            if !ids.insert(id.to_string()) {
                return Err(format!("duplicate virtual API key id '{id}'"));
            }
            let identity = ClientIdentity {
                key_id: id.to_string(),
                label: trim_nonempty(spec.label),
                owner_id: trim_nonempty(spec.owner_id),
                allowed_models: spec
                    .allowed_models
                    .into_iter()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
                    .collect::<Vec<_>>()
                    .into(),
            };
            if next.insert(digest, identity).is_some() {
                return Err("two virtual API keys have the same secret hash".to_string());
            }
        }
        *self.state.write().expect("client auth lock poisoned") = AuthState {
            require,
            by_digest: next,
        };
        Ok(())
    }

    pub fn is_enforced(&self) -> bool {
        let state = self.state.read().expect("client auth lock poisoned");
        state.require || !state.by_digest.is_empty()
    }

    pub fn authenticate_headers(&self, headers: &HeaderMap) -> ClientAuthOutcome {
        self.authenticate(presented_key(headers).as_deref())
    }

    pub fn authenticate(&self, presented: Option<&str>) -> ClientAuthOutcome {
        let state = self.state.read().expect("client auth lock poisoned");
        if !state.require && state.by_digest.is_empty() {
            return ClientAuthOutcome::Open;
        }
        let Some(presented) = presented.map(str::trim).filter(|value| !value.is_empty()) else {
            return ClientAuthOutcome::Rejected;
        };
        let digest = digest_secret(presented);

        // HashMap lookup is suitable here because the lookup key is already a
        // one-way, fixed-size digest. Re-check the selected digest in constant
        // time so verification never compares raw secret bytes.
        let Some((stored, identity)) = state.by_digest.get_key_value(&digest) else {
            return ClientAuthOutcome::Rejected;
        };
        if bool::from(stored.ct_eq(&digest)) {
            ClientAuthOutcome::Authenticated(identity.clone())
        } else {
            ClientAuthOutcome::Rejected
        }
    }
}

pub fn hash_secret(secret: &str) -> String {
    format!("{KEY_HASH_PREFIX}{}", hex::encode(digest_secret(secret)))
}

pub fn parse_secret_hash(value: &str) -> Result<[u8; 32], String> {
    let encoded = value
        .trim()
        .strip_prefix(KEY_HASH_PREFIX)
        .ok_or("virtual API key secret must be stored as a sha256 digest")?;
    let bytes = hex::decode(encoded).map_err(|_| "invalid virtual API key SHA-256 digest")?;
    bytes
        .try_into()
        .map_err(|_| "virtual API key SHA-256 digest must contain 32 bytes".to_string())
}

pub fn presented_key(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        && let Some((scheme, token)) = value.split_once(' ')
        && scheme.eq_ignore_ascii_case("bearer")
        && !token.trim().is_empty()
    {
        return Some(token.trim().to_string());
    }
    headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn digest_secret(secret: &str) -> [u8; 32] {
    Sha256::digest(secret.as_bytes()).into()
}

fn trim_nonempty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(id: &str, secret: &str, allowed_models: &[&str]) -> VirtualKeySpec {
        VirtualKeySpec {
            id: id.to_string(),
            label: Some(format!("label-{id}")),
            owner_id: Some("owner-1".to_string()),
            secret_hash: hash_secret(secret),
            allowed_models: allowed_models
                .iter()
                .map(|value| value.to_string())
                .collect(),
        }
    }

    #[test]
    fn open_registry_accepts_anonymous_requests() {
        assert_eq!(
            ClientAuth::open().authenticate(None),
            ClientAuthOutcome::Open
        );
    }

    #[test]
    fn authenticates_without_retaining_plaintext() {
        let auth = ClientAuth::from_specs(false, [spec("key-1", "sk-secret", &["small"])])
            .expect("registry");
        let rendered = format!("{auth:?}");
        assert!(!rendered.contains("sk-secret"));
        let ClientAuthOutcome::Authenticated(identity) = auth.authenticate(Some("sk-secret"))
        else {
            panic!("expected authenticated identity");
        };
        assert_eq!(identity.key_id, "key-1");
        assert!(identity.allows_model("SMALL"));
        assert!(!identity.allows_model("large"));
        assert_eq!(
            auth.authenticate(Some("wrong")),
            ClientAuthOutcome::Rejected
        );
    }

    #[test]
    fn any_configured_key_enforces_authentication() {
        let auth = ClientAuth::from_specs(false, [spec("key-1", "secret", &[])]).expect("registry");
        assert!(auth.is_enforced());
        assert_eq!(auth.authenticate(None), ClientAuthOutcome::Rejected);
        let ClientAuthOutcome::Authenticated(identity) = auth.authenticate(Some("secret")) else {
            panic!("expected authenticated identity");
        };
        assert!(identity.allows_model("anything"));
    }

    #[test]
    fn replacement_is_atomic_on_invalid_input() {
        let auth =
            ClientAuth::from_specs(false, [spec("old", "old-secret", &[])]).expect("registry");
        let invalid = VirtualKeySpec {
            secret_hash: "plaintext".to_string(),
            ..spec("new", "unused", &[])
        };
        assert!(auth.replace(false, [invalid]).is_err());
        assert!(matches!(
            auth.authenticate(Some("old-secret")),
            ClientAuthOutcome::Authenticated(_)
        ));
    }

    #[test]
    fn duplicate_key_ids_are_rejected_even_when_secrets_differ() {
        let error = ClientAuth::from_specs(
            true,
            [
                spec("same", "secret-one", &[]),
                spec("same", "secret-two", &[]),
            ],
        )
        .expect_err("duplicate ids must not create indistinguishable credentials");
        assert!(error.contains("duplicate virtual API key id"));
    }

    #[test]
    fn bearer_takes_precedence_over_x_api_key() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer bearer-key".parse().unwrap());
        headers.insert("x-api-key", "header-key".parse().unwrap());
        assert_eq!(presented_key(&headers).as_deref(), Some("bearer-key"));
    }
}
