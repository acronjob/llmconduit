use super::keys::{AuthPepper, digest_matches, generate_api_key, key_prefix};
use super::policy::{AuthContext, AuthError, AuthRequestId, PolicyRule, PolicySnapshot};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrincipalKind {
    User,
    ServiceAccount,
}

impl PrincipalKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::ServiceAccount => "service_account",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalRecord {
    pub id: String,
    pub kind: PrincipalKind,
    pub display_name: String,
    pub enabled: bool,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyRecord {
    pub id: String,
    pub principal_id: String,
    pub name: String,
    pub prefix: String,
    pub enabled: bool,
    pub created_at_ms: i64,
    pub expires_at_ms: Option<i64>,
    pub last_used_at_ms: Option<i64>,
}

pub struct CreatedApiKey {
    pub record: ApiKeyRecord,
    raw_key: String,
}

impl CreatedApiKey {
    /// Consumes the creation response so plaintext cannot be fetched again.
    pub fn expose_once(self) -> String {
        self.raw_key
    }

    pub fn raw_key(&self) -> &str {
        &self.raw_key
    }
}

impl fmt::Debug for CreatedApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreatedApiKey")
            .field("record", &self.record)
            .field("raw_key", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotRecords {
    pub epoch: u64,
    pub principal_groups: HashMap<String, HashSet<String>>,
    pub principal_roles: HashMap<String, HashSet<String>>,
}

impl SnapshotRecords {
    pub fn compile(self, rules: Vec<PolicyRule>) -> Arc<PolicySnapshot> {
        Arc::new(PolicySnapshot::new(
            self.epoch,
            rules,
            self.principal_groups,
            self.principal_roles,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct AuthStore {
    path: PathBuf,
    pepper: AuthPepper,
}

impl AuthStore {
    pub async fn open(path: impl AsRef<Path>, pepper: AuthPepper) -> Result<Self, AuthStoreError> {
        if let Some(parent) = path.as_ref().parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        let store = Self {
            path: path.as_ref().to_path_buf(),
            pepper,
        };
        store
            .with_conn(|conn| {
                conn.execute_batch(SCHEMA)?;
                let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
                if version > SCHEMA_VERSION {
                    return Err(AuthStoreError::UnsupportedSchema(version));
                }
                conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                Ok(())
            })
            .await?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn create_principal(
        &self,
        kind: PrincipalKind,
        display_name: impl Into<String>,
    ) -> Result<PrincipalRecord, AuthStoreError> {
        let display_name = display_name.into().trim().to_string();
        if display_name.is_empty() {
            return Err(AuthStoreError::InvalidInput("blank principal display name"));
        }
        let record = PrincipalRecord {
            id: format!("usr_{}", Uuid::new_v4().simple()),
            kind,
            display_name,
            enabled: true,
            created_at_ms: now_ms(),
        };
        let insert = record.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO auth_principals (id, kind, display_name, enabled, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, 1, ?4, ?4)",
                params![insert.id, insert.kind.as_str(), insert.display_name, insert.created_at_ms],
            )?;
            Ok(())
        }).await?;
        Ok(record)
    }

    pub async fn create_api_key(
        &self,
        principal_id: &str,
        name: impl Into<String>,
        expires_at_ms: Option<i64>,
    ) -> Result<CreatedApiKey, AuthStoreError> {
        let name = name.into().trim().to_string();
        if name.is_empty() {
            return Err(AuthStoreError::InvalidInput("blank API key name"));
        }
        if expires_at_ms.is_some_and(|expires| expires <= now_ms()) {
            return Err(AuthStoreError::InvalidInput(
                "API key expiry must be in the future",
            ));
        }
        let generated = generate_api_key(&self.pepper);
        let raw_key = generated.raw().to_string();
        let digest = generated.digest;
        let record = ApiKeyRecord {
            id: format!("key_{}", Uuid::new_v4().simple()),
            principal_id: principal_id.to_string(),
            name,
            prefix: generated.prefix,
            enabled: true,
            created_at_ms: now_ms(),
            expires_at_ms,
            last_used_at_ms: None,
        };
        let insert = record.clone();
        self.with_conn(move |conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let principal_enabled = tx.query_row(
                "SELECT enabled FROM auth_principals WHERE id = ?1",
                params![insert.principal_id],
                |row| row.get::<_, i64>(0),
            ).optional()?;
            if principal_enabled != Some(1) {
                return Err(AuthStoreError::InvalidInput("principal does not exist or is disabled"));
            }
            tx.execute(
                "INSERT INTO auth_api_keys (id, principal_id, name, prefix, hmac_sha256_digest, enabled, created_at_ms, expires_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7)",
                params![insert.id, insert.principal_id, insert.name, insert.prefix, digest.as_slice(), insert.created_at_ms, insert.expires_at_ms],
            )?;
            bump_epoch(&tx)?;
            tx.commit()?;
            Ok(())
        }).await?;
        Ok(CreatedApiKey { record, raw_key })
    }

    pub async fn authenticate(&self, raw_key: &str, at_ms: i64) -> Result<AuthContext, AuthError> {
        let raw_key = raw_key.trim();
        let prefix = key_prefix(raw_key)
            .map_err(|_| AuthError::InvalidCredential)?
            .to_string();
        let presented = self.pepper.digest(raw_key);
        let candidates = self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT k.id, k.principal_id, k.prefix, k.hmac_sha256_digest, k.enabled, k.expires_at_ms, p.enabled FROM auth_api_keys k JOIN auth_principals p ON p.id = k.principal_id WHERE k.prefix = ?1",
            )?;
            let rows = stmt.query_map(params![prefix], |row| Ok(Candidate {
                key_id: row.get(0)?, principal_id: row.get(1)?, prefix: row.get(2)?,
                digest: row.get(3)?, key_enabled: row.get::<_, i64>(4)? != 0,
                expires_at_ms: row.get(5)?, principal_enabled: row.get::<_, i64>(6)? != 0,
            }))?;
            rows.collect::<Result<Vec<_>, _>>().map_err(AuthStoreError::from)
        }).await.map_err(|_| AuthError::PolicyUnavailable)?;

        let mut selected = None;
        for candidate in candidates {
            let matches = digest_matches(&candidate.digest, &presented);
            if matches {
                selected = Some(candidate);
            }
        }
        let candidate = selected.ok_or(AuthError::InvalidCredential)?;
        if !candidate.key_enabled
            || !candidate.principal_enabled
            || candidate
                .expires_at_ms
                .is_some_and(|expires| expires <= at_ms)
        {
            return Err(AuthError::InvalidCredential);
        }
        let key_id = candidate.key_id.clone();
        let epoch = self
            .with_conn(move |conn| {
                conn.execute(
                    "UPDATE auth_api_keys SET last_used_at_ms = ?2 WHERE id = ?1",
                    params![key_id, at_ms],
                )?;
                current_epoch(conn)
            })
            .await
            .map_err(|_| AuthError::PolicyUnavailable)?;
        Ok(AuthContext {
            request_id: AuthRequestId::new(),
            key_id: candidate.key_id,
            key_prefix: candidate.prefix,
            principal_id: candidate.principal_id,
            policy_epoch: epoch,
        })
    }

    pub async fn revoke_api_key(&self, id: &str) -> Result<bool, AuthStoreError> {
        let id = id.to_string();
        self.with_conn(move |conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let changed = tx.execute("UPDATE auth_api_keys SET enabled = 0, revoked_at_ms = ?2 WHERE id = ?1 AND enabled != 0", params![id, now_ms()])?;
            if changed > 0 {
                tx.execute("UPDATE auth_dashboard_sessions SET revoked_at_ms = ?2, revoked_reason = 'key_revoked' WHERE key_id = ?1 AND revoked_at_ms IS NULL", params![id, now_ms()])?;
                bump_epoch(&tx)?;
            }
            tx.commit()?;
            Ok(changed > 0)
        }).await
    }

    pub async fn list_api_keys(&self) -> Result<Vec<ApiKeyRecord>, AuthStoreError> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, principal_id, name, prefix, enabled, created_at_ms, expires_at_ms, last_used_at_ms FROM auth_api_keys ORDER BY created_at_ms DESC, id")?;
            let rows = stmt.query_map([], |row| Ok(ApiKeyRecord {
                id: row.get(0)?, principal_id: row.get(1)?, name: row.get(2)?, prefix: row.get(3)?,
                enabled: row.get::<_, i64>(4)? != 0, created_at_ms: row.get(5)?,
                expires_at_ms: row.get(6)?, last_used_at_ms: row.get(7)?,
            }))?;
            rows.collect::<Result<Vec<_>, _>>().map_err(AuthStoreError::from)
        }).await
    }

    pub async fn snapshot_records(&self) -> Result<SnapshotRecords, AuthStoreError> {
        self.with_conn(|conn| {
            let epoch = current_epoch(conn)?;
            let mut principal_groups: HashMap<String, HashSet<String>> = HashMap::new();
            let mut stmt = conn.prepare("SELECT principal_id, group_id FROM auth_group_members")?;
            for row in stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))? {
                let (principal, group) = row?;
                principal_groups.entry(principal).or_default().insert(group);
            }
            let mut principal_roles: HashMap<String, HashSet<String>> = HashMap::new();
            let mut stmt = conn.prepare(
                "SELECT subject_id, role_id FROM auth_role_bindings WHERE subject_kind = 'principal' UNION SELECT gm.principal_id, rb.role_id FROM auth_role_bindings rb JOIN auth_group_members gm ON rb.subject_kind = 'group' AND rb.subject_id = gm.group_id",
            )?;
            for row in stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))? {
                let (principal, role) = row?;
                principal_roles.entry(principal).or_default().insert(role);
            }
            Ok(SnapshotRecords { epoch, principal_groups, principal_roles })
        }).await
    }

    async fn with_conn<F, T>(&self, operation: F) -> Result<T, AuthStoreError>
    where
        F: FnOnce(&mut Connection) -> Result<T, AuthStoreError> + Send + 'static,
        T: Send + 'static,
    {
        let path = self.path.clone();
        task::spawn_blocking(move || {
            let mut conn = Connection::open(path)?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            conn.busy_timeout(std::time::Duration::from_secs(5))?;
            operation(&mut conn)
        })
        .await
        .map_err(AuthStoreError::Join)?
    }
}

#[derive(Debug)]
struct Candidate {
    key_id: String,
    principal_id: String,
    prefix: String,
    digest: Vec<u8>,
    key_enabled: bool,
    expires_at_ms: Option<i64>,
    principal_enabled: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthStoreError {
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("auth store schema version {0} is newer than supported")]
    UnsupportedSchema(i64),
    #[error("invalid auth store input: {0}")]
    InvalidInput(&'static str),
}

const SCHEMA_VERSION: i64 = 1;
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS auth_principals (id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN ('user','service_account')), display_name TEXT NOT NULL, enabled INTEGER NOT NULL, created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS auth_api_keys (id TEXT PRIMARY KEY, principal_id TEXT NOT NULL REFERENCES auth_principals(id), name TEXT NOT NULL, prefix TEXT NOT NULL, hmac_sha256_digest BLOB NOT NULL UNIQUE, enabled INTEGER NOT NULL, created_at_ms INTEGER NOT NULL, expires_at_ms INTEGER, last_used_at_ms INTEGER, revoked_at_ms INTEGER, metadata_json TEXT);
CREATE INDEX IF NOT EXISTS auth_api_keys_prefix_idx ON auth_api_keys(prefix);
CREATE TABLE IF NOT EXISTS auth_groups (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, enabled INTEGER NOT NULL DEFAULT 1, created_at_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS auth_group_members (group_id TEXT NOT NULL REFERENCES auth_groups(id) ON DELETE CASCADE, principal_id TEXT NOT NULL REFERENCES auth_principals(id) ON DELETE CASCADE, PRIMARY KEY(group_id, principal_id));
CREATE TABLE IF NOT EXISTS auth_roles (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, enabled INTEGER NOT NULL DEFAULT 1, created_at_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS auth_role_bindings (role_id TEXT NOT NULL REFERENCES auth_roles(id) ON DELETE CASCADE, subject_kind TEXT NOT NULL CHECK(subject_kind IN ('principal','group')), subject_id TEXT NOT NULL, PRIMARY KEY(role_id, subject_kind, subject_id));
CREATE TABLE IF NOT EXISTS auth_policies (id TEXT PRIMARY KEY, effect TEXT NOT NULL CHECK(effect IN ('allow','deny')), subject_kind TEXT NOT NULL CHECK(subject_kind IN ('key','principal','group','role')), subject_id TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS auth_policy_scopes (policy_id TEXT NOT NULL REFERENCES auth_policies(id) ON DELETE CASCADE, dimension TEXT NOT NULL CHECK(dimension IN ('endpoint','requested_model','served_model','provider','route')), matcher TEXT NOT NULL, PRIMARY KEY(policy_id, dimension, matcher));
CREATE TABLE IF NOT EXISTS auth_management_permissions (policy_id TEXT NOT NULL REFERENCES auth_policies(id) ON DELETE CASCADE, permission TEXT NOT NULL, PRIMARY KEY(policy_id, permission));
CREATE TABLE IF NOT EXISTS auth_time_windows (id TEXT PRIMARY KEY, policy_id TEXT NOT NULL REFERENCES auth_policies(id) ON DELETE CASCADE, weekday_mask INTEGER NOT NULL DEFAULT 0, start_minute INTEGER NOT NULL DEFAULT 0, end_minute INTEGER NOT NULL DEFAULT 0, absolute_start_ms INTEGER, absolute_end_ms INTEGER);
CREATE TABLE IF NOT EXISTS auth_limits (policy_id TEXT PRIMARY KEY REFERENCES auth_policies(id) ON DELETE CASCADE, max_concurrent_sessions INTEGER, max_daily_session_starts INTEGER, max_tokens_per_day INTEGER, max_cost_nanos_per_day INTEGER);
CREATE TABLE IF NOT EXISTS auth_sessions (id TEXT PRIMARY KEY, key_id TEXT NOT NULL REFERENCES auth_api_keys(id), started_at_ms INTEGER NOT NULL, expires_at_ms INTEGER, endpoint TEXT NOT NULL, requested_model TEXT);
CREATE TABLE IF NOT EXISTS auth_dashboard_sessions (session_id TEXT PRIMARY KEY, principal_id TEXT NOT NULL REFERENCES auth_principals(id), key_id TEXT NOT NULL REFERENCES auth_api_keys(id), csrf_secret_digest BLOB NOT NULL, created_at_ms INTEGER NOT NULL, expires_at_ms INTEGER NOT NULL, policy_epoch_at_login INTEGER NOT NULL, revoked_at_ms INTEGER, revoked_reason TEXT, last_seen_at_ms INTEGER);
CREATE TABLE IF NOT EXISTS auth_audit_events (id TEXT PRIMARY KEY, actor_principal_id TEXT, actor_key_id TEXT, action TEXT NOT NULL, target_kind TEXT, target_id TEXT, created_at_ms INTEGER NOT NULL, metadata_json TEXT);
CREATE TABLE IF NOT EXISTS auth_policy_epoch (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), epoch INTEGER NOT NULL);
INSERT OR IGNORE INTO auth_policy_epoch(singleton, epoch) VALUES (1, 1);
"#;

fn current_epoch(conn: &Connection) -> Result<u64, AuthStoreError> {
    let epoch = conn.query_row(
        "SELECT epoch FROM auth_policy_epoch WHERE singleton = 1",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    u64::try_from(epoch).map_err(|_| AuthStoreError::InvalidInput("negative policy epoch"))
}

fn bump_epoch(tx: &rusqlite::Transaction<'_>) -> Result<(), AuthStoreError> {
    tx.execute(
        "UPDATE auth_policy_epoch SET epoch = epoch + 1 WHERE singleton = 1",
        [],
    )?;
    Ok(())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("llmconduit-auth-{name}-{}.sqlite", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn plaintext_is_returned_once_and_never_persisted_or_debugged() {
        let path = path("key");
        let store = AuthStore::open(&path, AuthPepper::new("pepper").unwrap())
            .await
            .unwrap();
        let principal = store
            .create_principal(PrincipalKind::ServiceAccount, "agent")
            .await
            .unwrap();
        let created = store
            .create_api_key(&principal.id, "primary", None)
            .await
            .unwrap();
        let raw = created.raw_key().to_string();
        assert!(!format!("{created:?}").contains(&raw));
        let bytes = std::fs::read(&path).unwrap();
        assert!(
            !bytes
                .windows(raw.len())
                .any(|window| window == raw.as_bytes())
        );
        let context = store.authenticate(&raw, now_ms()).await.unwrap();
        assert_eq!(context.principal_id, principal.id);
        assert_eq!(store.list_api_keys().await.unwrap()[0].prefix, &raw[..12]);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn revoked_expired_unknown_and_malformed_keys_fail_closed() {
        let path = path("reject");
        let store = AuthStore::open(&path, AuthPepper::new("pepper").unwrap())
            .await
            .unwrap();
        let principal = store
            .create_principal(PrincipalKind::User, "user")
            .await
            .unwrap();
        let created = store
            .create_api_key(&principal.id, "key", Some(now_ms() + 10_000))
            .await
            .unwrap();
        let raw = created.raw_key().to_string();
        assert!(store.revoke_api_key(&created.record.id).await.unwrap());
        assert_eq!(
            store.authenticate(&raw, now_ms()).await.unwrap_err(),
            AuthError::InvalidCredential
        );
        assert_eq!(
            store.authenticate("blank", now_ms()).await.unwrap_err(),
            AuthError::InvalidCredential
        );
        let _ = std::fs::remove_file(path);
    }
}
