use chrono::Utc;
use rusqlite::{Connection, params};
use serde::Serialize;
use std::fmt;
use std::path::Path;

pub(crate) struct AuthStore {
    connection: Connection,
}

pub(crate) struct StoredGrant {
    pub effect: String,
    pub endpoint: String,
    pub model_pattern: String,
}

pub(crate) struct StoredKey {
    pub id: String,
    pub principal_id: String,
    pub digest: [u8; 32],
    pub grants: Vec<StoredGrant>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiKeySummary {
    pub id: String,
    pub principal_id: String,
    pub name: String,
    pub prefix: String,
    pub enabled: bool,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub last_used_at: Option<i64>,
}

#[derive(Clone, Serialize)]
pub struct CreatedApiKey {
    #[serde(flatten)]
    pub summary: ApiKeySummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_key: Option<String>,
}

impl std::fmt::Debug for CreatedApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreatedApiKey")
            .field("summary", &self.summary)
            .field("raw_key", &self.raw_key.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

impl CreatedApiKey {
    pub(crate) fn with_raw(mut self, raw: String) -> Self {
        self.raw_key = Some(raw);
        self
    }
}

impl AuthStore {
    pub fn open(path: &Path) -> Result<Self, String> {
        let connection = Connection::open(path)
            .map_err(|err| format!("failed to open auth store {}: {err}", path.display()))?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA foreign_keys=ON;
                 CREATE TABLE IF NOT EXISTS auth_principals (
                   id TEXT PRIMARY KEY, kind TEXT NOT NULL, display_name TEXT NOT NULL,
                   enabled INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS auth_api_keys (
                   id TEXT PRIMARY KEY, principal_id TEXT NOT NULL REFERENCES auth_principals(id),
                   name TEXT NOT NULL, prefix TEXT NOT NULL, hmac_sha256_digest BLOB NOT NULL UNIQUE,
                   enabled INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL,
                   expires_at INTEGER, last_used_at INTEGER
                 );
                 CREATE TABLE IF NOT EXISTS auth_policies (
                   id TEXT PRIMARY KEY, key_id TEXT NOT NULL REFERENCES auth_api_keys(id),
                   effect TEXT NOT NULL CHECK(effect IN ('allow','deny')),
                   endpoint TEXT NOT NULL, model_pattern TEXT NOT NULL DEFAULT '*',
                   created_at INTEGER NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS auth_groups (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, enabled INTEGER NOT NULL DEFAULT 1);
                 CREATE TABLE IF NOT EXISTS auth_group_members (group_id TEXT NOT NULL, principal_id TEXT NOT NULL, PRIMARY KEY(group_id, principal_id));
                 CREATE TABLE IF NOT EXISTS auth_roles (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, enabled INTEGER NOT NULL DEFAULT 1);
                 CREATE TABLE IF NOT EXISTS auth_role_bindings (role_id TEXT NOT NULL, subject_type TEXT NOT NULL, subject_id TEXT NOT NULL, PRIMARY KEY(role_id, subject_type, subject_id));
                 CREATE TABLE IF NOT EXISTS auth_management_permissions (role_id TEXT NOT NULL, permission TEXT NOT NULL, PRIMARY KEY(role_id, permission));
                 CREATE TABLE IF NOT EXISTS auth_sessions (session_id TEXT PRIMARY KEY, key_id TEXT NOT NULL, started_at INTEGER NOT NULL, endpoint TEXT NOT NULL, requested_model TEXT);
                 CREATE TABLE IF NOT EXISTS auth_usage_events (auth_request_id TEXT PRIMARY KEY, api_call_id TEXT, key_id TEXT, principal_id TEXT, endpoint TEXT NOT NULL, requested_model TEXT, served_model TEXT, provider TEXT, status TEXT NOT NULL, prompt_tokens INTEGER, completion_tokens INTEGER, cached_tokens INTEGER, reasoning_tokens INTEGER, cost_nanos INTEGER, created_at INTEGER NOT NULL);
                 CREATE TABLE IF NOT EXISTS auth_audit_events (id INTEGER PRIMARY KEY AUTOINCREMENT, actor_id TEXT, action TEXT NOT NULL, target_id TEXT, metadata_json TEXT NOT NULL DEFAULT '{}', created_at INTEGER NOT NULL);
                 CREATE TABLE IF NOT EXISTS auth_policy_epoch (singleton INTEGER PRIMARY KEY CHECK(singleton=1), epoch INTEGER NOT NULL);
                 INSERT OR IGNORE INTO auth_policy_epoch(singleton, epoch) VALUES (1, 0);",
            )
            .map_err(|err| format!("failed to migrate auth store: {err}"))?;
        Ok(Self { connection })
    }

    pub fn key_count(&self) -> Result<u64, String> {
        self.connection
            .query_row("SELECT COUNT(*) FROM auth_api_keys", [], |row| row.get(0))
            .map_err(|err| format!("failed to inspect auth store: {err}"))
    }

    pub fn insert_bootstrap_key(&mut self, raw: &str, digest: &[u8; 32]) -> Result<(), String> {
        let now = Utc::now().timestamp();
        let tx = self
            .connection
            .transaction()
            .map_err(|err| err.to_string())?;
        tx.execute(
            "INSERT INTO auth_principals(id,kind,display_name,created_at) VALUES('usr_bootstrap','user','Bootstrap administrator',?1)",
            [now],
        ).map_err(|err| format!("failed to create bootstrap principal: {err}"))?;
        tx.execute(
            "INSERT INTO auth_api_keys(id,principal_id,name,prefix,hmac_sha256_digest,created_at) VALUES('key_bootstrap','usr_bootstrap','Bootstrap key',?1,?2,?3)",
            params![key_prefix(raw), digest.as_slice(), now],
        ).map_err(|err| format!("failed to create bootstrap key: {err}"))?;
        tx.execute(
            "INSERT INTO auth_policies(id,key_id,effect,endpoint,model_pattern,created_at) VALUES('pol_bootstrap','key_bootstrap','allow','*','*',?1)",
            [now],
        ).map_err(|err| format!("failed to create bootstrap policy: {err}"))?;
        bump_epoch(&tx)?;
        tx.commit()
            .map_err(|err| format!("failed to commit bootstrap auth state: {err}"))
    }

    pub fn create_key(
        &mut self,
        principal_name: &str,
        key_name: &str,
        raw: &str,
        digest: &[u8; 32],
        endpoints: &[String],
        models: &[String],
    ) -> Result<CreatedApiKey, String> {
        let principal_name = required(principal_name, "principal_name")?;
        let key_name = required(key_name, "name")?;
        let now = Utc::now().timestamp();
        let principal_id = format!("usr_{}", uuid::Uuid::new_v4().simple());
        let key_id = format!("key_{}", uuid::Uuid::new_v4().simple());
        let prefix = key_prefix(raw);
        let tx = self
            .connection
            .transaction()
            .map_err(|err| err.to_string())?;
        tx.execute(
            "INSERT INTO auth_principals(id,kind,display_name,created_at) VALUES(?1,'service_account',?2,?3)",
            params![principal_id, principal_name, now],
        ).map_err(|err| format!("failed to create principal: {err}"))?;
        tx.execute(
            "INSERT INTO auth_api_keys(id,principal_id,name,prefix,hmac_sha256_digest,created_at) VALUES(?1,?2,?3,?4,?5,?6)",
            params![key_id, principal_id, key_name, prefix, digest.as_slice(), now],
        ).map_err(|err| format!("failed to create API key: {err}"))?;
        let endpoints = if endpoints.is_empty() {
            vec!["*".to_string()]
        } else {
            endpoints.to_vec()
        };
        let models = if models.is_empty() {
            vec!["*".to_string()]
        } else {
            models.to_vec()
        };
        for endpoint in &endpoints {
            for model in &models {
                let policy_id = format!("pol_{}", uuid::Uuid::new_v4().simple());
                tx.execute(
                    "INSERT INTO auth_policies(id,key_id,effect,endpoint,model_pattern,created_at) VALUES(?1,?2,'allow',?3,?4,?5)",
                    params![policy_id, key_id, required(endpoint, "endpoint")?, required(model, "model")?, now],
                ).map_err(|err| format!("failed to create API-key policy: {err}"))?;
            }
        }
        bump_epoch(&tx)?;
        tx.execute(
            "INSERT INTO auth_audit_events(actor_id,action,target_id,created_at) VALUES('bootstrap','key.created',?1,?2)",
            params![key_id, now],
        ).map_err(|err| format!("failed to audit API-key creation: {err}"))?;
        tx.commit()
            .map_err(|err| format!("failed to commit API-key creation: {err}"))?;
        Ok(CreatedApiKey {
            summary: ApiKeySummary {
                id: key_id,
                principal_id,
                name: key_name.to_string(),
                prefix,
                enabled: true,
                created_at: now,
                expires_at: None,
                last_used_at: None,
            },
            raw_key: None,
        })
    }

    pub fn revoke_key(&mut self, key_id: &str) -> Result<bool, String> {
        let now = Utc::now().timestamp();
        let tx = self
            .connection
            .transaction()
            .map_err(|err| err.to_string())?;
        let changed = tx
            .execute(
                "UPDATE auth_api_keys SET enabled=0 WHERE id=?1 AND enabled=1",
                [key_id],
            )
            .map_err(|err| format!("failed to revoke API key: {err}"))?
            > 0;
        if changed {
            bump_epoch(&tx)?;
            tx.execute("INSERT INTO auth_audit_events(actor_id,action,target_id,created_at) VALUES('bootstrap','key.revoked',?1,?2)", params![key_id, now])
                .map_err(|err| format!("failed to audit API-key revocation: {err}"))?;
        }
        tx.commit()
            .map_err(|err| format!("failed to commit API-key revocation: {err}"))?;
        Ok(changed)
    }

    pub fn list_keys(&self) -> Result<Vec<ApiKeySummary>, String> {
        let mut statement = self.connection.prepare(
            "SELECT id,principal_id,name,prefix,enabled,created_at,expires_at,last_used_at FROM auth_api_keys ORDER BY created_at DESC"
        ).map_err(|err| err.to_string())?;
        statement
            .query_map([], |row| {
                Ok(ApiKeySummary {
                    id: row.get(0)?,
                    principal_id: row.get(1)?,
                    name: row.get(2)?,
                    prefix: row.get(3)?,
                    enabled: row.get::<_, i64>(4)? != 0,
                    created_at: row.get(5)?,
                    expires_at: row.get(6)?,
                    last_used_at: row.get(7)?,
                })
            })
            .map_err(|err| err.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| err.to_string())
    }

    pub fn load_active_keys(&self) -> Result<Vec<StoredKey>, String> {
        let now = Utc::now().timestamp();
        let mut statement = self.connection.prepare(
            "SELECT id,principal_id,hmac_sha256_digest FROM auth_api_keys WHERE enabled=1 AND (expires_at IS NULL OR expires_at>?1)"
        ).map_err(|err| err.to_string())?;
        let rows = statement
            .query_map([now], |row| {
                let digest: Vec<u8> = row.get(2)?;
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, digest))
            })
            .map_err(|err| err.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| err.to_string())?;
        let mut keys = Vec::with_capacity(rows.len());
        for (id, principal_id, digest) in rows {
            let digest: [u8; 32] = digest
                .try_into()
                .map_err(|_| format!("invalid digest length for {id}"))?;
            let mut grants_statement = self.connection.prepare(
                "SELECT effect,endpoint,model_pattern FROM auth_policies WHERE key_id=?1 ORDER BY CASE effect WHEN 'deny' THEN 0 ELSE 1 END"
            ).map_err(|err| err.to_string())?;
            let grants = grants_statement
                .query_map([&id], |row| {
                    Ok(StoredGrant {
                        effect: row.get(0)?,
                        endpoint: row.get(1)?,
                        model_pattern: row.get(2)?,
                    })
                })
                .map_err(|err| err.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| err.to_string())?;
            keys.push(StoredKey {
                id,
                principal_id,
                digest,
                grants,
            });
        }
        Ok(keys)
    }
}

fn bump_epoch(tx: &rusqlite::Transaction<'_>) -> Result<(), String> {
    tx.execute(
        "UPDATE auth_policy_epoch SET epoch=epoch+1 WHERE singleton=1",
        [],
    )
    .map(|_| ())
    .map_err(|err| format!("failed to increment auth policy epoch: {err}"))
}

fn key_prefix(raw: &str) -> String {
    raw.chars().take(13).collect()
}

fn required<'a>(value: &'a str, field: &str) -> Result<&'a str, String> {
    let value = value.trim();
    if value.is_empty() {
        Err(format!("{field} must not be blank"))
    } else {
        Ok(value)
    }
}
