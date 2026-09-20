use chrono::Utc;
use rusqlite::{Connection, params};
use serde::Serialize;
use std::path::Path;

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

fn required<'a>(value: &'a str, field: &str) -> Result<&'a str, String> {
    let value = value.trim();
    if value.is_empty() {
        Err(format!("{field} must not be blank"))
    } else {
        Ok(value)
    }
}
