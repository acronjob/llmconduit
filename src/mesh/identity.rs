use iroh::Endpoint;
use iroh::EndpointId;
use iroh::RelayMode;
use iroh::SecretKey;
use iroh::endpoint::presets;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use tokio::fs;
use uuid::Uuid;

const SECRET_KEY_BYTES: usize = 32;

#[derive(Clone)]
pub struct MeshIdentity {
    secret_key: SecretKey,
}

impl std::fmt::Debug for MeshIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshIdentity")
            .field("endpoint_id", &self.endpoint_id().to_string())
            .finish()
    }
}

impl MeshIdentity {
    pub fn secret_key(&self) -> SecretKey {
        self.secret_key.clone()
    }

    pub fn endpoint_id(&self) -> EndpointId {
        self.secret_key.public()
    }

    pub async fn bind_direct_endpoint(
        &self,
        alpns: Vec<Vec<u8>>,
    ) -> Result<Endpoint, iroh::endpoint::BindError> {
        Endpoint::builder(presets::Minimal)
            .secret_key(self.secret_key())
            .relay_mode(RelayMode::Disabled)
            .alpns(alpns)
            .bind()
            .await
    }
}

pub async fn load_or_create(path: impl AsRef<Path>) -> io::Result<MeshIdentity> {
    let path = path.as_ref();
    match fs::read(path).await {
        Ok(bytes) => return parse_identity_bytes(&bytes),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).await?;
    }

    let secret_key = SecretKey::generate();
    let encoded = hex::encode(secret_key.to_bytes());
    let tmp_path = temp_path(path);
    write_new_secret_file(&tmp_path, encoded.as_bytes()).await?;
    // Linking a fully-synced temporary file publishes it atomically without
    // replacing an identity another process may have created concurrently.
    match fs::hard_link(&tmp_path, path).await {
        Ok(()) => {
            let _ = fs::remove_file(&tmp_path).await;
            Ok(MeshIdentity { secret_key })
        }
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&tmp_path).await;
            let bytes = fs::read(path).await?;
            parse_identity_bytes(&bytes)
        }
        Err(err) => {
            let _ = fs::remove_file(&tmp_path).await;
            Err(err)
        }
    }
}

fn parse_identity_bytes(bytes: &[u8]) -> io::Result<MeshIdentity> {
    let text = std::str::from_utf8(bytes)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
        .trim();
    let mut key_bytes = [0_u8; SECRET_KEY_BYTES];
    hex::decode_to_slice(text, &mut key_bytes)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    Ok(MeshIdentity {
        secret_key: SecretKey::from_bytes(&key_bytes),
    })
}

fn temp_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_else(|| ".mesh-identity".into());
    name.push(format!(".{}.tmp", Uuid::new_v4()));
    path.with_file_name(name)
}

#[cfg(unix)]
async fn write_new_secret_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .await?;
    file.write_all(bytes).await?;
    file.write_all(b"\n").await?;
    file.sync_all().await?;
    Ok(())
}

#[cfg(not(unix))]
async fn write_new_secret_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await?;
    file.write_all(bytes).await?;
    file.write_all(b"\n").await?;
    file.sync_all().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_file(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("llmconduit-{name}-{}", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn identity_persists_across_loads() {
        let path = temp_file("identity");
        let first = load_or_create(&path).await.expect("create identity");
        let second = load_or_create(&path).await.expect("load identity");

        assert_eq!(first.endpoint_id(), second.endpoint_id());
        let _ = fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn rejects_invalid_identity_file() {
        let path = temp_file("bad-identity");
        fs::write(&path, b"not-a-key").await.expect("write");

        let err = load_or_create(&path).await.expect_err("invalid key");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let _ = fs::remove_file(path).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn created_identity_is_owner_only_on_unix() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_file("identity-mode");
        let _ = load_or_create(&path).await.expect("create identity");
        let mode = fs::metadata(&path)
            .await
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;

        assert_eq!(mode, 0o600);
        let _ = fs::remove_file(path).await;
    }
}
