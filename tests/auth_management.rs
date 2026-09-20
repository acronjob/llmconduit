use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode};
use llmconduit::authz::{AuthFailure, AuthzService};
use llmconduit::config::{AuthConfig, AuthMode};
use llmconduit::dashboard_access::{ManagementActor, ManagementPermission, routes};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

static AUTH_ENV_LOCK: Mutex<()> = Mutex::new(());

fn enforced_service() -> (AuthzService, PathBuf) {
    let _guard = AUTH_ENV_LOCK.lock().expect("auth env lock");
    let path = std::env::temp_dir().join(format!(
        "llmconduit-management-lifecycle-{}.sqlite3",
        uuid::Uuid::new_v4()
    ));
    let bootstrap = format!("llmc_{}", uuid::Uuid::new_v4().simple());
    let old_pepper = std::env::var_os("LLMCONDUIT_AUTH_PEPPER");
    let old_bootstrap = std::env::var_os("LLMCONDUIT_AUTH_BOOTSTRAP_KEY");
    // SAFETY: this integration-test binary serializes its environment mutation and restores
    // both variables before releasing the lock. Production code never mutates these values.
    unsafe {
        std::env::set_var("LLMCONDUIT_AUTH_PEPPER", "management-test-pepper-never-log");
        std::env::set_var("LLMCONDUIT_AUTH_BOOTSTRAP_KEY", &bootstrap);
    }
    let service = AuthzService::from_config(&AuthConfig {
        mode: AuthMode::Enforce,
        store_path: path.clone(),
    })
    .expect("enforced auth service");
    // SAFETY: paired with the serialized mutation above.
    unsafe {
        match old_pepper {
            Some(value) => std::env::set_var("LLMCONDUIT_AUTH_PEPPER", value),
            None => std::env::remove_var("LLMCONDUIT_AUTH_PEPPER"),
        }
        match old_bootstrap {
            Some(value) => std::env::set_var("LLMCONDUIT_AUTH_BOOTSTRAP_KEY", value),
            None => std::env::remove_var("LLMCONDUIT_AUTH_BOOTSTRAP_KEY"),
        }
    }
    (service, path)
}

fn delegated(permission: ManagementPermission) -> ManagementActor {
    ManagementActor::Delegated {
        session_id: "session_test".into(),
        principal_id: "principal_admin".into(),
        key_id: "key_admin".into(),
        permissions: Arc::from([permission]),
    }
}

async fn json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("bounded response body");
    serde_json::from_slice(&bytes).expect("JSON response")
}

fn remove_store(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
}

#[tokio::test]
async fn management_key_list_and_revoke_are_permissioned_and_never_recover_raw_secret() {
    let (service, path) = enforced_service();
    let created = service
        .create_key(
            "management lifecycle principal",
            "management lifecycle key",
            &["chat".to_string()],
            &["allowed-*".to_string()],
        )
        .expect("create key");
    let key_id = created.summary.id.clone();
    let raw = created
        .raw_key
        .as_deref()
        .expect("copy-once raw key")
        .to_owned();
    assert!(raw.starts_with("llmc_"));
    assert!(!format!("{created:?}").contains(&raw));

    let service = Arc::new(service);
    let list_response = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(delegated(
            ManagementPermission::KeysRead,
        )))
        .oneshot(
            Request::builder()
                .uri("/dashboard/api/auth/api-keys")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);
    let listed = json(list_response).await;
    let listed_wire = listed.to_string();
    assert!(
        listed["api_keys"]
            .as_array()
            .unwrap()
            .iter()
            .any(|key| key["id"] == key_id)
    );
    assert!(!listed_wire.contains(&raw));
    assert!(!listed_wire.contains("raw_key"));

    let denied = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(delegated(
            ManagementPermission::KeysRead,
        )))
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/dashboard/api/auth/api-keys/{key_id}/revoke"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(&raw).expect("generated key is a valid header"),
    );
    assert!(service.authenticate(&headers).unwrap().is_some());

    let revoked = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(delegated(
            ManagementPermission::KeysRevoke,
        )))
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/dashboard/api/auth/api-keys/{key_id}/revoke"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::OK);
    let revoked_body = json(revoked).await;
    let revoked_key = revoked_body["api_keys"]
        .as_array()
        .unwrap()
        .iter()
        .find(|key| key["id"] == key_id)
        .expect("revoked key remains listable");
    assert_eq!(revoked_key["enabled"], false);
    assert!(!revoked_body.to_string().contains(&raw));
    assert!(matches!(
        service.authenticate(&headers),
        Err(AuthFailure::Invalid)
    ));

    drop(service);
    remove_store(&path);
}
