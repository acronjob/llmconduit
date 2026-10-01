//! Eval-coordinator integration features: request correlation ids, windowed
//! usage, per-key eval safety flags and pinned eval run keys.

mod common;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::Extension;
use axum::http::{Method, Request, StatusCode};
use axum::response::Response;
use common::*;
use llmconduit::authz::AuthzService;
use llmconduit::config::{AuthConfig, AuthMode};
use llmconduit::dashboard_access::{ManagementActor, ManagementPermission, routes};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

static AUTH_ENV_LOCK: Mutex<()> = Mutex::new(());

fn enforced_service() -> (Arc<AuthzService>, PathBuf, String) {
    let _guard = AUTH_ENV_LOCK.lock().expect("auth env lock");
    let path = std::env::temp_dir().join(format!(
        "llmconduit-eval-integration-{}.sqlite3",
        uuid::Uuid::new_v4()
    ));
    let bootstrap = format!("llmc_{}", uuid::Uuid::new_v4().simple());
    let old_pepper = std::env::var_os("LLMCONDUIT_AUTH_PEPPER");
    let old_bootstrap = std::env::var_os("LLMCONDUIT_AUTH_BOOTSTRAP_KEY");
    // SAFETY: this integration-test binary serializes its environment mutation and restores
    // both variables before releasing the lock. Production code never mutates these values.
    unsafe {
        std::env::set_var("LLMCONDUIT_AUTH_PEPPER", "eval-test-pepper-never-log");
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
    (Arc::new(service), path, bootstrap)
}

fn remove_store(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
}

fn access(service: &Arc<AuthzService>, actor: ManagementActor) -> Router {
    routes::<()>(service.access_backend()).layer(Extension(actor))
}

fn delegated(permissions: &[ManagementPermission]) -> ManagementActor {
    ManagementActor::Delegated {
        session_id: "session_eval".into(),
        principal_id: "usr_coordinator".into(),
        key_id: "key_coordinator".into(),
        permissions: permissions.to_vec().into(),
    }
}

async fn json_body(response: Response) -> Value {
    let bytes = to_bytes(response.into_body(), 8 * 1024 * 1024)
        .await
        .expect("bounded body");
    serde_json::from_slice(&bytes).unwrap_or_else(|_| {
        panic!(
            "JSON body expected, got {:?}",
            String::from_utf8_lossy(&bytes)
        )
    })
}

async fn send(app: &Router, request: Request<Body>) -> Response {
    app.clone().oneshot(request).await.expect("response")
}

fn json_request(method: Method, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn chat(key: Option<&str>, model: &str, extra: Value) -> Request<Body> {
    let mut body = json!({
        "model": model,
        "stream": false,
        "messages": [{"role": "user", "content": "hi"}]
    });
    if let (Some(body), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
        for (k, v) in extra {
            body.insert(k.clone(), v.clone());
        }
    }
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json");
    if let Some(key) = key {
        builder = builder.header("authorization", format!("Bearer {key}"));
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

fn header<'a>(response: &'a Response, name: &str) -> Option<&'a str> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}

async fn create_service_account(service: &Arc<AuthzService>, name: &str) -> String {
    let response = send(
        &access(service, ManagementActor::Bootstrap),
        json_request(
            Method::POST,
            "/dashboard/api/auth/users",
            json!({"display_name": name, "kind": "service_account"}),
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    json_body(response).await["users"]
        .as_array()
        .unwrap()
        .iter()
        .find(|user| user["display_name"] == name)
        .expect("created principal")["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn wiremock_upstream(served: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"data": [{"id": served}, {"id": "served-b"}]})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[
                    json!({
                        "id": "chat-1",
                        "choices": [{"index": 0, "delta": {"content": "ok"}, "finish_reason": null}],
                        "usage": null
                    }),
                    json!({
                        "id": "chat-1",
                        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                        "usage": {"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4}
                    }),
                ])),
        )
        .mount(&server)
        .await;
    server
}

/// A routing (`upstreams:`) gateway over a wiremock upstream with enforced
/// auth, so a model the catalog lacks falls back to the catalog default under
/// the default `unknown_model_policy: passthrough`.
fn routing_app(server: &MockServer, service: &Arc<AuthzService>) -> Router {
    let config = config_from_yaml(&format!(
        "upstreams:\n  - name: \"local\"\n    upstream_base_url: \"{}/v1\"\n",
        server.uri()
    ));
    let (_app, gateway) = llmconduit::build_app_with_gateway(config);
    let gateway = Arc::new(
        gateway
            .as_ref()
            .clone()
            .with_authz(service.as_ref().clone()),
    );
    llmconduit::http::build_router(
        gateway,
        llmconduit::http::RouterOptions {
            with_debug_ui: false,
            register_protected_routes: false,
        },
    )
}

// ---- Request correlation ---------------------------------------------------

#[tokio::test]
async fn v1_responses_carry_gateway_and_client_request_ids() {
    let upstream = MockUpstream::default();
    for _ in 0..2 {
        upstream
            .push_response(vec![
                Ok(content_chunk("chat-1", "Hello")),
                Ok(usage_chunk("chat-1", 3, 1, 4)),
            ])
            .await;
    }
    let gateway = test_gateway(upstream, MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    // Streaming: both headers ride the initial response head.
    let mut request = chat(None, "glm-5.1", json!({"stream": true}));
    request
        .headers_mut()
        .insert("x-request-id", "harbor:trial-1.a_b-c".parse().unwrap());
    let streamed = send(&app, request).await;
    assert_eq!(streamed.status(), StatusCode::OK);
    let gateway_id = header(&streamed, "x-llmconduit-request-id")
        .expect("gateway id")
        .to_string();
    assert!(gateway_id.starts_with("api_"), "{gateway_id}");
    assert_eq!(
        header(&streamed, "x-request-id"),
        Some("harbor:trial-1.a_b-c")
    );
    let _ = to_bytes(streamed.into_body(), 1 << 20).await.unwrap();

    // Without a client id only the gateway id is returned, and ids differ
    // per request.
    let plain = send(&app, chat(None, "glm-5.1", json!({}))).await;
    assert_eq!(plain.status(), StatusCode::OK);
    let second_id = header(&plain, "x-llmconduit-request-id").expect("gateway id");
    assert_ne!(second_id, gateway_id);
    assert_eq!(header(&plain, "x-request-id"), None);

    let models = send(
        &app,
        Request::builder()
            .uri("/v1/models")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert!(header(&models, "x-llmconduit-request-id").is_some());

    // Non-/v1 routes are unchanged.
    let health = send(
        &app,
        Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(header(&health, "x-llmconduit-request-id"), None);
}

#[tokio::test]
async fn invalid_client_request_ids_are_rejected_with_protocol_shaped_400s() {
    let app = llmconduit::build_app_from_gateway(test_gateway(
        MockUpstream::default(),
        MockSearch::default(),
    ));
    for bad in ["has space", "semi;colon", &"x".repeat(129), ""] {
        let mut request = chat(None, "glm-5.1", json!({}));
        request
            .headers_mut()
            .insert("x-request-id", bad.parse().unwrap());
        let response = send(&app, request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{bad:?}");
        assert!(header(&response, "x-llmconduit-request-id").is_some());
        let body = json_body(response).await;
        assert_eq!(body["error"]["code"], "invalid_request_id");
    }

    let mut repeated = chat(None, "glm-5.1", json!({}));
    repeated
        .headers_mut()
        .append("x-request-id", "a".parse().unwrap());
    repeated
        .headers_mut()
        .append("x-request-id", "b".parse().unwrap());
    assert_eq!(send(&app, repeated).await.status(), StatusCode::BAD_REQUEST);

    let anthropic = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("content-type", "application/json")
        .header("x-request-id", "bad id")
        .body(Body::from(
            json!({"model": "m", "max_tokens": 8, "messages": [{"role": "user", "content": "hi"}]})
                .to_string(),
        ))
        .unwrap();
    let response = send(&app, anthropic).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = json_body(response).await;
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "invalid_request_error");
}

#[tokio::test]
async fn client_request_id_and_gateway_id_reach_the_usage_row() {
    let (service, store_path, bootstrap) = enforced_service();
    let server = wiremock_upstream("served-a").await;
    let app = routing_app(&server, &service);
    let mut request = chat(Some(&bootstrap), "served-a", json!({}));
    request
        .headers_mut()
        .insert("x-request-id", "trial-42".parse().unwrap());
    let response = send(&app, request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let gateway_id = header(&response, "x-llmconduit-request-id")
        .unwrap()
        .to_string();
    let _ = to_bytes(response.into_body(), 1 << 20).await.unwrap();

    let row: (Option<String>, Option<String>) =
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let connection = rusqlite::Connection::open(&store_path).unwrap();
                if let Ok(row) = connection.query_row(
                    "SELECT api_call_id, client_request_id FROM auth_usage_events",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                ) {
                    break row;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("usage row persisted");
    assert_eq!(row, (Some(gateway_id), Some("trial-42".to_string())));
    remove_store(&store_path);
}

// ---- Windowed usage ---------------------------------------------------------

fn insert_usage(
    store_path: &Path,
    id: &str,
    key: &str,
    served: Option<&str>,
    at_ms: i64,
    prompt: i64,
) {
    let connection = rusqlite::Connection::open(store_path).unwrap();
    connection
        .execute(
            "INSERT INTO auth_usage_events(auth_request_id,api_call_id,key_id,principal_id,endpoint,
             requested_model,served_model,status,prompt_tokens,completion_tokens,total_tokens,
             cost_confidence,created_at_ms)
             VALUES(?1,NULL,?2,'usr_x','chat',?3,?3,'completed',?4,1,?4+1,'unavailable',?5)",
            rusqlite::params![id, key, served, prompt, at_ms],
        )
        .unwrap();
}

#[tokio::test]
async fn usage_supports_windows_grouping_and_pagination_without_changing_the_legacy_shape() {
    let (service, store_path, _bootstrap) = enforced_service();
    insert_usage(&store_path, "r1", "key_a", Some("m1"), 1_000, 10);
    insert_usage(&store_path, "r2", "key_a", Some("m2"), 2_000, 20);
    insert_usage(&store_path, "r3", "key_b", Some("m1"), 3_000, 30);
    insert_usage(&store_path, "r4", "key_a", Some("m1"), 4_000, 40);
    insert_usage(&store_path, "r5", "key_b", None, 5_000, 50);
    let app = access(&service, delegated(&[ManagementPermission::UsageRead]));
    let get = |uri: &str| {
        let app = app.clone();
        let uri = uri.to_string();
        async move {
            send(
                &app,
                Request::builder().uri(uri).body(Body::empty()).unwrap(),
            )
            .await
        }
    };

    // Legacy: lifetime per key, exactly the historical `{usage}` shape.
    let legacy = json_body(get("/dashboard/api/auth/usage").await).await;
    assert_eq!(legacy.as_object().unwrap().len(), 1);
    let rows = legacy["usage"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["dimension"], "key");
    assert!(rows[0].get("key_id").is_none());
    assert!(rows[0].get("served_model").is_none());

    // Window [2000, 5000) for one key, grouped by served model.
    let page = json_body(
        get("/dashboard/api/auth/usage?key_id=key_a&since_ms=2000&until_ms=5000&group_by=served_model")
            .await,
    )
    .await;
    assert_eq!(page["group_by"], "served_model");
    assert_eq!(page["window"], json!({"since_ms": 2000, "until_ms": 5000}));
    assert_eq!(page["key_id"], "key_a");
    assert_eq!(page["next_offset"], Value::Null);
    let rows = page["usage"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    let m1 = rows.iter().find(|row| row["served_model"] == "m1").unwrap();
    assert_eq!(m1["requests"], 1);
    assert_eq!(m1["prompt_tokens"], 40);
    assert_eq!(m1["dimension"], "served_model");

    // Key x model grouping, paginated one row at a time.
    let first =
        json_body(get("/dashboard/api/auth/usage?group_by=key_and_served_model&limit=1").await)
            .await;
    assert_eq!(first["usage"].as_array().unwrap().len(), 1);
    assert_eq!(first["usage"][0]["key_id"], "key_a");
    assert_eq!(first["usage"][0]["served_model"], "m1");
    assert_eq!(first["usage"][0]["requests"], 2);
    assert_eq!(first["next_offset"], 1);
    let mut seen = 1;
    let mut offset = first["next_offset"].as_u64();
    while let Some(next) = offset {
        let page = json_body(
            get(&format!(
                "/dashboard/api/auth/usage?group_by=key_and_served_model&limit=1&offset={next}"
            ))
            .await,
        )
        .await;
        seen += page["usage"].as_array().unwrap().len();
        offset = page["next_offset"].as_u64();
    }
    assert_eq!(seen, 4, "(a,m1) (a,m2) (b,m1) (b,null)");

    // Default grouping with only a window keeps per-key rows.
    let windowed = json_body(get("/dashboard/api/auth/usage?since_ms=3000").await).await;
    assert_eq!(windowed["group_by"], "key");
    let rows = windowed["usage"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row["key_id"] == row["value"]));

    for bad in [
        "since_ms=abc",
        "since_ms=-1",
        "since_ms=5&until_ms=5",
        "group_by=provider",
        "limit=0",
        "limit=1001",
        "key_id=bad%20id",
    ] {
        let response = get(&format!("/dashboard/api/auth/usage?{bad}")).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{bad}");
    }
    // The permission is unchanged.
    let denied = send(
        &access(&service, delegated(&[ManagementPermission::KeysRead])),
        Request::builder()
            .uri("/dashboard/api/auth/usage?since_ms=1")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    remove_store(&store_path);
}

// ---- Per-key eval safety flags ----------------------------------------------

async fn set_flags(service: &Arc<AuthzService>, key_id: &str, flags: Value) -> Value {
    let response = send(
        &access(service, ManagementActor::Bootstrap),
        json_request(
            Method::PATCH,
            &format!("/dashboard/api/auth/api-keys/{key_id}"),
            flags,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    json_body(response).await["api_keys"]
        .as_array()
        .unwrap()
        .iter()
        .find(|key| key["id"] == key_id)
        .cloned()
        .expect("updated key listed")
}

#[tokio::test]
async fn reject_unknown_models_is_per_key_under_global_passthrough() {
    let (service, store_path, _bootstrap) = enforced_service();
    let server = wiremock_upstream("served-a").await;
    let app = routing_app(&server, &service);
    let lenient = service
        .create_key("lenient", "lenient", &["*".into()], &["*".into()])
        .unwrap();
    let strict = service
        .create_key("strict", "strict", &["*".into()], &["*".into()])
        .unwrap();
    let listed = set_flags(
        &service,
        &strict.summary.id,
        json!({"reject_unknown_models": true}),
    )
    .await;
    assert_eq!(listed["reject_unknown_models"], true);
    assert_eq!(listed["exact_max_tokens"], false);
    let lenient_key = lenient.raw_key.unwrap();
    let strict_key = strict.raw_key.unwrap();

    // The lenient key still gets the passthrough fallback.
    let fallback = send(&app, chat(Some(&lenient_key), "not-served", json!({}))).await;
    assert_eq!(fallback.status(), StatusCode::OK);
    assert_eq!(header(&fallback, "x-llmconduit-model"), Some("served-a"));
    // The strict key gets 404 for the same request ...
    let refused = send(&app, chat(Some(&strict_key), "not-served", json!({}))).await;
    assert_eq!(refused.status(), StatusCode::NOT_FOUND);
    // ... while served models keep working.
    let served = send(&app, chat(Some(&strict_key), "served-a", json!({}))).await;
    assert_eq!(served.status(), StatusCode::OK);

    // Flags are validated: an empty update is a 400, unknown fields 4xx.
    let empty = send(
        &access(&service, ManagementActor::Bootstrap),
        json_request(
            Method::PATCH,
            &format!("/dashboard/api/auth/api-keys/{}", strict.summary.id),
            json!({}),
        ),
    )
    .await;
    assert_eq!(empty.status(), StatusCode::BAD_REQUEST);
    let unknown = send(
        &access(&service, ManagementActor::Bootstrap),
        json_request(
            Method::PATCH,
            &format!("/dashboard/api/auth/api-keys/{}", strict.summary.id),
            json!({"bogus": true}),
        ),
    )
    .await;
    assert!(unknown.status().is_client_error());
    remove_store(&store_path);
}

#[tokio::test]
async fn exact_max_tokens_keys_skip_the_preflight_budget_cap() {
    let (service, store_path, _bootstrap) = enforced_service();
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["model-a"]).await;
    upstream.set_context_limits([("model-a", 32_768_i64)]).await;
    for _ in 0..2 {
        upstream
            .push_response(vec![
                Ok(content_chunk("chat-1", "hi")),
                Ok(usage_chunk("chat-1", 5, 1, 6)),
            ])
            .await;
    }
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), test_config());
    let gateway = Arc::new(
        gateway
            .as_ref()
            .clone()
            .with_authz(service.as_ref().clone()),
    );
    let app = llmconduit::build_app_from_gateway(gateway);
    let normal = service
        .create_key("normal", "normal", &["*".into()], &["*".into()])
        .unwrap();
    let exact = service
        .create_key("exact", "exact", &["*".into()], &["*".into()])
        .unwrap();
    set_flags(
        &service,
        &exact.summary.id,
        json!({"exact_max_tokens": true}),
    )
    .await;

    for key in [normal.raw_key.unwrap(), exact.raw_key.unwrap()] {
        let response = send(
            &app,
            chat(Some(&key), "model-a", json!({"max_tokens": 1_000_000})),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    }
    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 2);
    assert!(
        requests[0].max_output_tokens.unwrap() < 32_768,
        "a normal key is capped to the context window"
    );
    assert_eq!(
        requests[1].max_output_tokens,
        Some(1_000_000),
        "an exact key's budget is sent unchanged"
    );
    remove_store(&store_path);
}

// ---- Pinned eval run keys ---------------------------------------------------

fn in_days(days: i64) -> String {
    (chrono::Utc::now() + chrono::Duration::days(days)).to_rfc3339()
}

#[tokio::test]
async fn eval_keys_are_pinned_capped_uncaptured_and_revocable() {
    let (service, store_path, _bootstrap) = enforced_service();
    let server = wiremock_upstream("served-a").await;
    let app = routing_app(&server, &service);
    let principal = create_service_account(&service, "harbor eval runs").await;
    let coordinator = access(&service, delegated(&[ManagementPermission::EvalKeysCreate]));

    let created = send(
        &coordinator,
        json_request(
            Method::POST,
            "/dashboard/api/auth/eval-keys",
            json!({
                "principal_id": principal,
                "name": "run-42",
                "served_model": "served-a",
                "max_concurrent_sessions": 4,
                "expires_at": in_days(1),
                "reject_unknown_models": true,
                "exact_max_tokens": true
            }),
        ),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let created = json_body(created).await;
    assert_eq!(created["requested_model"], "served-a");
    assert_eq!(created["served_model"], "served-a");
    assert_eq!(created["endpoints"], json!(["chat", "models"]));
    assert_eq!(created["max_concurrent_sessions"], 4);
    assert_eq!(created["capture_payloads"], false);
    assert_eq!(created["reject_unknown_models"], true);
    assert_eq!(created["exact_max_tokens"], true);
    assert!(created["policy_id"].as_str().unwrap().starts_with("pol_"));
    let key_id = created["key_id"].as_str().unwrap().to_string();
    let raw = created["raw_key"].as_str().unwrap().to_string();

    // Pinning is enforced on inference.
    assert_eq!(
        send(&app, chat(Some(&raw), "served-a", json!({})))
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        send(&app, chat(Some(&raw), "served-b", json!({})))
            .await
            .status(),
        StatusCode::FORBIDDEN,
        "another served model is outside the pin"
    );
    assert_eq!(
        send(&app, chat(Some(&raw), "other-model", json!({})))
            .await
            .status(),
        StatusCode::NOT_FOUND,
        "reject_unknown_models: no passthrough fallback"
    );
    let responses = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("authorization", format!("Bearer {raw}"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"model": "served-a", "input": "hi"}).to_string(),
        ))
        .unwrap();
    assert_eq!(send(&app, responses).await.status(), StatusCode::FORBIDDEN);

    // Payload capture is off and the key lists its flags.
    let keys = json_body(
        send(
            &access(&service, ManagementActor::Bootstrap),
            Request::builder()
                .uri("/dashboard/api/auth/api-keys")
                .body(Body::empty())
                .unwrap(),
        )
        .await,
    )
    .await;
    let listed = keys["api_keys"]
        .as_array()
        .unwrap()
        .iter()
        .find(|key| key["id"] == key_id.as_str())
        .unwrap()
        .clone();
    assert_eq!(listed["capture_payloads"], false);
    assert_eq!(listed["exact_max_tokens"], true);

    // The coordinator's own privileges are not widened.
    let policy = send(
        &coordinator,
        json_request(
            Method::POST,
            "/dashboard/api/auth/policies",
            json!({"name": "wide", "effect": "allow", "subjects": ["key:key_coordinator"],
                   "endpoints": ["chat"], "models": ["*"]}),
        ),
    )
    .await;
    assert_eq!(policy.status(), StatusCode::FORBIDDEN);
    let patch = send(
        &coordinator,
        json_request(
            Method::PATCH,
            &format!("/dashboard/api/auth/api-keys/{key_id}"),
            json!({"capture_payloads": true}),
        ),
    )
    .await;
    assert_eq!(patch.status(), StatusCode::FORBIDDEN);

    // Revocation is limited to eval keys and takes effect immediately.
    let ordinary = service
        .create_key("ordinary", "ordinary", &["*".into()], &["*".into()])
        .unwrap();
    let not_eval = send(
        &coordinator,
        json_request(
            Method::POST,
            &format!(
                "/dashboard/api/auth/eval-keys/{}/revoke",
                ordinary.summary.id
            ),
            json!({}),
        ),
    )
    .await;
    assert_eq!(not_eval.status(), StatusCode::NOT_FOUND);
    let revoked = send(
        &coordinator,
        json_request(
            Method::POST,
            &format!("/dashboard/api/auth/eval-keys/{key_id}/revoke"),
            json!({}),
        ),
    )
    .await;
    assert_eq!(revoked.status(), StatusCode::OK);
    assert_eq!(
        json_body(revoked).await,
        json!({"key_id": key_id, "revoked": true})
    );
    assert_eq!(
        send(&app, chat(Some(&raw), "served-a", json!({})))
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let again = send(
        &coordinator,
        json_request(
            Method::POST,
            &format!("/dashboard/api/auth/eval-keys/{key_id}/revoke"),
            json!({}),
        ),
    )
    .await;
    assert_eq!(again.status(), StatusCode::NOT_FOUND);
    remove_store(&store_path);
}

#[tokio::test]
async fn eval_key_requests_are_validated() {
    let (service, store_path, _bootstrap) = enforced_service();
    let principal = create_service_account(&service, "eval validation").await;
    let bootstrap = access(&service, ManagementActor::Bootstrap);
    let base = json!({
        "principal_id": principal,
        "name": "run",
        "served_model": "served-a",
        "max_concurrent_sessions": 1,
        "expires_at": in_days(1)
    });
    let with = |patch: Value| {
        let mut body = base.clone();
        for (k, v) in patch.as_object().unwrap() {
            body[k] = v.clone();
        }
        body
    };
    for (patch, why) in [
        (json!({"served_model": "served-*"}), "glob served model"),
        (json!({"requested_model": "req?"}), "glob requested model"),
        (json!({"served_model": " "}), "blank model"),
        (json!({"max_concurrent_sessions": 0}), "zero sessions"),
        (json!({"max_concurrent_sessions": 257}), "too many sessions"),
        (json!({"expires_at": in_days(8)}), "expiry beyond 7 days"),
        (json!({"expires_at": in_days(-1)}), "expiry in the past"),
        (json!({"expires_at": "tomorrow"}), "unparseable expiry"),
    ] {
        let response = send(
            &bootstrap,
            json_request(Method::POST, "/dashboard/api/auth/eval-keys", with(patch)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY, "{why}");
    }
    // Unknown fields (e.g. trying to force capture) are rejected outright.
    let capture = send(
        &bootstrap,
        json_request(
            Method::POST,
            "/dashboard/api/auth/eval-keys",
            with(json!({"capture_payloads": true})),
        ),
    )
    .await;
    assert!(capture.status().is_client_error());

    // A principal with other grants (here: a group membership) would leak
    // them into the key, so it is refused.
    let group = send(
        &bootstrap,
        json_request(
            Method::POST,
            "/dashboard/api/auth/groups",
            json!({"name": "wide-group", "members": [principal]}),
        ),
    )
    .await;
    assert_eq!(group.status(), StatusCode::OK);
    let bound = send(
        &bootstrap,
        json_request(Method::POST, "/dashboard/api/auth/eval-keys", base.clone()),
    )
    .await;
    assert_eq!(bound.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let missing = send(
        &bootstrap,
        json_request(
            Method::POST,
            "/dashboard/api/auth/eval-keys",
            with(json!({"principal_id": "usr_missing"})),
        ),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    // The dedicated permission is required.
    let denied = send(
        &access(&service, delegated(&[ManagementPermission::KeysCreate])),
        json_request(Method::POST, "/dashboard/api/auth/eval-keys", base),
    )
    .await;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    remove_store(&store_path);
}
