use crate::adapters::anthropic_to_responses;
use crate::adapters::chat_completions;
use crate::adapters::chat_completions::ChatCompletionCollector;
use crate::adapters::chat_completions::ChatCompletionStreamConverter;
use crate::adapters::responses_to_anthropic::AnthropicStreamCollector;
use crate::adapters::responses_to_anthropic::AnthropicStreamConverter;
use crate::adapters::responses_to_chat;
use crate::client_auth::ClientAuthOutcome;
use crate::client_auth::ClientIdentity;
use crate::dashboard_api::dashboard_catalog;
use crate::dashboard_api::dashboard_flow_detail;
use crate::dashboard_api::dashboard_flows;
use crate::dashboard_api::dashboard_metrics;
use crate::dashboard_api::dashboard_providers;
use crate::dashboard_api::dashboard_snapshot;
use crate::dashboard_api::dashboard_topology;
use crate::dashboard_auth::AuthSession;
use crate::dashboard_auth::DashboardAuth;
use crate::dashboard_auth::MutationDenied;
use crate::dashboard_auth::MutationPolicy;
use crate::dashboard_auth::dashboard_login;
use crate::dashboard_auth::dashboard_logout;
use crate::dashboard_auth::delegated_login_response;
use crate::dashboard_auth::require_session;
use crate::dashboard_mesh;
use crate::dashboard_ui::dashboard_asset;
use crate::dashboard_ui::dashboard_index;
use crate::dashboard_ws::dashboard_ws;
use crate::debug_ui::debug_app_js;
use crate::debug_ui::debug_index;
use crate::debug_ui::debug_ws;
use crate::engine::Gateway;
use crate::error::AppError;
use crate::error::AppResult;
use crate::models::anthropic::AnthropicRequest;
use crate::models::anthropic::AnthropicThinking;
use crate::models::chat::ChatCompletionRequest;
use crate::models::chat::normalize_stop;
use crate::models::responses::ResponsesRequest;
use crate::persistent_history_api::history_metrics;
use crate::persistent_history_api::history_request_detail;
use crate::persistent_history_api::history_requests;
use crate::persistent_history_api::history_usage;
use crate::proxy_headers::header_name_eq;
use crate::proxy_headers::is_hop_by_hop_header;
use crate::upstream::BackendChatRequest;
use crate::upstream::collect_models_response;
use axum::Extension;
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::body::to_bytes;
use axum::extract::DefaultBodyLimit;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::Request;
use axum::extract::State;
use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::http::HeaderMap;
use axum::http::HeaderName;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::response::Sse;
use axum::routing::MethodFilter;
use axum::routing::get;
use axum::routing::on;
use axum::routing::post;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use http_body::Frame;
use http_body::SizeHint;
use serde::Deserialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

const API_LOG_PAYLOAD_DUMP_LIMIT_BYTES: usize = 16 * 1024;
const API_LOG_PREVIEW_CHARS: usize = 160;
/// Fable review (Finding 1): inbound bodies at or below this size have their
/// turn-capture redaction done INLINE; a larger body moves the parse+redact+
/// re-serialize onto the blocking pool so a multi-MB Claude Code (1M-context)
/// request never stalls the tokio worker (which the synchronous path did — 100ms+
/// plus a 3–6× transient allocation spike per concurrent request). Set to the SAME
/// 16 KiB as the journal dump gate ([`API_LOG_PAYLOAD_DUMP_LIMIT_BYTES`]).
const TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES: usize = API_LOG_PAYLOAD_DUMP_LIMIT_BYTES;
const UNKNOWN_MODEL_CREATED_AT: &str = "1970-01-01T00:00:00Z";

#[derive(Debug, Clone, Copy, Default)]
pub struct RouterOptions {
    pub with_debug_ui: bool,
    /// D7 startup gate: whether the protected `/debug` + `/dashboard` routes may
    /// be registered. `false` when the bind/secret configuration refuses them
    /// (e.g. non-loopback without a token + https origin). Independent of
    /// `with_debug_ui` so an operator sees a clear "refused to register" startup
    /// log rather than a silent 404. Only consulted when `with_debug_ui` is set.
    pub register_protected_routes: bool,
}

pub fn build_router(gateway: Arc<Gateway>, options: RouterOptions) -> Router {
    // Read before `gateway` is moved into `.with_state(...)` below. Replaces
    // axum's stock 2 MiB `DefaultBodyLimit` with the configured cap (default
    // 10 MiB) so oversized inbound bodies are the operator's choice, not a
    // silent framework default.
    let max_request_body_bytes = gateway.config().max_request_body_bytes;
    let inference_routes = Router::new()
        .route("/v1/responses", post(post_responses).get(get_responses))
        .route("/v1/messages", post(post_messages))
        .route("/v1/messages/count_tokens", post(post_count_tokens))
        .route("/v1/messages", on(MethodFilter::HEAD, probe_messages))
        .route("/v1/messages", on(MethodFilter::OPTIONS, probe_messages))
        .route("/v1/chat/completions", post(post_chat_completions))
        .route("/v1/completions", post(post_completions))
        .route("/v1/models", get(get_models))
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&gateway),
            require_inference_auth,
        ));
    let router = Router::new()
        .merge(inference_routes)
        .route("/health", get(get_health))
        .route("/openapi.json", get(get_openapi))
        .route("/", {
            let dashboard = options.with_debug_ui && options.register_protected_routes;
            get(move |headers: HeaderMap| get_root(headers, dashboard))
        });

    // D7: the debug UI + dashboard routes register only when `--with-debug-ui`
    // is set AND the startup decision permits it AND the env-built auth context
    // exists. All three hold or none of the protected routes appear (production
    // untouched; a misconfigured non-loopback server refuses rather than serving
    // transcripts/credentials in the clear).
    let router = match (
        options.with_debug_ui && options.register_protected_routes,
        gateway.dashboard_auth(),
    ) {
        (true, Some(auth)) => router.merge(protected_routes(Arc::clone(&gateway), auth)),
        _ => router,
    };

    router
        .fallback(api_not_found)
        // `log_api_call` enforces the inbound body cap as a HARD memory bound for
        // EVERY route (Content-Length precheck + capped buffered read), so an
        // oversized upload is rejected with 413 before it can be buffered. The
        // `DefaultBodyLimit` makes the POST handlers' JSON/Bytes extractors agree
        // on that same ceiling instead of axum's stock 2 MiB default. Both read
        // the single configured value (`max_request_body_bytes`, which the
        // middleware re-reads from the same gateway config) — there is no second,
        // larger hidden limit. The middleware state stays `Arc<Gateway>` because
        // `log_api_call` also opens the dashboard flow record from it.
        .layer(middleware::from_fn_with_state(
            Arc::clone(&gateway),
            log_api_call,
        ))
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .with_state(gateway)
}

/// The D7-gated `/debug` + `/dashboard` sub-router (state `Arc<Gateway>`, merged
/// into the main router so it shares the outer `.with_state`).
///
/// Auth topology:
/// - `/dashboard/login` + `/dashboard/logout` — read the auth `Extension` (so
///   the handlers can sign/clear cookies) but are NOT behind `require_session`
///   (login is how you authenticate; logout must work for any state).
/// - `/dashboard` — auth `Extension` only; the shell handler serves the login
///   page vs. the SPA from `Option<AuthSession>` itself (no 401).
/// - `/dashboard/assets/{*path}` — public sub-resources (hashed, immutable); the
///   SPA shell behind them is already gated, and the asset bytes carry no
///   secrets.
/// - `/debug` + `/debug/app.js` — behind `require_session` (401 when unauthed).
/// - `/debug/ws` — self-gated inside the handler (cookie + `Origin` + `exp`); it
///   needs to OWN the rejection so the WS `Origin` check is authoritative.
///
/// The shared `Arc<DashboardAuth>` is attached as a request `Extension` scoped to
/// this sub-router so the middleware/handlers/extractors can read it
/// (`/debug/ws` reads it via `gateway.dashboard_auth()` instead).
fn protected_routes(gateway: Arc<Gateway>, auth: Arc<DashboardAuth>) -> Router<Arc<Gateway>> {
    // D13 `/dashboard/api/*` REST surface. `no-store` + the dashboard security
    // headers are applied as ROUTE-LEVEL response middleware on the WHOLE api router
    // (D13 R1 MED), so EVERY response carries them — including an axum EXTRACTOR
    // rejection (an invalid `page`/`limit`/`at` query → a bare `400` produced before
    // any handler runs), which previously escaped the per-handler stamping. The
    // handlers' own `json_no_store` re-stamp is idempotent (same static header
    // values), so double-application is harmless. `require_session` is layered
    // OUTSIDE the response map (added last → outermost) so it 401's an unauthed
    // caller BEFORE any handler/extractor work AND its 401 also flows back through
    // the `no_store` map.
    let api_routes = Router::new()
        .route("/dashboard/api/flows", get(dashboard_flows))
        .route(
            "/dashboard/api/sessions/active",
            get(crate::dashboard_api::dashboard_sessions_active),
        )
        .route("/dashboard/api/flows/{id}", get(dashboard_flow_detail))
        .route("/dashboard/api/flows/{id}/kill", post(dashboard_flow_kill))
        .route("/dashboard/api/metrics", get(dashboard_metrics))
        .route("/dashboard/api/topology", get(dashboard_topology))
        .route("/dashboard/api/catalog", get(dashboard_catalog))
        .route("/dashboard/api/providers", get(dashboard_providers))
        .route(
            "/dashboard/api/fleet",
            get(crate::dashboard_fleet::fleet_models),
        )
        .route(
            "/dashboard/api/fleet/models/{id}/load",
            post(crate::dashboard_fleet::fleet_load_model),
        )
        .route(
            "/dashboard/api/fleet/models/{id}/unload",
            post(crate::dashboard_fleet::fleet_unload_model),
        )
        .route("/dashboard/api/mesh", get(dashboard_mesh::mesh_state))
        .route(
            "/dashboard/api/mesh/join-keys",
            post(dashboard_mesh::create_join_key),
        )
        .route(
            "/dashboard/api/mesh/join-keys/{id}/revoke",
            post(dashboard_mesh::revoke_join_key),
        )
        .route(
            "/dashboard/api/mesh/nodes/{endpoint_id}/disable",
            post(dashboard_mesh::disable_node),
        )
        .route(
            "/dashboard/api/mesh/nodes/{endpoint_id}/enable",
            post(dashboard_mesh::enable_node),
        )
        .route(
            "/dashboard/api/mesh/models/disable",
            post(dashboard_mesh::disable_model),
        )
        .route(
            "/dashboard/api/mesh/models/enable",
            post(dashboard_mesh::enable_model),
        )
        .route("/dashboard/api/snapshot", get(dashboard_snapshot))
        .route("/dashboard/api/history/requests", get(history_requests))
        .route(
            "/dashboard/api/history/requests/{id}",
            get(history_request_detail),
        )
        .route(
            "/dashboard/api/history/requests/{id}/body",
            get(crate::persistent_history_api::history_request_body),
        )
        .route(
            "/dashboard/api/history/throughput",
            get(crate::persistent_history_api::history_throughput),
        )
        .route(
            "/dashboard/api/history/sessions",
            get(crate::persistent_history_api::history_sessions),
        )
        .route(
            "/dashboard/api/history/sessions/{id}",
            get(crate::persistent_history_api::history_session_detail),
        )
        .route("/dashboard/api/history/usage", get(history_usage))
        .route("/dashboard/api/history/metrics", get(history_metrics))
        .route(
            "/dashboard/api/history/activity",
            get(crate::persistent_history_api::history_activity),
        )
        .route("/dashboard/api/me", get(crate::accounts_api::me))
        .route(
            "/dashboard/api/users",
            get(crate::accounts_api::list_users).post(crate::accounts_api::create_user),
        )
        .route(
            "/dashboard/api/users/{id}",
            axum::routing::patch(crate::accounts_api::update_user)
                .delete(crate::accounts_api::delete_user),
        )
        .route(
            "/dashboard/api/keys",
            get(crate::accounts_api::list_keys).post(crate::accounts_api::create_key),
        )
        .route(
            "/dashboard/api/keys/{id}",
            axum::routing::delete(crate::accounts_api::delete_key),
        )
        .merge(crate::provider_metrics::dashboard_routes::<Arc<Gateway>>(
            gateway.provider_metrics(),
        ))
        .route_layer(middleware::map_response(dashboard_api_no_store));

    let access_routes =
        crate::dashboard_access::routes::<Arc<Gateway>>(gateway.authz_arc().access_backend())
            .route_layer(middleware::map_response(dashboard_api_no_store))
            .route_layer(middleware::from_fn_with_state(
                Arc::clone(&gateway),
                require_management_access,
            ));

    // The `/debug` HTML/JS endpoints share the same session gate but stamp their own
    // headers in-handler (they serve HTML, not the JSON `no-store` set), so they are
    // NOT under the api router's `no_store` response map.
    let debug_routes = Router::new()
        .route("/debug", get(debug_index))
        .route("/debug/app.js", get(debug_app_js));

    // Both groups require a valid session (401 when missing/expired/invalid): every
    // dashboard read AND the kill mutation is 401'd for an unauthenticated caller
    // BEFORE any handler work (the kill's CSRF/mutation gate runs only for an
    // authenticated request).
    let api_gated = api_routes.route_layer(middleware::from_fn_with_state(
        Arc::clone(&gateway),
        require_dashboard_session,
    ));
    let debug_gated = debug_routes.route_layer(middleware::from_fn(require_session));
    let dashboard_shell = Router::new()
        .route("/dashboard", get(dashboard_index))
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&gateway),
            resolve_optional_dashboard_session,
        ));

    // Routes that read the auth context but manage their own access decision,
    // plus the self-gated WS and the public hashed assets.
    //
    // D7b: the dashboard data socket is a separate `/dashboard/ws` route carrying
    // the batched `DashboardFrame` envelope (Monitor/Usage/FlowStatus/MetricTick/
    // TopologyUpdate). Like `/debug/ws` it is SELF-gated inside the handler (cookie
    // + `Origin` allow-list + cookie-`exp` close, via D7a's `authenticate_ws`), so
    // it OWNS its rejection and the WS `Origin` check stays authoritative.
    let open = Router::new()
        .route("/dashboard/login", post(dashboard_login))
        .route("/dashboard/logout", post(dashboard_logout))
        .route("/dashboard/auth/key-login", post(dashboard_key_login))
        .route("/dashboard/auth/logout", post(dashboard_auth_logout))
        .route("/debug/ws", get(debug_ws))
        .route("/dashboard/ws", get(dashboard_ws))
        .route("/dashboard/assets/{*path}", get(dashboard_asset));

    api_gated
        .merge(debug_gated)
        .merge(access_routes)
        .merge(dashboard_shell)
        .merge(open)
        // Scope the auth context to ONLY the protected routes (not `/v1/*`).
        .layer(Extension(auth))
}

async fn require_dashboard_session(
    State(gateway): State<Arc<Gateway>>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(auth) = request.extensions().get::<Arc<DashboardAuth>>().cloned() else {
        return management_error(StatusCode::UNAUTHORIZED, "unauthorized");
    };
    if let Some(session) = auth.authenticate(request.headers()) {
        request.extensions_mut().insert(session);
        return next.run(request).await;
    }
    if let Some((session_id, exp)) = auth.delegated_session(request.headers()) {
        match gateway
            .authz()
            .authenticate_delegated_session(&session_id)
            .await
        {
            Ok(Some(actor)) => {
                request
                    .extensions_mut()
                    .insert(AuthSession { exp, user: None });
                request.extensions_mut().insert(actor);
                return next.run(request).await;
            }
            Ok(None) => {}
            Err(_) => {
                return management_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "authorization unavailable",
                );
            }
        }
    }
    management_error(StatusCode::UNAUTHORIZED, "unauthorized")
}

async fn resolve_optional_dashboard_session(
    State(gateway): State<Arc<Gateway>>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(auth) = request.extensions().get::<Arc<DashboardAuth>>().cloned() else {
        return next.run(request).await;
    };
    let session = if let Some(session) = auth.authenticate(request.headers()) {
        Some(session)
    } else if let Some((session_id, exp)) = auth.delegated_session(request.headers()) {
        match gateway
            .authz()
            .authenticate_delegated_session(&session_id)
            .await
        {
            Ok(Some(actor)) => {
                request.extensions_mut().insert(actor);
                Some(AuthSession { exp, user: None })
            }
            _ => None,
        }
    } else {
        None
    };
    if let Some(session) = session {
        request.extensions_mut().insert(session);
    }
    next.run(request).await
}

#[derive(Deserialize)]
struct DashboardKeyLogin {
    api_key: String,
}

#[utoipa::path(
    post,
    path = "/dashboard/auth/key-login",
    tag = "dashboard-auth",
    request_body(content = serde_json::Value, content_type = "application/json"),
    responses(
        (status = 200, body = serde_json::Value, description = "Creates a delegated dashboard session for a management-enabled API key."),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 503, body = crate::openapi::DashboardError)
    )
)]
async fn dashboard_key_login(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    headers: HeaderMap,
    Json(body): Json<DashboardKeyLogin>,
) -> Response {
    if !auth.origin_allowed(&headers) {
        return management_error(StatusCode::FORBIDDEN, "cross-origin login denied");
    }
    let Ok(value) = HeaderValue::from_str(&format!("Bearer {}", body.api_key)) else {
        return management_error(StatusCode::UNAUTHORIZED, "invalid API key");
    };
    let mut key_headers = HeaderMap::new();
    key_headers.insert(header::AUTHORIZATION, value);
    let context = match gateway.authz().authenticate(&key_headers) {
        Ok(Some(context)) => context,
        Ok(None) | Err(crate::authz::AuthFailure::Missing | crate::authz::AuthFailure::Invalid) => {
            return management_error(StatusCode::UNAUTHORIZED, "invalid API key");
        }
        Err(crate::authz::AuthFailure::Forbidden) => {
            return management_error(StatusCode::FORBIDDEN, "management permission denied");
        }
        Err(crate::authz::AuthFailure::Unavailable) => {
            return management_error(StatusCode::SERVICE_UNAVAILABLE, "authorization unavailable");
        }
    };
    let csrf = auth.issue_csrf_token();
    let digest = Sha256::digest(csrf.as_bytes());
    let exp =
        chrono::Utc::now().timestamp().max(0) as u64 + crate::dashboard_auth::SESSION_TTL_SECS;
    let actor = match gateway
        .authz()
        .create_delegated_session(
            &context,
            digest.as_slice(),
            i64::try_from(exp).unwrap_or(i64::MAX),
        )
        .await
    {
        Ok(actor) => actor,
        Err(crate::authz::AuthError::Forbidden) => {
            return management_error(StatusCode::FORBIDDEN, "management permission denied");
        }
        Err(_) => {
            return management_error(StatusCode::SERVICE_UNAVAILABLE, "authorization unavailable");
        }
    };
    let crate::dashboard_access::ManagementActor::Delegated { session_id, .. } = actor else {
        return management_error(StatusCode::INTERNAL_SERVER_ERROR, "internal server error");
    };
    delegated_login_response(&auth, &session_id, &csrf, exp)
}

#[utoipa::path(
    post,
    path = "/dashboard/auth/logout",
    tag = "dashboard-auth",
    responses(
        (status = 204, description = "Revokes a delegated session when present and clears dashboard cookies."),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 503, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
async fn dashboard_auth_logout(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    headers: HeaderMap,
) -> Response {
    if let Some((session_id, _)) = auth.delegated_session(&headers) {
        if !auth.origin_allowed(&headers) {
            return management_error(StatusCode::FORBIDDEN, "cross-origin logout denied");
        }
        if let Err(denied) = auth.authorize_mutation(&headers) {
            return management_error(denied.status(), denied.message());
        }
        let Some(csrf) = headers
            .get(crate::dashboard_auth::CSRF_HEADER)
            .and_then(|value| value.to_str().ok())
        else {
            return management_error(StatusCode::FORBIDDEN, "missing or invalid CSRF token");
        };
        let digest = Sha256::digest(csrf.as_bytes());
        match gateway
            .authz()
            .verify_delegated_csrf_digest(&session_id, digest.as_slice())
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return management_error(StatusCode::FORBIDDEN, "missing or invalid CSRF token");
            }
            Err(_) => {
                return management_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "authorization unavailable",
                );
            }
        }
        if gateway
            .authz()
            .revoke_delegated_session(&session_id)
            .await
            .is_err()
        {
            return management_error(StatusCode::SERVICE_UNAVAILABLE, "authorization unavailable");
        }
    }
    dashboard_logout(Extension(auth)).await
}

async fn require_management_access(
    State(gateway): State<Arc<Gateway>>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(dashboard_auth) = request.extensions().get::<Arc<DashboardAuth>>().cloned() else {
        return management_error(StatusCode::UNAUTHORIZED, "unauthorized");
    };

    let (actor, cookie_or_dashboard_token) =
        if dashboard_auth.authenticate(request.headers()).is_some() {
            (crate::dashboard_access::ManagementActor::Bootstrap, true)
        } else if let Some((session_id, _)) = dashboard_auth.delegated_session(request.headers()) {
            match gateway
                .authz()
                .authenticate_delegated_session(&session_id)
                .await
            {
                Ok(Some(actor)) => (actor, true),
                Ok(None) => return management_error(StatusCode::UNAUTHORIZED, "unauthorized"),
                Err(_) => {
                    return management_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "authorization unavailable",
                    );
                }
            }
        } else {
            match gateway.authz().authenticate(request.headers()) {
                Ok(Some(context)) => (context.management_actor(), false),
                Ok(None)
                | Err(crate::authz::AuthFailure::Missing)
                | Err(crate::authz::AuthFailure::Invalid) => {
                    return management_error(StatusCode::UNAUTHORIZED, "unauthorized");
                }
                Err(crate::authz::AuthFailure::Forbidden) => {
                    return management_error(StatusCode::FORBIDDEN, "management permission denied");
                }
                Err(crate::authz::AuthFailure::Unavailable) => {
                    return management_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "authorization unavailable",
                    );
                }
            }
        };

    if request.method() != axum::http::Method::GET && request.method() != axum::http::Method::HEAD {
        if cookie_or_dashboard_token {
            if let Err(denied) = dashboard_auth.authorize_mutation(request.headers()) {
                return management_error(denied.status(), denied.message());
            }
            if let crate::dashboard_access::ManagementActor::Delegated { session_id, .. } = &actor {
                let Some(csrf) = request
                    .headers()
                    .get(crate::dashboard_auth::CSRF_HEADER)
                    .and_then(|value| value.to_str().ok())
                else {
                    return management_error(
                        StatusCode::FORBIDDEN,
                        "missing or invalid CSRF token",
                    );
                };
                let digest = Sha256::digest(csrf.as_bytes());
                match gateway
                    .authz()
                    .verify_delegated_csrf_digest(session_id, digest.as_slice())
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        return management_error(
                            StatusCode::FORBIDDEN,
                            "missing or invalid CSRF token",
                        );
                    }
                    Err(_) => {
                        return management_error(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "authorization unavailable",
                        );
                    }
                }
            }
        } else if !dashboard_auth.mutations_enabled() {
            return management_error(StatusCode::FORBIDDEN, "dashboard mutations are disabled");
        } else if request.headers().contains_key(header::ORIGIN)
            && !dashboard_auth.origin_allowed(request.headers())
        {
            return management_error(
                StatusCode::FORBIDDEN,
                "cross-origin management request denied",
            );
        }
    }

    request.extensions_mut().insert(actor);
    next.run(request).await
}

fn management_error(status: StatusCode, message: &'static str) -> Response {
    crate::dashboard_auth::no_store(
        (status, Json(serde_json::json!({ "error": message }))).into_response(),
    )
}

async fn require_inference_auth(
    State(gateway): State<Arc<Gateway>>,
    mut request: Request,
    next: Next,
) -> Response {
    if !gateway.authz().is_enabled() {
        return next.run(request).await;
    }
    let endpoint = auth_endpoint(request.uri().path());
    let context = match gateway.authz().authenticate(request.headers()) {
        Ok(Some(context)) => context,
        Ok(None) => return next.run(request).await,
        Err(failure) => return auth_failure_response(request.uri().path(), failure),
    };
    if !context.allows_endpoint(endpoint) {
        return auth_failure_response(request.uri().path(), crate::authz::AuthFailure::Forbidden);
    }
    request.extensions_mut().insert(context);
    next.run(request).await
}

fn auth_endpoint(path: &str) -> &'static str {
    match path {
        "/v1/responses" => "responses",
        "/v1/chat/completions" => "chat",
        "/v1/messages" => "messages",
        "/v1/messages/count_tokens" => "count_tokens",
        "/v1/completions" => "completions",
        "/v1/models" => "models",
        _ => "unknown",
    }
}

fn auth_failure_response(path: &str, failure: crate::authz::AuthFailure) -> Response {
    let (status, message) = match failure {
        crate::authz::AuthFailure::Missing => (StatusCode::UNAUTHORIZED, "missing API key"),
        crate::authz::AuthFailure::Invalid => (StatusCode::UNAUTHORIZED, "invalid API key"),
        crate::authz::AuthFailure::Forbidden => (
            StatusCode::FORBIDDEN,
            "the API key is not authorized for this request",
        ),
        crate::authz::AuthFailure::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "authorization service unavailable",
        ),
    };
    let mut response = if path.starts_with("/v1/messages") {
        (
            status,
            Json(serde_json::json!({
                "type": "error",
                "error": {
                    "type": if status == StatusCode::UNAUTHORIZED { "authentication_error" } else { "permission_error" },
                    "message": message
                }
            })),
        )
            .into_response()
    } else {
        (
            status,
            Json(serde_json::json!({
                "error": {
                    "message": message,
                    "type": if status == StatusCode::UNAUTHORIZED { "invalid_request_error" } else { "permission_denied" },
                    "code": if status == StatusCode::UNAUTHORIZED { "invalid_api_key" } else { "permission_denied" }
                }
            })),
        )
            .into_response()
    };
    if status == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"llmconduit\""),
        );
    }
    response
}

fn authorize_inference(
    context: Option<&crate::authz::AuthContext>,
    endpoint: crate::upstream::InferenceEndpoint,
    requested_model: &str,
) -> AppResult<crate::upstream::AuthorizationScope> {
    context.map_or_else(
        || Ok(crate::upstream::AuthorizationScope::unrestricted()),
        |context| {
            context
                .authorization_scope(endpoint, requested_model)
                .map_err(|_| {
                    AppError::forbidden("the API key is not authorized for the requested model")
                })
        },
    )
}

async fn acquire_inference_session(
    gateway: &Gateway,
    context: Option<&crate::authz::AuthContext>,
) -> AppResult<Option<crate::authz::SessionLease>> {
    match context {
        Some(context) => gateway
            .authz()
            .acquire_session(context)
            .await
            .map_err(|err| AppError::forbidden(err.to_string())),
        None => Ok(None),
    }
}

/// Route-level response middleware (D13 R1 MED): stamp `no-store` + the dashboard
/// security header set on EVERY `/dashboard/api/*` response, including an axum
/// extractor-rejection `400` produced before any handler runs (an invalid
/// `page`/`limit`/`at` query). Delegates to the single header authority
/// [`crate::dashboard_auth::no_store`]; the handlers' own `json_no_store` re-stamps
/// the same static values, so applying this on top is idempotent.
async fn dashboard_api_no_store(response: Response) -> Response {
    crate::dashboard_auth::no_store(response)
}

/// D6 — the outcome of a `POST /dashboard/api/flows/:id/kill` attempt, decoupled from
/// axum so the policy + abort logic is unit-testable against a MOCK [`MutationPolicy`]
/// (the spec's "compiles + tests against a mocked auth/CSRF gate"). The axum handler is
/// the only place that maps this to an HTTP status.
#[derive(Debug, PartialEq, Eq)]
pub enum FlowKillOutcome {
    /// A live token was found and cancelled → `200 OK`.
    Killed,
    /// No live flow for that `api_call_id` (unknown OR already finished) → `404`.
    NotFound,
    /// The mutation policy refused (mutations disabled, or CSRF missing/invalid) → the
    /// `MutationDenied` status (`403`). Carries the reason so the body can be precise.
    Denied(MutationDenied),
}

/// D6 — the pure kill core: authorize the mutation, then cancel the flow. Separated
/// from the axum handler so tests drive it with a mock `MutationPolicy` + a real
/// `Gateway` (no HTTP stack). CSRF/mutation gating runs FIRST (a refused mutation must
/// not even probe whether the id is live — no existence oracle for an unauthorized
/// caller); only an authorized request consults the AbortHub. `gateway.abort` is
/// idempotent, so a double-kill of a still-live flow simply re-cancels an
/// already-cancelled token (`true` both times until the guard removes it), and a kill
/// of a finished/unknown flow is `false` → 404.
pub fn flow_kill_outcome(
    policy: &dyn MutationPolicy,
    headers: &HeaderMap,
    gateway: &Gateway,
    api_call_id: &str,
) -> FlowKillOutcome {
    if let Err(denied) = policy.authorize_mutation(headers) {
        return FlowKillOutcome::Denied(denied);
    }
    if gateway.abort(api_call_id) {
        FlowKillOutcome::Killed
    } else {
        FlowKillOutcome::NotFound
    }
}

/// D6 — the `POST /dashboard/api/flows/:id/kill` handler. `:id` IS the flow's
/// `api_call_id` (the AbortHub key == the route param, no rekeying — spec decision), so
/// it cancels the live server-side stream: a `200` flips the flow's `CancellationToken`
/// and the engine's compose-with-`tx.closed()` sites surface `AppError::cancelled()`
/// (499) to the client while the L1 guard finalizes the record `Cancelled`; a `404`
/// means no live flow. The mutation+CSRF gate (`LLMCONDUIT_DASHBOARD_ALLOW_MUTATIONS` +
/// a double-submit CSRF token) is enforced via the shared `DashboardAuth`
/// [`MutationPolicy`] BEFORE any abort. Behind `require_session` when registered (D13),
/// so an unauthenticated caller is already 401'd before reaching here.
///
/// REGISTRATION is D13's job (this is the only mutation route in the phase; replay is
/// deferred). The handler is provided here so D6 ships the kill behavior + tests
/// independent of D13's route table (breaking the D6↔D13 cycle).
/// Abort a live flow (in-flight request) by its api-call id.
#[utoipa::path(
    post,
    path = "/dashboard/api/flows/{id}/kill",
    tag = "dashboard",
    operation_id = "dashboard_flow_kill",
    params(
        ("id" = String, Path, description = "The flow's `api_call_id` as shown by `/dashboard/api/flows`."),
        ("x-csrf-token" = String, Header, description = "Double-submit CSRF token; must equal the `llmconduit_csrf` cookie.")
    ),
    responses(
        (status = 200, body = crate::openapi::FlowKillResponse, description = "The flow was aborted."),
        (status = 401, description = "No valid dashboard session (plain text `unauthorized`)."),
        (status = 403, body = crate::openapi::DashboardError, description = "Mutations are disabled for this deployment (`LLMCONDUIT_DASHBOARD_ALLOW_MUTATIONS` unset) or the CSRF token is missing or wrong."),
        (status = 404, body = crate::openapi::DashboardError, description = "No live flow with that id.")
    )
)]
pub async fn dashboard_flow_kill(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Path(api_call_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let response = match flow_kill_outcome(auth.as_ref(), &headers, gateway.as_ref(), &api_call_id)
    {
        // The 200 body MUST match the frozen `KillResponse {api_call_id, killed}`
        // (dashboard-frontend/src/api/types.ts) — the SPA decodes both fields.
        FlowKillOutcome::Killed => (
            StatusCode::OK,
            Json(serde_json::json!({"api_call_id": api_call_id, "killed": true})),
        )
            .into_response(),
        FlowKillOutcome::NotFound => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "no live flow for that id"})),
        )
            .into_response(),
        FlowKillOutcome::Denied(denied) => (
            denied.status(),
            Json(serde_json::json!({"error": denied.message()})),
        )
            .into_response(),
    };
    // Dashboard API responses are never cached (mutation result, auth-scoped).
    crate::dashboard_auth::no_store(response)
}

/// Whether a request is an instrumented inference flow (D1, incl. R1 #1): the
/// METHOD must be `POST` AND the path one of the three canonical inference entry
/// points. The method check matters because `/v1/messages` also serves HEAD/OPTIONS
/// probes — those (and any non-POST) must NOT open an orphan flow record.
/// `/v1/completions` is a raw upstream passthrough that bypasses the engine (never
/// instrumented); `/dashboard*`/`/debug*`/`/health`/`/`/`/v1/models` carry no flow.
fn is_flow_capture_request(method: &axum::http::Method, path: &str) -> bool {
    method == axum::http::Method::POST
        && matches!(
            path,
            "/v1/responses" | "/v1/messages" | "/v1/chat/completions"
        )
}

fn is_authenticated_client_api_request(method: &axum::http::Method, path: &str) -> bool {
    (path == "/v1" || path.starts_with("/v1/"))
        && !(path == "/v1/messages"
            && matches!(
                *method,
                axum::http::Method::HEAD | axum::http::Method::OPTIONS
            ))
}

fn client_auth_error(
    path: &str,
    anthropic_surface: bool,
    status: StatusCode,
    message: &'static str,
) -> Response {
    let error_type = if status == StatusCode::UNAUTHORIZED {
        "authentication_error"
    } else {
        "permission_error"
    };
    if anthropic_surface || matches!(path, "/v1/messages" | "/v1/messages/count_tokens") {
        return (
            status,
            Json(serde_json::json!({
                "type": "error",
                "error": { "type": error_type, "message": message }
            })),
        )
            .into_response();
    }
    (
        status,
        Json(serde_json::json!({
            "error": { "message": message, "type": error_type }
        })),
    )
        .into_response()
}

fn unknown_model_error(anthropic_surface: bool) -> Response {
    if anthropic_surface {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "type": "error",
                "error": {
                    "type": "not_found_error",
                    "message": "requested model is not configured"
                }
            })),
        )
            .into_response();
    }
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "error": {
                "message": "requested model is not configured",
                "type": "invalid_request_error"
            }
        })),
    )
        .into_response()
}

/// Whether `path` is a dashboard auth endpoint whose request body carries the
/// session secret (the login `{"token": ...}`; logout is bodyless but symmetric).
/// D7a R2 #1: the bare JSON key `token` is NOT in the global sensitive-key set
/// (too many legitimate `token` fields elsewhere), so the access token would leak
/// through the small-body `body_payload` dump. D7a R3 #1: a `body_sha256` + a
/// `body_bytes` length on a login body form an OFFLINE verification oracle — an
/// attacker with the logs can brute-force the token against the known digest and
/// length. So for these endpoints we suppress ALL body-derived fields (digest,
/// length, summary, AND payload), logging only non-body metadata.
fn is_dashboard_auth_path(path: &str) -> bool {
    matches!(
        path,
        "/dashboard/login"
            | "/dashboard/logout"
            | "/dashboard/auth/key-login"
            | "/dashboard/auth/logout"
    )
}

/// Body-derived tracing fields for the inbound-request log line. `None` for a
/// dashboard auth endpoint (D7a R3 #1): emitting the body length or its SHA-256
/// for a login body leaks an offline token-verification oracle, so an auth-path
/// request logs NO body-derived field at all (not the digest, length, summary,
/// nor — separately — the payload dump). `Some` for every other path carries the
/// length, hex digest, and the (already-redacted) summary.
struct BodyLogFields {
    bytes: usize,
    sha256: String,
    summary: String,
}

/// Compute the body-derived log fields for `path`/`body`, returning `None` for a
/// dashboard auth endpoint so the caller emits no body-derived field (D7a R3 #1
/// — the digest + length are a token-verification oracle).
fn body_log_fields(path: &str, body: &Bytes) -> Option<BodyLogFields> {
    if is_dashboard_auth_path(path) {
        return None;
    }
    Some(BodyLogFields {
        bytes: body.len(),
        sha256: hex::encode(Sha256::digest(body)),
        summary: summarize_api_body(path, body),
    })
}

/// Inbound `Content-Encoding` decompression. codex-tui 0.145+ zstd-compresses
/// Responses request bodies (`enable_request_compression`); without this the
/// `Json` extractor parses still-compressed bytes and rejects with
/// `expected value at line 1 column 1`. Bodies are already fully buffered by
/// `log_api_call`, so this decodes synchronously in place. `identity`/absent ⇒
/// passthrough. Unknown encodings ⇒ `Err` so the caller surfaces a 415.
fn decode_content_encoding(body: &Bytes, encoding: &str) -> Result<Bytes, String> {
    let enc = encoding.trim().to_ascii_lowercase();
    if enc.is_empty() || enc == "identity" {
        return Ok(body.clone());
    }
    let decoded = match enc.as_str() {
        "gzip" => {
            let mut out = Vec::with_capacity(body.len());
            flate2::read::GzDecoder::new(&body[..])
                .read_to_end(&mut out)
                .map_err(|e| format!("gzip: {e}"))?;
            out
        }
        "deflate" => {
            let mut out = Vec::with_capacity(body.len());
            flate2::read::ZlibDecoder::new(&body[..])
                .read_to_end(&mut out)
                .map_err(|e| format!("deflate: {e}"))?;
            out
        }
        "br" => {
            let mut out = Vec::with_capacity(body.len());
            brotli::Decompressor::new(&body[..], 4096)
                .read_to_end(&mut out)
                .map_err(|e| format!("br: {e}"))?;
            out
        }
        "zstd" => zstd::decode_all(&body[..]).map_err(|e| format!("zstd: {e}"))?,
        other => return Err(format!("unsupported content-encoding: {other}")),
    };
    Ok(Bytes::from(decoded))
}

/// Parse the inbound `Content-Length` as a byte count, if present and valid.
fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
}

/// 413 response for an inbound body that exceeds `limit_bytes`.
fn payload_too_large(limit_bytes: usize) -> Response {
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        format!("request body exceeds the {limit_bytes}-byte limit"),
    )
        .into_response()
}

/// True when an `axum::body::to_bytes` error is an over-cap length-limit
/// rejection (the inbound body exceeded the configured byte cap), as opposed to
/// a truncated or otherwise broken stream. `to_bytes` collects the body through
/// `http_body_util::Limited`, which surfaces a `LengthLimitError` in the error
/// source chain on overflow — so the classification is exact and does not depend
/// on whether a `Content-Length` was sent.
fn is_length_limit_error(err: &axum::Error) -> bool {
    let mut source = std::error::Error::source(err);
    while let Some(cause) = source {
        if cause.is::<http_body_util::LengthLimitError>() {
            return true;
        }
        source = cause.source();
    }
    false
}

async fn log_api_call(
    State(gateway): State<Arc<Gateway>>,
    request: Request,
    next: Next,
) -> Response {
    let api_call_id = format!("api_{}", Uuid::new_v4().simple());
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let started_at = Instant::now();
    let anthropic_surface =
        headers.contains_key("anthropic-version") || headers.contains_key("anthropic-beta");

    // Authenticate client-facing calls before buffering or logging their body.
    // Dashboard/session authentication remains a separate env-only layer.
    let client_identity = if is_authenticated_client_api_request(&method, uri.path()) {
        match gateway.client_auth().authenticate_headers(&headers) {
            ClientAuthOutcome::Authenticated(identity) => Some(identity),
            ClientAuthOutcome::Open => None,
            ClientAuthOutcome::Rejected => {
                return client_auth_error(
                    uri.path(),
                    anthropic_surface,
                    StatusCode::UNAUTHORIZED,
                    "missing or invalid API key",
                );
            }
        }
    } else {
        None
    };

    // The configurable inbound body cap (default 10 MiB), read from the gateway
    // config — the SAME value `build_router` hands `DefaultBodyLimit::max`, so the
    // middleware's buffered-read cap and the POST extractors' ceiling never diverge.
    let max_request_body_bytes = gateway.config().max_request_body_bytes;

    // Reject before buffering when the declared Content-Length already exceeds
    // the inbound cap: a hostile multi-hundred-MiB upload is refused with 413
    // without reading a byte. `DefaultBodyLimit` (checked later by the JSON
    // extractor) cannot bound memory here because this middleware buffers the
    // whole body for logging FIRST — so the cap is also enforced at the read
    // below for bodies that arrive without a (trustworthy) Content-Length.
    //
    // F1b turn-capture scope (review #3): these PRE-body-read rejections (the 413
    // here, and the read-failure 413/400 just below) return BEFORE the capture
    // gate runs and BEFORE any `api_call_id` turn/`inbound_request` section is
    // minted — so they are intentionally NOT captured: there is no turn to attach
    // a `served_response` to, and we do not mint one just to record a 413.
    // Every POST-gate response (an engine error, a `Reject`, any 4xx/5xx produced
    // AFTER the gate) IS teed, because the tee wraps the WHOLE `next.run` result
    // below regardless of status.
    let declared_length = content_length(&headers);
    if let Some(declared) = declared_length
        && declared > max_request_body_bytes as u64
    {
        tracing::warn!(
            api_call_id = %api_call_id,
            method = %method,
            path = %uri.path(),
            content_length = declared,
            limit_bytes = max_request_body_bytes,
            "rejected oversized inbound API request: Content-Length over limit"
        );
        return payload_too_large(max_request_body_bytes);
    }

    let (mut parts, body) = request.into_parts();
    // Cap the buffered read at the CONFIGURED limit (not a fixed ceiling) so a
    // chunked / length-less body cannot grow memory past the cap. An over-cap
    // body surfaces a `LengthLimitError` -> 413 oversize; any other read failure
    // (truncated / broken stream) -> 400. The classification is exact, so it is
    // correct even for length-less bodies that lacked the Content-Length precheck.
    let body_bytes = match to_bytes(body, max_request_body_bytes).await {
        Ok(bytes) => bytes,
        Err(err) => {
            if is_length_limit_error(&err) {
                tracing::warn!(
                    api_call_id = %api_call_id,
                    method = %method,
                    path = %uri.path(),
                    limit_bytes = max_request_body_bytes,
                    error = %err,
                    "rejected inbound API request: body exceeded limit"
                );
                return payload_too_large(max_request_body_bytes);
            }
            tracing::warn!(
                api_call_id = %api_call_id,
                method = %method,
                path = %uri.path(),
                error = %err,
                "failed to read inbound API request body"
            );
            return (
                StatusCode::BAD_REQUEST,
                format!("failed to read request body: {err}"),
            )
                .into_response();
        }
    };

    // Inbound `Content-Encoding` decompression (codex-tui 0.145+ sends
    // `Content-Encoding: zstd` for Responses bodies). Decoded here, BEFORE the
    // log/body-summary/flow-store/turn-capture seams, so every downstream
    // consumer — including the `Json<...>` extractor on the rebuilt request —
    // sees plain JSON bytes. The `Content-Encoding` header is stripped and
    // `Content-Length` is corrected so the rebuilt request is self-consistent
    // (the upstream builds its own request from the parsed body, so forwarding
    // the inbound `Content-Encoding` would make the upstream double-decode).
    let body_bytes = match headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
    {
        Some(encoding) if !encoding.trim().eq_ignore_ascii_case("identity") => {
            match decode_content_encoding(&body_bytes, encoding) {
                Ok(decoded) => decoded,
                Err(err) => {
                    tracing::warn!(
                        api_call_id = %api_call_id,
                        method = %method,
                        path = %uri.path(),
                        content_encoding = %encoding,
                        error = %err,
                        "failed to decode inbound Content-Encoding"
                    );
                    return (
                        StatusCode::UNSUPPORTED_MEDIA_TYPE,
                        format!("failed to decode Content-Encoding: {err}"),
                    )
                        .into_response();
                }
            }
        }
        _ => body_bytes,
    };
    // `parts.headers` owns the headers of the rebuilt request; strip
    // `Content-Encoding` (already decoded) and fix `Content-Length` so the
    // axum extractors and any downstream reader see the true decoded size.
    parts.headers.remove(header::CONTENT_ENCODING);
    if let Ok(len) = HeaderValue::from_str(&body_bytes.len().to_string()) {
        parts.headers.insert(header::CONTENT_LENGTH, len);
    }

    // Parse once for the existing authorization/logging surfaces. Large
    // inference bodies move this CPU-bound JSON walk to the blocking pool and
    // return a right-sized redacted copy for durable ingress, so persistence
    // never adds another 10 MiB scan on the Tokio worker.
    let instrument = is_flow_capture_request(&method, uri.path());
    let persistence_requested = instrument && gateway.persistence_enabled();
    let mut persistence_inbound = if persistence_requested {
        let protocol = crate::flow_persistence::client_protocol_for_path(uri.path())
            .expect("instrumented paths have a protocol");
        Some(
            offload_persistence_inbound(
                body_bytes.clone(),
                protocol,
                gateway.persistence_keep_media(),
                Some((Arc::clone(gateway.harness_detector()), headers.clone())),
            )
            .await,
        )
    } else {
        None
    };
    // Authorize the client-facing name before any profile, alias, or backend
    // rewrite. Invalid JSON remains the protocol handler's responsibility.
    let body_is_json = persistence_inbound.as_ref().map_or_else(
        || serde_json::from_slice::<Value>(&body_bytes).is_ok(),
        |body| body.valid_json,
    );
    let requested_model = match &persistence_inbound {
        Some(body) => body.model.clone(),
        None => serde_json::from_slice::<Value>(&body_bytes)
            .ok()
            .and_then(|value| {
                value
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            }),
    };
    if method == axum::http::Method::POST
        && is_client_api_path_with_model(uri.path())
        && gateway
            .validate_operational_model(requested_model.as_deref().unwrap_or_default())
            .is_err()
    {
        return unknown_model_error(anthropic_surface);
    }
    if let Some(identity) = &client_identity
        && !identity.allowed_models().is_empty()
    {
        match requested_model.as_deref() {
            Some(model) if identity.allows_model(model) => {}
            Some(_) => {
                return client_auth_error(
                    uri.path(),
                    anthropic_surface,
                    StatusCode::FORBIDDEN,
                    "API key is not authorized for the requested model",
                );
            }
            None if method == axum::http::Method::POST => {
                return client_auth_error(
                    uri.path(),
                    anthropic_surface,
                    StatusCode::FORBIDDEN,
                    "a model is required for a model-scoped API key",
                );
            }
            None => {}
        }
    }

    // Durable persistence has its own gate on the same three inference routes.
    // It remains active with both the dashboard FlowStore and disk turn capture
    // disabled. Begin + inbound are queued only after authentication, bounded
    // body collection, and model authorization have all succeeded.
    let persistence_gate = instrument && body_is_json && gateway.persistence_enabled();
    // Harness + session facts for the live dashboard row; filled inside the
    // persistence block (they exist only when persistence ran) and consumed by
    // the flow-store open below.
    let mut session_facts = crate::dashboard_flow::FlowSessionFacts::default();
    let persistence_capture = if persistence_gate {
        let queue = gateway
            .persistence_queue()
            .expect("persistence_enabled implies a queue")
            .clone();
        let client_model = requested_model.as_deref().unwrap_or_default();
        let conversation_id = headers
            .get(gateway.conversation_id_header())
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty());
        // Client attribution is derived here, while the raw key is readable,
        // so the durable row carries it from ingress on.
        let attribution = crate::dashboard_flow::ClientAttribution::derive(
            &headers,
            dashboard_client_header().as_deref(),
        );
        let client_source = attribution
            .source
            .map(crate::flow_persistence::client_source_name);
        let virtual_key_id = client_identity
            .as_ref()
            .map(|identity| identity.key_id.as_str());
        let now_ms = epoch_millis();
        // Session-tree linkage: warm the in-memory index from durable rows
        // for a session we have not seen since startup, then link.
        let link = match persistence_inbound.as_ref() {
            Some(inbound) => match (&inbound.harness, &inbound.split) {
                (Some(identity), Some(split)) => {
                    warm_session_index(&gateway, identity, attribution.label.as_deref()).await;
                    let items = split
                        .items
                        .iter()
                        .map(crate::sessions::ItemFingerprint::from)
                        .collect::<Vec<_>>();
                    Some(
                        gateway.session_linker().link(crate::sessions::LinkInput {
                            api_call_id: &api_call_id,
                            identity,
                            client_label: attribution.label.as_deref(),
                            virtual_key_id,
                            user_id: client_identity
                                .as_ref()
                                .and_then(|identity| identity.owner_id.as_deref()),
                            items: &items,
                            now_ms: i64::try_from(now_ms).unwrap_or(i64::MAX),
                        }),
                    )
                }
                _ => None,
            },
            None => None,
        };
        if let Some(link) = &link {
            for row in &link.upserts {
                let _ = queue.try_session(row.clone());
            }
            // Feed the live active-session hub (no-op when the debug UI is
            // off). The stub mirrors what the durable begin row will carry.
            gateway.session_hub().record_begin(
                &link.upserts,
                &link.session_id,
                crate::session_hub::SessionRequestStub {
                    api_call_id: api_call_id.clone(),
                    client_model: requested_model.as_deref().unwrap_or_default().to_owned(),
                    created_at_ms: u64::try_from(now_ms).unwrap_or(u64::MAX),
                    status: "running".to_string(),
                    input_tokens: None,
                    output_tokens: None,
                    cached_tokens: None,
                    reasoning_tokens: None,
                    error: None,
                    terminal_reason: None,
                },
            );
        }
        session_facts = crate::dashboard_flow::FlowSessionFacts::from_parts(
            persistence_inbound
                .as_ref()
                .and_then(|inbound| inbound.harness.as_ref()),
            link.as_ref(),
        );
        let row = crate::flow_persistence::begin_request(
            crate::flow_persistence::BeginPersistenceInput {
                api_call_id: &api_call_id,
                conversation_id,
                virtual_key_id,
                client_protocol: crate::flow_persistence::client_protocol_for_path(uri.path())
                    .expect("instrumented paths have a protocol"),
                client_model,
                alias: gateway.operational_alias(client_model),
                created_at_ms: now_ms,
                harness: persistence_inbound
                    .as_ref()
                    .and_then(|inbound| inbound.harness.as_ref()),
                link: link.as_ref(),
                client_label: attribution.label.as_deref(),
                client_source,
                user_id: client_identity
                    .as_ref()
                    .and_then(|identity| identity.owner_id.as_deref()),
            },
        );
        let _ = queue.try_begin(row);
        let inbound = persistence_inbound
            .take()
            .expect("persistence gate has inbound capture");
        match inbound.split {
            Some(split) => {
                let _ = queue.try_body(crate::flow_persistence::body_write(
                    &api_call_id,
                    crate::flow_persistence::PayloadSection::InboundRequest,
                    epoch_millis(),
                    u64::try_from(body_bytes.len()).unwrap_or(u64::MAX),
                    split,
                    inbound.partial,
                    Some(&headers),
                ));
            }
            None => {
                let _ = queue.try_event(crate::flow_persistence::redacted_request_payload_event(
                    &api_call_id,
                    crate::flow_persistence::PayloadSection::InboundRequest,
                    epoch_millis(),
                    body_bytes.len(),
                    &inbound.redacted,
                    inbound.partial,
                    Some(&headers),
                ));
            }
        }
        let capture = crate::flow_persistence::PersistenceCapture::with_options(
            queue,
            &api_call_id,
            gateway.persistence_keep_media(),
        );
        parts.extensions.insert(Arc::clone(&capture));
        Some(capture)
    } else {
        None
    };
    if let Some(identity) = client_identity {
        parts.extensions.insert(identity);
    }

    // D7a R3 #1: for a dashboard auth endpoint (login/logout) NO body-derived
    // field may be logged — a `body_sha256` + `body_bytes` length on the login
    // body is an offline token-verification oracle. `body_log_fields` returns
    // `None` there so we emit only non-body metadata; every other path logs the
    // length, hex digest, and the redacted summary.
    let is_auth_path = is_dashboard_auth_path(uri.path());
    match body_log_fields(uri.path(), &body_bytes) {
        Some(fields) => tracing::info!(
            api_call_id = %api_call_id,
            method = %method,
            path = %uri.path(),
            query = uri.query().unwrap_or(""),
            content_type = %header_for_log(&headers, header::CONTENT_TYPE.as_str()),
            user_agent = %header_for_log(&headers, header::USER_AGENT.as_str()),
            anthropic_version = %header_for_log(&headers, "anthropic-version"),
            anthropic_beta = %header_for_log(&headers, "anthropic-beta"),
            openai_beta = %header_for_log(&headers, "openai-beta"),
            request_id = %header_for_log(&headers, "x-request-id"),
            authorization_present = headers.contains_key(header::AUTHORIZATION),
            x_api_key_present = headers.contains_key("x-api-key"),
            body_bytes = fields.bytes,
            body_sha256 = %fields.sha256,
            body_summary = %fields.summary,
            "inbound API request"
        ),
        // Auth endpoint: log only non-body metadata (no length, digest, summary).
        None => tracing::info!(
            api_call_id = %api_call_id,
            method = %method,
            path = %uri.path(),
            query = uri.query().unwrap_or(""),
            content_type = %header_for_log(&headers, header::CONTENT_TYPE.as_str()),
            user_agent = %header_for_log(&headers, header::USER_AGENT.as_str()),
            anthropic_version = %header_for_log(&headers, "anthropic-version"),
            anthropic_beta = %header_for_log(&headers, "anthropic-beta"),
            openai_beta = %header_for_log(&headers, "openai-beta"),
            request_id = %header_for_log(&headers, "x-request-id"),
            authorization_present = headers.contains_key(header::AUTHORIZATION),
            x_api_key_present = headers.contains_key("x-api-key"),
            "inbound API request"
        ),
    }
    // Never dump the auth-endpoint body (it carries the token, and even its
    // length/digest are an oracle — handled above).
    if !is_auth_path && body_bytes.len() <= API_LOG_PAYLOAD_DUMP_LIMIT_BYTES {
        tracing::info!(
            api_call_id = %api_call_id,
            method = %method,
            path = %uri.path(),
            body_payload = %payload_for_log(&body_bytes),
            "inbound API request payload"
        );
    }

    // Shared "instrument this request?" predicate (POST + whitelisted inference
    // path). Both the dashboard FlowStore gate and the F1 turn-capture gate hang
    // off it, so a HEAD/OPTIONS probe or a non-whitelisted path opens neither.
    // D1: dashboard FlowStore capture is gated on the debug UI (`flow_store()` is
    // `disabled()` off `--with-debug-ui`).
    let flow_gate = instrument && gateway.flow_store().is_enabled();
    // F1b (spec Design #1): turn capture has its OWN gate on the SAME paths but
    // keyed on `turn_capture().is_enabled()` INDEPENDENT of the flow store / debug
    // UI — so `api_call_id` reaches the engine and the artifact is written with the
    // dashboard OFF.
    let capture_gate = instrument && gateway.turn_capture().is_enabled();

    // The `api_call_id` extension the engine reads to link `response_id →
    // api_call_id` (D1) and to reach the per-turn capture state (F1c) is inserted
    // ONCE if EITHER gate wants it — never double-inserted when both fire.
    if flow_gate || capture_gate || persistence_gate {
        parts
            .extensions
            .insert(crate::dashboard_flow::ApiCallId(api_call_id.clone()));
    }

    // D1 (incl. R1 #1/#6): capture the inbound body + headers and open the record.
    // Secrets (auth headers, `api_key`, image URIs) are redacted INLINE by the
    // serializer/header redactor — none persist here. D3 L0: the RAII middleware
    // guard. `None` for disabled-store / non-whitelisted requests (zero overhead).
    // When `Some`, it is held across `next.run`: if the request never reaches the
    // engine (an extractor/`Json` rejection, a layer panic above the handler) the
    // record is still `OpenL0` at the guard's `Drop`, which CASes it to `Finalized`
    // + `Failed("unhandled")` — no orphan stuck `Open`. If the engine claimed it
    // (`ClaimedL1`), the L0 `Drop` is inert and L1 owns finalization.
    let _l0_guard = if flow_gate {
        let inbound_body = Some(crate::dashboard_flow::capture_body(&body_bytes));
        // Gap 04: derive the client attribution from the RAW headers BEFORE they are
        // redacted — this is the only point the raw API key is still readable, and
        // `derive` hashes it in-place (a one-way SHA-256 prefix becomes the label; the
        // raw key is dropped, never stored/logged). The optional configured caller-id
        // header NAME is read env-only (`LLMCONDUIT_DASHBOARD_CLIENT_HEADER`) so no
        // secret/identity config lands in the `Debug`/`Clone` persisted `Config`
        // struct — mirroring the dashboard auth env-only posture. The header name is
        // non-secret; only the api-key VALUE is, and it is never persisted.
        let client = crate::dashboard_flow::ClientAttribution::derive(
            &headers,
            dashboard_client_header().as_deref(),
        );
        let headers_redacted = crate::dashboard_flow::redact_headers(&headers);
        gateway.flow_store().open_with_session(
            api_call_id.clone(),
            method.to_string(),
            uri.path().to_string(),
            headers_redacted,
            inbound_body,
            client,
            session_facts.clone(),
        );
        gateway.flow_store().middleware_guard(&api_call_id)
    } else {
        None
    };

    // F1b: start the per-turn artifact and write the redacted inbound-request
    // section. `redacted_inbound_section` COPIES + redacts the body (secret keys +
    // image URIs, the SAME path `payload_for_log` uses — AGENTS.md line 137/144),
    // never retaining a slice of the 256 MiB buffer.
    let turn_capture_state = if capture_gate {
        // Finding 1: redact OFF the tokio worker for large bodies (spawn_blocking),
        // AWAITED here before `write_inbound_request` so the section is written +
        // closed before the finalize barrier can read it. `body_bytes.clone()` is a
        // cheap Arc-backed `Bytes` clone; the offload copies it into an OWNED `Vec` for
        // the blocking task (F1 — so nothing pins the 256 MiB backing) and this clone
        // is dropped before `body_bytes` is moved into the rebuilt request below.
        let (model_requested, inbound_section, inbound_partial) =
            offload_redacted_inbound_section(body_bytes.clone()).await;
        let state = gateway
            .turn_capture()
            .start(&api_call_id, model_requested, epoch_millis());
        if let Some(state) = &state {
            state.write_inbound_request(&inbound_section);
            if inbound_partial {
                // F3 (Fable-fix): the redaction offload could not capture the body (a
                // spawn_blocking join failure); mark the section partial so it never
                // reads as a complete inbound body (don't-lie-with-zeros).
                state.mark_inbound_request_degraded();
            }
        }
        state
    } else {
        None
    };

    // F1c: the turn-capture MIDDLEWARE backstop, held across `next.run`. If the
    // request NEVER reaches the engine (a `Json`/extractor rejection, a
    // `convert_request` error — so no engine `CaptureGuard` is ever built), the turn
    // is UNCLAIMED at this guard's `Drop`, which then finalizes the engine side
    // `failed`/`"unhandled"`. That closes the both-`done` barrier (the served tee
    // below always fires `served_done`), so the registry entry + `.work` dir are
    // evicted and a useful `status:"failed"` artifact is still written — no hang, no
    // leak. A turn that reached the engine is CLAIMED synchronously (before
    // `next.run` returns), so this backstop is inert for it. Mirrors the dashboard's
    // L0 `MiddlewareGuard`.
    let _capture_backstop = turn_capture_state
        .as_ref()
        .map(|state| crate::turn_capture::MiddlewareCaptureGuard::new(Arc::clone(state)));

    let request = Request::from_parts(parts, Body::from(body_bytes));
    let response = next.run(request).await;
    if let Some(capture) = &persistence_capture {
        // If an extractor/adapter rejected before the engine claimed the turn,
        // close the two absent upstream hops as partial and terminate the row.
        // Engine-owned turns make this an atomic no-op.
        capture.finish_unclaimed(response.status().as_u16());
    }
    // F1b served-body tee (spec Design #4): wrap the outbound response `Body` so
    // every served byte — streaming SSE, non-streaming JSON, or a handler error
    // body — is copied to the `served_response` section; its `Drop` marks
    // `served_done(partial)` when the stream did not reach a clean end (a client
    // disconnect drops the body mid-stream). One wrapper covers all served shapes.
    // Review #3: this wraps the WHOLE `next.run` result unconditionally, so every
    // POST-gate error response (an engine error, a `Reject`, any 4xx/5xx minted
    // after the gate inserted the `ApiCallId` extension) is teed too — only the
    // PRE-body-read 413/400 rejections above (no turn minted) are out of scope.
    let response = if turn_capture_state.is_some() || persistence_capture.is_some() {
        tee_served_body(response, turn_capture_state, persistence_capture)
    } else {
        response
    };
    // Per-request model-resolution audit: the handler tags the response with the
    // served model (and the requested model when it differs) via
    // `with_model_headers`; echo both here so every response record shows whether
    // a model fell back — un-throttled, unlike the engine WARN. `requested_model`
    // is empty when the requested model was served as-is.
    let served_model = header_for_log(response.headers(), "x-llmconduit-model").to_string();
    let requested_model = header_for_log(response.headers(), "x-llmconduit-requested").to_string();
    tracing::info!(
        api_call_id = %api_call_id,
        method = %method,
        path = %uri.path(),
        status = response.status().as_u16(),
        served_model = %served_model,
        requested_model = %requested_model,
        elapsed_ms = started_at.elapsed().as_millis(),
        "inbound API response prepared"
    );
    response
}

fn is_client_api_path_with_model(path: &str) -> bool {
    matches!(
        path,
        "/v1/responses"
            | "/v1/messages"
            | "/v1/messages/count_tokens"
            | "/v1/chat/completions"
            | "/v1/completions"
    )
}

async fn api_not_found() -> Response {
    (StatusCode::NOT_FOUND, "not found").into_response()
}

fn probe_response(allow: &str) -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::ALLOW,
        HeaderValue::try_from(allow).expect("valid header value"),
    );
    response
}

/// `HEAD`/`OPTIONS /v1/messages`: capability probe used by Anthropic SDKs.
#[utoipa::path(
    method(head, options),
    path = "/v1/messages",
    tag = "inference",
    operation_id = "probe_messages",
    responses((status = 204, description = "No body; the `Allow` header lists `POST, HEAD, OPTIONS`."))
)]
async fn probe_messages() -> Response {
    probe_response("POST, HEAD, OPTIONS")
}

/// This document: the OpenAPI 3.1 description of every route, generated from
/// the handler annotations at compile time.
#[utoipa::path(
    get,
    path = "/openapi.json",
    tag = "system",
    operation_id = "get_openapi",
    responses((status = 200, body = serde_json::Value, description = "OpenAPI 3.1 document."))
)]
async fn get_openapi() -> Response {
    (StatusCode::OK, Json(crate::openapi::document())).into_response()
}

/// Liveness probe. Always `200 {"status":"healthy"}` once the server accepts connections.
#[utoipa::path(
    get,
    path = "/health",
    tag = "system",
    operation_id = "get_health",
    responses((status = 200, body = crate::openapi::HealthResponse))
)]
async fn get_health() -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({"status": "healthy"})),
    )
        .into_response()
}

/// Root: the JSON status probe. When the dashboard is registered, a browser
/// (an `Accept` naming `text/html`) is sent to `/dashboard` instead; every
/// other client keeps the JSON so scripted probes of `/` are unchanged.
#[utoipa::path(
    get,
    path = "/",
    tag = "system",
    operation_id = "get_root",
    params(("accept" = Option<String>, Header, description = "When it names `text/html` and the dashboard is registered, the response is a redirect to `/dashboard`.")),
    responses(
        (status = 200, body = crate::openapi::RootStatus, description = "JSON status probe (API clients, or no dashboard)."),
        (status = 303, description = "Browser request while the dashboard is registered: `Location: /dashboard`.")
    )
)]
async fn get_root(headers: HeaderMap, dashboard: bool) -> Response {
    let wants_html = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.contains("text/html"));
    if dashboard && wants_html {
        return axum::response::Redirect::to("/dashboard").into_response();
    }
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response()
}

fn header_for_log(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(compact_for_log)
        .unwrap_or_default()
}

/// Env var naming the OPTIONAL non-secret request header that carries an explicit
/// caller id (e.g. `x-client-id`) for the dashboard's client attribution (gap 04).
const ENV_DASHBOARD_CLIENT_HEADER: &str = "LLMCONDUIT_DASHBOARD_CLIENT_HEADER";

/// The operator-configured caller-id header NAME, read ENV-ONLY (never from the
/// persisted `Config`, which is `Debug`/`Clone` — keeping attribution config out of
/// it mirrors the dashboard auth env-only posture; AGENTS.md). The header name itself
/// is non-secret — only the api-key VALUE is sensitive, and that is never persisted.
/// `None`/blank ⇒ the configured-header attribution source is simply skipped (the
/// derivation falls through to the User-Agent fallback). Trimmed; blank ⇒ `None`.
fn dashboard_client_header() -> Option<String> {
    std::env::var(ENV_DASHBOARD_CLIENT_HEADER)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn summarize_api_body(path: &str, body: &Bytes) -> String {
    if body.is_empty() {
        return "empty".to_string();
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(value) => summarize_json_api_body(path, &value),
        Err(err) => {
            // Redact image URIs from the raw preview before logging (round-4 #2):
            // a non-JSON body could still embed a `data:`/signed image URL.
            let preview = crate::redaction::redact_image_uris(&String::from_utf8_lossy(body));
            format!(
                "non_json parse_error={} preview={}",
                compact_for_log(&err.to_string()),
                compact_for_log(&preview)
            )
        }
    }
}

fn payload_for_log(body: &Bytes) -> String {
    match serde_json::from_slice::<Value>(body) {
        Ok(mut value) => {
            redact_payload_secrets(&mut value);
            // G4 round-4 #2: an inbound body under the dump limit would otherwise
            // log raw `data:` image bytes / signed `image_url`s. Strip image URIs
            // from every remaining string via the shared redactor BEFORE
            // serializing, so no logged surface carries request image content.
            crate::redaction::redact_image_uris_in_value(&mut value);
            serde_json::to_string(&value)
                .unwrap_or_else(|_| "<failed to serialize json>".to_string())
        }
        Err(_) => {
            // Non-JSON body: still strip image URIs from the raw text so a
            // `data:`/signed URL in a malformed/odd payload is not logged raw.
            crate::redaction::redact_image_uris(&String::from_utf8_lossy(body))
        }
    }
}

fn redact_payload_secrets(value: &mut Value) {
    // Single sensitive-key authority AND walker now live in `crate::redaction`
    // (D1 R1 #10; F1d extended the shared authority from just the key-list to the
    // walk itself, so `upstream.rs`'s turn-capture `upstream_request` section can
    // reuse the EXACT same redaction without duplicating the tree-walk here).
    // This name stays as the documented call-through (AGENTS.md line 137, the F1
    // spec) — only its body changed.
    crate::redaction::redact_payload_secrets_in_value(value);
}

/// F1b: the redacted bytes for the turn-capture `inbound_request` section, plus
/// the requested `model` (outcome metadata). Redaction MIRRORS `payload_for_log`
/// EXACTLY — secret keys via [`redact_payload_secrets`], image/data URIs via
/// [`crate::redaction::redact_image_uris_in_value`] — so the on-disk artifact is a
/// NEW logged surface that does NOT bypass `redact_payload_secrets` (AGENTS.md
/// line 137) and never carries raw image bytes. Parses/serializes a fresh owned
/// `Value`, so it COPIES out of `body` and never retains a slice of the 256 MiB
/// middleware buffer (AGENTS.md line 144).
fn redacted_inbound_section(body: &[u8]) -> (Option<String>, Vec<u8>) {
    match serde_json::from_slice::<Value>(body) {
        Ok(mut value) => {
            let model = value
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_string);
            redact_payload_secrets(&mut value);
            crate::redaction::redact_image_uris_in_value(&mut value);
            let bytes = serde_json::to_vec(&value)
                .unwrap_or_else(|_| b"<failed to serialize json>".to_vec());
            (model, bytes)
        }
        Err(_) => {
            // A malformed payload has no trustworthy key boundaries, so image-
            // only redaction could retain an unterminated `api_key` value. The
            // shared capped redactor emits a fixed marker for malformed/non-UTF8
            // input and retains none of the source bytes.
            (
                None,
                crate::redaction::capture_capped_redacted(
                    body,
                    crate::flow_persistence::EVENT_PAYLOAD_CAP_BYTES,
                    4 * 1024,
                ),
            )
        }
    }
}

/// Fable review (Finding 1): produce the redacted `inbound_request` bytes, moving the
/// CPU-bound parse+redact+re-serialize OFF the tokio worker for a LARGE body via
/// `spawn_blocking` (mirroring `upstream::UpstreamRequestLogger`). Small bodies
/// (`<= TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES`) stay inline — the blocking-pool hop
/// isn't worth it. Redaction is IDENTICAL on both paths (it is the SAME
/// [`redacted_inbound_section`]). The caller AWAITS this INLINE, before
/// `write_inbound_request` (append + close), so the section is fully written and
/// closed before the both-`done` finalize barrier can read it (no section race, no
/// hang). Returns `(model_requested, redacted_bytes, partial)`; `partial` is `true`
/// ONLY on a join failure (the body could not be captured -- the caller then marks
/// the section degraded rather than reporting a false "complete", don't-lie-with-zeros).
///
/// F1 (Fable-fix): the blocking task is handed an OWNED `Vec<u8>` copy of the body,
/// NOT the Arc-backed `Bytes` — moving a `Bytes` clone into `spawn_blocking` would PIN
/// the whole 256 MiB inbound middleware backing allocation for the task's lifetime,
/// and a DETACHED task (outer future cancelled) would keep it pinned (AGENTS.md line
/// 144 — no retained slice of that buffer). The owned right-sized copy is the intended
/// cost; the redacted output is likewise a fresh owned `Vec`.
async fn offload_redacted_inbound_section(body: Bytes) -> (Option<String>, Vec<u8>, bool) {
    if body.len() <= TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES {
        let (model, bytes) = redacted_inbound_section(&body);
        return (model, bytes, false);
    }
    // Copy to an OWNED, right-sized `Vec` and DROP the Arc-backed `Bytes` BEFORE the
    // blocking hop, so nothing pins the 256 MiB backing across the task (or after, on
    // cancellation when the task detaches).
    let owned: Vec<u8> = body.to_vec();
    drop(body);
    match tokio::task::spawn_blocking(move || redacted_inbound_section(&owned)).await {
        Ok((model, bytes)) => (model, bytes, false),
        // `redacted_inbound_section` is panic-free (serde failures fall back to a
        // marker), so a `JoinError` here means the runtime is shutting down. Record an
        // honest, NON-EMPTY marker AND signal `partial` so the section still closes
        // (never a hang) and never reads as a fabricated empty/complete body
        // (don't-lie-with-zeros; F3).
        Err(err) => {
            tracing::warn!(error = %err, "turn-capture: inbound redaction task failed");
            (
                None,
                b"<turn-capture: inbound redaction task failed>".to_vec(),
                true,
            )
        }
    }
}

/// Upper bound on the time a cold-session warm-up may spend reading durable
/// rows on the request path. On timeout the request links cold (a fresh node).
const SESSION_WARM_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);
/// Upper bound on descendants warmed for one session.
const SESSION_WARM_DESCENDANTS: usize = 64;

/// Seed the in-memory session index from the durable store for a session the
/// process has not seen since startup. Declared sessions are looked up by
/// `(harness, session_id)`; requests without a session id warm their
/// per-client bucket. Failures and timeouts are logged and ignored.
async fn warm_session_index(
    gateway: &Gateway,
    identity: &crate::harness::HarnessIdentity,
    client_label: Option<&str>,
) {
    let Some(store) = gateway.persistence_store() else {
        return;
    };
    let linker = gateway.session_linker();
    let external_id = identity.session_id.as_deref();
    if let Some(external_id) = external_id
        && linker.knows_declared(&identity.harness, external_id)
    {
        return;
    }
    if external_id.is_none() && linker.knows_anonymous(&identity.harness, client_label) {
        return;
    }
    let warm = async {
        let Some(root) = store
            .find_session(&identity.harness, external_id, client_label)
            .await?
        else {
            return Ok::<_, String>(());
        };
        let mut nodes = vec![root.clone()];
        nodes.extend(
            store
                .session_descendants(&root.id, SESSION_WARM_DESCENDANTS)
                .await?,
        );
        let mut seeded = Vec::with_capacity(nodes.len());
        for node in nodes {
            let head = store.chain_head(&node.id).await?;
            seeded.push((node, head));
        }
        linker.seed(seeded);
        Ok(())
    };
    match tokio::time::timeout(SESSION_WARM_TIMEOUT, warm).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(error = %error, harness = %identity.harness, "session warm-up read failed");
        }
        Err(_) => {
            tracing::warn!(harness = %identity.harness, "session warm-up timed out; linking cold");
        }
    }
}

/// The inbound capture handed from the middleware to the persistence seam.
/// `split` is the content-addressed body (present whenever the body parsed as a
/// JSON object); `redacted` is the bounded fallback marker used otherwise.
struct PersistenceInbound {
    valid_json: bool,
    model: Option<String>,
    split: Option<crate::content_store::SplitBody>,
    redacted: Vec<u8>,
    partial: bool,
    /// Harness/session identity, detected from headers + parsed body.
    harness: Option<crate::harness::HarnessIdentity>,
}

/// Parse the inbound body once, extract `model`, and split it into
/// content-addressed items. The split retains the whole body (that is the
/// point: full bodies are stored, deduplicated per item), so large bodies do
/// the parse + hash work on the blocking pool rather than a Tokio worker.
async fn offload_persistence_inbound(
    body: Bytes,
    protocol: &'static str,
    keep_media: bool,
    detection: Option<(Arc<crate::harness::HarnessDetector>, HeaderMap)>,
) -> PersistenceInbound {
    fn split_inbound(
        raw: &[u8],
        protocol: &str,
        keep_media: bool,
        detection: Option<&(Arc<crate::harness::HarnessDetector>, HeaderMap)>,
    ) -> PersistenceInbound {
        match serde_json::from_slice::<Value>(raw) {
            Ok(value) => {
                let model = value
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                // Detect while the parsed body is still whole (before splitting
                // moves the items out); headers are the middleware's clone.
                let harness =
                    detection.map(|(detector, headers)| detector.detect(headers, Some(&value)));
                match crate::content_store::split_value(protocol, value, keep_media) {
                    Ok(split) => PersistenceInbound {
                        valid_json: true,
                        model,
                        split: Some(split),
                        redacted: Vec::new(),
                        partial: false,
                        harness,
                    },
                    Err(error) => PersistenceInbound {
                        valid_json: true,
                        model,
                        split: None,
                        redacted: format!("[redacted: body not splittable: {error}]").into_bytes(),
                        partial: false,
                        harness,
                    },
                }
            }
            Err(_) => PersistenceInbound {
                valid_json: false,
                model: None,
                split: None,
                // Malformed input has no trustworthy key boundaries; the shared
                // redactor stores a fixed marker with none of the source bytes.
                redacted: crate::redaction::capture_capped_redacted(
                    raw,
                    crate::flow_persistence::EVENT_PAYLOAD_CAP_BYTES,
                    4 * 1024,
                ),
                partial: false,
                harness: None,
            },
        }
    }

    if body.len() <= TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES {
        return split_inbound(&body, protocol, keep_media, detection.as_ref());
    }
    let owned = body.to_vec();
    drop(body);
    match tokio::task::spawn_blocking(move || {
        split_inbound(&owned, protocol, keep_media, detection.as_ref())
    })
    .await
    {
        Ok(capture) => capture,
        Err(err) => {
            tracing::warn!(error = %err, "persistence inbound split task failed");
            PersistenceInbound {
                // A detached/panicked parse cannot establish valid JSON. Do not
                // open a durable row that the typed extractor may never claim.
                valid_json: false,
                model: None,
                split: None,
                redacted: b"[redacted: persistence inbound task failed]".to_vec(),
                partial: true,
                harness: None,
            }
        }
    }
}

/// Current wall-clock time as epoch milliseconds (the `started_ms` clock the
/// dashboard FlowStore's `started_ms` also uses, for a consistent turn timestamp).
fn epoch_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or(0)
}

fn summarize_json_api_body(path: &str, value: &Value) -> String {
    let Some(map) = value.as_object() else {
        return format!("json_type={}", json_type(value));
    };

    let mut parts = Vec::new();
    parts.push(format!(
        "keys={}",
        summarized_list(map.keys().cloned().collect(), 24)
    ));
    append_common_json_fields(&mut parts, map);

    if path.contains("/messages") {
        append_anthropic_json_summary(&mut parts, map);
    } else if path.contains("/responses") {
        append_responses_json_summary(&mut parts, map);
    } else if path.contains("/chat/completions") || path.ends_with("/completions") {
        append_chat_json_summary(&mut parts, map);
    } else {
        append_generic_json_summary(&mut parts, map);
    }

    parts.join(" ")
}

fn append_common_json_fields(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    for key in [
        "model",
        "stream",
        "max_tokens",
        "max_output_tokens",
        "max_completion_tokens",
        "store",
        "parallel_tool_calls",
        "temperature",
        "top_p",
    ] {
        append_scalar_field(parts, map, key);
    }
    append_typed_field(parts, map, "tool_choice");
    append_typed_field(parts, map, "thinking");
    append_typed_field(parts, map, "reasoning");
}

fn append_anthropic_json_summary(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    append_anthropic_system_summary(parts, map.get("system"));
    if let Some(messages) = map.get("messages").and_then(Value::as_array) {
        parts.push(format!("messages={}", messages.len()));
        append_anthropic_message_summary(parts, messages);
    }
    if let Some(tools) = map.get("tools").and_then(Value::as_array) {
        append_tool_summary(parts, "tools", tools);
    }
    append_metadata_summary(parts, map.get("metadata"));
    append_array_len(parts, map, "stop_sequences");
}

fn append_responses_json_summary(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    if let Some(instructions) = map.get("instructions").and_then(Value::as_str) {
        parts.push(format!(
            "instructions_chars={}",
            instructions.chars().count()
        ));
    }
    match map.get("input") {
        Some(Value::String(text)) => {
            parts.push("input=string".to_string());
            parts.push(format!("input_chars={}", text.chars().count()));
        }
        Some(Value::Array(items)) => {
            parts.push(format!("input_items={}", items.len()));
            append_responses_input_summary(parts, items);
        }
        Some(other) => {
            parts.push(format!("input_type={}", json_type(other)));
        }
        None => {}
    }
    if let Some(tools) = map.get("tools").and_then(Value::as_array) {
        append_tool_summary(parts, "tools", tools);
    }
    append_array_len(parts, map, "include");
    append_metadata_summary(parts, map.get("metadata"));
}

fn append_chat_json_summary(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    if let Some(messages) = map.get("messages").and_then(Value::as_array) {
        parts.push(format!("messages={}", messages.len()));
        append_chat_message_summary(parts, messages);
    }
    if let Some(tools) = map.get("tools").and_then(Value::as_array) {
        append_tool_summary(parts, "tools", tools);
    }
    append_typed_field(parts, map, "response_format");
    append_typed_field(parts, map, "stream_options");
}

fn append_generic_json_summary(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    if let Some(messages) = map.get("messages").and_then(Value::as_array) {
        parts.push(format!("messages={}", messages.len()));
        append_chat_message_summary(parts, messages);
    }
    match map.get("input") {
        Some(Value::String(text)) => {
            parts.push("input=string".to_string());
            parts.push(format!("input_chars={}", text.chars().count()));
        }
        Some(Value::Array(items)) => {
            parts.push(format!("input_items={}", items.len()));
        }
        _ => {}
    }
    if let Some(tools) = map.get("tools").and_then(Value::as_array) {
        append_tool_summary(parts, "tools", tools);
    }
}

fn append_anthropic_system_summary(parts: &mut Vec<String>, system: Option<&Value>) {
    match system {
        Some(Value::String(text)) => {
            parts.push("system=string".to_string());
            parts.push(format!("system_chars={}", text.chars().count()));
        }
        Some(Value::Array(blocks)) => {
            let mut text_chars = 0usize;
            let mut counts = BTreeMap::new();
            for block in blocks {
                let kind = typed_json_value(block);
                increment_count(&mut counts, kind);
                if let Some(text) = block.get("text").and_then(Value::as_str) {
                    text_chars += text.chars().count();
                }
            }
            parts.push(format!("system_blocks={}", blocks.len()));
            parts.push(format!("system_chars={text_chars}"));
            push_counts(parts, "system_block_types", &counts);
        }
        Some(other) => {
            parts.push(format!("system_type={}", json_type(other)));
        }
        None => {}
    }
}

fn append_anthropic_message_summary(parts: &mut Vec<String>, messages: &[Value]) {
    let mut roles = Vec::new();
    let mut content_counts = BTreeMap::new();
    let mut text_chars = 0usize;

    for message in messages {
        roles.push(
            message
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
        );
        if let Some(content) = message.get("content") {
            accumulate_anthropic_content(content, &mut text_chars, &mut content_counts);
        }
    }

    parts.push(format!("message_roles={}", summarized_list(roles, 16)));
    parts.push(format!("message_text_chars={text_chars}"));
    push_counts(parts, "message_content", &content_counts);
}

fn append_chat_message_summary(parts: &mut Vec<String>, messages: &[Value]) {
    let mut roles = Vec::new();
    let mut content_counts = BTreeMap::new();
    let mut text_chars = 0usize;
    let mut tool_calls = 0usize;

    for message in messages {
        roles.push(
            message
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
        );
        if let Some(content) = message.get("content") {
            accumulate_chat_content(content, &mut text_chars, &mut content_counts);
        }
        tool_calls += message
            .get("tool_calls")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
    }

    parts.push(format!("message_roles={}", summarized_list(roles, 16)));
    parts.push(format!("message_text_chars={text_chars}"));
    parts.push(format!("message_tool_calls={tool_calls}"));
    push_counts(parts, "message_content", &content_counts);
}

fn append_responses_input_summary(parts: &mut Vec<String>, items: &[Value]) {
    let mut roles = Vec::new();
    let mut item_counts = BTreeMap::new();
    let mut text_chars = 0usize;

    for item in items {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_else(|| {
            if item.get("role").is_some() {
                "message"
            } else {
                "unknown"
            }
        });
        increment_count(&mut item_counts, item_type.to_string());
        if let Some(role) = item.get("role").and_then(Value::as_str) {
            roles.push(role.to_string());
        }
        if let Some(content) = item.get("content") {
            accumulate_responses_content(content, &mut text_chars);
        }
    }

    parts.push(format!("input_roles={}", summarized_list(roles, 16)));
    parts.push(format!("input_text_chars={text_chars}"));
    push_counts(parts, "input_item_types", &item_counts);
}

fn accumulate_anthropic_content(
    content: &Value,
    text_chars: &mut usize,
    counts: &mut BTreeMap<String, usize>,
) {
    match content {
        Value::String(text) => {
            *text_chars += text.chars().count();
            increment_count(counts, "string".to_string());
        }
        Value::Array(blocks) => {
            for block in blocks {
                let kind = typed_json_value(block);
                increment_count(counts, kind.clone());
                match kind.as_str() {
                    "text" => {
                        if let Some(text) = block.get("text").and_then(Value::as_str) {
                            *text_chars += text.chars().count();
                        }
                    }
                    "thinking" => {
                        if let Some(text) = block.get("thinking").and_then(Value::as_str) {
                            *text_chars += text.chars().count();
                        }
                    }
                    "tool_result" => {
                        if let Some(nested) = block.get("content") {
                            accumulate_anthropic_content(nested, text_chars, counts);
                        }
                    }
                    _ => {}
                }
            }
        }
        other => {
            increment_count(counts, json_type(other).to_string());
        }
    }
}

fn accumulate_chat_content(
    content: &Value,
    text_chars: &mut usize,
    counts: &mut BTreeMap<String, usize>,
) {
    match content {
        Value::String(text) => {
            *text_chars += text.chars().count();
            increment_count(counts, "string".to_string());
        }
        Value::Array(parts) => {
            for part in parts {
                let kind = typed_json_value(part);
                increment_count(counts, kind.clone());
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    *text_chars += text.chars().count();
                }
            }
        }
        other => {
            increment_count(counts, json_type(other).to_string());
        }
    }
}

fn accumulate_responses_content(content: &Value, text_chars: &mut usize) {
    match content {
        Value::String(text) => {
            *text_chars += text.chars().count();
        }
        Value::Array(parts) => {
            for part in parts {
                for key in ["text", "input_text", "output_text"] {
                    if let Some(text) = part.get(key).and_then(Value::as_str) {
                        *text_chars += text.chars().count();
                    }
                }
            }
        }
        _ => {}
    }
}

fn append_tool_summary(parts: &mut Vec<String>, label: &str, tools: &[Value]) {
    let names = tools
        .iter()
        .filter_map(tool_name_for_summary)
        .collect::<Vec<_>>();
    parts.push(format!("{label}={}", tools.len()));
    if !names.is_empty() {
        parts.push(format!("{label}_names={}", summarized_list(names, 12)));
    }
}

fn tool_name_for_summary(tool: &Value) -> Option<String> {
    tool.get("name")
        .and_then(Value::as_str)
        .or_else(|| {
            tool.get("function")
                .and_then(|function| function.get("name"))
                .and_then(Value::as_str)
        })
        .map(ToString::to_string)
}

fn append_metadata_summary(parts: &mut Vec<String>, metadata: Option<&Value>) {
    if let Some(Value::Object(map)) = metadata {
        parts.push(format!(
            "metadata_keys={}",
            summarized_list(map.keys().cloned().collect(), 16)
        ));
    }
}

fn append_array_len(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>, key: &str) {
    if let Some(values) = map.get(key).and_then(Value::as_array) {
        parts.push(format!("{key}={}", values.len()));
    }
}

fn append_scalar_field(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>, key: &str) {
    if let Some(value) = map.get(key).and_then(scalar_for_log) {
        parts.push(format!("{key}={value}"));
    }
}

fn append_typed_field(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>, key: &str) {
    if let Some(value) = map.get(key) {
        parts.push(format!("{key}={}", typed_json_value(value)));
    }
}

fn typed_json_value(value: &Value) -> String {
    match value {
        Value::Object(map) => map
            .get("type")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .unwrap_or_else(|| "object".to_string()),
        Value::Array(_) => "array".to_string(),
        Value::String(text) => compact_for_log(text),
        other => json_type(other).to_string(),
    }
}

fn scalar_for_log(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(compact_for_log(text)),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        Value::Null => Some("null".to_string()),
        Value::Array(_) | Value::Object(_) => None,
    }
}

fn push_counts(parts: &mut Vec<String>, label: &str, counts: &BTreeMap<String, usize>) {
    if counts.is_empty() {
        return;
    }
    let values = counts
        .iter()
        .map(|(key, count)| format!("{key}:{count}"))
        .collect::<Vec<_>>();
    parts.push(format!("{label}={}", summarized_list(values, 16)));
}

fn increment_count(counts: &mut BTreeMap<String, usize>, key: String) {
    *counts.entry(key).or_default() += 1;
}

fn summarized_list(mut values: Vec<String>, max: usize) -> String {
    let total = values.len();
    values.truncate(max);
    if total > max {
        values.push(format!("+{}", total - max));
    }
    format!("[{}]", values.join(","))
}

fn compact_for_log(value: &str) -> String {
    let mut compact = String::new();
    for ch in value.chars().take(API_LOG_PREVIEW_CHARS) {
        if ch.is_control() {
            compact.push(' ');
        } else {
            compact.push(ch);
        }
    }
    if value.chars().count() > API_LOG_PREVIEW_CHARS {
        compact.push_str("...");
    }
    compact
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// OpenAI Responses API. The body is the standard `POST /v1/responses` request
/// (`model`, `input` as a string or item list, `instructions`, `tools`,
/// `tool_choice`, `reasoning`, `stream`, `store`, `prompt_cache_key`,
/// `previous_response_id`, `temperature`, `top_p`, …); `model` is resolved
/// through the configured aliases before the request reaches an upstream.
/// `x-llm-harness`, `x-llm-session-id`, `x-llm-parent-session-id` and
/// `x-llm-session-kind` headers attribute the call to a harness session.
#[utoipa::path(
    post,
    path = "/v1/responses",
    tag = "inference",
    operation_id = "post_responses",
    request_body(content = serde_json::Value, content_type = "application/json", description = "OpenAI Responses request. See https://platform.openai.com/docs/api-reference/responses/create."),
    responses(
        (status = 200, description = "`application/json`: Response object (non-streaming). `text/event-stream`: Server-sent `response.*` events when `stream: true`.", content((serde_json::Value = "application/json"), (String = "text/event-stream"))),
        (status = 400, body = crate::openapi::ApiError, description = "Malformed body."),
        (status = 401, body = crate::openapi::ApiError, description = "Missing or invalid API key (when client auth is required)."),
        (status = 403, body = crate::openapi::ApiError, description = "The key may not use the requested model."),
        (status = 404, body = crate::openapi::ApiError, description = "Unknown model under `unknown_model_policy: reject`."),
        (status = 413, body = crate::openapi::ApiError, description = "Body larger than `max_request_body_bytes`."),
        (status = 502, body = crate::openapi::ApiError, description = "Every backend in the alias chain failed.")
    ),
    security(("bearer" = []), ("api_key" = []))
)]
async fn post_responses(
    State(gateway): State<Arc<Gateway>>,
    auth: Option<Extension<crate::authz::AuthContext>>,
    api_call_id: Option<axum::Extension<crate::dashboard_flow::ApiCallId>>,
    persistence: Option<axum::Extension<Arc<crate::flow_persistence::PersistenceCapture>>>,
    Json(request): Json<ResponsesRequest>,
) -> AppResult<Response> {
    let requested = request.model.clone();
    let served = gateway.resolve_request_model(&request.model).await.0;
    let authorization = authorize_inference(
        auth.as_ref().map(|value| &value.0),
        crate::upstream::InferenceEndpoint::Responses,
        &requested,
    )?;
    let auth = auth.map(|value| value.0);
    let lease = acquire_inference_session(&gateway, auth.as_ref()).await?;
    let wants_stream = request.stream;
    let stream = gateway
        .clone()
        .stream_responses_with_capture_authorized_context(
            request,
            api_call_id.map(|extension| extension.0.0),
            persistence.map(|extension| extension.0),
            authorization,
            crate::upstream::InferenceEndpoint::Responses,
            auth,
        )
        .await?;
    let response = if wants_stream {
        stream_responses_response(stream, lease)
    } else {
        collect_responses_response(stream).await?
    };
    Ok(with_model_headers(response, &requested, &served))
}

/// `GET /v1/responses` — the OpenAI Responses WebSockets beta
/// (`openai-beta: responses_websockets=2026-02-06`). codex-tui 0.145+ attempts a
/// WS upgrade here FIRST; on any non-101 it falls back to HTTPS POST (above), so
/// the existing SSE path remains the fallback.
///
/// Protocol (best-effort, reverse-engineered from the codex binary — the beta is
/// undocumented): after the 101, the client sends ONE frame carrying the
/// `ResponsesRequest` JSON — either a text frame (plain JSON) or a binary frame
/// (zstd-compressed JSON, mirroring codex's `enable_request_compression` HTTP
/// path). The server then streams `response.*` events as text frames, each a
/// single JSON object whose `type` field names the event (identical to the SSE
/// `data:` payload of the HTTP path), terminating on `response.completed`/
/// `failed`/`incomplete`/`error` and closing the socket.
///
/// The stream reuses the SAME engine path as `post_responses`
/// (`stream_responses_with_api_call_id`), so model resolution, failover, and the
/// tool loop are unchanged. WS turns are NOT instrumented in the dashboard
/// FlowStore/turn-capture (those gate on POST); the api_call_id passed here is
/// `None` — extend by minting a flow record off the GET if dashboard visibility
/// for WS turns is needed.
/// WebSocket transport for the Responses API (used by Codex): upgrade, then send
/// `response.create` frames and receive the same `response.*` events as the SSE stream.
#[utoipa::path(
    get,
    path = "/v1/responses",
    tag = "inference",
    operation_id = "get_responses_ws",
    responses(
        (status = 101, description = "Switching to the Responses WebSocket protocol."),
        (status = 401, body = crate::openapi::ApiError, description = "Missing or invalid API key (when client auth is required)."),
        (status = 426, description = "Plain GET without `Upgrade: websocket`; `Allow: POST, GET`.")
    ),
    security(("bearer" = []), ("api_key" = []))
)]
async fn get_responses(
    State(gateway): State<Arc<Gateway>>,
    auth: Option<Extension<crate::authz::AuthContext>>,
    upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    match upgrade {
        Ok(upgrade) => upgrade
            .on_upgrade(move |socket| {
                responses_ws_serve(socket, gateway, auth.map(|value| value.0))
            })
            .into_response(),
        Err(_) => {
            // Plain GET without an `Upgrade: websocket` header. 426 tells the
            // client it must upgrade (or use POST); codex's WS attempt always
            // sends the upgrade header, so it takes the 101 branch.
            let mut resp = StatusCode::UPGRADE_REQUIRED.into_response();
            resp.headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("POST, GET"));
            resp
        }
    }
}

/// zstd magic number (`28 b5 2f fd`) — codex's compressed WS request frames
/// start with this. Used to decide whether a binary frame is zstd-compressed
/// JSON or raw JSON bytes.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

/// The Responses-WS socket loop. See [`get_responses`] for the protocol rationale.
async fn responses_ws_serve(
    socket: WebSocket,
    gateway: Arc<Gateway>,
    auth: Option<crate::authz::AuthContext>,
) {
    // `split` so the inbound `recv` and outbound `send` can be raced in the same
    // `select!` without a double-`&mut` borrow conflict (the dashboard/debug WS
    // loops use the same pattern).
    let (mut sink, mut ws_rx) = socket.split();

    // 1. Read the request frame. Respond to Ping; ignore Pong; bail on
    //    Close/EOF. codex sends a single Text (JSON) or Binary (zstd JSON).
    let request_bytes: Bytes = loop {
        match ws_rx.next().await {
            Some(Ok(Message::Text(t))) => break Bytes::copy_from_slice(t.as_bytes()),
            Some(Ok(Message::Binary(b))) => {
                break if b.starts_with(&ZSTD_MAGIC) {
                    match zstd::decode_all(&b[..]) {
                        Ok(decoded) => Bytes::from(decoded),
                        // Not actually zstd — let JSON parsing produce the error.
                        Err(_) => b,
                    }
                } else {
                    b
                };
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = sink.send(Message::Pong(p)).await;
            }
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
        }
    };

    // 2. Parse + force streaming (WS is inherently streaming; a non-stream
    //    `ResponsesRequest` would make the engine emit only the terminal
    //    `response.completed`, which is legal but not what a WS client expects).
    let mut request: ResponsesRequest = match serde_json::from_slice(&request_bytes) {
        Ok(r) => r,
        Err(err) => {
            let _ = send_responses_ws_error(
                &mut sink,
                "invalid_request",
                &format!("invalid request body: {err}"),
            )
            .await;
            let _ = sink.send(Message::Close(None)).await;
            return;
        }
    };
    request.stream = true;

    let requested = request.model.clone();
    let authorization = match authorize_inference(
        auth.as_ref(),
        crate::upstream::InferenceEndpoint::Responses,
        &requested,
    ) {
        Ok(authorization) => authorization,
        Err(_) => {
            let _ = send_responses_ws_error(
                &mut sink,
                "permission_denied",
                "the API key is not authorized for the requested model",
            )
            .await;
            let _ = sink.send(Message::Close(None)).await;
            return;
        }
    };

    let _lease = match acquire_inference_session(&gateway, auth.as_ref()).await {
        Ok(lease) => lease,
        Err(err) => {
            let _ = send_responses_ws_error(&mut sink, "permission_denied", &err.to_string()).await;
            let _ = sink.send(Message::Close(None)).await;
            return;
        }
    };

    // 3. Run the turn through the SAME engine path as the HTTP POST.
    let event_stream = match gateway
        .clone()
        .stream_responses_authorized_with_context(
            request,
            None,
            authorization,
            crate::upstream::InferenceEndpoint::Responses,
            auth,
        )
        .await
    {
        Ok(s) => s,
        Err(err) => {
            let _ = send_responses_ws_error(&mut sink, "internal_error", &err.to_string()).await;
            let _ = sink.send(Message::Close(None)).await;
            return;
        }
    };

    // 4. Forward each `SseEvent` as a WS text frame (the JSON `data` object,
    //    which already carries `"type": "<event>"`). Race the engine stream
    //    against the client side so a client hang-up / Close cancels the turn
    //    promptly — mirrors the engine's `tx.closed()` cancellation invariant.
    let mut events = std::pin::pin!(event_stream);
    loop {
        tokio::select! {
            ev = events.next() => {
                match ev {
                    Some(event) => {
                        let terminal = matches!(
                            event.event.as_str(),
                            "response.completed"
                                | "response.failed"
                                | "response.incomplete"
                                | "response.error"
                        );
                        let payload = responses_wire_event_data(&event);
                        if sink.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                        if terminal {
                            let _ = sink.send(Message::Close(None)).await;
                            break;
                        }
                    }
                    None => {
                        // Engine stream ended without a terminal event (shouldn't
                        // happen for a well-formed turn, but don't hang).
                        let _ = sink.send(Message::Close(None)).await;
                        break;
                    }
                }
            }
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Ping(p))) => {
                        let _ = sink.send(Message::Pong(p)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    // A mid-turn Text/Binary frame from the client is unexpected;
                    // ignore it rather than tearing down a working turn.
                    Some(Ok(_)) => {}
                }
            }
        }
    }
}

/// Emit a `response.failed` text frame on the WS socket. codex treats
/// `response.failed` as terminal and stops, so a parse/engine-setup error
/// surfaces as a normal failure rather than a silent socket close.
async fn send_responses_ws_error(
    sink: &mut futures::stream::SplitSink<WebSocket, Message>,
    code: &str,
    message: &str,
) -> bool {
    let payload = serde_json::json!({
        "type": "response.failed",
        "response": {
            "error": { "code": code, "message": message }
        }
    });
    let Ok(text) = serde_json::to_string(&payload) else {
        return true;
    };
    sink.send(Message::Text(text.into())).await.is_ok()
}

/// Anthropic Messages API. The body is the standard `POST /v1/messages` request
/// (`model`, `max_tokens`, `system`, `messages`, `tools`, `tool_choice`,
/// `stream`, `thinking`, `metadata`, …); it is translated to the upstream's
/// native API when the resolved backend is not Anthropic-compatible.
#[utoipa::path(
    post,
    path = "/v1/messages",
    tag = "inference",
    operation_id = "post_messages",
    request_body(content = serde_json::Value, content_type = "application/json", description = "Anthropic Messages request. See https://docs.anthropic.com/en/api/messages."),
    responses(
        (status = 200, description = "`application/json`: Message object (non-streaming). `text/event-stream`: Anthropic `message_start`…`message_stop` events when `stream: true`.", content((serde_json::Value = "application/json"), (String = "text/event-stream"))),
        (status = 400, body = crate::openapi::AnthropicError, description = "Malformed body (`invalid_request_error`)."),
        (status = 401, body = crate::openapi::AnthropicError, description = "Missing or invalid API key (`authentication_error`)."),
        (status = 403, body = crate::openapi::AnthropicError, description = "The key may not use the requested model (`permission_error`)."),
        (status = 404, body = crate::openapi::AnthropicError, description = "Unknown model under `unknown_model_policy: reject`."),
        (status = 413, body = crate::openapi::AnthropicError, description = "Body larger than `max_request_body_bytes`."),
        (status = 502, body = crate::openapi::AnthropicError, description = "Every backend in the alias chain failed.")
    ),
    security(("bearer" = []), ("api_key" = []))
)]
async fn post_messages(
    State(gateway): State<Arc<Gateway>>,
    auth: Option<Extension<crate::authz::AuthContext>>,
    api_call_id: Option<axum::Extension<crate::dashboard_flow::ApiCallId>>,
    persistence: Option<axum::Extension<Arc<crate::flow_persistence::PersistenceCapture>>>,
    Json(request): Json<AnthropicRequest>,
) -> Response {
    let api_call_id = api_call_id.map(|extension| extension.0.0);
    match handle_post_messages(
        gateway,
        request,
        api_call_id,
        persistence.map(|extension| extension.0),
        auth.map(|value| value.0),
    )
    .await
    {
        Ok(response) => response,
        Err(err) => anthropic_error_response(err),
    }
}

/// Anthropic token counting for a Messages request body.
#[utoipa::path(
    post,
    path = "/v1/messages/count_tokens",
    tag = "inference",
    operation_id = "post_count_tokens",
    request_body(content = serde_json::Value, content_type = "application/json", description = "Anthropic Messages request (same shape as `/v1/messages`)."),
    responses(
        (status = 200, body = crate::openapi::CountTokensResponse),
        (status = 400, body = crate::openapi::AnthropicError, description = "Malformed body."),
        (status = 401, body = crate::openapi::AnthropicError, description = "Missing or invalid API key.")
    ),
    security(("bearer" = []), ("api_key" = []))
)]
async fn post_count_tokens(
    State(gateway): State<Arc<Gateway>>,
    auth: Option<Extension<crate::authz::AuthContext>>,
    body: Bytes,
) -> Response {
    let request: AnthropicRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(err) => {
            return anthropic_error_response(AppError::bad_request(format!(
                "invalid request body: {err}"
            )));
        }
    };
    match handle_count_tokens(gateway, request, auth.map(|value| value.0)).await {
        Ok(response) => response,
        Err(err) => anthropic_error_response(err),
    }
}

async fn handle_count_tokens(
    gateway: Arc<Gateway>,
    request: AnthropicRequest,
    auth: Option<crate::authz::AuthContext>,
) -> AppResult<Response> {
    use crate::engine::TokenizeCapability;

    if gateway.tokenize_capability() == TokenizeCapability::Unsupported {
        return Err(AppError::not_found("upstream does not support /tokenize"));
    }

    let original_model = request.model.clone();
    let responses_request = anthropic_to_responses::convert_request(request)?;
    let resolved_model = gateway.resolve_request_model(&original_model).await.0;
    let authorization = authorize_inference(
        auth.as_ref(),
        crate::upstream::InferenceEndpoint::CountTokens,
        &original_model,
    )?;
    let responses_request = gateway.apply_system_prompt_prefix(responses_request, &resolved_model);
    let roles = gateway.roles_for_request(&original_model, &resolved_model);
    let lowered = responses_to_chat::lower_request_with_image_agent_and_roles(
        &responses_request,
        Vec::new(),
        false,
        roles,
    )?;
    let client_chat_template_kwargs = responses_request
        .extra_body
        .get("chat_template_kwargs")
        .and_then(Value::as_object)
        .cloned();
    let thinking_override = responses_request.thinking;
    let backend = BackendChatRequest::new(
        ChatCompletionRequest {
            model: resolved_model,
            messages: lowered.messages,
            stream: false,
            tools: (!lowered.tools.is_empty()).then_some(lowered.tools),
            tool_choice: Some(responses_request.tool_choice),
            parallel_tool_calls: responses_request.parallel_tool_calls,
            reasoning_effort: lowered.reasoning_effort,
            response_format: lowered.response_format,
            stream_options: None,
            temperature: responses_request.temperature,
            top_p: responses_request.top_p,
            max_output_tokens: None,
            frequency_penalty: lowered.frequency_penalty,
            presence_penalty: lowered.presence_penalty,
            stop: normalize_stop(responses_request.stop)?,
            extra_body: responses_request.extra_body,
        },
        client_chat_template_kwargs,
        None,
        None,
    )
    .with_thinking_override(thinking_override)
    .with_authorization(
        authorization,
        crate::upstream::InferenceEndpoint::CountTokens,
    );

    let _lease = acquire_inference_session(&gateway, auth.as_ref()).await?;

    match gateway.upstream_client().count_tokens(&backend).await {
        Ok(Some(count)) => {
            gateway.set_tokenize_capability(TokenizeCapability::Supported);
            Ok((
                StatusCode::OK,
                Json(serde_json::json!({ "input_tokens": count })),
            )
                .into_response())
        }
        Ok(None) | Err(_) => {
            gateway.set_tokenize_capability(TokenizeCapability::Unsupported);
            Err(AppError::not_found("upstream does not support /tokenize"))
        }
    }
}

/// OpenAI Chat Completions API. The body is the standard request (`model`,
/// `messages`, `tools`, `tool_choice`, `stream`, `stream_options`,
/// `max_tokens`/`max_completion_tokens`, `temperature`, `top_p`, `stop`,
/// `response_format`, `reasoning_effort`, …); unknown fields are forwarded to
/// the upstream unchanged. `model` is resolved through the configured aliases.
#[utoipa::path(
    post,
    path = "/v1/chat/completions",
    tag = "inference",
    operation_id = "post_chat_completions",
    request_body(content = serde_json::Value, content_type = "application/json", description = "OpenAI Chat Completions request. See https://platform.openai.com/docs/api-reference/chat/create."),
    responses(
        (status = 200, description = "`application/json`: Chat completion object (non-streaming). `text/event-stream`: `data: {chunk}` events ending in `data: [DONE]` when `stream: true`.", content((serde_json::Value = "application/json"), (String = "text/event-stream"))),
        (status = 400, body = crate::openapi::ApiError, description = "Malformed body."),
        (status = 401, body = crate::openapi::ApiError, description = "Missing or invalid API key (when client auth is required)."),
        (status = 403, body = crate::openapi::ApiError, description = "The key may not use the requested model."),
        (status = 404, body = crate::openapi::ApiError, description = "Unknown model under `unknown_model_policy: reject`."),
        (status = 413, body = crate::openapi::ApiError, description = "Body larger than `max_request_body_bytes`."),
        (status = 502, body = crate::openapi::ApiError, description = "Every backend in the alias chain failed.")
    ),
    security(("bearer" = []), ("api_key" = []))
)]
async fn post_chat_completions(
    State(gateway): State<Arc<Gateway>>,
    auth: Option<Extension<crate::authz::AuthContext>>,
    api_call_id: Option<axum::Extension<crate::dashboard_flow::ApiCallId>>,
    persistence: Option<axum::Extension<Arc<crate::flow_persistence::PersistenceCapture>>>,
    Json(request): Json<ChatCompletionRequest>,
) -> AppResult<Response> {
    let requested = request.model.clone();
    let model = gateway.resolve_request_model(&request.model).await.0;
    let authorization = authorize_inference(
        auth.as_ref().map(|value| &value.0),
        crate::upstream::InferenceEndpoint::ChatCompletions,
        &requested,
    )?;
    let auth = auth.map(|value| value.0);
    let lease = acquire_inference_session(&gateway, auth.as_ref()).await?;
    let wants_stream = request.stream;
    let include_usage = request
        .stream_options
        .as_ref()
        .is_some_and(|options| options.include_usage);
    let responses_request = chat_completions::convert_request(request)?;
    let stream = gateway
        .clone()
        .stream_responses_with_capture_authorized_context(
            responses_request,
            api_call_id.map(|extension| extension.0.0),
            persistence.map(|extension| extension.0),
            authorization,
            crate::upstream::InferenceEndpoint::ChatCompletions,
            auth,
        )
        .await?;

    let response = if wants_stream {
        stream_chat_completions_response(model.clone(), include_usage, stream, lease)
    } else {
        collect_chat_completions_response(model.clone(), stream).await?
    };
    Ok(with_model_headers(response, &requested, &model))
}

/// Legacy OpenAI Completions API, proxied byte-for-byte to the upstream; the
/// upstream's status and body are returned as-is.
#[utoipa::path(
    post,
    path = "/v1/completions",
    tag = "inference",
    operation_id = "post_completions",
    request_body(content = serde_json::Value, content_type = "application/json", description = "OpenAI Completions request (`model`, `prompt`, `max_tokens`, `stream`, …)."),
    responses(
        (status = 200, description = "`application/json`: Completion object, as returned by the upstream. `text/event-stream`: Upstream SSE stream when `stream: true`.", content((serde_json::Value = "application/json"), (String = "text/event-stream"))),
        (status = 401, body = crate::openapi::ApiError, description = "Missing or invalid API key (when client auth is required)."),
        (status = 502, body = crate::openapi::ApiError, description = "The upstream could not be reached.")
    ),
    security(("bearer" = []), ("api_key" = []))
)]
async fn post_completions(
    State(gateway): State<Arc<Gateway>>,
    auth: Option<Extension<crate::authz::AuthContext>>,
    headers: HeaderMap,
    body: Bytes,
) -> AppResult<Response> {
    let request_metadata = auth
        .as_ref()
        .map(|_| {
            let value = serde_json::from_slice::<Value>(&body)
                .map_err(|_| AppError::bad_request("request body must be valid JSON"))?;
            let model = value
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| AppError::bad_request("request body must include a model"))?;
            let streaming = value
                .get("stream")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Ok::<_, AppError>((model, streaming))
        })
        .transpose()?;
    let authorization =
        if let (Some(context), Some((model, _))) = (auth.as_ref(), request_metadata.as_ref()) {
            authorize_inference(
                Some(&context.0),
                crate::upstream::InferenceEndpoint::Completions,
                model,
            )?
        } else {
            crate::upstream::AuthorizationScope::unrestricted()
        };
    let auth = auth.map(|value| value.0);
    let lease = acquire_inference_session(&gateway, auth.as_ref()).await?;
    let response = gateway
        .upstream_client()
        .proxy_completions(
            crate::upstream::ProxyCompletionsRequest::new(headers, body)
                .with_authorization(authorization),
        )
        .await?;
    let accounting = request_metadata.map(|(model, streaming)| {
        RawCompletionAccounting::new(
            Arc::clone(&gateway),
            auth,
            model,
            streaming,
            response.status().is_success(),
        )
    });
    Ok(proxy_upstream_response(response, lease, accounting))
}

async fn handle_post_messages(
    gateway: Arc<Gateway>,
    request: AnthropicRequest,
    api_call_id: Option<String>,
    persistence: Option<Arc<crate::flow_persistence::PersistenceCapture>>,
    auth: Option<crate::authz::AuthContext>,
) -> AppResult<Response> {
    let requested = request.model.clone();
    let model = gateway.resolve_request_model(&request.model).await.0;
    let authorization = authorize_inference(
        auth.as_ref(),
        crate::upstream::InferenceEndpoint::Messages,
        &requested,
    )?;
    let lease = acquire_inference_session(&gateway, auth.as_ref()).await?;
    let wants_stream = request.stream;
    let suppress_reasoning = !matches!(
        request.thinking.as_ref(),
        Some(AnthropicThinking::Enabled { .. } | AnthropicThinking::Adaptive { .. })
    );
    let responses_request = anthropic_to_responses::convert_request(request)?;
    let stream = gateway
        .clone()
        .stream_responses_with_capture_authorized_context(
            responses_request,
            api_call_id,
            persistence,
            authorization,
            crate::upstream::InferenceEndpoint::Messages,
            auth,
        )
        .await?;

    let response = if wants_stream {
        stream_anthropic_response(model.clone(), suppress_reasoning, stream, lease)?
    } else {
        collect_anthropic_response(model.clone(), suppress_reasoning, stream).await?
    };
    Ok(with_model_headers(response, &requested, &model))
}

/// Tag a response with the model that actually served it, so a model mismatch
/// (requested model not served → fell back to the loaded model) is visible
/// PER-REQUEST to anyone inspecting the response (`curl -v`, a proxy, or
/// response logging) without tailing the throttled engine WARN. The
/// `x-llmconduit-requested` header is added ONLY when the requested model
/// differs from the served one (the mismatch signal); an exact/canonical match
/// omits it to keep the common case quiet. The `log_api_call` middleware echoes
/// both headers into the per-request "response prepared" log line.
fn with_model_headers(mut response: Response, requested: &str, served: &str) -> Response {
    let headers = response.headers_mut();
    if !served.is_empty()
        && let Ok(value) = HeaderValue::from_str(served)
    {
        headers.insert(HeaderName::from_static("x-llmconduit-model"), value);
    }
    if !requested.is_empty()
        && !requested.eq_ignore_ascii_case(served)
        && let Ok(value) = HeaderValue::from_str(requested)
    {
        headers.insert(HeaderName::from_static("x-llmconduit-requested"), value);
    }
    response
}

/// F1b served-response tee: an `http_body::Body` that mirrors `inner` frame for
/// frame, COPYING each DATA frame's bytes into the turn-capture `served_response`
/// section (never retaining the frame's backing allocation) and passing every
/// frame through UNCHANGED — so SSE framing, keep-alive comments, and trailers are
/// preserved byte-for-byte. Capture is BACK-PRESSURED (F1b review #1): before
/// pulling each frame it reserves a slot in the section's BOUNDED writer channel
/// and, when the writer is behind, returns `Poll::Pending` — throttling the served
/// stream to disk pace rather than buffering the whole body in RAM (bounded
/// memory, AGENTS.md). Its `Drop` reports `served_done`: `partial` unless the
/// stream reached a clean end (a `Ready(None)` poll, or delivering its full
/// promised length). A client disconnect drops the body mid-stream → partial.
/// F1b review r2: if `served_sink` ever closes early (writer gone mid-stream —
/// see the field doc), that alone permanently marks the section `partial` via
/// `mark_served_degraded`, REGARDLESS of how `Drop`'s own clean-end check comes
/// out — a truncated capture must never be reported complete just because the
/// client-facing stream itself still ended cleanly.
struct TeeBody {
    inner: Body,
    state: Option<Arc<crate::turn_capture::TurnCaptureState>>,
    persistence: Option<Arc<crate::flow_persistence::PersistenceCapture>>,
    /// Back-pressured sink into the `served_response` section's BOUNDED writer
    /// channel (F1b review #1). `None` once the writer is gone (a section write
    /// error closed the channel) — capture then stops but the served stream
    /// continues byte-for-byte (a diagnostic failure must never break the served
    /// bytes). The transition to `None` also permanently marks the section
    /// `partial` (F1b review r2) — see `poll_frame`.
    served_sink: Option<crate::turn_capture::ServedSink>,
    /// Set once `inner` yields `Poll::Ready(None)` (clean end-of-stream).
    clean_eos: bool,
    /// Total DATA bytes forwarded so far (for the exact-length clean check).
    forwarded: u64,
    /// The inner body's exact promised length at construction (a non-streaming JSON
    /// body has `Some`; a chunked SSE stream has `None`). When `Some`, delivering
    /// that many bytes is a clean end even if hyper stops polling the
    /// Content-Length body before it yields `Ready(None)`.
    exact_len: Option<u64>,
}

impl http_body::Body for TeeBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        // `axum::body::Body` is `Unpin`, so the fields can be reached by `&mut`.
        let this = self.get_mut();

        // Bounded-memory back-pressure (F1b review #1): reserve a slot in the
        // served-section writer channel BEFORE pulling the next frame. If the disk
        // writer is behind, the BOUNDED channel is full → `poll_reserve` returns
        // `Pending` and we propagate it, throttling the served stream to the
        // writer's pace instead of piling the whole body into RAM. A closed channel
        // (writer gone) drops the sink; we then forward WITHOUT capture — a
        // diagnostic failure must never break or stall the served stream (AGENTS.md).
        // F1b review r2 (don't-lie-with-zeros): that also means every byte from
        // here on is missing the section, so mark the section degraded RIGHT NOW —
        // sticky, so a later clean end-of-stream can never report this capture
        // complete (`TurnCaptureState::mark_served_degraded`).
        if let Some(sink) = this.served_sink.as_mut() {
            match sink.poll_reserve(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(_)) => {
                    this.served_sink = None;
                    if let Some(state) = &this.state {
                        state.mark_served_degraded();
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref()
                    && !data.is_empty()
                {
                    this.forwarded = this.forwarded.saturating_add(data.len() as u64);
                    // Send the COPY into the slot reserved above; the frame passes
                    // through to the client UNCHANGED (never a retained slice —
                    // AGENTS.md). A failed send just means the writer went away —
                    // mark the section degraded (F1b review r2; see above).
                    if let Some(sink) = this.served_sink.as_mut()
                        && sink.send(data.to_vec()).is_err()
                    {
                        this.served_sink = None;
                        if let Some(state) = &this.state {
                            state.mark_served_degraded();
                        }
                    }
                    if let Some(persistence) = &this.persistence {
                        persistence.push_served_response(data);
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(None) => {
                this.clean_eos = true;
                Poll::Ready(None)
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for TeeBody {
    fn drop(&mut self) {
        // Clean when we observed end-of-stream, OR forwarded the full promised
        // length (hyper can stop polling a Content-Length body before it ever
        // yields `Ready(None)`, so a complete non-streaming response would else be
        // mis-flagged partial). Otherwise the served stream was cut short (client
        // disconnect / mid-stream error) → partial. F1b review r2: this `clean`
        // check is about the CLIENT-facing stream only — if the section itself
        // was marked degraded mid-stream (`poll_frame`'s `mark_served_degraded`
        // calls, section write errors), `served_done`'s `close` cannot unset that
        // sticky mark no matter what we pass here, so a `served_sink` failure can
        // never be reported as a complete capture just because the client still
        // saw a clean end.
        let clean = self.clean_eos || self.exact_len.is_some_and(|len| self.forwarded >= len);
        if let Some(state) = &self.state {
            state.served_done(!clean);
        }
        if let Some(persistence) = &self.persistence {
            persistence.finish_served_response(!clean);
        }
    }
}

/// Wrap `response`'s body in a [`TeeBody`] so its served bytes are captured to the
/// turn's `served_response` section. Status/headers are preserved; only the body
/// is wrapped, and `size_hint`/`is_end_stream` are forwarded so a non-streaming
/// response keeps its `Content-Length` and framing.
fn tee_served_body(
    response: Response,
    state: Option<Arc<crate::turn_capture::TurnCaptureState>>,
    persistence: Option<Arc<crate::flow_persistence::PersistenceCapture>>,
) -> Response {
    let (parts, body) = response.into_parts();
    let exact_len = http_body::Body::size_hint(&body).exact();
    // F1c (finding #2): record that the served tee is now installed BEFORE the
    // `MiddlewareCaptureGuard` served backstop can drop (it drops when `log_api_call`
    // returns, AFTER this runs). With the tee installed, the tee's own `Drop` owns
    // `served_done`; the backstop stays inert. Only a pre-tee unwind (this never
    // runs) leaves the flag false, so the backstop fires `served_done` to resolve the
    // barrier instead of leaking the turn.
    if let Some(state) = &state {
        state.mark_served_tee_installed();
    }
    // Take the back-pressured served sink BEFORE moving `state` into the tee.
    let served_sink = state.as_ref().and_then(|state| state.served_sink());
    let tee = TeeBody {
        inner: body,
        state,
        persistence,
        served_sink,
        clean_eos: false,
        forwarded: 0,
        exact_len,
    };
    Response::from_parts(parts, Body::new(tee))
}

fn stream_chat_completions_response(
    model: String,
    include_usage: bool,
    stream: ReceiverStream<crate::engine::SseEvent>,
    lease: Option<crate::authz::SessionLease>,
) -> Response {
    let (tx, rx) = mpsc::channel(128);
    tokio::spawn(async move {
        let _lease = lease;
        let mut converter = ChatCompletionStreamConverter::new(model, include_usage);
        let mut stream = std::pin::pin!(stream);
        'streaming: while let Some(event) = stream.next().await {
            let chat_events = converter.convert(&event);
            for chat_event in chat_events {
                if tx.send(chat_event).await.is_err() {
                    break 'streaming;
                }
            }
        }
    });

    let mapped = ReceiverStream::new(rx).map(|event| {
        Ok::<_, Infallible>(axum::response::sse::Event::default().data(event.to_sse_data()))
    });

    let mut response = Sse::new(mapped)
        .keep_alive(axum::response::sse::KeepAlive::new())
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    response
}

fn stream_anthropic_response(
    model: String,
    suppress_reasoning: bool,
    stream: ReceiverStream<crate::engine::SseEvent>,
    lease: Option<crate::authz::SessionLease>,
) -> AppResult<Response> {
    let (tx, rx) = mpsc::channel(128);
    tokio::spawn(async move {
        let _lease = lease;
        let mut converter =
            AnthropicStreamConverter::with_reasoning_suppression(model, suppress_reasoning);
        let mut stream = std::pin::pin!(stream);
        while let Some(event) = stream.next().await {
            let anthropic_events = converter.convert(&event);
            for anthropic_event in anthropic_events {
                if tx.send(anthropic_event).await.is_err() {
                    return;
                }
            }
        }
        // The upstream event stream ended. If it never produced a
        // `response.completed` (engine error, dropped/stalled turn, aborted
        // web-search round-trip), emit a terminal `message_delta` +
        // `message_stop` so the client is not left hanging behind the SSE
        // keep-alive forever.
        for anthropic_event in converter.finalize() {
            if tx.send(anthropic_event).await.is_err() {
                return;
            }
        }
    });

    let mapped = ReceiverStream::new(rx).map(|event| {
        Ok::<_, Infallible>(
            axum::response::sse::Event::default()
                .event(event.sse_event_type())
                .data(event.to_json()),
        )
    });

    let mut response = Sse::new(mapped)
        .keep_alive(axum::response::sse::KeepAlive::new())
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    Ok(response)
}

fn proxy_upstream_response(
    response: reqwest::Response,
    lease: Option<crate::authz::SessionLease>,
    accounting: Option<RawCompletionAccounting>,
) -> Response {
    let status = response.status();
    let upstream_headers = response.headers().clone();
    let mut builder = Response::builder().status(status);
    if let Some(headers) = builder.headers_mut() {
        copy_proxy_response_headers(&upstream_headers, headers);
    }
    let mut upstream = Box::pin(response.bytes_stream());
    let mut accounting = accounting;
    let stream = futures::stream::poll_fn(move |cx| {
        let _keep_lease_alive = &lease;
        match upstream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                if let Some(accounting) = accounting.as_mut() {
                    accounting.push(&bytes);
                }
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(error))) => {
                if let Some(accounting) = accounting.as_mut() {
                    accounting.finish("failed");
                }
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                if let Some(accounting) = accounting.as_mut() {
                    accounting.finish(if accounting.upstream_success {
                        "completed"
                    } else {
                        "failed"
                    });
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    });
    builder
        .body(Body::from_stream(stream))
        .expect("valid upstream proxy response")
}

const COMPLETIONS_USAGE_PARSE_LIMIT_BYTES: usize = 256 * 1024;

struct RawCompletionAccounting {
    gateway: Arc<Gateway>,
    context: Option<crate::authz::AuthContext>,
    requested_model: String,
    parser: RawCompletionUsageParser,
    upstream_success: bool,
}

impl RawCompletionAccounting {
    fn new(
        gateway: Arc<Gateway>,
        context: Option<crate::authz::AuthContext>,
        requested_model: String,
        streaming: bool,
        upstream_success: bool,
    ) -> Self {
        Self {
            gateway,
            context,
            requested_model,
            parser: RawCompletionUsageParser::new(streaming),
            upstream_success,
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        self.parser.push(bytes);
    }

    fn finish(&mut self, status: &'static str) {
        let Some(context) = self.context.take() else {
            return;
        };
        let (served_model, usage) = self.parser.finish();
        self.gateway.record_authenticated_usage_values(
            Some(context),
            None,
            crate::upstream::InferenceEndpoint::Completions,
            self.requested_model.clone(),
            status,
            served_model,
            None,
            None,
            usage,
        );
    }
}

impl Drop for RawCompletionAccounting {
    fn drop(&mut self) {
        self.finish("cancelled");
    }
}

enum RawCompletionUsageParser {
    Json {
        body: Vec<u8>,
        overflowed: bool,
    },
    Sse {
        pending: Vec<u8>,
        overflowed_line: bool,
        served_model: Option<String>,
        usage: Option<crate::dashboard_flow::FlowUsage>,
    },
}

impl RawCompletionUsageParser {
    fn new(streaming: bool) -> Self {
        if streaming {
            Self::Sse {
                pending: Vec::new(),
                overflowed_line: false,
                served_model: None,
                usage: None,
            }
        } else {
            Self::Json {
                body: Vec::new(),
                overflowed: false,
            }
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        match self {
            Self::Json { body, overflowed } => {
                if *overflowed {
                    return;
                }
                let remaining = COMPLETIONS_USAGE_PARSE_LIMIT_BYTES.saturating_sub(body.len());
                if bytes.len() > remaining {
                    body.clear();
                    *overflowed = true;
                } else {
                    body.extend_from_slice(bytes);
                }
            }
            Self::Sse {
                pending,
                overflowed_line,
                served_model,
                usage,
            } => {
                for &byte in bytes {
                    if byte == b'\n' {
                        if !*overflowed_line {
                            parse_completion_sse_line(pending, served_model, usage);
                        }
                        pending.clear();
                        *overflowed_line = false;
                    } else if !*overflowed_line {
                        if pending.len() == COMPLETIONS_USAGE_PARSE_LIMIT_BYTES {
                            pending.clear();
                            *overflowed_line = true;
                        } else {
                            pending.push(byte);
                        }
                    }
                }
            }
        }
    }

    fn finish(&mut self) -> (Option<String>, Option<crate::dashboard_flow::FlowUsage>) {
        match self {
            Self::Json { body, overflowed } => {
                if *overflowed {
                    return (None, None);
                }
                serde_json::from_slice::<Value>(body)
                    .ok()
                    .map(|value| completion_usage_from_value(&value))
                    .unwrap_or_default()
            }
            Self::Sse {
                pending,
                overflowed_line,
                served_model,
                usage,
            } => {
                if !*overflowed_line && !pending.is_empty() {
                    parse_completion_sse_line(pending, served_model, usage);
                }
                (served_model.take(), usage.take())
            }
        }
    }
}

fn parse_completion_sse_line(
    line: &[u8],
    served_model: &mut Option<String>,
    usage: &mut Option<crate::dashboard_flow::FlowUsage>,
) {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let Some(data) = line.strip_prefix(b"data:") else {
        return;
    };
    let data = data.strip_prefix(b" ").unwrap_or(data);
    if data == b"[DONE]" {
        return;
    }
    let Ok(value) = serde_json::from_slice::<Value>(data) else {
        return;
    };
    let (model, parsed_usage) = completion_usage_from_value(&value);
    if model.is_some() {
        *served_model = model;
    }
    if parsed_usage.is_some() {
        *usage = parsed_usage;
    }
}

fn completion_usage_from_value(
    value: &Value,
) -> (Option<String>, Option<crate::dashboard_flow::FlowUsage>) {
    let served_model = value
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string);
    let usage = value.get("usage").and_then(|usage| {
        let prompt = usage.get("prompt_tokens")?.as_i64()?;
        let completion = usage.get("completion_tokens")?.as_i64()?;
        let total = usage.get("total_tokens")?.as_i64()?;
        if prompt < 0 || completion < 0 || total < 0 {
            return None;
        }
        let cached = usage
            .get("prompt_tokens_details")
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_i64)
            .filter(|value| *value >= 0);
        let reasoning = usage
            .get("completion_tokens_details")
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(Value::as_i64)
            .filter(|value| *value >= 0);
        Some(crate::dashboard_flow::FlowUsage {
            prompt,
            completion,
            total,
            cached,
            reasoning,
        })
    });
    (served_model, usage)
}

fn copy_proxy_response_headers(source: &HeaderMap, target: &mut HeaderMap) {
    for (name, value) in source {
        if should_proxy_response_header(name) {
            target.append(name.clone(), value.clone());
        }
    }
}

fn should_proxy_response_header(name: &HeaderName) -> bool {
    !is_hop_by_hop_header(name) && !header_name_eq(name, "content-length")
}

/// CR1.1: `engine.rs::created_event` stamps `estimated_input_tokens` onto the
/// canonical `response.created` event as an INTERNAL transport hint -- its
/// only reader is `AnthropicStreamConverter::handle_created`, which seeds
/// `message_start.usage.input_tokens` from it (the real upstream count isn't
/// known until `response.completed`, much later). Every OTHER egress
/// CONVERTS `response.created` into its own wire shape and never copies the
/// field across (Chat's `ChatCompletionStreamConverter` reads only `id`); but
/// this fn is a raw byte-forward of `event.data`, so without this strip a
/// `/v1/responses` streaming client would see a non-standard field OpenAI's
/// Responses API has no concept of, breaking the "Responses wire shape
/// unchanged" contract (a `deny_unknown_fields` consumer or exact-bytes
/// snapshot). Scoped to `response.created` only -- the only event that can
/// ever carry the field (`response.in_progress` reuses the same `ResponseStub`
/// struct but always passes `None`, which `skip_serializing_if` already
/// omits) -- so every other event is serialized untouched with no clone.
fn responses_wire_event_data(event: &crate::engine::SseEvent) -> String {
    if event.event != "response.created" {
        return event.data.to_string();
    }
    let mut data = event.data.clone();
    if let Some(response) = data.get_mut("response").and_then(Value::as_object_mut) {
        response.remove("estimated_input_tokens");
    }
    data.to_string()
}

fn stream_responses_response(
    stream: ReceiverStream<crate::engine::SseEvent>,
    lease: Option<crate::authz::SessionLease>,
) -> Response {
    let mapped = stream.map(move |event| {
        let _keep_lease_alive = &lease;
        let data = responses_wire_event_data(&event);
        Ok::<_, Infallible>(
            axum::response::sse::Event::default()
                .event(event.event)
                .data(data),
        )
    });
    let mut response = Sse::new(mapped)
        .keep_alive(axum::response::sse::KeepAlive::new())
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    response
}

async fn collect_responses_response(
    stream: ReceiverStream<crate::engine::SseEvent>,
) -> AppResult<Response> {
    let mut final_payload: Option<Value> = None;
    let mut stream = std::pin::pin!(stream);
    while let Some(event) = stream.next().await {
        match event.event.as_str() {
            "response.completed" | "response.incomplete" => {
                final_payload = event.data.get("response").cloned();
            }
            "response.failed" => {
                let message = event
                    .data
                    .get("response")
                    .and_then(|response| response.get("error"))
                    .and_then(|error| error.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("upstream request failed");
                return Err(AppError::upstream(message));
            }
            _ => {}
        }
    }

    match final_payload {
        Some(payload) => Ok(Json(payload).into_response()),
        None => Err(AppError::upstream(
            "stream ended before a final response resource was emitted",
        )),
    }
}

async fn collect_chat_completions_response(
    model: String,
    stream: ReceiverStream<crate::engine::SseEvent>,
) -> AppResult<Response> {
    let mut collector = ChatCompletionCollector::new(model);
    let mut stream = std::pin::pin!(stream);
    while let Some(event) = stream.next().await {
        collector.process(&event);
    }
    Ok(Json(collector.into_response()?).into_response())
}

async fn collect_anthropic_response(
    model: String,
    suppress_reasoning: bool,
    stream: ReceiverStream<crate::engine::SseEvent>,
) -> AppResult<Response> {
    let mut collector =
        AnthropicStreamCollector::with_reasoning_suppression(model, suppress_reasoning);
    let mut stream = std::pin::pin!(stream);
    while let Some(event) = stream.next().await {
        collector.process(&event);
    }
    match collector.into_response() {
        Ok(msg) => Ok(Json(msg).into_response()),
        Err(err) => Ok(anthropic_error_response(AppError::upstream(err.message))),
    }
}

fn anthropic_error_response(err: AppError) -> Response {
    let status = err.status_code();
    let error_type = match err.status_code() {
        axum::http::StatusCode::BAD_REQUEST => "invalid_request_error",
        axum::http::StatusCode::CONFLICT => "invalid_request_error",
        axum::http::StatusCode::NOT_FOUND => "not_found_error",
        _ => "api_error",
    };
    let body = serde_json::json!({
        "type": "error",
        "error": {
            "type": error_type,
            "message": err.to_string(),
        }
    });
    (status, Json(body)).into_response()
}

/// Anthropic-style pagination, honoured only for the Anthropic-shaped listing.
#[derive(Debug, Default, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
struct ModelsListQuery {
    /// Return models after this id.
    after_id: Option<String>,
    /// Return models before this id.
    before_id: Option<String>,
    /// Page size (decimal string).
    limit: Option<String>,
}

/// The models a client may request: the configured aliases plus the upstream
/// catalog, filtered to the key's allowed models. With an `anthropic-version`
/// or `anthropic-beta` header the Anthropic list shape
/// (`{"data":[{"id","type":"model","display_name","created_at"}],"has_more","first_id","last_id"}`)
/// is returned instead of the OpenAI shape
/// (`{"object":"list","data":[{"id","object":"model","created","owned_by"}]}`).
#[utoipa::path(
    get,
    path = "/v1/models",
    tag = "inference",
    operation_id = "get_models",
    params(
        ModelsListQuery,
        ("anthropic-version" = Option<String>, Header, description = "Presence selects the Anthropic list shape.")
    ),
    responses(
        (status = 200, body = serde_json::Value, description = "Model list (OpenAI or Anthropic shape, see above). Carries an `ETag`."),
        (status = 401, body = crate::openapi::ApiError, description = "Missing or invalid API key (when client auth is required)."),
        (status = 502, body = crate::openapi::ApiError, description = "The upstream catalog could not be fetched.")
    ),
    security(("bearer" = []), ("api_key" = []))
)]
async fn get_models(
    headers: HeaderMap,
    Query(query): Query<ModelsListQuery>,
    State(gateway): State<Arc<Gateway>>,
    identity: Option<Extension<ClientIdentity>>,
    auth: Option<Extension<crate::authz::AuthContext>>,
) -> AppResult<Response> {
    let anthropic_models = is_anthropic_models_request(&headers);
    let response = gateway.upstream_client().list_models().await?;
    let (status, mut body, etag) = collect_models_response(response).await?;
    let identity_filtered = if let Some(Extension(identity)) = identity.as_ref() {
        if identity.allowed_models().is_empty() {
            false
        } else {
            filter_models_body_for_identity(&mut body, identity);
            true
        }
    } else {
        false
    };
    if let Some(context) = auth.as_ref() {
        filter_models_body(&mut body, &context.0);
    }
    let body = if anthropic_models {
        transform_models_response_for_anthropic(body, &query, gateway.config())?
    } else {
        body
    };
    let mut headers = HeaderMap::new();
    if auth.is_none()
        && !identity_filtered
        && !anthropic_models
        && let Some(etag) = etag
    {
        headers.insert(
            http::header::ETAG,
            HeaderValue::from_str(&etag)
                .map_err(|err| AppError::internal(format!("invalid ETag header: {err}")))?,
        );
    }
    Ok((status, headers, Json(body)).into_response())
}

fn filter_models_body_for_identity(body: &mut Value, identity: &ClientIdentity) {
    fn retain_allowed(entries: &mut Vec<Value>, identity: &ClientIdentity) {
        entries.retain(|entry| {
            entry
                .as_str()
                .or_else(|| entry.get("id").and_then(Value::as_str))
                .is_some_and(|id| identity.allows_model(id))
        });
    }

    match body {
        Value::Array(entries) => retain_allowed(entries, identity),
        Value::Object(map) => {
            if let Some(entries) = map.get_mut("data").and_then(Value::as_array_mut) {
                retain_allowed(entries, identity);
            }
            if let Some(entries) = map.get_mut("models").and_then(Value::as_array_mut) {
                retain_allowed(entries, identity);
            }
        }
        _ => {}
    }
}

fn filter_models_body(body: &mut Value, context: &crate::authz::AuthContext) {
    let models = match body {
        Value::Array(models) => Some(models),
        Value::Object(map) => {
            let key = if map.contains_key("data") {
                "data"
            } else {
                "models"
            };
            map.get_mut(key).and_then(Value::as_array_mut)
        }
        _ => None,
    };
    if let Some(models) = models {
        models.retain(|model| {
            model
                .as_str()
                .or_else(|| {
                    model
                        .get("id")
                        .or_else(|| model.get("name"))
                        .and_then(Value::as_str)
                })
                .is_some_and(|id| context.allows_model("models", id))
        });
    }
}

fn is_anthropic_models_request(headers: &HeaderMap) -> bool {
    headers.contains_key("anthropic-version") || headers.contains_key("anthropic-beta")
}

fn transform_models_response_for_anthropic(
    body: Value,
    query: &ModelsListQuery,
    config: &crate::config::Config,
) -> AppResult<Value> {
    if query.after_id.is_some() && query.before_id.is_some() {
        return Err(AppError::bad_request(
            "after_id and before_id cannot both be specified",
        ));
    }

    let limit = parse_anthropic_models_limit(query.limit.as_deref())?;
    let models = extract_model_entries(&body)
        .into_iter()
        .filter_map(|entry| anthropic_model_entry(&entry, config))
        .collect::<Vec<_>>();

    let (page, has_more) = page_anthropic_models(&models, query, limit)?;
    let first_id = page
        .first()
        .and_then(model_id_from_value)
        .map(Value::String)
        .unwrap_or(Value::Null);
    let last_id = page
        .last()
        .and_then(model_id_from_value)
        .map(Value::String)
        .unwrap_or(Value::Null);

    Ok(serde_json::json!({
        "data": page,
        "first_id": first_id,
        "has_more": has_more,
        "last_id": last_id,
    }))
}

fn parse_anthropic_models_limit(limit: Option<&str>) -> AppResult<usize> {
    match limit {
        Some(raw) => {
            let parsed = raw
                .parse::<usize>()
                .map_err(|_| AppError::bad_request("limit must be an integer from 1 to 1000"))?;
            if !(1..=1000).contains(&parsed) {
                return Err(AppError::bad_request("limit must be between 1 and 1000"));
            }
            Ok(parsed)
        }
        None => Ok(20),
    }
}

fn extract_model_entries(body: &Value) -> Vec<Value> {
    match body {
        Value::Array(entries) => entries.clone(),
        Value::Object(map) => map
            .get("data")
            .or_else(|| map.get("models"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn anthropic_model_entry(entry: &Value, config: &crate::config::Config) -> Option<Value> {
    match entry {
        Value::String(id) => {
            let caps =
                merge_configured_capabilities(config, id, infer_capabilities_from_model_id(id));
            Some(build_anthropic_model_entry(
                id,
                id,
                UNKNOWN_MODEL_CREATED_AT,
                None,
                None,
                Some(&caps),
            ))
        }
        Value::Object(map) => {
            let id = map.get("id").and_then(Value::as_str)?;
            let display_name = map
                .get("display_name")
                .and_then(Value::as_str)
                .or_else(|| map.get("id").and_then(Value::as_str))
                .unwrap_or(id);
            let created_at =
                parse_created_at(map).unwrap_or_else(|| UNKNOWN_MODEL_CREATED_AT.to_string());

            let max_input_tokens = map
                .get("max_input_tokens")
                .or_else(|| map.get("context_length"))
                .or_else(|| map.get("context_window"))
                .or_else(|| map.get("max_context_length"))
                .or_else(|| map.get("max_model_len"));
            let max_tokens = map
                .get("max_tokens")
                .or_else(|| map.get("max_output_tokens"));
            let capabilities = map
                .get("capabilities")
                .filter(|value| value.is_object())
                .cloned()
                .unwrap_or_else(|| infer_capabilities_from_model_id(id));
            let capabilities = merge_configured_capabilities(config, id, capabilities);

            Some(build_anthropic_model_entry(
                id,
                display_name,
                &created_at,
                max_input_tokens,
                max_tokens,
                Some(&capabilities),
            ))
        }
        _ => None,
    }
}

/// Parse a creation timestamp from common upstream formats.
///
/// - `created_at` → ISO 8601 string (passed through)
/// - `created`    → Unix epoch integer / float (⇒ ISO 8601 string)
fn parse_created_at(map: &serde_json::Map<String, Value>) -> Option<String> {
    match map.get("created_at").and_then(Value::as_str) {
        Some(iso) => Some(iso.to_string()),
        None => {
            let epoch = map.get("created").and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_f64().and_then(|f| (f as u64).checked_add(0)))
            })?;
            chrono::DateTime::from_timestamp(epoch as i64, 0)
                .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        }
    }
}

fn infer_capabilities_from_model_id(_id: &str) -> Value {
    default_anthropic_model_capabilities()
}

fn merge_configured_capabilities(config: &crate::config::Config, id: &str, base: Value) -> Value {
    config
        .resolve_capabilities_for_upstream(id)
        .map_or(base.clone(), |capabilities| capabilities.merge_into(base))
}

fn build_anthropic_model_entry(
    id: &str,
    display_name: &str,
    created_at: &str,
    max_input_tokens: Option<&Value>,
    max_tokens: Option<&Value>,
    capabilities: Option<&Value>,
) -> Value {
    serde_json::json!({
        "id": id,
        "capabilities": capabilities
            .filter(|value| value.is_object())
            .cloned()
            .unwrap_or_else(default_anthropic_model_capabilities),
        "created_at": created_at,
        "display_name": display_name,
        "max_input_tokens": numeric_field_or_zero(max_input_tokens),
        "max_tokens": numeric_field_or_zero(max_tokens),
        "type": "model",
    })
}

fn numeric_field_or_zero(value: Option<&Value>) -> Value {
    value
        .and_then(Value::as_u64)
        .map(|number| serde_json::json!(number))
        .unwrap_or_else(|| serde_json::json!(0))
}

fn default_anthropic_model_capabilities() -> Value {
    let unsupported = || serde_json::json!({ "supported": false });
    serde_json::json!({
        "batch": unsupported(),
        "citations": unsupported(),
        "code_execution": unsupported(),
        "context_management": {
            "clear_thinking_20251015": unsupported(),
            "clear_tool_uses_20250919": unsupported(),
            "compact_20260112": unsupported(),
            "supported": false
        },
        "effort": {
            "high": unsupported(),
            "low": unsupported(),
            "max": unsupported(),
            "medium": unsupported(),
            "supported": false
        },
        "image_input": unsupported(),
        "pdf_input": unsupported(),
        "structured_outputs": unsupported(),
        "thinking": {
            "supported": false,
            "types": {
                "adaptive": unsupported(),
                "enabled": unsupported()
            }
        }
    })
}

fn page_anthropic_models(
    models: &[Value],
    query: &ModelsListQuery,
    limit: usize,
) -> AppResult<(Vec<Value>, bool)> {
    if let Some(before_id) = query.before_id.as_deref() {
        let end = model_index(models, before_id)?;
        let start = end.saturating_sub(limit);
        return Ok((models[start..end].to_vec(), start > 0));
    }

    let start = match query.after_id.as_deref() {
        Some(after_id) => model_index(models, after_id)? + 1,
        None => 0,
    };
    let end = (start + limit).min(models.len());
    Ok((models[start..end].to_vec(), end < models.len()))
}

fn model_index(models: &[Value], id: &str) -> AppResult<usize> {
    models
        .iter()
        .position(|model| model_id_from_value(model).as_deref() == Some(id))
        .ok_or_else(|| AppError::bad_request(format!("model cursor not found: {id}")))
}

fn model_id_from_value(model: &Value) -> Option<String> {
    model
        .as_object()
        .and_then(|map| map.get("id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::body_log_fields;
    use super::responses_wire_event_data;
    use super::should_proxy_response_header;
    use axum::body::Bytes;
    use axum::http::HeaderName;
    use sha2::Digest as _;

    /// Finding 1: the inbound redaction produces IDENTICAL redacted output on the
    /// inline (small) and `spawn_blocking` (large) paths — a secret-bearing field and
    /// an image `data:` URI are BOTH redacted, with no raw secret/image bytes
    /// surviving, and the requested `model` is still extracted. The large body forces
    /// the off-worker path (`> TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES`); the small one
    /// stays inline. Both must round-trip to valid JSON with the same redaction.
    #[tokio::test]
    async fn offload_inbound_redacts_small_inline_and_large_spawn_blocking_identically() {
        use serde_json::Value;
        let body_with_filler = |filler: &str| {
            Bytes::from(format!(
                r#"{{"model":"claude-x","api_key":"sk-LEAK-INBOUND-9999","messages":[{{"role":"user","content":[{{"type":"image_url","image_url":{{"url":"data:image/png;base64,RAWINBOUNDIMG7777"}}}},{{"type":"text","text":"{filler}"}}]}}]}}"#
            ))
        };

        let small = body_with_filler("hi");
        assert!(
            small.len() <= super::TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES,
            "small body takes the inline path"
        );
        let (model_small, red_small, partial_small) =
            super::offload_redacted_inbound_section(small.clone()).await;
        assert!(!partial_small, "a clean inline redaction is not partial");

        let large = body_with_filler(&"x".repeat(32 * 1024));
        assert!(
            large.len() > super::TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES,
            "large body takes the spawn_blocking path"
        );
        let (model_large, red_large, partial_large) =
            super::offload_redacted_inbound_section(large).await;
        assert!(
            !partial_large,
            "a clean spawn_blocking redaction is not partial"
        );

        for (label, model, redacted) in [
            ("small/inline", model_small, red_small),
            ("large/spawn_blocking", model_large, red_large),
        ] {
            assert_eq!(
                model.as_deref(),
                Some("claude-x"),
                "{label}: model extracted"
            );
            let text = String::from_utf8(redacted).expect("redacted section is UTF-8");
            assert!(
                !text.contains("sk-LEAK-INBOUND-9999"),
                "{label}: secret value redacted"
            );
            assert!(
                text.contains("[redacted]"),
                "{label}: secret marker present"
            );
            assert!(
                !text.contains("RAWINBOUNDIMG7777"),
                "{label}: raw image bytes redacted"
            );
            assert!(
                text.contains("<redacted uri>"),
                "{label}: image URI marker present"
            );
            let value: Value = serde_json::from_str(&text).expect("redacted section is valid JSON");
            assert_eq!(value["model"], "claude-x", "{label}: structure intact");
        }
    }

    /// F1 (Fable-fix, BLOCKING): the large-body `spawn_blocking` offload must be
    /// handed an OWNED `Vec` copy, NOT the Arc-backed `Bytes` (which would PIN the
    /// 256 MiB inbound middleware backing for the task's lifetime — AGENTS.md line
    /// 144). A large body forces the off-worker path; the capture completes correctly
    /// AND the offload retains NO clone of the inbound `Bytes` backing after it returns
    /// (the observable half of the no-pin contract): a second handle to the same shared
    /// backing is uniquely reclaimable once the offload dropped its `Bytes` in favor of
    /// the owned copy.
    #[tokio::test]
    async fn offload_large_inbound_hands_owned_copy_not_pinned_bytes() {
        let large = Bytes::from(format!(
            r#"{{"model":"claude-x","api_key":"sk-LEAK-PIN-1234","messages":[{{"role":"user","content":"{}"}}]}}"#,
            "x".repeat(64 * 1024)
        ));
        assert!(
            large.len() > super::TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES,
            "body forces the spawn_blocking path"
        );
        // A SECOND handle to the SAME shared backing; if the offload retained a clone
        // (old bug: moved the `Bytes` into `spawn_blocking`) this could not reclaim it.
        let observer = large.clone();

        let (model, redacted, partial) = super::offload_redacted_inbound_section(large).await;
        assert_eq!(model.as_deref(), Some("claude-x"), "model extracted");
        assert!(!partial, "a clean spawn_blocking redaction is not partial");
        let text = String::from_utf8(redacted).expect("redacted section is UTF-8");
        assert!(
            !text.contains("sk-LEAK-PIN-1234"),
            "secret redacted on the offloaded path"
        );

        // The offload converted the body to an OWNED `Vec` for the blocking task and
        // dropped its `Bytes`, so nothing pins the shared backing: `observer` is now the
        // SOLE owner and reclaims uniquely (no retained slice of the inbound buffer).
        assert!(
            observer.try_into_mut().is_ok(),
            "offload retained no clone of the inbound Bytes backing"
        );
    }

    #[tokio::test]
    async fn persistence_inbound_large_body_is_split_in_full_and_does_not_pin_bytes() {
        let filler = "x".repeat(2 * 1024 * 1024);
        let body = Bytes::from(format!(
            r#"{{"model":"large-model","api_key":"sk-large-secret","input":"{filler}"}}"#
        ));
        let observer = body.clone();
        let capture = super::offload_persistence_inbound(
            body,
            crate::content_store::PROTOCOL_RESPONSES,
            true,
            None,
        )
        .await;
        assert!(capture.valid_json);
        assert_eq!(capture.model.as_deref(), Some("large-model"));
        assert!(!capture.partial);
        let split = capture.split.expect("a JSON object body splits");
        assert_eq!(split.items.len(), 1, "string `input` is one message item");
        assert!(
            split.items[0].canonical.len() >= filler.len(),
            "the full body is retained, not a capped preview"
        );
        assert!(!split.skeleton.contains("sk-large-secret"));
        assert!(split.skeleton.contains("[redacted]"));
        assert!(
            observer.try_into_mut().is_ok(),
            "offload pins no Bytes clone"
        );
    }

    #[tokio::test]
    async fn persistence_inbound_malformed_body_opens_no_row_and_keeps_no_bytes() {
        let body = Bytes::from_static(br#"{"api_key":"super-secret-without-a-close"#);
        let capture = super::offload_persistence_inbound(
            body,
            crate::content_store::PROTOCOL_RESPONSES,
            true,
            None,
        )
        .await;
        assert!(!capture.valid_json);
        assert!(capture.split.is_none());
        let marker = String::from_utf8(capture.redacted).unwrap();
        assert!(!marker.contains("super-secret"));
    }

    #[test]
    fn malformed_inbound_never_retains_unterminated_secret() {
        let malformed = br#"{"api_key":"super-secret-without-a-close"#;
        let (model, captured) = super::redacted_inbound_section(malformed);
        assert!(model.is_none());
        let captured = String::from_utf8(captured).unwrap();
        assert!(!captured.contains("super-secret"));
        assert_eq!(
            captured,
            format!("[redacted: unparseable body {} bytes]", malformed.len())
        );
    }

    /// D7a R3 #1 (REGRESSION): a `/dashboard/login` body must yield NO
    /// body-derived log field. Emitting `body_sha256` + `body_bytes` (length) for
    /// the login body is an offline token-verification oracle — an attacker with
    /// the logs can brute-force the token against the digest and the known length.
    /// So `body_log_fields` returns `None`, and the request line that follows it
    /// carries NEITHER the token NOR its SHA-256 NOR the body length.
    #[test]
    fn auth_path_body_emits_no_body_derived_log_fields() {
        let token = "s3cret-login-token";
        let body = Bytes::from(format!(r#"{{"token":"{token}"}}"#));
        let token_sha = hex::encode(sha2::Sha256::digest(token.as_bytes()));
        let body_sha = hex::encode(sha2::Sha256::digest(&body));

        // The login endpoint suppresses every body-derived field.
        assert!(
            body_log_fields("/dashboard/login", &body).is_none(),
            "login body must produce no body-derived log fields (token oracle)"
        );
        // Logout is symmetric (bodyless, but the same path class).
        assert!(body_log_fields("/dashboard/logout", &Bytes::new()).is_none());
        assert!(body_log_fields("/dashboard/auth/key-login", &body).is_none());
        assert!(body_log_fields("/dashboard/auth/logout", &Bytes::new()).is_none());

        // A normal inference path still logs the length + digest + summary, and
        // that digest is over the body (never resembles the bare-token digest).
        let normal =
            body_log_fields("/v1/messages", &body).expect("non-auth path logs body-derived fields");
        assert_eq!(normal.bytes, body.len());
        assert_eq!(normal.sha256, body_sha);
        // Sanity: the body digest is not the standalone token digest, so even the
        // normal path never logs a digest of the bare token.
        assert_ne!(normal.sha256, token_sha);
    }

    /// The full RFC 7230 §6.1 hop-by-hop set; must match the canonical list and
    /// the request-direction parity test in `upstream.rs`.
    const HOP_BY_HOP: [&str; 8] = [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ];

    #[test]
    fn response_direction_strips_full_hop_by_hop_set() {
        for header in HOP_BY_HOP {
            let name = HeaderName::from_bytes(header.as_bytes()).unwrap();
            assert!(
                !should_proxy_response_header(&name),
                "response proxy must strip hop-by-hop header {header}",
            );
        }
    }

    #[test]
    fn response_direction_strips_content_length() {
        let name = HeaderName::from_static("content-length");
        assert!(!should_proxy_response_header(&name));
    }

    #[test]
    fn response_direction_passes_representative_passthrough_header() {
        let name = HeaderName::from_static("content-type");
        assert!(should_proxy_response_header(&name));
    }

    /// CR1.1: `response.created` is the only event that can carry the
    /// internal `estimated_input_tokens` hint (`engine.rs::created_event`
    /// stamps it there for the Anthropic egress's `handle_created` to read);
    /// the raw-forward `/v1/responses` egress must strip it before it reaches
    /// the wire. The rest of the `response` object survives untouched, and a
    /// different event carrying an incidentally-named field is passed through
    /// byte-identical -- the strip is scoped to `response.created` only, not
    /// a blanket key filter.
    #[test]
    fn responses_wire_event_data_strips_estimate_from_created_only() {
        let created = crate::engine::SseEvent {
            event: "response.created".to_string(),
            data: serde_json::json!({
                "type": "response.created",
                "response": { "id": "resp_1", "estimated_input_tokens": 42 }
            }),
        };
        let stripped: serde_json::Value =
            serde_json::from_str(&responses_wire_event_data(&created)).expect("valid json");
        assert_eq!(stripped["response"]["id"], "resp_1");
        assert!(
            stripped["response"].get("estimated_input_tokens").is_none(),
            "estimated_input_tokens must be stripped from response.created: {stripped}"
        );

        let other = crate::engine::SseEvent {
            event: "response.in_progress".to_string(),
            data: serde_json::json!({
                "type": "response.in_progress",
                "response": { "id": "resp_1" }
            }),
        };
        assert_eq!(
            responses_wire_event_data(&other),
            other.data.to_string(),
            "non-created events must pass through untouched"
        );
    }

    /// Defensive: a `response.created` event with no `response` object at all
    /// (malformed/unexpected shape) must not panic -- it just serializes
    /// through unchanged.
    #[test]
    fn responses_wire_event_data_tolerates_missing_response_object() {
        let event = crate::engine::SseEvent {
            event: "response.created".to_string(),
            data: serde_json::json!({ "type": "response.created" }),
        };
        assert_eq!(responses_wire_event_data(&event), event.data.to_string());
    }

    /// Round-trips every supported `Content-Encoding` and the passthrough cases.
    /// codex-tui 0.145+ ships `Content-Encoding: zstd`; the gzip/deflate/br arms
    /// cover generic HTTP clients. An unknown encoding surfaces a 415-carrying
    /// `Err` rather than silently passing compressed bytes to the JSON extractor.
    #[test]
    fn decode_content_encoding_roundtrips_all_supported_encodings() {
        use std::io::Read as _;
        let original = br#"{"model":"glm-5.1","input":[]}"#;
        let original_bytes = Bytes::copy_from_slice(original);

        // zstd (the codex path — magic 28 b5 2f fd).
        let z = zstd::encode_all(&original[..], 3).expect("zstd encode");
        assert_eq!(
            super::decode_content_encoding(&Bytes::from(z), "zstd").expect("zstd decode"),
            original_bytes
        );

        // gzip
        let mut gz = flate2::read::GzEncoder::new(
            std::io::Cursor::new(original),
            flate2::Compression::default(),
        );
        let mut out = Vec::new();
        gz.read_to_end(&mut out).expect("gz encode");
        assert_eq!(
            super::decode_content_encoding(&Bytes::from(out), "gzip").expect("gzip decode"),
            original_bytes
        );

        // deflate (zlib-wrapped)
        let mut zlib = flate2::read::ZlibEncoder::new(
            std::io::Cursor::new(original),
            flate2::Compression::default(),
        );
        let mut out = Vec::new();
        zlib.read_to_end(&mut out).expect("zlib encode");
        assert_eq!(
            super::decode_content_encoding(&Bytes::from(out), "deflate").expect("deflate decode"),
            original_bytes
        );

        // brotli
        let mut br = brotli::CompressorReader::new(std::io::Cursor::new(original), 4096, 11, 22);
        let mut out = Vec::new();
        br.read_to_end(&mut out).expect("br encode");
        assert_eq!(
            super::decode_content_encoding(&Bytes::from(out), "br").expect("br decode"),
            original_bytes
        );
    }

    /// `identity` and a missing/blank encoding are passthroughs, and the lookup
    /// is case-insensitive (HTTP header values are case-insensitive). An unknown
    /// encoding returns `Err` so the middleware can surface 415 instead of
    /// forwarding opaque bytes.
    #[test]
    fn decode_content_encoding_identity_blank_case_insensitive_and_unknown() {
        let body = Bytes::from_static(b"hello");
        assert_eq!(
            super::decode_content_encoding(&body, "identity").unwrap(),
            body
        );
        assert_eq!(super::decode_content_encoding(&body, "").unwrap(), body);
        assert_eq!(super::decode_content_encoding(&body, "  ").unwrap(), body);
        // Header values are case-insensitive — codex sends lowercase, but a
        // generic client may send `ZSTD` / `GZIP`.
        let z = zstd::encode_all(&b"hello"[..], 3).unwrap();
        assert_eq!(
            super::decode_content_encoding(&Bytes::from(z), "ZSTD").unwrap(),
            body
        );
        // Unknown encoding ⇒ Err (caller returns 415, not a silent 400 JSON parse).
        assert!(super::decode_content_encoding(&body, "snappy").is_err());
    }

    #[test]
    fn raw_completions_nonstream_usage_preserves_optional_breakdowns() {
        let mut parser = super::RawCompletionUsageParser::new(false);
        parser.push(
            br#"{"model":"served-v1","usage":{"prompt_tokens":12,"completion_tokens":5,"total_tokens":17,"prompt_tokens_details":{"cached_tokens":3},"completion_tokens_details":{"reasoning_tokens":2}}}"#,
        );
        let (model, usage) = parser.finish();
        assert_eq!(model.as_deref(), Some("served-v1"));
        assert_eq!(
            usage,
            Some(crate::dashboard_flow::FlowUsage {
                prompt: 12,
                completion: 5,
                total: 17,
                cached: Some(3),
                reasoning: Some(2),
            })
        );
    }

    #[test]
    fn raw_completions_stream_usage_survives_arbitrary_chunking() {
        let wire = b"data: {\"model\":\"served-v2\",\"choices\":[{\"text\":\"ok\"}]}\r\n\r\ndata: {\"model\":\"served-v2\",\"choices\":[],\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":2,\"total_tokens\":10}}\n\ndata: [DONE]\n\n";
        for split in 0..=wire.len() {
            let mut parser = super::RawCompletionUsageParser::new(true);
            parser.push(&wire[..split]);
            parser.push(&wire[split..]);
            let (model, usage) = parser.finish();
            assert_eq!(model.as_deref(), Some("served-v2"), "split={split}");
            assert_eq!(usage.unwrap().total, 10, "split={split}");
        }
    }

    #[test]
    fn raw_completions_bounded_parser_reports_oversize_as_unavailable() {
        let mut nonstream = super::RawCompletionUsageParser::new(false);
        nonstream.push(&vec![b'x'; super::COMPLETIONS_USAGE_PARSE_LIMIT_BYTES + 1]);
        assert_eq!(nonstream.finish(), (None, None));

        let mut streaming = super::RawCompletionUsageParser::new(true);
        streaming.push(&vec![b'x'; super::COMPLETIONS_USAGE_PARSE_LIMIT_BYTES + 1]);
        streaming.push(b"\ndata: {\"model\":\"recovered\",\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n");
        let (model, usage) = streaming.finish();
        assert_eq!(model.as_deref(), Some("recovered"));
        assert_eq!(usage.unwrap().total, 2);
    }
}
