//! Auth-gated, read-only access to durable inference history.
//!
//! Route registration lives inside the existing dashboard API router, so the
//! same `require_session`, `no-store`, CSP, and clickjacking protections apply.
//! This module never exposes users, settings, API-key rows, or secret digests.

use crate::control_plane_store::{
    EventRow, MetricSample, RequestSummary, UsageBucket, UsageFilter,
};
use crate::engine::Gateway;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

pub const DEFAULT_REQUEST_LIMIT: i64 = 100;
pub const MAX_REQUEST_LIMIT: i64 = 500;
pub const DEFAULT_METRICS_LIMIT: usize = 1_000;
pub const MAX_METRICS_LIMIT: usize = 5_000;
pub const DEFAULT_USAGE_LIMIT: usize = 100;
pub const MAX_USAGE_LIMIT: usize = 500;
pub const DEFAULT_METRICS_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;
pub const DEFAULT_USAGE_WINDOW_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
const HISTORY_QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_EVENTS: usize = 64;
const MAX_ID_BYTES: usize = 256;
const MAX_KEY_ID_BYTES: usize = 256;

const _: () = {
    assert!(DEFAULT_REQUEST_LIMIT >= 1 && DEFAULT_REQUEST_LIMIT <= MAX_REQUEST_LIMIT);
    assert!(DEFAULT_METRICS_LIMIT >= 1 && DEFAULT_METRICS_LIMIT <= MAX_METRICS_LIMIT);
    assert!(DEFAULT_USAGE_LIMIT >= 1 && DEFAULT_USAGE_LIMIT <= MAX_USAGE_LIMIT);
    assert!(MAX_REQUEST_LIMIT <= 500);
    assert!(MAX_METRICS_LIMIT <= 5_000);
    assert!(DEFAULT_METRICS_WINDOW_MS > 0);
    assert!(DEFAULT_USAGE_WINDOW_MS > 0);
};

#[derive(Debug, Default, Deserialize)]
pub struct HistoryRequestsQuery {
    pub limit: Option<i64>,
}

#[derive(Debug, Default, Deserialize)]
pub struct HistoryUsageQuery {
    pub virtual_key_id: Option<String>,
    pub since_ms: Option<i64>,
    pub limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
pub struct HistoryMetricsQuery {
    pub since_ms: Option<i64>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct RequestsBody {
    requests: Vec<RequestSummary>,
    limit: i64,
}

#[derive(Debug, Serialize)]
struct RequestDetailBody {
    request: RequestSummary,
    events: Vec<EventRow>,
    events_truncated: bool,
}

#[derive(Debug, Serialize)]
struct UsageBody {
    usage: Vec<UsageBucket>,
    since_ms: i64,
    limit: usize,
    truncated: bool,
}

#[derive(Debug, Serialize)]
struct MetricsBody {
    samples: Vec<MetricSample>,
    since_ms: i64,
    limit: usize,
    truncated: bool,
}

/// `GET /dashboard/api/history/requests?limit=`. Newest first; the SQL query is
/// bounded before execution, rather than loading an unbounded result and slicing.
pub async fn history_requests(
    State(gateway): State<Arc<Gateway>>,
    Query(query): Query<HistoryRequestsQuery>,
) -> Response {
    history_requests_from(gateway.persistence_store(), query).await
}

async fn history_requests_from(
    store: Option<Arc<dyn crate::control_plane_store::PersistenceStore>>,
    query: HistoryRequestsQuery,
) -> Response {
    let Some(store) = store else {
        return unavailable();
    };
    let limit = query
        .limit
        .unwrap_or(DEFAULT_REQUEST_LIMIT)
        .clamp(1, MAX_REQUEST_LIMIT);
    match tokio::time::timeout(HISTORY_QUERY_TIMEOUT, store.list_requests(limit)).await {
        Ok(Ok(requests)) => json_response(StatusCode::OK, &RequestsBody { requests, limit }),
        Ok(Err(error)) => internal_error("list durable requests", &error),
        Err(_) => query_timeout(),
    }
}

/// `GET /dashboard/api/history/requests/:id`. Both the terminal aggregate and
/// its bounded/redacted four-hop event records are returned. Unknown ids are 404.
pub async fn history_request_detail(
    State(gateway): State<Arc<Gateway>>,
    Path(id): Path<String>,
) -> Response {
    history_request_detail_from(gateway.persistence_store(), id).await
}

async fn history_request_detail_from(
    store: Option<Arc<dyn crate::control_plane_store::PersistenceStore>>,
    id: String,
) -> Response {
    let Some(store) = store else {
        return unavailable();
    };
    if id.is_empty() || id.len() > MAX_ID_BYTES {
        return bad_request("invalid request id");
    }
    let request = match tokio::time::timeout(HISTORY_QUERY_TIMEOUT, store.get_request(&id)).await {
        Ok(Ok(Some(request))) => request,
        Ok(Ok(None)) => {
            return json_response(StatusCode::NOT_FOUND, &error_body("request not found"));
        }
        Ok(Err(error)) => return internal_error("read durable request", &error),
        Err(_) => return query_timeout(),
    };
    match tokio::time::timeout(
        HISTORY_QUERY_TIMEOUT,
        store.request_events_limited(&id, MAX_EVENTS.saturating_add(1)),
    )
    .await
    {
        Ok(Ok(mut events)) => {
            let events_truncated = events.len() > MAX_EVENTS;
            if events_truncated {
                events.drain(..events.len() - MAX_EVENTS);
            }
            json_response(
                StatusCode::OK,
                &RequestDetailBody {
                    request,
                    events,
                    events_truncated,
                },
            )
        }
        Ok(Err(error)) => internal_error("read durable request events", &error),
        Err(_) => query_timeout(),
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct HistoryBodyQuery {
    pub hop: Option<String>,
}

/// `GET /dashboard/api/history/requests/:id/body?hop=client_in|upstream_out`.
/// Reassembles the full request body of one hop from its skeleton event and
/// content-addressed items. The response is the body itself (JSON), not an
/// envelope. 404 when the request or its hop body is not stored.
pub async fn history_request_body(
    State(gateway): State<Arc<Gateway>>,
    Path(id): Path<String>,
    Query(query): Query<HistoryBodyQuery>,
) -> Response {
    history_request_body_from(gateway.persistence_store(), id, query).await
}

async fn history_request_body_from(
    store: Option<Arc<dyn crate::control_plane_store::PersistenceStore>>,
    id: String,
    query: HistoryBodyQuery,
) -> Response {
    use crate::flow_persistence::PayloadSection;

    let Some(store) = store else {
        return unavailable();
    };
    if id.is_empty() || id.len() > MAX_ID_BYTES {
        return bad_request("invalid request id");
    }
    let section = match query.hop.as_deref().unwrap_or("client_in") {
        "client_in" => PayloadSection::InboundRequest,
        "upstream_out" => PayloadSection::UpstreamRequest,
        _ => return bad_request("hop must be client_in or upstream_out"),
    };
    let request = match tokio::time::timeout(HISTORY_QUERY_TIMEOUT, store.get_request(&id)).await {
        Ok(Ok(Some(request))) => request,
        Ok(Ok(None)) => {
            return json_response(StatusCode::NOT_FOUND, &error_body("request not found"));
        }
        Ok(Err(error)) => return internal_error("read durable request", &error),
        Err(_) => return query_timeout(),
    };
    let protocol = match section {
        PayloadSection::InboundRequest => request.client_protocol.clone(),
        _ => crate::content_store::PROTOCOL_CHAT_COMPLETIONS.to_string(),
    };
    let events = match tokio::time::timeout(
        HISTORY_QUERY_TIMEOUT,
        store.request_events_limited(&id, MAX_EVENTS),
    )
    .await
    {
        Ok(Ok(events)) => events,
        Ok(Err(error)) => return internal_error("read durable request events", &error),
        Err(_) => return query_timeout(),
    };
    let Some(skeleton_event) = events.into_iter().find(|event| event.seq == section.seq()) else {
        return json_response(
            StatusCode::NOT_FOUND,
            &error_body("request body not stored"),
        );
    };
    let skeleton = match skeleton_event
        .payload
        .as_deref()
        .and_then(|payload| serde_json::from_str::<serde_json::Value>(payload).ok())
        .and_then(|envelope| {
            envelope
                .get("content")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        }) {
        Some(skeleton) => skeleton,
        None => {
            return json_response(
                StatusCode::NOT_FOUND,
                &error_body("request body not stored"),
            );
        }
    };
    let items = match tokio::time::timeout(
        HISTORY_QUERY_TIMEOUT,
        store.request_items(&id, section.hop()),
    )
    .await
    {
        Ok(Ok(items)) => items,
        Ok(Err(error)) => return internal_error("read durable request items", &error),
        Err(_) => return query_timeout(),
    };
    let hashes = items
        .iter()
        .map(|item| item.blob_hash.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let blobs = match tokio::time::timeout(HISTORY_QUERY_TIMEOUT, store.get_blobs(&hashes)).await {
        Ok(Ok(blobs)) => blobs,
        Ok(Err(error)) => return internal_error("read durable content blobs", &error),
        Err(_) => return query_timeout(),
    };
    let lookup = blobs
        .into_iter()
        .map(|blob| (blob.hash, blob.content))
        .collect::<std::collections::HashMap<_, _>>();
    // A body may reference the same blob at several positions, so resolve by
    // clone rather than by removal.
    match crate::content_store::assemble(&protocol, &skeleton, |hash| lookup.get(hash).cloned()) {
        Ok(body) => {
            let response = (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response();
            crate::dashboard_auth::no_store(response)
        }
        Err(error) => {
            let error = error.to_string();
            internal_error("assemble durable request body", &error)
        }
    }
}

/// `GET /dashboard/api/history/usage?virtual_key_id=&since_ms=`. The key filter
/// is a stable database id, never a presented credential or its digest.
pub async fn history_usage(
    State(gateway): State<Arc<Gateway>>,
    Query(query): Query<HistoryUsageQuery>,
) -> Response {
    history_usage_from(gateway.persistence_store(), query).await
}

async fn history_usage_from(
    store: Option<Arc<dyn crate::control_plane_store::PersistenceStore>>,
    query: HistoryUsageQuery,
) -> Response {
    let Some(store) = store else {
        return unavailable();
    };
    let virtual_key_id = match query.virtual_key_id {
        Some(value) => {
            let value = value.trim();
            if value.is_empty() || value.len() > MAX_KEY_ID_BYTES {
                return bad_request("invalid virtual_key_id");
            }
            Some(value.to_string())
        }
        None => None,
    };
    if query.since_ms.is_some_and(|since| since < 0) {
        return bad_request("since_ms must not be negative");
    }
    let limit = query
        .limit
        .unwrap_or(DEFAULT_USAGE_LIMIT)
        .clamp(1, MAX_USAGE_LIMIT);
    let since_ms = query
        .since_ms
        .unwrap_or_else(|| now_ms().saturating_sub(DEFAULT_USAGE_WINDOW_MS));
    match tokio::time::timeout(
        HISTORY_QUERY_TIMEOUT,
        store.usage_summary_limited(
            &UsageFilter {
                virtual_key_id,
                since_ms: Some(since_ms),
            },
            limit.saturating_add(1),
        ),
    )
    .await
    {
        Ok(Ok(mut usage)) => {
            let truncated = usage.len() > limit;
            usage.truncate(limit);
            json_response(
                StatusCode::OK,
                &UsageBody {
                    usage,
                    since_ms,
                    limit,
                    truncated,
                },
            )
        }
        Ok(Err(error)) => internal_error("read durable usage", &error),
        Err(_) => query_timeout(),
    }
}

/// `GET /dashboard/api/history/metrics?since_ms=&limit=`. The store query is
/// time-bounded and the response is row-bounded. The newest `limit` samples in
/// the selected window are returned in chronological order.
pub async fn history_metrics(
    State(gateway): State<Arc<Gateway>>,
    Query(query): Query<HistoryMetricsQuery>,
) -> Response {
    history_metrics_from(gateway.persistence_store(), query).await
}

async fn history_metrics_from(
    store: Option<Arc<dyn crate::control_plane_store::PersistenceStore>>,
    query: HistoryMetricsQuery,
) -> Response {
    let Some(store) = store else {
        return unavailable();
    };
    let since_ms = query
        .since_ms
        .unwrap_or_else(|| now_ms().saturating_sub(DEFAULT_METRICS_WINDOW_MS));
    if since_ms < 0 {
        return bad_request("since_ms must not be negative");
    }
    let limit = query
        .limit
        .unwrap_or(DEFAULT_METRICS_LIMIT)
        .clamp(1, MAX_METRICS_LIMIT);
    // Fetch one sentinel row beyond the response limit so `truncated` is exact
    // while the SQL read remains bounded. The store returns the newest rows in
    // chronological order.
    match tokio::time::timeout(
        HISTORY_QUERY_TIMEOUT,
        store.backend_metrics_history_limited(since_ms, limit.saturating_add(1)),
    )
    .await
    {
        Ok(Ok(mut samples)) => {
            let truncated = samples.len() > limit;
            if truncated {
                samples.drain(..samples.len() - limit);
            }
            json_response(
                StatusCode::OK,
                &MetricsBody {
                    samples,
                    since_ms,
                    limit,
                    truncated,
                },
            )
        }
        Ok(Err(error)) => internal_error("read durable metric history", &error),
        Err(_) => query_timeout(),
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn unavailable() -> Response {
    json_response(
        StatusCode::SERVICE_UNAVAILABLE,
        &error_body("persistent history is disabled"),
    )
}

fn bad_request(message: &'static str) -> Response {
    json_response(StatusCode::BAD_REQUEST, &error_body(message))
}

fn query_timeout() -> Response {
    json_response(
        StatusCode::GATEWAY_TIMEOUT,
        &error_body("persistent history query timed out"),
    )
}

fn internal_error(operation: &'static str, error: &str) -> Response {
    tracing::error!(operation, error, "persistent history read failed");
    // The operation label is a static string chosen by this module; the
    // underlying error text stays in the log only.
    json_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        &serde_json::json!({ "error": "persistent history read failed", "operation": operation }),
    )
}

fn error_body(message: &'static str) -> serde_json::Value {
    serde_json::json!({ "error": message })
}

fn json_response<T: Serialize>(status: StatusCode, body: &T) -> Response {
    crate::dashboard_auth::no_store((status, Json(body)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane_store::{PersistenceStore, RequestFinish, RequestRow, SqlStore};
    use http_body_util::BodyExt;

    async fn response_json(response: Response) -> serde_json::Value {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("collect response")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("JSON response")
    }

    fn request(id: &str, created_at_ms: i64) -> RequestRow {
        RequestRow {
            id: id.to_string(),
            response_id: None,
            conversation_id: Some("conversation".to_string()),
            virtual_key_id: Some("key-id-not-secret".to_string()),
            client_protocol: "responses".to_string(),
            client_model: "public-model".to_string(),
            alias: Some("public-model".to_string()),
            backend: None,
            resolved_model: None,
            status: "running".to_string(),
            created_at_ms,
            ..RequestRow::default()
        }
    }

    async fn sqlite_store() -> Arc<dyn PersistenceStore> {
        Arc::new(
            SqlStore::connect_sqlite("sqlite::memory:")
                .await
                .expect("connect"),
        )
    }

    #[tokio::test]
    async fn error_responses_never_echo_store_details() {
        let response = internal_error("unit test", "postgres://secret@host/table missing");
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store")
        );
        let body = response_json(response).await;
        assert_eq!(body["error"], "persistent history read failed");
        assert!(!body.to_string().contains("secret"));
    }

    #[tokio::test]
    async fn disabled_history_is_an_explicit_no_store_503() {
        let response = history_requests_from(None, HistoryRequestsQuery::default()).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store")
        );
        assert_eq!(
            response_json(response).await["error"],
            "persistent history is disabled"
        );
    }

    #[tokio::test]
    async fn request_list_is_bounded_and_detail_preserves_event_order() {
        let store = sqlite_store().await;
        for (id, timestamp) in [("old", 1), ("new", 2)] {
            store
                .begin_request(request(id, timestamp))
                .await
                .expect("begin");
        }
        for seq in [2, 1] {
            store
                .append_event(EventRow {
                    request_id: "new".to_string(),
                    seq,
                    ts_ms: seq,
                    hop: "safe-hop".to_string(),
                    kind: "safe-kind".to_string(),
                    payload: Some(format!(r#"{{"seq":{seq}}}"#)),
                    bytes: None,
                })
                .await
                .expect("event");
        }

        let list = history_requests_from(
            Some(Arc::clone(&store)),
            HistoryRequestsQuery { limit: Some(1) },
        )
        .await;
        assert_eq!(list.status(), StatusCode::OK);
        let list = response_json(list).await;
        assert_eq!(list["limit"], 1);
        assert_eq!(list["requests"].as_array().unwrap().len(), 1);
        assert_eq!(list["requests"][0]["id"], "new");

        let clamped = history_requests_from(
            Some(Arc::clone(&store)),
            HistoryRequestsQuery {
                limit: Some(i64::MAX),
            },
        )
        .await;
        assert_eq!(response_json(clamped).await["limit"], MAX_REQUEST_LIMIT);

        let detail = history_request_detail_from(Some(store), "new".to_string()).await;
        assert_eq!(detail.status(), StatusCode::OK);
        let detail = response_json(detail).await;
        assert_eq!(detail["events"][0]["seq"], 1);
        assert_eq!(detail["events"][1]["seq"], 2);
        assert!(detail.get("secret").is_none());
    }

    #[tokio::test]
    async fn usage_validates_key_ids_and_does_not_accept_negative_time() {
        let store = sqlite_store().await;
        let too_long = "x".repeat(MAX_KEY_ID_BYTES + 1);
        let invalid_key = history_usage_from(
            Some(Arc::clone(&store)),
            HistoryUsageQuery {
                virtual_key_id: Some(too_long),
                since_ms: None,
                limit: None,
            },
        )
        .await;
        assert_eq!(invalid_key.status(), StatusCode::BAD_REQUEST);

        let invalid_time = history_usage_from(
            Some(store),
            HistoryUsageQuery {
                virtual_key_id: None,
                since_ms: Some(-1),
                limit: None,
            },
        )
        .await;
        assert_eq!(invalid_time.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn usage_and_metrics_return_only_persistence_dtos() {
        let store = sqlite_store().await;
        store
            .begin_request(request("done", 10))
            .await
            .expect("begin");
        store
            .finish_request(
                "done",
                RequestFinish {
                    status: "completed".to_string(),
                    completed_at_ms: 20,
                    input_tokens: Some(3),
                    output_tokens: Some(4),
                    ..RequestFinish::default()
                },
            )
            .await
            .expect("finish");
        store
            .record_backend_metrics(MetricSample {
                backend: "provider".to_string(),
                ts_ms: 10,
                data: r#"{"healthy":true}"#.to_string(),
            })
            .await
            .expect("metric");
        store
            .record_backend_metrics(MetricSample {
                backend: "provider-new".to_string(),
                ts_ms: 20,
                data: r#"{"healthy":false}"#.to_string(),
            })
            .await
            .expect("metric");

        let usage = history_usage_from(
            Some(Arc::clone(&store)),
            HistoryUsageQuery {
                since_ms: Some(0),
                ..HistoryUsageQuery::default()
            },
        )
        .await;
        let usage = response_json(usage).await;
        assert_eq!(usage["usage"][0]["input_tokens"], 3);
        assert_eq!(usage["since_ms"], 0);
        assert!(usage.get("api_keys").is_none());

        let metrics = history_metrics_from(
            Some(store),
            HistoryMetricsQuery {
                since_ms: Some(0),
                limit: Some(1),
            },
        )
        .await;
        let metrics = response_json(metrics).await;
        assert_eq!(metrics["samples"].as_array().unwrap().len(), 1);
        assert_eq!(metrics["samples"][0]["backend"], "provider-new");
        assert_eq!(metrics["truncated"], true);
        assert!(metrics.get("users").is_none());
    }
}
