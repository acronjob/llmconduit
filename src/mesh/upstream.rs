use crate::error::{AppError, AppResult, FailoverDisposition};
use crate::mesh::io::{
    Admission, MAX_HTTP_CHUNK_BYTES, MAX_HTTP_CHUNK_LINE_BYTES, RequestOpen, content_length,
    drain_http_chunk, is_chunked, read_admission, read_http_response_head,
    split_http_response_head, write_request_open,
};
use crate::mesh::protocol::{AdmissionRejectCode, ModelAdvertisement, REQUEST_PROTOCOL_VERSION};
use crate::mesh::registry::MeshRegistry;
use crate::models::chat::{ChatCompletionChunk, ChatCompletionRequest};
use crate::upstream::{
    BackendCandidate, BackendCandidatePlan, BackendChatRequest, BackendFinalizationPolicies,
    ProviderHealth, ProviderInventoryEntry, ProviderStatus, UpstreamClient, UpstreamModelEntry,
    UpstreamModelsResponse, UpstreamStream, finalize_request_for_backend,
    offload_redacted_upstream_request_bytes, sanitize_chat_request, stamp_header_byte,
};
use async_stream::try_stream;
use async_trait::async_trait;
use axum::body::Bytes;
use futures::StreamExt;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio_util::bytes::BytesMut;
use uuid::Uuid;

const MESH_ERROR_BODY_READ_LIMIT: usize = 16 * 1024;
const MESH_ERROR_BODY_DISPLAY_LIMIT: usize = 500;

#[derive(Debug, Clone)]
pub struct MeshUpstreamClient {
    registry: Arc<MeshRegistry>,
    finalization_policies: BackendFinalizationPolicies,
    flatten_content: bool,
    max_sse_frame_bytes: usize,
    flow_store: crate::dashboard_flow::DashboardFlowStore,
}

impl MeshUpstreamClient {
    pub(crate) fn new(
        registry: Arc<MeshRegistry>,
        finalization_policies: BackendFinalizationPolicies,
        flatten_content: bool,
        max_sse_frame_bytes: usize,
        flow_store: crate::dashboard_flow::DashboardFlowStore,
    ) -> Self {
        Self {
            registry,
            finalization_policies,
            flatten_content,
            max_sse_frame_bytes,
            flow_store,
        }
    }
}

#[async_trait]
impl UpstreamClient for MeshUpstreamClient {
    async fn stream_chat_completion(
        &self,
        backend: &BackendChatRequest,
    ) -> AppResult<UpstreamStream> {
        let mut backend = backend.clone();
        finalize_request_for_backend(&mut backend, &self.finalization_policies);
        let model = backend.request.model.clone();
        let request = sanitize_chat_request(backend.request, self.flatten_content);
        if self.flow_store.is_enabled()
            && let Some(response_id) = backend.response_id.as_deref()
        {
            self.flow_store.set_upstream(
                response_id,
                Some("mesh://workers".to_string()),
                Some(model.clone()),
                Some(crate::dashboard_flow::capture_body_from_value(&request)),
            );
        }
        if let Some(capture) = backend.capture.as_ref() {
            match offload_redacted_upstream_request_bytes(&request).await {
                Some(redacted) => capture.write_upstream_request(&redacted),
                None => capture.mark_upstream_request_redaction_failed(),
            }
        }
        let request = build_chat_http_request(&request)?;
        let mut excluded = HashSet::new();
        let mut last_error = None;
        loop {
            let authorization = backend.authorization.clone();
            let route = backend.authorization_route.clone();
            let Some(reservation) = self.registry.reserve_excluding_where(
                &model,
                &excluded,
                |endpoint_id, resource_id| {
                    authorization.allows_candidate(
                        &format!("mesh:{endpoint_id}"),
                        route.as_deref().or(Some(resource_id)),
                        &model,
                        backend.endpoint,
                    )
                },
            ) else {
                let any_authorized =
                    self.registry
                        .has_candidate_where(&model, |endpoint_id, resource_id| {
                            authorization.allows_candidate(
                                &format!("mesh:{endpoint_id}"),
                                route.as_deref().or(Some(resource_id)),
                                &model,
                                backend.endpoint,
                            )
                        });
                if !any_authorized && self.registry.has_candidate_where(&model, |_, _| true) {
                    return Err(AppError::forbidden(
                        "no authorized mesh resource is available for this request",
                    ));
                }
                return Err(last_error.unwrap_or_else(|| {
                    AppError::upstream_with_disposition(
                        "mesh capacity exhausted",
                        FailoverDisposition::FailoverNoCooldown,
                    )
                }));
            };
            let candidate = (
                reservation.resource.endpoint_id,
                reservation.resource.resource_id.clone(),
            );
            if let Some(capture) = backend.capture.as_ref() {
                capture.reset_upstream_response();
            }
            match open_mesh_http_stream(reservation, request.clone(), self.max_sse_frame_bytes)
                .await
            {
                Ok(stream) => {
                    stamp_header_byte(backend.serving.as_ref());
                    let mut stream =
                        parse_sse_stream(stream, self.max_sse_frame_bytes, backend.capture.clone());
                    match stream.next().await {
                        Some(Ok(first)) => {
                            if let Some(serving) = &backend.serving {
                                serving.set_provider(format!("mesh:{}", candidate.0));
                                serving.set_model_served_final(model.clone());
                            }
                            return Ok(Box::pin(
                                futures::stream::once(async move { Ok(first) }).chain(stream),
                            ));
                        }
                        Some(Err(err))
                            if err.failover_disposition() != FailoverDisposition::Terminal =>
                        {
                            tracing::warn!(
                                endpoint_id = %candidate.0,
                                resource_id = %candidate.1,
                                error = %err,
                                "mesh attempt failed before its first SSE chunk; trying another resource"
                            );
                            excluded.insert(candidate);
                            last_error = Some(err);
                        }
                        Some(Err(err)) => return Err(err),
                        None => {
                            excluded.insert(candidate);
                            last_error = Some(AppError::upstream(
                                "mesh worker response ended before its first SSE chunk",
                            ));
                        }
                    }
                }
                Err(err) if err.failover_disposition() != FailoverDisposition::Terminal => {
                    tracing::warn!(
                        endpoint_id = %candidate.0,
                        resource_id = %candidate.1,
                        error = %err,
                        "mesh attempt failed before the first response body; trying another resource"
                    );
                    excluded.insert(candidate);
                    last_error = Some(err);
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn list_models(&self) -> AppResult<UpstreamModelsResponse> {
        let body = mesh_models_list_body(self.registry.model_catalog());
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        Ok(UpstreamModelsResponse {
            status: http::StatusCode::OK,
            headers,
            body: Bytes::from(serde_json::to_vec(&body).map_err(|err| {
                AppError::internal(format!("failed to encode mesh model catalog: {err}"))
            })?),
        })
    }

    async fn supported_model_catalog(&self) -> AppResult<Vec<UpstreamModelEntry>> {
        Ok(self
            .registry
            .model_catalog()
            .into_iter()
            .map(|model| UpstreamModelEntry {
                id: model.id,
                context_limit: model.context_limit,
            })
            .collect())
    }

    async fn provider_inventory(&self) -> AppResult<Vec<ProviderInventoryEntry>> {
        Ok(self.registry.provider_inventory())
    }

    async fn backend_candidate_plan(&self, requested_model: &str) -> BackendCandidatePlan {
        let candidates = self
            .registry
            .model_catalog()
            .into_iter()
            .filter(|model| model.id.eq_ignore_ascii_case(requested_model))
            .map(|model| BackendCandidate {
                model: model.id,
                context_limit: model.context_limit,
            })
            .collect();
        BackendCandidatePlan { candidates }
    }

    fn provider_base_url(&self) -> String {
        "mesh://workers".to_string()
    }

    fn provider_health(&self) -> Vec<ProviderHealth> {
        let mut by_provider = BTreeMap::<String, (bool, HashSet<String>)>::new();
        for entry in self.registry.provider_inventory() {
            let aggregate = by_provider
                .entry(entry.provider_id)
                .or_insert_with(|| (false, HashSet::new()));
            aggregate.0 |= entry.healthy;
            aggregate
                .1
                .extend(entry.models.into_iter().map(|model| model.id));
        }
        by_provider
            .into_iter()
            .map(|(id, (healthy, models))| ProviderHealth {
                name: id.clone(),
                id: id.clone(),
                route: None,
                base_url: id.replacen("mesh:", "mesh://", 1),
                status: if healthy {
                    ProviderStatus::Healthy
                } else {
                    ProviderStatus::Down
                },
                cooling_until_ms: None,
                last_error: (!healthy).then(|| "mesh worker is unavailable".to_string()),
                served_count: 0,
                failover_count: 0,
                consecutive_failures: 0,
                catalog_fetched_ms: None,
                catalog_size: Some(models.len() as u64),
            })
            .collect()
    }

    fn model_catalog_cache_ttl(&self) -> std::time::Duration {
        std::time::Duration::from_secs(1)
    }
}

struct MeshHttpStream {
    _reservation: crate::mesh::registry::MeshReservation,
    recv: iroh::endpoint::RecvStream,
    write_task: Option<tokio::task::JoinHandle<AppResult<()>>>,
    remaining_content_length: Option<usize>,
    chunked: bool,
    chunk_buf: BytesMut,
    max_chunk_bytes: usize,
}

async fn open_mesh_http_stream(
    reservation: crate::mesh::registry::MeshReservation,
    http_request: Vec<u8>,
    max_chunk_bytes: usize,
) -> AppResult<MeshHttpStream> {
    let connection = reservation
        .resource
        .connection
        .clone()
        .ok_or_else(|| AppError::upstream("mesh reservation has no live worker connection"))?;
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| AppError::upstream(format!("failed to open mesh request stream: {err}")))?;
    let open = RequestOpen {
        protocol_version: REQUEST_PROTOCOL_VERSION,
        request_id: Uuid::new_v4(),
        resource_id: reservation.resource.resource_id.clone(),
    };
    write_request_open(&mut send, &open).await?;

    let write_task = tokio::spawn(async move {
        send.write_all(&http_request).await.map_err(|err| {
            AppError::upstream(format!("failed to write mesh HTTP request: {err}"))
        })?;
        send.shutdown()
            .await
            .map_err(|err| AppError::upstream(format!("failed to finish mesh HTTP request: {err}")))
    });

    match read_admission(&mut recv).await? {
        Admission::Accepted => {}
        Admission::Rejected { code } => return Err(admission_error(code)),
    }

    let head = read_http_response_head(&mut recv, 64 * 1024).await?;
    let (status, headers) = split_http_response_head(&head)?;
    let mut stream = MeshHttpStream {
        _reservation: reservation,
        recv,
        write_task: Some(write_task),
        remaining_content_length: content_length(&headers),
        chunked: is_chunked(&headers),
        chunk_buf: BytesMut::new(),
        max_chunk_bytes,
    };
    if !status.is_success() {
        let body = stream.read_body_prefix(MESH_ERROR_BODY_READ_LIMIT).await?;
        stream.finish_writer().await?;
        let message = mesh_upstream_error_message(status, &body);
        if matches!(
            status,
            http::StatusCode::BAD_REQUEST
                | http::StatusCode::PAYLOAD_TOO_LARGE
                | http::StatusCode::UNSUPPORTED_MEDIA_TYPE
                | http::StatusCode::UNPROCESSABLE_ENTITY
        ) {
            return Err(AppError::upstream_with_disposition(
                message,
                FailoverDisposition::Terminal,
            ));
        }
        return Err(AppError::upstream(message));
    }

    Ok(stream)
}

fn mesh_upstream_error_message(status: http::StatusCode, body: &[u8]) -> String {
    let prefix = format!("mesh worker local upstream returned {status}");
    if body.is_empty() {
        return prefix;
    }
    let body = String::from_utf8_lossy(body);
    format!(
        "{prefix}: {}",
        crate::upstream::redact_and_truncate_error_body(body.trim(), MESH_ERROR_BODY_DISPLAY_LIMIT,)
    )
}

fn admission_error(code: AdmissionRejectCode) -> AppError {
    match code {
        AdmissionRejectCode::CapacityExhausted
        | AdmissionRejectCode::ResourceUnhealthy
        | AdmissionRejectCode::LocalConnectFailed => AppError::upstream_with_disposition(
            format!("mesh worker rejected request: {code:?}"),
            FailoverDisposition::FailoverNoCooldown,
        ),
        other => AppError::upstream(format!("mesh worker rejected request: {other:?}")),
    }
}

fn build_chat_http_request(request: &ChatCompletionRequest) -> AppResult<Vec<u8>> {
    let body = serde_json::to_vec(request)
        .map_err(|err| AppError::internal(format!("failed to encode mesh chat request: {err}")))?;
    let mut bytes = Vec::with_capacity(body.len() + 256);
    bytes.extend_from_slice(b"POST /v1/chat/completions HTTP/1.1\r\n");
    bytes.extend_from_slice(b"host: llmconduit-mesh-worker\r\n");
    bytes.extend_from_slice(b"content-type: application/json\r\n");
    bytes.extend_from_slice(format!("content-length: {}\r\n", body.len()).as_bytes());
    bytes.extend_from_slice(b"accept: text/event-stream\r\n");
    bytes.extend_from_slice(b"connection: close\r\n\r\n");
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

fn parse_sse_stream(
    mut stream: MeshHttpStream,
    max_frame_bytes: usize,
    capture: Option<Arc<crate::turn_capture::TurnCaptureState>>,
) -> UpstreamStream {
    let s = try_stream! {
        let mut capture_guard = capture.clone().map(MeshCaptureGuard::new);
        let mut capture_sink = capture.as_ref().and_then(|capture| capture.upstream_response_sink());
        let mut capture_started = false;
        let mut frame_guard = crate::sse_guard::SseFrameGuard::new(max_frame_bytes);
        let mut event = String::new();
        let mut line_buf = Vec::new();
        let mut saw_done = false;
        let mut saw_finish_reason = false;
        while let Some(bytes) = stream.next_body_bytes().await? {
            if let Some(capture) = capture.as_ref() {
                if !capture_started {
                    capture.mark_upstream_response_streamed();
                    capture_started = true;
                }
                if let Some(sink) = capture_sink.as_mut() {
                    match std::future::poll_fn(|cx| sink.poll_reserve(cx)).await {
                        Ok(()) => {
                            if sink.send(bytes.to_vec()).is_err() {
                                capture.mark_upstream_response_degraded();
                                capture_sink = None;
                            }
                        }
                        Err(_) => {
                            capture.mark_upstream_response_degraded();
                            capture_sink = None;
                        }
                    }
                }
            }
            frame_guard.accept(&bytes)?;
            line_buf.extend_from_slice(&bytes);
            while let Some(line_end) = line_buf.iter().position(|byte| *byte == b'\n') {
                let mut line = line_buf.drain(..=line_end).collect::<Vec<_>>();
                line.pop();
                if line.last() == Some(&b'\r') { line.pop(); }
                let line = std::str::from_utf8(&line)
                    .map_err(|err| AppError::upstream(format!("invalid UTF-8 in mesh SSE event: {err}")))?;
                if line.is_empty() {
                    if !event.is_empty() {
                        let data = std::mem::take(&mut event);
                        if data == "[DONE]" {
                            saw_done = true;
                        } else {
                            let chunk = parse_chat_completion_chunk(&data)?;
                            saw_finish_reason |= chunk
                                .choices
                                .iter()
                                .any(|choice| choice.finish_reason.is_some());
                            yield chunk;
                        }
                    }
                } else if let Some(data) = line.strip_prefix("data:") {
                    if !event.is_empty() {
                        event.push('\n');
                    }
                    event.push_str(data.trim_start());
                }
            }
        }
        if !line_buf.is_empty() {
            if line_buf.last() == Some(&b'\r') { line_buf.pop(); }
            let line = std::str::from_utf8(&line_buf)
                .map_err(|err| AppError::upstream(format!("invalid UTF-8 in mesh SSE event: {err}")))?;
            if let Some(data) = line.strip_prefix("data:") {
                if !event.is_empty() {
                    event.push('\n');
                }
                event.push_str(data.trim_start());
            }
        }
        if event == "[DONE]" {
            saw_done = true;
        } else if !event.is_empty() {
            let chunk = parse_chat_completion_chunk(&event)?;
            saw_finish_reason |= chunk
                .choices
                .iter()
                .any(|choice| choice.finish_reason.is_some());
            yield chunk;
        }
        frame_guard.finish()?;
        stream.finish_writer().await?;
        if !saw_done {
            Err(AppError::upstream(
                "mesh upstream SSE ended before the [DONE] marker",
            ))?;
        }
        if !saw_finish_reason {
            Err(AppError::upstream(
                "mesh upstream SSE ended without a terminal finish_reason",
            ))?;
        }
        if let Some(capture) = capture.as_ref() {
            capture.mark_upstream_response_streamed();
        }
        drop(capture_sink);
        if let Some(guard) = capture_guard.as_mut() {
            guard.clean = true;
        }
    };
    Box::pin(s)
}

struct MeshCaptureGuard {
    capture: Arc<crate::turn_capture::TurnCaptureState>,
    clean: bool,
}

impl MeshCaptureGuard {
    fn new(capture: Arc<crate::turn_capture::TurnCaptureState>) -> Self {
        Self {
            capture,
            clean: false,
        }
    }
}

impl Drop for MeshCaptureGuard {
    fn drop(&mut self) {
        self.capture.upstream_response_done(!self.clean);
    }
}

impl MeshHttpStream {
    async fn finish_writer(&mut self) -> AppResult<()> {
        let Some(write_task) = self.write_task.take() else {
            return Ok(());
        };
        write_task
            .await
            .map_err(|err| AppError::internal(format!("mesh writer task failed: {err}")))?
    }

    async fn next_body_bytes(&mut self) -> AppResult<Option<Bytes>> {
        if self.chunked {
            loop {
                if let Some(chunk) = drain_http_chunk(&mut self.chunk_buf, self.max_chunk_bytes)? {
                    if chunk.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(Bytes::from(chunk)));
                }
                let mut buf = [0_u8; 8192];
                let read = tokio::io::AsyncReadExt::read(&mut self.recv, &mut buf)
                    .await
                    .map_err(|err| {
                        AppError::upstream(format!("failed to read mesh response body: {err}"))
                    })?;
                if read == 0 {
                    return Err(AppError::upstream(
                        "mesh chunked response ended before its terminator",
                    ));
                }
                let max_buffered = MAX_HTTP_CHUNK_LINE_BYTES
                    .checked_add(2)
                    .and_then(|value| {
                        value.checked_add(self.max_chunk_bytes.min(MAX_HTTP_CHUNK_BYTES))
                    })
                    .and_then(|value| value.checked_add(2))
                    .ok_or_else(|| AppError::upstream("mesh HTTP chunk buffer limit overflow"))?;
                if self.chunk_buf.len().saturating_add(read) > max_buffered {
                    return Err(AppError::upstream(format!(
                        "mesh HTTP chunk buffer exceeds {max_buffered} byte limit"
                    )));
                }
                self.chunk_buf.extend_from_slice(&buf[..read]);
            }
        }

        if matches!(self.remaining_content_length, Some(0)) {
            return Ok(None);
        }
        let mut buf = [0_u8; 8192];
        let read = tokio::io::AsyncReadExt::read(&mut self.recv, &mut buf)
            .await
            .map_err(|err| {
                AppError::upstream(format!("failed to read mesh response body: {err}"))
            })?;
        if read == 0 {
            if let Some(remaining) = self.remaining_content_length
                && remaining > 0
            {
                return Err(AppError::upstream(format!(
                    "mesh response body ended with {remaining} Content-Length bytes remaining"
                )));
            }
            return Ok(None);
        }
        if let Some(remaining) = self.remaining_content_length.as_mut() {
            if read > *remaining {
                return Err(AppError::upstream(format!(
                    "mesh response body exceeded Content-Length by {} bytes",
                    read - *remaining
                )));
            }
            *remaining -= read;
        }
        Ok(Some(Bytes::copy_from_slice(&buf[..read])))
    }

    async fn read_body_prefix(&mut self, limit: usize) -> AppResult<Vec<u8>> {
        let mut body = Vec::new();
        while body.len() < limit {
            let Some(bytes) = self.next_body_bytes().await? else {
                break;
            };
            let remaining = limit - body.len();
            body.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
        }
        Ok(body)
    }
}

impl Drop for MeshHttpStream {
    fn drop(&mut self) {
        if let Some(write_task) = self.write_task.take() {
            write_task.abort();
        }
    }
}

fn parse_chat_completion_chunk(data: &str) -> AppResult<ChatCompletionChunk> {
    match serde_json::from_str::<ChatCompletionChunk>(data) {
        Ok(chunk) => Ok(chunk),
        Err(first_error) => {
            let mut value = serde_json::from_str::<Value>(data).map_err(|_| {
                AppError::upstream(format!(
                    "failed to parse upstream chat chunk: {first_error}"
                ))
            })?;
            if !normalize_sparse_tool_call_types(&mut value) {
                return Err(AppError::upstream(format!(
                    "failed to parse upstream chat chunk: {first_error}"
                )));
            }
            serde_json::from_value(value).map_err(|err| {
                AppError::upstream(format!("failed to parse upstream chat chunk: {err}"))
            })
        }
    }
}

fn normalize_sparse_tool_call_types(value: &mut Value) -> bool {
    let Some(choices) = value.get_mut("choices").and_then(Value::as_array_mut) else {
        return false;
    };
    let mut changed = false;
    for choice in choices {
        let Some(tool_calls) = choice
            .get_mut("delta")
            .and_then(|delta| delta.get_mut("tool_calls"))
            .and_then(Value::as_array_mut)
        else {
            continue;
        };
        for tool_call in tool_calls {
            let Some(object) = tool_call.as_object_mut() else {
                continue;
            };
            if !object.contains_key("type") {
                object.insert("type".to_string(), Value::String("function".to_string()));
                changed = true;
            }
        }
    }
    changed
}

fn mesh_models_list_body(catalog: Vec<ModelAdvertisement>) -> Value {
    let mut by_id = BTreeMap::<String, Option<i64>>::new();
    for model in catalog {
        by_id.entry(model.id).or_insert(model.context_limit);
    }
    let data: Vec<_> = by_id
        .into_iter()
        .map(|(id, context_limit)| {
            let mut object = serde_json::Map::new();
            object.insert("id".to_string(), Value::String(id));
            object.insert("object".to_string(), Value::String("model".to_string()));
            if let Some(limit) = context_limit {
                object.insert("context_length".to_string(), Value::from(limit));
            }
            Value::Object(object)
        })
        .collect();
    serde_json::json!({ "object": "list", "data": data })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AvailabilitySchedule, MeshWorkerConfig, MeshWorkerResourceConfig};
    use crate::mesh::protocol::{
        PROTOCOL_VERSION, ResourceAdvertisement, WORKER_ALPN, WorkerAdvertisement,
    };
    use crate::mesh::worker::{WorkerRuntime, handle_request};
    use iroh::endpoint::presets;
    use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey, TransportAddr};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn read_test_request(socket: &mut TcpStream, buf: &mut [u8]) {
        let mut request = Vec::new();
        let header_end = loop {
            let read = socket.read(buf).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&buf[..read]);
            if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers = std::str::from_utf8(&request[..header_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length: ")
                    .map(str::to_owned)
            })
            .unwrap()
            .trim()
            .parse::<usize>()
            .unwrap();
        while request.len() < header_end + content_length {
            let read = socket.read(buf).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&buf[..read]);
        }
    }

    #[test]
    fn sparse_tool_calls_are_normalized() {
        let chunk = r#"{"id":"x","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{}"}}]}}]}"#;
        let parsed = parse_chat_completion_chunk(chunk).expect("chunk");
        assert_eq!(
            parsed.choices[0].delta.tool_calls.as_ref().unwrap()[0].kind,
            "function"
        );
    }

    #[test]
    fn mesh_upstream_error_includes_redacted_body() {
        let message = mesh_upstream_error_message(
            http::StatusCode::BAD_REQUEST,
            br#"{"error":{"message":"Unexpected reasoning effort high","image_url":"data:image/png;base64,secret"}}"#,
        );

        assert!(message.contains("Unexpected reasoning effort high"));
        assert!(message.contains("400 Bad Request"));
        assert!(!message.contains("base64,secret"));
    }

    #[test]
    fn mesh_upstream_error_without_body_keeps_status_message() {
        assert_eq!(
            mesh_upstream_error_message(http::StatusCode::BAD_GATEWAY, b""),
            "mesh worker local upstream returned 502 Bad Gateway"
        );
    }

    #[test]
    fn mesh_catalog_dedupes_models() {
        let body = mesh_models_list_body(vec![
            ModelAdvertisement {
                id: "a".to_string(),
                context_limit: Some(42),
            },
            ModelAdvertisement {
                id: "a".to_string(),
                context_limit: None,
            },
        ]);
        assert_eq!(body["data"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn failover_wrapper_preserves_mesh_resource_inventory() {
        let registry = Arc::new(MeshRegistry::new(Duration::from_secs(30)));
        let endpoint = SecretKey::generate().public();
        registry.register_test(
            endpoint,
            WorkerAdvertisement {
                protocol_version: PROTOCOL_VERSION,
                node_name: Some("workstation".into()),
                agent_version: "test".into(),
                resources: vec![ResourceAdvertisement {
                    resource_id: "local-vllm".into(),
                    models: vec![ModelAdvertisement {
                        id: "local-model".into(),
                        context_limit: Some(32_768),
                    }],
                    availability: AvailabilitySchedule {
                        timezone: "America/Chicago".into(),
                        default_capacity: 3,
                        weekly: Vec::new(),
                        exceptions: Vec::new(),
                    },
                    effective_capacity: 3,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                }],
            },
        );
        let mesh = MeshUpstreamClient::new(
            registry,
            BackendFinalizationPolicies::default(),
            true,
            1024 * 1024,
            crate::dashboard_flow::DashboardFlowStore::disabled(),
        );
        let wrapped = crate::upstream::FailoverUpstreamClient::new(
            vec![crate::upstream::FailoverUpstreamProvider::new(
                "mesh",
                mesh,
                None,
                None,
                serde_json::Map::new(),
            )],
            Duration::from_secs(30),
        );

        let inventory = wrapped.provider_inventory().await.expect("inventory");
        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].provider_id, format!("mesh:{endpoint}"));
        assert_eq!(inventory[0].resource_id.as_deref(), Some("local-vllm"));
        assert_eq!(inventory[0].capacity_limit, Some(3));
        assert_eq!(
            inventory[0].availability.as_ref().unwrap().default_capacity,
            3
        );
    }

    #[tokio::test]
    async fn streams_chat_completion_over_one_quic_request_stream() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let target = listener.local_addr().unwrap();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        let local_server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0_u8; 2048];
            let header_end = loop {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buf[..read]);
                if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(str::to_owned)
                })
                .unwrap()
                .trim()
                .parse::<usize>()
                .unwrap();
            while request.len() < header_end + content_length {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buf[..read]);
            }
            request_tx
                .send(request[header_end..header_end + content_length].to_vec())
                .unwrap();

            match tokio::time::timeout(Duration::from_millis(30), socket.read(&mut buf)).await {
                Err(_) => {}
                Ok(Ok(0)) => panic!("worker half-closed local TCP before the response"),
                Ok(Ok(read)) => panic!("unexpected {read} request bytes after Content-Length"),
                Ok(Err(err)) => panic!("local TCP read failed before response: {err}"),
            }

            socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n").await.unwrap();
            socket.write_all(b"data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n").await.unwrap();
            socket.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            socket.write_all(b"data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n").await.unwrap();
            drop(socket);

            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let header_end = loop {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buf[..read]);
                if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(str::to_owned)
                })
                .unwrap()
                .trim()
                .parse::<usize>()
                .unwrap();
            while request.len() < header_end + content_length {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buf[..read]);
            }
            let error_body = br#"{"error":{"message":"Unexpected reasoning effort high"}}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        error_body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(error_body).await.unwrap();
            drop(socket);

            let (mut socket, _) = listener.accept().await.unwrap();
            read_test_request(&mut socket, &mut buf).await;
            let event = b"data: {\"id\":\"chunked-cut\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\n";
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            socket
                .write_all(format!("{:x}\r\n", event.len()).as_bytes())
                .await
                .unwrap();
            socket.write_all(event).await.unwrap();
            socket.write_all(b"\r\n").await.unwrap();
            drop(socket);

            let (mut socket, _) = listener.accept().await.unwrap();
            read_test_request(&mut socket, &mut buf).await;
            let event = b"data: {\"id\":\"length-cut\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\n";
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        event.len() + 32
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(event).await.unwrap();
            drop(socket);

            let (mut socket, _) = listener.accept().await.unwrap();
            read_test_request(&mut socket, &mut buf).await;
            socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\ndata: {\"id\":\"missing-done\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"stop\"}]}\n\n").await.unwrap();
            drop(socket);

            let (mut socket, _) = listener.accept().await.unwrap();
            read_test_request(&mut socket, &mut buf).await;
            socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\ndata: {\"id\":\"missing-finish\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\ndata: [DONE]\n\n").await.unwrap();
            drop(socket);

            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let header_end = loop {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buf[..read]);
                if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(str::to_owned)
                })
                .unwrap()
                .trim()
                .parse::<usize>()
                .unwrap();
            while request.len() < header_end + content_length {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buf[..read]);
            }
            socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\ndata: {\"id\":\"cancel\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"started\"}}]}\n\n").await.unwrap();
            socket.flush().await.unwrap();
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            let _ = cancel_tx.send(());
        });

        let controller_key = SecretKey::generate();
        let controller_id = controller_key.public();
        let controller = Endpoint::builder(presets::Minimal)
            .secret_key(controller_key)
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![WORKER_ALPN.to_vec()])
            .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .unwrap()
            .bind()
            .await
            .unwrap();
        let controller_addr = EndpointAddr::from_parts(
            controller_id,
            controller
                .bound_sockets()
                .into_iter()
                .map(TransportAddr::Ip),
        );
        let worker_key = SecretKey::generate();
        let worker_id = worker_key.public();
        let worker = Endpoint::builder(presets::Minimal)
            .secret_key(worker_key)
            .relay_mode(RelayMode::Disabled)
            .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .unwrap()
            .bind()
            .await
            .unwrap();
        let worker_connect = worker.connect(controller_addr, WORKER_ALPN);
        let controller_accept = async {
            controller
                .accept()
                .await
                .unwrap()
                .accept()
                .unwrap()
                .await
                .unwrap()
        };
        let (worker_connection, controller_connection) =
            tokio::join!(worker_connect, controller_accept);
        let worker_connection = worker_connection.unwrap();
        let _worker_keepalive = worker_connection.clone();

        let availability = AvailabilitySchedule {
            default_capacity: 1,
            ..Default::default()
        };
        let resource_config = MeshWorkerResourceConfig {
            id: "primary".to_string(),
            target,
            models: Vec::new(),
            model_refresh_secs: 60,
            availability: availability.clone(),
        };
        let runtime = Arc::new(
            WorkerRuntime::new(MeshWorkerConfig {
                resources: vec![resource_config],
                ..Default::default()
            })
            .await,
        );
        let worker_task = tokio::spawn(async move {
            for _ in 0..7 {
                let (send, recv) = worker_connection.accept_bi().await.unwrap();
                let _ = handle_request(send, recv, Arc::clone(&runtime)).await;
            }
        });

        let registry = Arc::new(MeshRegistry::new(Duration::from_secs(30)));
        registry.register(
            worker_id,
            controller_connection,
            WorkerAdvertisement {
                protocol_version: PROTOCOL_VERSION,
                node_name: None,
                agent_version: "test".to_string(),
                resources: vec![ResourceAdvertisement {
                    resource_id: "primary".to_string(),
                    models: vec![ModelAdvertisement {
                        id: "mesh-model".to_string(),
                        context_limit: Some(4096),
                    }],
                    availability,
                    effective_capacity: 1,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                }],
            },
        );
        let client = MeshUpstreamClient::new(
            registry,
            BackendFinalizationPolicies::default(),
            true,
            1024 * 1024,
            crate::dashboard_flow::DashboardFlowStore::disabled(),
        );
        let request: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
            "model": "mesh-model",
            "messages": [{"role": "user", "content": "ping"}],
            "stream": true
        }))
        .unwrap();
        let mut stream = client
            .stream_chat_completion(&BackendChatRequest::new(request, None, None, None))
            .await
            .unwrap();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.choices[0].delta.content.as_deref(), Some("hello"));
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.choices[0].delta.content.as_deref(), Some(" world"));
        assert!(stream.next().await.is_none());

        let forwarded: Value = serde_json::from_slice(&request_rx.await.unwrap()).unwrap();
        assert_eq!(forwarded["model"], "mesh-model");
        assert_eq!(forwarded["messages"][0]["content"], "ping");

        let request: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
            "model": "mesh-model",
            "messages": [{"role": "user", "content": "bad effort"}],
            "stream": true
        }))
        .unwrap();
        let error = match client
            .stream_chat_completion(&BackendChatRequest::new(request, None, None, None))
            .await
        {
            Ok(_) => panic!("400 response should fail before returning a stream"),
            Err(error) => error,
        };
        assert_eq!(error.failover_disposition(), FailoverDisposition::Terminal);
        assert!(
            error
                .to_string()
                .contains("Unexpected reasoning effort high")
        );

        for (prompt, expected_error) in [
            (
                "truncate chunked",
                "chunked response ended before its terminator",
            ),
            ("truncate content length", "response body ended with"),
        ] {
            let request: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
                "model": "mesh-model",
                "messages": [{"role": "user", "content": prompt}],
                "stream": true
            }))
            .unwrap();
            let mut stream = client
                .stream_chat_completion(&BackendChatRequest::new(request, None, None, None))
                .await
                .unwrap();
            assert_eq!(
                stream.next().await.unwrap().unwrap().choices[0]
                    .delta
                    .content
                    .as_deref(),
                Some("partial")
            );
            let error = stream
                .next()
                .await
                .expect("truncation error")
                .expect_err("truncated body must fail");
            assert!(error.to_string().contains(expected_error), "{error}");
        }

        for (prompt, expected_error) in [
            ("missing done", "[DONE]"),
            ("missing finish", "finish_reason"),
        ] {
            let request: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
                "model": "mesh-model",
                "messages": [{"role": "user", "content": prompt}],
                "stream": true
            }))
            .unwrap();
            let mut stream = client
                .stream_chat_completion(&BackendChatRequest::new(request, None, None, None))
                .await
                .unwrap();
            assert_eq!(
                stream.next().await.unwrap().unwrap().choices[0]
                    .delta
                    .content
                    .as_deref(),
                Some("partial")
            );
            let error = stream
                .next()
                .await
                .expect("terminal protocol error")
                .expect_err("incomplete terminal protocol must fail");
            assert!(error.to_string().contains(expected_error), "{error}");
        }

        let request: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
            "model": "mesh-model",
            "messages": [{"role": "user", "content": "cancel me"}],
            "stream": true
        }))
        .unwrap();
        let mut cancelled_stream = client
            .stream_chat_completion(&BackendChatRequest::new(request, None, None, None))
            .await
            .unwrap();
        assert_eq!(
            cancelled_stream.next().await.unwrap().unwrap().choices[0]
                .delta
                .content
                .as_deref(),
            Some("started")
        );
        drop(cancelled_stream);
        tokio::time::timeout(Duration::from_secs(2), cancel_rx)
            .await
            .expect("worker should close local vLLM connection after hub cancellation")
            .unwrap();
        local_server.await.unwrap();
        worker_task.await.unwrap();
    }
}
