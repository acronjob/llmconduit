use crate::config::{
    AvailabilitySchedule, MeshWeekday, MeshWorkerConfig, MeshWorkerResourceConfig,
};
use crate::error::{AppError, AppResult};
use crate::mesh::capacity::CapacityGate;
use crate::mesh::identity::load_or_create;
use crate::mesh::io::{Admission, read_request_open, write_admission};
use crate::mesh::protocol::{
    AdmissionRejectCode, ENROLL_ALPN, EnrollRequest, EnrollResponse, Heartbeat, HubToWorker,
    ModelAdvertisement, PROTOCOL_VERSION, ResourceAdvertisement, ResourceRuntimeState, WORKER_ALPN,
    WorkerAdvertisement, WorkerToHub, read_control, write_control,
};
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use iroh::{Endpoint, EndpointAddr, EndpointId, TransportAddr};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant as TokioInstant, MissedTickBehavior};

#[derive(Debug)]
pub(super) struct WorkerRuntime {
    config: MeshWorkerConfig,
    resources: HashMap<String, Arc<LocalResource>>,
}

#[derive(Debug)]
struct LocalResource {
    config: MeshWorkerResourceConfig,
    gate: CapacityGate,
    models: Mutex<Vec<ModelAdvertisement>>,
    healthy: Mutex<bool>,
    revision: Mutex<u64>,
}

pub async fn run_worker(config: MeshWorkerConfig, join_key: Option<String>) -> AppResult<()> {
    let identity_path = config
        .identity_path
        .clone()
        .ok_or_else(|| AppError::bad_request("mesh.worker.identity_path is required"))?;
    let identity = load_or_create(&identity_path)
        .await
        .map_err(|err| AppError::internal(format!("failed to load mesh worker identity: {err}")))?;
    let controller = controller_addr(&config).await?;
    let endpoint = identity
        .bind_direct_endpoint(Vec::new())
        .await
        .map_err(|err| AppError::internal(format!("failed to bind mesh worker endpoint: {err}")))?;
    let runtime = Arc::new(WorkerRuntime::new(config).await);

    if let Some(join_key) = join_key.filter(|key| !key.trim().is_empty()) {
        enroll(&endpoint, controller.clone(), &runtime.config, join_key).await?;
    }

    reconnect_loop(endpoint, controller, runtime).await
}

impl WorkerRuntime {
    pub(super) async fn new(config: MeshWorkerConfig) -> Self {
        let mut resources = HashMap::new();
        let now = Utc::now();
        for resource in &config.resources {
            let limit = effective_capacity_at(&resource.availability, now);
            resources.insert(
                resource.id.clone(),
                Arc::new(LocalResource {
                    config: resource.clone(),
                    gate: CapacityGate::new(limit),
                    models: Mutex::new(Vec::new()),
                    healthy: Mutex::new(true),
                    revision: Mutex::new(0),
                }),
            );
        }
        Self { config, resources }
    }

    async fn advertisement(&self) -> WorkerAdvertisement {
        let mut resources = Vec::with_capacity(self.resources.len());
        for resource in self.resources.values() {
            resources.push(resource.advertisement().await);
        }
        WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: None,
            agent_version: crate::VERSION.to_string(),
            resources,
        }
    }

    async fn heartbeat(&self, sequence: u64) -> Heartbeat {
        let mut resources = Vec::with_capacity(self.resources.len());
        for resource in self.resources.values() {
            let snapshot = resource.gate.snapshot();
            let (limit, active) = (snapshot.limit, snapshot.active);
            let healthy = *resource.healthy.lock().await;
            resources.push(ResourceRuntimeState {
                resource_id: resource.config.id.clone(),
                effective_capacity: limit,
                worker_active_requests: active,
                accepting_requests: healthy && limit > active,
                healthy,
            });
        }
        Heartbeat {
            sequence,
            resources,
        }
    }

    async fn apply_schedule_updates(&self) -> Vec<ResourceAdvertisement> {
        let now = Utc::now();
        let mut updates = Vec::new();
        for resource in self.resources.values() {
            if let Some(update) = resource.apply_schedule(now).await {
                updates.push(update);
            }
        }
        updates
    }

    fn next_schedule_delay(&self) -> Duration {
        let now = Utc::now();
        self.resources
            .values()
            .filter_map(|resource| next_schedule_boundary(&resource.config.availability, now))
            .filter_map(|boundary| (boundary - now).to_std().ok())
            .min()
            .unwrap_or_else(|| Duration::from_secs(300))
            .clamp(Duration::from_secs(1), Duration::from_secs(300))
    }
}

impl LocalResource {
    async fn advertisement(&self) -> ResourceAdvertisement {
        let snapshot = self.gate.snapshot();
        let (limit, active) = (snapshot.limit, snapshot.active);
        let healthy = *self.healthy.lock().await;
        ResourceAdvertisement {
            resource_id: self.config.id.clone(),
            models: self.models.lock().await.clone(),
            availability: self.config.availability.clone(),
            effective_capacity: limit,
            accepting_requests: healthy && limit > active,
            healthy,
            revision: *self.revision.lock().await,
        }
    }

    async fn apply_schedule(&self, now: DateTime<Utc>) -> Option<ResourceAdvertisement> {
        let limit = effective_capacity_at(&self.config.availability, now);
        if self.gate.set_limit_if_changed(limit) {
            *self.revision.lock().await += 1;
            Some(self.advertisement().await)
        } else {
            None
        }
    }
}

async fn enroll(
    endpoint: &Endpoint,
    controller: EndpointAddr,
    _config: &MeshWorkerConfig,
    join_key: String,
) -> AppResult<()> {
    let connection = endpoint
        .connect(controller, ENROLL_ALPN)
        .await
        .map_err(|err| AppError::upstream(format!("mesh enrollment connect failed: {err}")))?;
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| AppError::upstream(format!("mesh enrollment stream failed: {err}")))?;
    let request = EnrollRequest {
        protocol_version: PROTOCOL_VERSION,
        join_key,
        node_name: None,
        agent_version: crate::VERSION.to_string(),
    };
    write_control(&mut send, &request).await?;
    let response: EnrollResponse = read_control(&mut recv).await?;
    match response {
        EnrollResponse::Accepted { .. } => Ok(()),
        EnrollResponse::Rejected { reason } => Err(AppError::upstream(format!(
            "mesh enrollment rejected: {reason}"
        ))),
    }
}

async fn reconnect_loop(
    endpoint: Endpoint,
    controller: EndpointAddr,
    runtime: Arc<WorkerRuntime>,
) -> AppResult<()> {
    let mut backoff = Duration::from_millis(250);
    loop {
        match endpoint.connect(controller.clone(), WORKER_ALPN).await {
            Ok(connection) => {
                let connected_at = TokioInstant::now();
                if let Err(err) = run_connected(connection, Arc::clone(&runtime)).await {
                    tracing::warn!(error = %err, "mesh worker connection ended");
                }
                if connected_at.elapsed()
                    >= Duration::from_secs(
                        runtime
                            .config
                            .heartbeat_interval_secs
                            .max(1)
                            .saturating_mul(2),
                    )
                {
                    backoff = Duration::from_millis(250);
                }
            }
            Err(err) => tracing::warn!(error = %err, "mesh worker connect failed"),
        }
        tokio::time::sleep(jittered_backoff(backoff)).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

fn jittered_backoff(backoff: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.subsec_nanos());
    let permille = 800 + nanos % 401;
    backoff.mul_f64(f64::from(permille) / 1000.0)
}

async fn run_connected(
    connection: iroh::endpoint::Connection,
    runtime: Arc<WorkerRuntime>,
) -> AppResult<()> {
    refresh_models(&runtime).await;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (update_tx, update_rx) = mpsc::channel(32);
    let mut tasks = JoinSet::new();
    tasks.spawn(control_task(
        connection.clone(),
        Arc::clone(&runtime),
        update_rx,
        shutdown_rx.clone(),
    ));
    tasks.spawn(request_task(
        connection.clone(),
        Arc::clone(&runtime),
        shutdown_rx.clone(),
    ));
    tasks.spawn(model_refresh_task(
        Arc::clone(&runtime),
        update_tx,
        shutdown_rx,
    ));
    let result = tokio::select! {
        reason = connection.closed() => Err(AppError::upstream(format!("mesh worker connection closed: {reason}"))),
        Some(joined) = tasks.join_next() => match joined {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(AppError::internal(format!("mesh worker task panicked: {err}"))),
        },
    };
    let _ = shutdown_tx.send(true);
    tasks.abort_all();
    result
}

async fn control_task(
    connection: iroh::endpoint::Connection,
    runtime: Arc<WorkerRuntime>,
    mut updates: mpsc::Receiver<ResourceAdvertisement>,
    mut shutdown: watch::Receiver<bool>,
) -> AppResult<()> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| AppError::upstream(format!("failed to open mesh control stream: {err}")))?;
    write_control(
        &mut send,
        &WorkerToHub::Hello(runtime.advertisement().await),
    )
    .await?;
    let mut sequence = 0;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(
        runtime.config.heartbeat_interval_secs.max(1),
    ));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let schedule_sleep = tokio::time::sleep(runtime.next_schedule_delay());
    tokio::pin!(schedule_sleep);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
            }
            frame = read_control::<_, HubToWorker>(&mut recv) => {
                match frame? {
                    HubToWorker::HelloAck { protocol_version } if protocol_version == PROTOCOL_VERSION => {}
                    HubToWorker::HelloAck { .. } => return Err(AppError::upstream("mesh controller protocol version mismatch")),
                    HubToWorker::Ping { nonce: _ } => {}
                    HubToWorker::Close { reason } => return Err(AppError::upstream(format!("mesh controller closed worker: {reason}"))),
                }
            }
            _ = heartbeat.tick() => {
                sequence += 1;
                write_control(&mut send, &WorkerToHub::Heartbeat(runtime.heartbeat(sequence).await)).await?;
            }
            _ = &mut schedule_sleep => {
                for update in runtime.apply_schedule_updates().await {
                    write_control(&mut send, &WorkerToHub::ResourceUpdate(update)).await?;
                }
                schedule_sleep.as_mut().reset(TokioInstant::now() + runtime.next_schedule_delay());
            }
            Some(update) = updates.recv() => {
                write_control(&mut send, &WorkerToHub::ResourceUpdate(update)).await?;
            }
        }
    }
    Ok(())
}

async fn model_refresh_task(
    runtime: Arc<WorkerRuntime>,
    updates: mpsc::Sender<ResourceAdvertisement>,
    mut shutdown: watch::Receiver<bool>,
) -> AppResult<()> {
    let refresh_secs = runtime
        .config
        .resources
        .iter()
        .map(|resource| resource.model_refresh_secs)
        .min()
        .unwrap_or(60)
        .max(1);
    let mut refresh = tokio::time::interval(Duration::from_secs(refresh_secs));
    refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);
    refresh.tick().await;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
            }
            _ = refresh.tick() => {
                for update in refresh_models(&runtime).await {
                    if updates.send(update).await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }
    Ok(())
}

async fn request_task(
    connection: iroh::endpoint::Connection,
    runtime: Arc<WorkerRuntime>,
    mut shutdown: watch::Receiver<bool>,
) -> AppResult<()> {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
            }
            stream = connection.accept_bi() => {
                let (send, recv) = stream.map_err(|err| {
                    AppError::upstream(format!("failed to accept mesh request stream: {err}"))
                })?;
                let runtime = Arc::clone(&runtime);
                tokio::spawn(async move {
                    if let Err(err) = handle_request(send, recv, runtime).await {
                        tracing::warn!(error = %err, "mesh worker request failed");
                    }
                });
            }
        }
    }
    Ok(())
}

pub(super) async fn handle_request(
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    runtime: Arc<WorkerRuntime>,
) -> AppResult<()> {
    let open = read_request_open(&mut recv).await?;
    if let Err(err) = crate::mesh::protocol::validate_request_open(&open) {
        write_admission(
            &mut send,
            &Admission::Rejected {
                code: AdmissionRejectCode::ProtocolError,
            },
        )
        .await?;
        return Err(AppError::bad_request(format!(
            "invalid mesh request preface: {err}"
        )));
    }
    let Some(resource) = runtime.resources.get(&open.resource_id).cloned() else {
        write_admission(
            &mut send,
            &Admission::Rejected {
                code: AdmissionRejectCode::UnknownResource,
            },
        )
        .await?;
        return Ok(());
    };
    if !*resource.healthy.lock().await {
        write_admission(
            &mut send,
            &Admission::Rejected {
                code: AdmissionRejectCode::ResourceUnhealthy,
            },
        )
        .await?;
        return Ok(());
    }
    let Some(_permit) = resource.gate.try_acquire() else {
        write_admission(
            &mut send,
            &Admission::Rejected {
                code: AdmissionRejectCode::CapacityExhausted,
            },
        )
        .await?;
        return Ok(());
    };
    let local = match TcpStream::connect(resource.config.target).await {
        Ok(local) => local,
        Err(err) => {
            *resource.healthy.lock().await = false;
            write_admission(
                &mut send,
                &Admission::Rejected {
                    code: AdmissionRejectCode::LocalConnectFailed,
                },
            )
            .await?;
            return Err(AppError::upstream(format!(
                "failed to connect local mesh resource: {err}"
            )));
        }
    };
    write_admission(&mut send, &Admission::Accepted).await?;
    let (mut local_read, mut local_write) = local.into_split();
    let (response_done_tx, response_done_rx) = tokio::sync::oneshot::channel();
    let response_stopped = send.stopped();
    let forward_request = async {
        tokio::io::copy(&mut recv, &mut local_write).await?;
        // Content-Length delimits the request. Keep the TCP write half alive while
        // the model generates: uvicorn interprets a client EOF as cancellation.
        let _ = response_done_rx.await;
        Ok::<(), std::io::Error>(())
    };
    let forward_response = async {
        let result = tokio::select! {
            result = async {
                tokio::io::copy(&mut local_read, &mut send).await?;
                tokio::io::AsyncWriteExt::shutdown(&mut send).await
            } => result,
            _ = response_stopped => Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "mesh response receiver closed",
            )),
        };
        let _ = response_done_tx.send(());
        result
    };
    tokio::try_join!(forward_request, forward_response)
        .map_err(|err| AppError::upstream(format!("mesh request forwarding failed: {err}")))?;
    Ok(())
}

async fn refresh_models(runtime: &WorkerRuntime) -> Vec<ResourceAdvertisement> {
    let mut updates = Vec::new();
    for resource in runtime.resources.values() {
        let (models, healthy) = match fetch_models(resource.config.target).await {
            Ok(models) => (
                filter_advertised_models(models, &resource.config.models),
                true,
            ),
            Err(err) => {
                tracing::warn!(
                    resource_id = %resource.config.id,
                    error = %err,
                    "mesh worker local model discovery failed"
                );
                (Vec::new(), false)
            }
        };
        let models_changed = {
            let mut current = resource.models.lock().await;
            let changed = *current != models;
            *current = models;
            changed
        };
        let health_changed = {
            let mut current = resource.healthy.lock().await;
            let changed = *current != healthy;
            *current = healthy;
            changed
        };
        if models_changed || health_changed {
            *resource.revision.lock().await += 1;
            updates.push(resource.advertisement().await);
        }
    }
    updates
}

fn filter_advertised_models(
    discovered: Vec<ModelAdvertisement>,
    configured: &[String],
) -> Vec<ModelAdvertisement> {
    if configured.is_empty() {
        return discovered;
    }
    let discovered_by_lowercase = discovered
        .into_iter()
        .map(|model| (model.id.to_ascii_lowercase(), model))
        .collect::<HashMap<_, _>>();
    let mut seen = HashSet::new();
    configured
        .iter()
        .filter_map(|id| {
            let normalized = id.to_ascii_lowercase();
            if !seen.insert(normalized.clone()) {
                return None;
            }
            discovered_by_lowercase
                .get(&normalized)
                .map(|model| ModelAdvertisement {
                    id: id.clone(),
                    context_limit: model.context_limit,
                })
        })
        .collect()
}

async fn fetch_models(target: SocketAddr) -> AppResult<Vec<ModelAdvertisement>> {
    let url = format!("http://{target}/v1/models");
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|err| {
            AppError::internal(format!("failed to build model discovery client: {err}"))
        })?
        .get(url)
        .send()
        .await
        .map_err(|err| AppError::upstream(format!("local model discovery failed: {err}")))?;
    let value = response
        .error_for_status()
        .map_err(|err| AppError::upstream(format!("local model discovery failed: {err}")))?
        .json::<serde_json::Value>()
        .await
        .map_err(|err| AppError::upstream(format!("invalid local model catalog: {err}")))?;
    let entries = value
        .get("data")
        .or_else(|| value.get("models"))
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(entries
        .into_iter()
        .filter_map(|entry| {
            let id = entry.get("id").and_then(serde_json::Value::as_str)?;
            let context_limit = entry
                .get("context_length")
                .or_else(|| entry.get("max_model_len"))
                .or_else(|| entry.get("max_context_length"))
                .and_then(serde_json::Value::as_i64)
                .filter(|value| *value > 0);
            Some(ModelAdvertisement {
                id: id.to_string(),
                context_limit,
            })
        })
        .collect())
}

async fn controller_addr(config: &MeshWorkerConfig) -> AppResult<EndpointAddr> {
    let endpoint_id = config
        .controller_endpoint_id
        .as_ref()
        .ok_or_else(|| AppError::bad_request("mesh.worker.controller_endpoint_id is required"))?;
    let endpoint_id = EndpointId::from_str(endpoint_id)
        .map_err(|err| AppError::bad_request(format!("invalid controller endpoint id: {err}")))?;
    let controller_addr = config
        .controller_addr
        .as_ref()
        .ok_or_else(|| AppError::bad_request("mesh.worker.controller_addr is required"))?;
    let addrs: Vec<TransportAddr> = tokio::net::lookup_host(controller_addr)
        .await
        .map_err(|err| AppError::bad_request(format!("invalid controller address: {err}")))?
        .map(TransportAddr::Ip)
        .collect();
    if addrs.is_empty() {
        return Err(AppError::bad_request(
            "mesh.worker.controller_addr resolved to no addresses",
        ));
    }
    Ok(EndpointAddr::from_parts(endpoint_id, addrs))
}

fn effective_capacity_at(schedule: &AvailabilitySchedule, now: DateTime<Utc>) -> u32 {
    for exception in schedule.exceptions.iter().rev() {
        let start = exception.start.with_timezone(&Utc);
        let end = exception.end.with_timezone(&Utc);
        if now >= start && now < end {
            return exception.capacity;
        }
    }

    let tz = schedule.timezone.parse::<Tz>().unwrap_or(chrono_tz::UTC);
    let local = now.with_timezone(&tz);
    let minute = local.hour() as u16 * 60 + local.minute() as u16;
    let weekday = mesh_weekday_from_chrono(local.weekday());
    let previous_weekday = previous_weekday(weekday);

    for window in &schedule.weekly {
        let Some(start) = local_time_minutes(window.start_local) else {
            continue;
        };
        let Some(end) = local_time_minutes(window.end_local) else {
            continue;
        };
        let today_matches = window.days.contains(&weekday);
        if start == end {
            if today_matches {
                return window.capacity;
            }
        } else if start < end {
            if today_matches && minute >= start && minute < end {
                return window.capacity;
            }
        } else if (today_matches && minute >= start)
            || (window.days.contains(&previous_weekday) && minute < end)
        {
            return window.capacity;
        }
    }

    schedule.default_capacity
}

fn next_schedule_boundary(
    schedule: &AvailabilitySchedule,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let tz = schedule.timezone.parse::<Tz>().unwrap_or(chrono_tz::UTC);
    let local_now = now.with_timezone(&tz);
    let mut best: Option<DateTime<Utc>> = None;

    for exception in &schedule.exceptions {
        for boundary in [
            exception.start.with_timezone(&Utc),
            exception.end.with_timezone(&Utc),
        ] {
            if boundary > now {
                best = Some(best.map_or(boundary, |current| current.min(boundary)));
            }
        }
    }

    let Some(base_date) =
        NaiveDate::from_ymd_opt(local_now.year(), local_now.month(), local_now.day())
    else {
        return best;
    };

    for day_offset in 0..=8 {
        let Some(date) = base_date.checked_add_days(chrono::Days::new(day_offset)) else {
            continue;
        };
        let weekday = mesh_weekday_from_chrono(date.weekday());
        for window in &schedule.weekly {
            if !window.days.contains(&weekday) {
                continue;
            }
            for (boundary_date, minutes) in [
                (Some(date), local_time_minutes(window.start_local)),
                (
                    if local_time_minutes(window.start_local) > local_time_minutes(window.end_local)
                    {
                        date.checked_add_days(chrono::Days::new(1))
                    } else {
                        Some(date)
                    },
                    local_time_minutes(window.end_local),
                ),
            ] {
                let (Some(boundary_date), Some(minutes)) = (boundary_date, minutes) else {
                    continue;
                };
                let Some(boundary) = local_boundary_to_utc(tz, boundary_date, minutes) else {
                    continue;
                };
                if boundary > now {
                    best = Some(best.map_or(boundary, |current| current.min(boundary)));
                }
            }
        }
    }

    best
}

fn local_boundary_to_utc(tz: Tz, date: NaiveDate, minutes: u16) -> Option<DateTime<Utc>> {
    let time = NaiveTime::from_hms_opt(u32::from(minutes / 60), u32::from(minutes % 60), 0)?;
    let local = date.and_time(time);
    tz.from_local_datetime(&local)
        .earliest()
        .or_else(|| tz.from_local_datetime(&local).latest())
        .map(|value| value.with_timezone(&Utc))
}

fn local_time_minutes(time: crate::config::LocalTime) -> Option<u16> {
    let value = serde_json::to_value(time).ok()?;
    let text = value.as_str()?;
    let (hour, minute) = text.split_once(':')?;
    let hour = hour.parse::<u16>().ok()?;
    let minute = minute.parse::<u16>().ok()?;
    (hour < 24 && minute < 60).then_some(hour * 60 + minute)
}

fn mesh_weekday_from_chrono(day: chrono::Weekday) -> MeshWeekday {
    match day {
        chrono::Weekday::Mon => MeshWeekday::Mon,
        chrono::Weekday::Tue => MeshWeekday::Tue,
        chrono::Weekday::Wed => MeshWeekday::Wed,
        chrono::Weekday::Thu => MeshWeekday::Thu,
        chrono::Weekday::Fri => MeshWeekday::Fri,
        chrono::Weekday::Sat => MeshWeekday::Sat,
        chrono::Weekday::Sun => MeshWeekday::Sun,
    }
}

fn previous_weekday(day: MeshWeekday) -> MeshWeekday {
    match day {
        MeshWeekday::Mon => MeshWeekday::Sun,
        MeshWeekday::Tue => MeshWeekday::Mon,
        MeshWeekday::Wed => MeshWeekday::Tue,
        MeshWeekday::Thu => MeshWeekday::Wed,
        MeshWeekday::Fri => MeshWeekday::Thu,
        MeshWeekday::Sat => MeshWeekday::Fri,
        MeshWeekday::Sun => MeshWeekday::Sat,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_capacity_gate_drains_after_shrink() {
        let gate = CapacityGate::new(2);
        let a = gate.try_acquire().expect("a");
        let b = gate.try_acquire().expect("b");
        assert!(gate.try_acquire().is_none());
        gate.set_limit(1);
        drop(a);
        assert!(gate.try_acquire().is_none());
        drop(b);
        assert!(gate.try_acquire().is_some());
    }

    #[test]
    fn configured_models_filter_discovered_catalog() {
        let discovered = vec![
            ModelAdvertisement {
                id: "safe-model".to_string(),
                context_limit: Some(4096),
            },
            ModelAdvertisement {
                id: "surprise-model".to_string(),
                context_limit: Some(8192),
            },
        ];

        let filtered = filter_advertised_models(discovered, &["SAFE-MODEL".to_string()]);

        assert_eq!(
            filtered,
            vec![ModelAdvertisement {
                id: "SAFE-MODEL".to_string(),
                context_limit: Some(4096),
            }]
        );
    }

    fn schedule(json: serde_json::Value) -> AvailabilitySchedule {
        serde_json::from_value(json).expect("schedule")
    }

    #[test]
    fn schedule_handles_weekly_cross_midnight_and_exceptions() {
        let schedule = schedule(serde_json::json!({
            "timezone": "America/Chicago",
            "default_capacity": 1,
            "weekly": [
                {"days":["mon"], "start_local":"22:00", "end_local":"02:00", "capacity": 4}
            ],
            "exceptions": [
                {"start":"2026-09-22T01:00:00-05:00", "end":"2026-09-22T01:30:00-05:00", "capacity": 0}
            ]
        }));
        let active = DateTime::parse_from_rfc3339("2026-09-22T04:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let inactive = DateTime::parse_from_rfc3339("2026-09-23T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let exception = DateTime::parse_from_rfc3339("2026-09-22T06:15:00Z")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(effective_capacity_at(&schedule, active), 4);
        assert_eq!(effective_capacity_at(&schedule, inactive), 1);
        assert_eq!(effective_capacity_at(&schedule, exception), 0);
    }

    #[test]
    fn schedule_uses_iana_timezone_across_dst_transition() {
        let schedule = schedule(serde_json::json!({
            "timezone": "America/Chicago",
            "default_capacity": 1,
            "weekly": [
                {"days":["sun"], "start_local":"01:00", "end_local":"04:00", "capacity": 8}
            ],
            "exceptions": []
        }));
        let before_jump = DateTime::parse_from_rfc3339("2026-03-08T07:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let after_jump = DateTime::parse_from_rfc3339("2026-03-08T08:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let after_window = DateTime::parse_from_rfc3339("2026-03-08T09:30:00Z")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(effective_capacity_at(&schedule, before_jump), 8);
        assert_eq!(effective_capacity_at(&schedule, after_jump), 8);
        assert_eq!(effective_capacity_at(&schedule, after_window), 1);
    }
}
