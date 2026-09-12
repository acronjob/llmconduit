use clap::Parser;
use llmconduit::AppOptions;
use llmconduit::ControlPlaneRuntime;
use llmconduit::build_app_with_gateway_control_plane_runtime;
use llmconduit::cli::Cli;
use llmconduit::cli::Commands;
use llmconduit::cli::migrate_config_file;
use llmconduit::cli::resolve_config_path;
use llmconduit::cli::run_configure_flow;
use llmconduit::config::Config;
use llmconduit::config::PersistedConfig;
use llmconduit::config::load_persisted_config;
use llmconduit::control_plane::ControlPlaneConfig;
use llmconduit::control_plane::ControlPlaneSection;
use llmconduit::control_plane::OperationalConfig;
use llmconduit::control_plane::OperationalRoutePlan;
use llmconduit::control_plane::StorageBackend;
use llmconduit::control_plane::StorageBootstrap;
use llmconduit::control_plane::UnknownModelPolicy;
use llmconduit::control_plane_store::JsonlWriter;
use llmconduit::control_plane_store::LegacyOperationalRead;
use llmconduit::control_plane_store::PersistenceQueue;
use llmconduit::control_plane_store::PersistenceStore;
use llmconduit::control_plane_store::PersistenceWriter;
use llmconduit::control_plane_store::SqlStore;
use llmconduit::log_rotation::cleanup_scoped;
use llmconduit::raw::RawOutput;
use llmconduit::request_log::analyze_request_log;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    init_tracing(command_uses_dedicated_terminal(&cli.command));
    let app_options = AppOptions {
        with_debug_ui: cli.with_debug_ui,
    };

    match cli.command {
        Some(Commands::Configure { config }) => {
            let path = resolve_config_path(config)?;
            let _ = run_configure_flow(path.clone())?;
            println!("Wrote configuration to {}", path.display());
            Ok(())
        }
        Some(Commands::MigrateConfig { config }) => {
            let path = resolve_config_path(config)?;
            let migrated = migrate_config_file(&path)?;
            if migrated {
                println!(
                    "Migrated legacy control-plane configuration in {}",
                    path.display()
                );
            } else {
                println!(
                    "Configuration is current; securely rewrote {}",
                    path.display()
                );
            }
            Ok(())
        }
        Some(Commands::AnalyzeLog {
            config,
            path,
            pairs,
        }) => {
            let config_path = resolve_config_path(config)?;
            let config = Config::from_env_and_file(Some(&config_path))?;
            let log_path = path.or(config.upstream_request_log_path).ok_or_else(|| {
                format!(
                    "no request log path configured; pass --path or set upstream_request_log_path in {}",
                    config_path.display()
                )
            })?;
            let report = analyze_request_log(&log_path, pairs)?;
            println!("{report}");
            Ok(())
        }
        Some(Commands::Start {
            config,
            raw,
            model_route,
        }) => {
            let path = resolve_config_path(config)?;
            let loaded = load_runtime_config(&path, &model_route)?;
            run_server(path, loaded, raw.then(RawOutput::stdout), app_options).await
        }
        None => {
            let path = resolve_config_path(None)?;
            let loaded = load_runtime_config(&path, &[])?;
            run_server(path, loaded, None, app_options).await
        }
    }
}

struct LoadedRuntimeConfig {
    /// Upstream-owned bootstrap document before any operational profile overlay.
    /// Retained so a legacy SQL control plane can replace YAML operational state
    /// without reconstructing modern upstream fields.
    gateway: PersistedConfig,
    route_specs: Vec<String>,
    config: Config,
    routes: Vec<OperationalRoutePlan>,
    unknown_model_policy: UnknownModelPolicy,
    client_auth_required: bool,
    client_auth_specs: Vec<llmconduit::client_auth::VirtualKeySpec>,
    storage: StorageBootstrap,
    conversation_id_header: String,
    sessions: llmconduit::harness::SessionsBootstrap,
    metrics: llmconduit::upstream_metrics::MetricsBootstrap,
}

/// Load the namespaced YAML control plane without changing TOML's upstream
/// read-only contract. Environment and CLI route overrides retain their normal
/// precedence after the operational profile overlay is materialized.
fn load_runtime_config(
    path: &std::path::Path,
    route_specs: &[String],
) -> Result<LoadedRuntimeConfig, String> {
    if llmconduit::config::path_is_toml(path) {
        let mut storage = StorageBootstrap::default();
        let mut client_auth_required = false;
        let mut conversation_id_header =
            llmconduit::control_plane::DEFAULT_CONVERSATION_ID_HEADER.to_string();
        apply_control_plane_env_overrides(
            &mut storage,
            &mut client_auth_required,
            &mut conversation_id_header,
        )?;
        let gateway = load_persisted_config(path)?;
        return Ok(LoadedRuntimeConfig {
            config: Config::from_persisted_env_and_routes(gateway.clone(), route_specs)?,
            gateway,
            route_specs: route_specs.to_vec(),
            routes: Vec::new(),
            unknown_model_policy: UnknownModelPolicy::Passthrough,
            client_auth_required,
            client_auth_specs: Vec::new(),
            storage,
            conversation_id_header,
            sessions: llmconduit::harness::SessionsBootstrap::default(),
            metrics: llmconduit::upstream_metrics::MetricsBootstrap::default(),
        });
    }

    let document = if path.exists() {
        let source = std::fs::read_to_string(path)
            .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
        ControlPlaneConfig::from_yaml_str(&source)?
    } else {
        ControlPlaneConfig::from_gateway(
            llmconduit::config::PersistedConfig::default(),
            ControlPlaneSection::default(),
        )?
    };
    let materialized = document.materialize()?;
    if document.migrated_legacy_root() {
        tracing::warn!(
            path = %path.display(),
            "legacy root-level control-plane config was migrated in memory only; run `llmconduit migrate-config --config <path>` to atomically persist digested keys and the namespaced schema"
        );
    }
    let section = document.control_plane();
    let (unknown_model_policy, client_auth_specs) = match &section.operational {
        Some(operational) => {
            operational.validate_persisted_key_digests()?;
            (
                operational.unknown_model_policy,
                operational.client_auth_specs()?,
            )
        }
        None => (UnknownModelPolicy::Passthrough, Vec::new()),
    };
    let mut storage = section.storage.clone();
    let mut client_auth_required = section.auth.require;
    let mut conversation_id_header = section.auth.conversation_id_header().to_string();
    apply_control_plane_env_overrides(
        &mut storage,
        &mut client_auth_required,
        &mut conversation_id_header,
    )?;
    Ok(LoadedRuntimeConfig {
        gateway: document.gateway().clone(),
        route_specs: route_specs.to_vec(),
        config: Config::from_persisted_env_and_routes(materialized.gateway, route_specs)?,
        routes: materialized.routes,
        unknown_model_policy,
        client_auth_required,
        client_auth_specs,
        storage,
        conversation_id_header,
        sessions: section.sessions.clone(),
        metrics: section.metrics.clone(),
    })
}

async fn run_server(
    path: std::path::PathBuf,
    loaded: LoadedRuntimeConfig,
    raw_output: Option<RawOutput>,
    app_options: AppOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut loaded = loaded;
    let (runtime, client_auth, flush_queue) = prepare_control_plane_runtime(&mut loaded).await?;
    let bind_addr = loaded.config.bind_addr;
    let metrics_config = loaded.metrics.clone();
    run_debug_log_cleanup(&loaded.config, &loaded.routes).await;
    let (app, gateway) = build_app_with_gateway_control_plane_runtime(
        loaded.config,
        raw_output,
        app_options,
        loaded.routes,
        loaded.unknown_model_policy,
        client_auth,
        runtime,
    )?;
    spawn_persistent_backend_metrics(Arc::clone(&gateway));
    spawn_upstream_metrics_scraper(Arc::clone(&gateway), metrics_config);
    let listener = TcpListener::bind(bind_addr).await?;
    log_listening(bind_addr);
    log_debug_ui_status(&gateway, app_options, bind_addr);
    tracing::info!("using config file {}", path.display());

    const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
    const FLUSH_TIMEOUT: Duration = Duration::from_secs(10);
    let (shutdown_started, mut shutdown_observer) = tokio::sync::watch::channel(false);
    let server = std::future::IntoFuture::into_future(
        axum::serve(listener, app).with_graceful_shutdown(async move {
            shutdown_signal().await;
            let _ = shutdown_started.send(true);
        }),
    );
    tokio::pin!(server);
    let serve_result = tokio::select! {
        result = &mut server => result,
        changed = shutdown_observer.changed() => {
            if changed.is_err() {
                (&mut server).await
            } else {
                tokio::select! {
                    result = &mut server => result,
                    () = tokio::time::sleep(DRAIN_TIMEOUT) => {
                        tracing::warn!(timeout_secs = DRAIN_TIMEOUT.as_secs(), "graceful connection drain deadline exceeded; forcing shutdown");
                        Ok(())
                    }
                    () = wait_for_shutdown_signal() => {
                        tracing::warn!("second shutdown signal received; forcing shutdown");
                        Ok(())
                    }
                }
            }
        }
    };
    if let Some(queue) = flush_queue {
        match tokio::time::timeout(FLUSH_TIMEOUT, queue.flush()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "failed to flush persistence queue during shutdown");
            }
            Err(_) => {
                tracing::warn!(
                    timeout_secs = FLUSH_TIMEOUT.as_secs(),
                    "persistence queue flush deadline exceeded"
                );
            }
        }
        let stats = queue.stats();
        tracing::info!(
            accepted = stats.accepted,
            dropped_full = stats.dropped_full,
            dropped_closed = stats.dropped_closed,
            write_failures = stats.write_failures,
            "persistence queue stopped"
        );
    }
    serve_result?;
    Ok(())
}

async fn prepare_control_plane_runtime(
    loaded: &mut LoadedRuntimeConfig,
) -> Result<
    (
        ControlPlaneRuntime,
        Option<llmconduit::client_auth::ClientAuth>,
        Option<PersistenceQueue>,
    ),
    String,
> {
    if loaded.storage.queue_capacity > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(format!(
            "control_plane.storage.queue_capacity must be at most {}",
            tokio::sync::Semaphore::MAX_PERMITS
        ));
    }
    let capacity = NonZeroUsize::new(loaded.storage.queue_capacity)
        .ok_or("control_plane.storage.queue_capacity must be greater than zero")?;
    let retention_days = NonZeroU64::new(loaded.storage.retention_days)
        .ok_or("control_plane.storage.retention_days must be greater than zero")?;
    let mut persistence_store: Option<Arc<dyn PersistenceStore>> = None;
    let persistence_queue = match loaded.storage.backend {
        StorageBackend::None => None,
        StorageBackend::Jsonl => {
            let dir = loaded
                .storage
                .jsonl_dir
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or("control_plane.storage.jsonl_dir is required for the jsonl backend")?;
            let writer: Arc<dyn PersistenceWriter> =
                Arc::new(JsonlWriter::new_with_retention(dir, retention_days)?);
            tracing::info!(directory = %dir, retention_days = retention_days.get(), "daily-rotated JSONL persistence enabled");
            Some(PersistenceQueue::spawn(writer, capacity))
        }
        StorageBackend::Sqlite | StorageBackend::Postgres => {
            let url = loaded
                .storage
                .url
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    format!(
                        "control_plane.storage.url is required for the {} backend",
                        storage_backend_name(loaded.storage.backend)
                    )
                })?;
            let sql = Arc::new(match loaded.storage.backend {
                StorageBackend::Sqlite => SqlStore::connect_sqlite(url).await?,
                StorageBackend::Postgres => SqlStore::connect_postgres(url).await?,
                StorageBackend::None | StorageBackend::Jsonl => unreachable!(),
            });
            if let Some(legacy) = sql.load_legacy_operational().await? {
                let operational = match legacy {
                    LegacyOperationalRead::SettingsDocument(document) => {
                        OperationalConfig::from_stored(&document).map_err(|error| {
                            format!("invalid legacy settings.operational document: {error}")
                        })?
                    }
                    LegacyOperationalRead::Relational(operational) => operational,
                };
                apply_legacy_sql_operational(loaded, operational)?;
                tracing::info!(
                    "loaded pre-upstream SQL operational routing/auth state with database precedence"
                );
            }
            let store: Arc<dyn PersistenceStore> = sql.clone();
            spawn_sql_retention(Arc::clone(&store), retention_days);
            let writer: Arc<dyn PersistenceWriter> = sql;
            persistence_store = Some(store);
            tracing::info!(
                backend = storage_backend_name(loaded.storage.backend),
                "SQL persistence enabled and migrations applied"
            );
            Some(PersistenceQueue::spawn(writer, capacity))
        }
    };
    let client_auth = if loaded.client_auth_required || !loaded.client_auth_specs.is_empty() {
        Some(llmconduit::client_auth::ClientAuth::from_specs(
            loaded.client_auth_required,
            loaded.client_auth_specs.clone(),
        )?)
    } else {
        None
    };
    let harness_detector = llmconduit::harness::HarnessDetector::from_config(
        &loaded.sessions,
        &loaded.conversation_id_header,
    )
    .map_err(|error| error.to_string())?;
    tracing::info!(
        profiles = ?harness_detector.profile_names(),
        infer_sub_sessions = harness_detector.infer_sub_sessions(),
        "harness detection profiles compiled"
    );
    let session_linker = Arc::new(llmconduit::sessions::SessionLinker::new(
        harness_detector.infer_sub_sessions(),
    ));
    let runtime = ControlPlaneRuntime {
        persistence_store,
        persistence_queue: persistence_queue.clone(),
        persistence_keep_media: loaded.storage.keep_media,
        conversation_id_header: loaded.conversation_id_header.clone(),
        harness_detector: Arc::new(harness_detector),
        session_linker,
    };
    Ok((runtime, client_auth, persistence_queue))
}

/// The old SQL control plane was authoritative after its first seed. Recover it
/// read-only and replace only operational profiles/routes/keys/policy; all modern
/// upstream-owned bootstrap fields remain sourced from the current config file.
fn apply_legacy_sql_operational(
    loaded: &mut LoadedRuntimeConfig,
    operational: OperationalConfig,
) -> Result<(), String> {
    operational.validate()?;
    let gateway = operational.apply_profiles_to(&loaded.gateway)?;
    let routes = operational.route_plans(&gateway)?;
    let config = Config::from_persisted_env_and_routes(gateway, &loaded.route_specs)?;
    let client_auth_specs = operational.client_auth_specs()?;
    loaded.config = config;
    loaded.routes = routes;
    loaded.unknown_model_policy = operational.unknown_model_policy;
    loaded.client_auth_specs = client_auth_specs;
    Ok(())
}

fn apply_control_plane_env_overrides(
    storage: &mut StorageBootstrap,
    client_auth_required: &mut bool,
    conversation_id_header: &mut String,
) -> Result<(), String> {
    if let Some(value) = nonempty_env("LLMCONDUIT_STORAGE_BACKEND") {
        storage.backend = match value.to_ascii_lowercase().as_str() {
            "none" => StorageBackend::None,
            "jsonl" => StorageBackend::Jsonl,
            "sqlite" => StorageBackend::Sqlite,
            "postgres" | "postgresql" => StorageBackend::Postgres,
            _ => {
                return Err(format!(
                    "invalid LLMCONDUIT_STORAGE_BACKEND '{value}' (expected none|jsonl|sqlite|postgres)"
                ));
            }
        };
    }
    if let Some(value) = nonempty_env("LLMCONDUIT_DATABASE_URL") {
        storage.url = Some(value);
    }
    if let Some(value) = nonempty_env("LLMCONDUIT_STORAGE_JSONL_DIR") {
        storage.jsonl_dir = Some(value);
    }
    if let Some(value) = nonempty_env("LLMCONDUIT_PERSISTENCE_QUEUE_CAPACITY") {
        storage.queue_capacity = value.parse::<usize>().map_err(|_| {
            "LLMCONDUIT_PERSISTENCE_QUEUE_CAPACITY must be a positive integer".to_string()
        })?;
    }
    if let Some(value) = nonempty_env("LLMCONDUIT_PERSISTENCE_RETENTION_DAYS") {
        storage.retention_days = value.parse::<u64>().map_err(|_| {
            "LLMCONDUIT_PERSISTENCE_RETENTION_DAYS must be a positive integer".to_string()
        })?;
    }
    if let Some(value) = nonempty_env("LLMCONDUIT_REQUIRE_AUTH") {
        *client_auth_required = value
            .parse::<bool>()
            .map_err(|_| "LLMCONDUIT_REQUIRE_AUTH must be true or false".to_string())?;
    }
    if let Some(value) = nonempty_env("LLMCONDUIT_CONVERSATION_ID_HEADER") {
        *conversation_id_header = value;
    }
    axum::http::HeaderName::from_bytes(conversation_id_header.as_bytes())
        .map_err(|_| format!("invalid conversation id header name '{conversation_id_header}'"))?;
    if llmconduit::control_plane::is_sensitive_conversation_header(conversation_id_header) {
        return Err(format!(
            "conversation id header '{conversation_id_header}' is a sensitive credential carrier"
        ));
    }
    Ok(())
}

fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

const fn storage_backend_name(backend: StorageBackend) -> &'static str {
    match backend {
        StorageBackend::None => "none",
        StorageBackend::Jsonl => "jsonl",
        StorageBackend::Sqlite => "sqlite",
        StorageBackend::Postgres => "postgres",
    }
}

async fn shutdown_signal() {
    wait_for_shutdown_signal().await;
    tracing::info!("shutdown signal received; draining active connections");
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %error, "failed to install Ctrl-C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::warn!(error = %error, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

fn spawn_sql_retention(store: Arc<dyn PersistenceStore>, retention_days: NonZeroU64) {
    tokio::spawn(async move {
        const RETENTION_INTERVAL: Duration = Duration::from_secs(60 * 60);
        loop {
            let age_ms = retention_days.get().saturating_mul(24 * 60 * 60 * 1_000);
            let cutoff = chrono::Utc::now()
                .timestamp_millis()
                .saturating_sub(i64::try_from(age_ms).unwrap_or(i64::MAX));
            if let Err(error) = store.prune_request_history(cutoff).await {
                tracing::warn!(error = %error, "failed to prune durable request history");
            }
            match store.prune_orphan_blobs().await {
                Ok(removed) if removed > 0 => {
                    tracing::info!(removed, "pruned unreferenced content blobs");
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(error = %error, "failed to prune unreferenced content blobs");
                }
            }
            if let Err(error) = store.prune_backend_metrics(cutoff).await {
                tracing::warn!(error = %error, "failed to prune durable backend metrics");
            }
            tokio::time::sleep(RETENTION_INTERVAL).await;
        }
    });
}

fn spawn_persistent_backend_metrics(gateway: Arc<llmconduit::engine::Gateway>) {
    let Some(store) = gateway.persistence_store() else {
        return;
    };
    tokio::spawn(async move {
        const SAMPLE_INTERVAL: Duration = Duration::from_secs(60);
        let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let timestamp = chrono::Utc::now().timestamp_millis();
            for provider in gateway.upstream_health() {
                // Persist operational counters/status only. Base URLs and raw
                // provider errors may contain credentials or response content.
                let data = serde_json::json!({
                    "kind": "health",
                    "name": provider.name,
                    "route": provider.route,
                    "status": provider.status,
                    "cooling_until_ms": provider.cooling_until_ms,
                    "served_count": provider.served_count,
                    "failover_count": provider.failover_count,
                    "consecutive_failures": provider.consecutive_failures,
                    "catalog_fetched_ms": provider.catalog_fetched_ms,
                    "catalog_size": provider.catalog_size,
                });
                let sample = llmconduit::control_plane_store::MetricSample {
                    backend: provider.id,
                    ts_ms: timestamp,
                    data: data.to_string(),
                };
                if let Err(error) = store.record_backend_metrics(sample).await {
                    tracing::warn!(error = %error, "failed to persist backend health sample");
                    break;
                }
            }
        }
    });
}

/// Scrape every backend's Prometheus `/metrics` (vLLM / SGLang) on the
/// configured interval and persist a per-model sample per backend. Backends
/// that do not answer are retried with backoff; the request path is never
/// touched. Requires a SQL store (samples share the health-sample table).
fn spawn_upstream_metrics_scraper(
    gateway: Arc<llmconduit::engine::Gateway>,
    config: llmconduit::upstream_metrics::MetricsBootstrap,
) {
    if !config.enabled {
        tracing::info!("upstream metrics scraping disabled by configuration");
        return;
    }
    let Some(store) = gateway.persistence_store() else {
        tracing::info!("upstream metrics scraping needs a SQL store; skipping");
        return;
    };
    let interval_secs = config.scrape_interval_secs.max(1);
    tokio::spawn(async move {
        /// Skip a backend for this many intervals after repeated failures.
        const BACKOFF_INTERVALS: u32 = 20;
        const FAILURES_BEFORE_BACKOFF: u32 = 3;
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(error = %error, "upstream metrics scraper could not build an HTTP client");
                return;
            }
        };
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut failures: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
        let mut skip_until: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        let mut tick: u64 = 0;
        loop {
            interval.tick().await;
            tick += 1;
            for provider in gateway.upstream_health() {
                let override_cfg = config.backends.get(&provider.name);
                if override_cfg.and_then(|cfg| cfg.enabled) == Some(false) {
                    continue;
                }
                if skip_until
                    .get(&provider.id)
                    .is_some_and(|until| *until > tick)
                {
                    continue;
                }
                let url = match override_cfg.and_then(|cfg| cfg.url.clone()) {
                    Some(url) => url,
                    None => {
                        match llmconduit::upstream_metrics::derive_metrics_url(&provider.base_url) {
                            Some(url) => url,
                            None => continue,
                        }
                    }
                };
                let scraped_at_ms = chrono::Utc::now().timestamp_millis();
                let outcome = async {
                    let response = client.get(&url).send().await.map_err(|e| e.to_string())?;
                    if !response.status().is_success() {
                        return Err(format!("status {}", response.status()));
                    }
                    let text = response.text().await.map_err(|e| e.to_string())?;
                    llmconduit::upstream_metrics::parse_exposition(
                        &provider.id,
                        &text,
                        scraped_at_ms,
                    )
                    .map_err(|e| e.to_string())
                }
                .await;
                match outcome {
                    Ok(sample) => {
                        failures.remove(&provider.id);
                        let data = match serde_json::to_string(&sample) {
                            Ok(data) => data,
                            Err(error) => {
                                tracing::warn!(error = %error, backend = %provider.id, "could not encode upstream metrics sample");
                                continue;
                            }
                        };
                        if let Err(error) = store
                            .record_backend_metrics(llmconduit::control_plane_store::MetricSample {
                                backend: provider.id.clone(),
                                ts_ms: scraped_at_ms,
                                data,
                            })
                            .await
                        {
                            tracing::warn!(error = %error, backend = %provider.id, "failed to persist upstream metrics sample");
                        }
                    }
                    Err(error) => {
                        let count = failures.entry(provider.id.clone()).or_insert(0);
                        *count += 1;
                        if *count == 1 {
                            tracing::info!(backend = %provider.id, %url, error = %error, "upstream metrics scrape failed");
                        }
                        if *count >= FAILURES_BEFORE_BACKOFF {
                            skip_until
                                .insert(provider.id.clone(), tick + u64::from(BACKOFF_INTERVALS));
                            *count = 0;
                            tracing::info!(backend = %provider.id, "upstream metrics scrape backing off");
                        }
                    }
                }
            }
        }
    });
}

/// Log the startup banner with embedded build provenance (version, commit,
/// dirty flag, UTC build time) so a running process is traceable to its source.
fn log_listening(bind_addr: impl std::fmt::Display) {
    tracing::info!(
        "llmconduit {} commit={} dirty={} built={} listening on {bind_addr}",
        env!("CARGO_PKG_VERSION"),
        llmconduit::GIT_HASH,
        llmconduit::GIT_DIRTY,
        llmconduit::BUILD_TIME,
    );
}

/// Log the debug-UI / dashboard availability honestly: when `--with-debug-ui`
/// is set, the D7 startup decision may have REFUSED to register the protected
/// routes (non-loopback bind without a token + validated https origin). The
/// gateway holds the auth context iff the routes registered, so we key the
/// message off `dashboard_auth().is_some()` rather than the flag alone — and the
/// precise refusal reason was already logged by `build_app_*` at WARN.
fn log_debug_ui_status(
    gateway: &llmconduit::engine::Gateway,
    options: AppOptions,
    bind_addr: impl std::fmt::Display,
) {
    if !options.with_debug_ui {
        return;
    }
    if gateway.dashboard_auth().is_some() {
        tracing::info!("debug UI + dashboard available at http://{bind_addr}/debug and /dashboard");
    } else {
        tracing::warn!(
            "--with-debug-ui set but /debug and /dashboard were NOT registered \
             (see the dashboard auth WARN above)"
        );
    }
}

/// Spawn opt-in age-based cleanup of debug/request-log dump files. No-op unless
/// `debug_log_max_age_hours` is set. Cleanup runs on the blocking pool, never
/// blocking serve startup. The artifact/dump prune spans every configured log
/// directory; the destructive orphan `.work/` sweep is scoped to `turn_capture_dir`
/// ALONE (F1f review r1 — turn capture is the sole creator of `.work/<id>/` subdirs,
/// so the sweep must never touch a request-log dir).
async fn run_debug_log_cleanup(config: &Config, routes: &[OperationalRoutePlan]) {
    let max_age_hours = config.debug_log_max_age_hours;
    if max_age_hours.is_none() {
        return;
    }
    let mut files = active_request_log_paths(config);
    for path in routes
        .iter()
        .flat_map(|route| &route.providers)
        .filter_map(|provider| provider.request_log_path.as_ref())
    {
        if !files.contains(path) {
            files.push(path.clone());
        }
    }
    cleanup_scoped(files, config.turn_capture_dir.clone(), max_age_hours).await;
}

fn active_request_log_paths(config: &Config) -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    let mut push = |path: Option<&std::path::PathBuf>| {
        if let Some(path) = path
            && !paths.contains(path)
        {
            paths.push(path.clone());
        }
    };
    if config.upstreams.is_empty() {
        push(config.upstream_request_log_path.as_ref());
        for fallback in &config.fallback_upstreams {
            push(fallback.upstream_request_log_path.as_ref());
        }
    } else {
        for upstream in &config.upstreams {
            push(upstream.upstream_request_log_path.as_ref());
            for fallback in &upstream.fallback_upstreams {
                push(fallback.upstream_request_log_path.as_ref());
            }
        }
        if !config.model_routes.is_empty() {
            push(config.upstream_request_log_path.as_ref());
        }
    }
    paths
}

fn init_tracing(raw_active: bool) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if raw_active {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::sink)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }
}

fn command_uses_dedicated_terminal(command: &Option<Commands>) -> bool {
    matches!(command, Some(Commands::Start { raw: true, .. }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn detects_raw_start_command() {
        assert!(command_uses_dedicated_terminal(&Some(Commands::Start {
            config: None,
            raw: true,
            model_route: Vec::new(),
        })));
    }

    #[test]
    fn does_not_suppress_logs_for_non_raw_commands() {
        assert!(!command_uses_dedicated_terminal(&None));
        assert!(!command_uses_dedicated_terminal(&Some(Commands::Start {
            config: None,
            raw: false,
            model_route: Vec::new(),
        })));
        assert!(!command_uses_dedicated_terminal(&Some(
            Commands::AnalyzeLog {
                config: None,
                path: Some(PathBuf::from("/tmp/requests.jsonl")),
                pairs: 1,
            }
        )));
    }

    #[test]
    fn parses_debug_ui_flag_for_start() {
        let cli = Cli::parse_from(["llmconduit", "start", "--with-debug-ui"]);

        assert!(cli.with_debug_ui);
        assert!(matches!(cli.command, Some(Commands::Start { .. })));
    }
}
