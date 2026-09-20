use clap::Parser;
use llmconduit::AppOptions;
use llmconduit::build_app_with_gateway_and_options;
use llmconduit::cli::Cli;
use llmconduit::cli::Commands;
use llmconduit::cli::JoinKeyCommands;
use llmconduit::cli::MeshCommands;
use llmconduit::cli::NodeCommands;
use llmconduit::cli::PricingCommands;
use llmconduit::cli::PricingSource;
use llmconduit::cli::resolve_config_path;
use llmconduit::cli::run_configure_flow;
use llmconduit::config::Config;
use llmconduit::log_rotation::spawn_cleanup;
use llmconduit::mesh::identity;
use llmconduit::mesh::store::MeshStore;
use llmconduit::mesh::store::now_ms;
use llmconduit::raw::RawOutput;
use llmconduit::request_log::analyze_request_log;
use std::path::Path;
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
            let config = Config::from_env_file_and_routes(Some(&path), &model_route)?;
            let bind_addr = config.bind_addr;
            run_debug_log_cleanup(&config);
            let (app, gateway) = build_app_with_gateway_and_options(
                config,
                raw.then(RawOutput::stdout),
                app_options,
            );
            let listener = TcpListener::bind(bind_addr).await?;
            log_listening(bind_addr);
            log_debug_ui_status(&gateway, app_options, bind_addr);
            tracing::info!("using config file {}", path.display());
            axum::serve(listener, app).await?;
            Ok(())
        }
        Some(Commands::Worker { config, join_key }) => {
            let path = resolve_config_path(config)?;
            let config = Config::from_env_and_file(Some(&path))?;
            let join_key = join_key.or_else(|| std::env::var("LLMCONDUIT_MESH_JOIN_KEY").ok());
            tracing::info!(config = %path.display(), "starting mesh worker");
            llmconduit::mesh::run_worker(config.mesh.worker, join_key).await?;
            Ok(())
        }
        Some(Commands::Mesh { config, command }) => {
            let path = resolve_config_path(config)?;
            run_mesh_command(&path, command).await?;
            Ok(())
        }
        Some(Commands::Pricing { config, command }) => {
            let path = resolve_config_path(config)?;
            let config = Config::from_env_and_file(Some(&path))?;
            match command {
                PricingCommands::Sync {
                    source: PricingSource::Openrouter,
                    models,
                } => {
                    let authz = llmconduit::authz::AuthzService::from_config(&config.auth)?;
                    let pricing = authz.sync_openrouter_pricing_models(models).await?;
                    println!("{}", serde_json::to_string_pretty(&pricing)?);
                }
            }
            Ok(())
        }
        None => {
            let path = resolve_config_path(None)?;
            let config = Config::from_env_and_file(Some(&path))?;
            let bind_addr = config.bind_addr;
            run_debug_log_cleanup(&config);
            let (app, gateway) = build_app_with_gateway_and_options(config, None, app_options);
            let listener = TcpListener::bind(bind_addr).await?;
            log_listening(bind_addr);
            log_debug_ui_status(&gateway, app_options, bind_addr);
            tracing::info!("using config file {}", path.display());
            axum::serve(listener, app).await?;
            Ok(())
        }
    }
}

async fn run_mesh_command(
    path: &Path,
    command: MeshCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env_and_file(Some(path))?;
    match command {
        MeshCommands::Info => {
            let identity_path = config
                .mesh
                .controller
                .identity_path
                .as_ref()
                .ok_or("mesh.controller.identity_path is required for mesh info")?;
            let identity = identity::load_or_create(identity_path).await?;
            println!("endpoint_id: {}", identity.endpoint_id());
            println!("bind_addr: {}", config.mesh.controller.bind_addr);
            match &config.mesh.controller.state_path {
                Some(state_path) => println!("state_path: {}", state_path.display()),
                None => println!("state_path: <unset>"),
            }
        }
        MeshCommands::JoinKey { command } => {
            let store = open_mesh_store(&config).await?;
            match command {
                JoinKeyCommands::Create {
                    label,
                    max_uses,
                    expires_in,
                } => {
                    if max_uses.is_some_and(|uses| uses <= 0) {
                        return Err("--max-uses must be greater than zero".into());
                    }
                    let expires_at_ms = expires_in
                        .as_deref()
                        .map(parse_relative_duration_ms)
                        .transpose()?
                        .map(|duration_ms| now_ms().saturating_add(duration_ms));
                    let created = store
                        .create_join_key(label, expires_at_ms, max_uses)
                        .await?;
                    println!("id: {}", created.id);
                    if let Some(label) = created.label {
                        println!("label: {label}");
                    }
                    if let Some(expires_at_ms) = created.expires_at_ms {
                        println!("expires_at_ms: {expires_at_ms}");
                    }
                    if let Some(max_uses) = created.max_uses {
                        println!("max_uses: {max_uses}");
                    }
                    println!("token: {}", created.token);
                }
                JoinKeyCommands::List => {
                    for key in store.list_join_keys().await? {
                        println!(
                            "{}\t{}\tuses={}\tmax={}\texpires={}\t{}",
                            key.id,
                            if key.enabled { "enabled" } else { "disabled" },
                            key.use_count,
                            key.max_uses
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "-".to_string()),
                            key.expires_at_ms
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "-".to_string()),
                            key.label.unwrap_or_default(),
                        );
                    }
                }
                JoinKeyCommands::Revoke { key_id } => {
                    if store.revoke_join_key(&key_id).await? {
                        println!("revoked {key_id}");
                    } else {
                        println!("join key not found or already disabled: {key_id}");
                    }
                }
            }
        }
        MeshCommands::Node { command } => {
            let store = open_mesh_store(&config).await?;
            match command {
                NodeCommands::List => {
                    for node in store.list_nodes().await? {
                        println!(
                            "{}\t{}\tjoined={}\tlast_seen={}\tkey={}\t{}",
                            node.endpoint_id,
                            if node.enabled { "enabled" } else { "disabled" },
                            node.joined_at_ms,
                            node.last_seen_at_ms
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "-".to_string()),
                            node.join_key_id.unwrap_or_else(|| "-".to_string()),
                            node.label.unwrap_or_default(),
                        );
                    }
                }
                NodeCommands::Revoke { endpoint_id } => {
                    if store.set_node_enabled(&endpoint_id, false).await? {
                        println!("revoked {endpoint_id}");
                    } else {
                        println!("node not found: {endpoint_id}");
                    }
                }
                NodeCommands::Enable { endpoint_id } => {
                    if store.set_node_enabled(&endpoint_id, true).await? {
                        println!("enabled {endpoint_id}");
                    } else {
                        println!("node not found: {endpoint_id}");
                    }
                }
            }
        }
    }
    Ok(())
}

async fn open_mesh_store(config: &Config) -> Result<MeshStore, Box<dyn std::error::Error>> {
    let state_path = config
        .mesh
        .controller
        .state_path
        .as_ref()
        .ok_or("mesh.controller.state_path is required for mesh admin commands")?;
    Ok(MeshStore::open(state_path).await?)
}

fn parse_relative_duration_ms(value: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let value = value.trim();
    if value.len() < 2 {
        return Err("duration must look like 30m, 12h, or 7d".into());
    }
    let (amount, unit) = value.split_at(value.len() - 1);
    let amount = amount.parse::<i64>()?;
    if amount <= 0 {
        return Err("duration amount must be greater than zero".into());
    }
    let seconds = match unit {
        "s" => amount,
        "m" => amount.saturating_mul(60),
        "h" => amount.saturating_mul(60 * 60),
        "d" => amount.saturating_mul(24 * 60 * 60),
        _ => return Err("duration unit must be one of s, m, h, d".into()),
    };
    Ok(seconds.saturating_mul(1000))
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
fn run_debug_log_cleanup(config: &Config) {
    let max_age_hours = config.debug_log_max_age_hours;
    if max_age_hours.is_none() {
        return;
    }
    spawn_cleanup(
        config.debug_log_dirs(),
        config.turn_capture_dir.clone(),
        max_age_hours,
    );
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
