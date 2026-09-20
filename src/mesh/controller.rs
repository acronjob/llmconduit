#![allow(dead_code)]

use crate::config::MeshControllerConfig;
use crate::error::{AppError, AppResult};
use crate::mesh::auth::MeshAuthorizer;
use crate::mesh::identity::load_or_create;
use crate::mesh::protocol::{
    ENROLL_ALPN, EnrollRequest, EnrollResponse, HubToWorker, PROTOCOL_VERSION, WORKER_ALPN,
    WorkerToHub, read_control, validate_enroll_request, validate_heartbeat, validate_model_catalog,
    validate_resource_advertisement, validate_worker_advertisement, write_control,
};
use crate::mesh::registry::MeshRegistry;
use crate::mesh::store::{JoinKeyDecision, MeshStore};
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn spawn_controller(config: &MeshControllerConfig) -> AppResult<Arc<MeshRegistry>> {
    let registry = Arc::new(MeshRegistry::new(Duration::from_secs(
        config.heartbeat_timeout_secs,
    )));
    let state_path = config
        .state_path
        .clone()
        .ok_or_else(|| AppError::bad_request("mesh.controller.state_path is required"))?;
    let config = config.clone();
    let registry_for_task = Arc::clone(&registry);
    tokio::spawn(async move {
        if let Err(err) = run_controller(config, state_path, registry_for_task).await {
            tracing::error!(error = %err, "mesh controller stopped");
        }
    });
    Ok(registry)
}

async fn run_controller(
    config: MeshControllerConfig,
    state_path: PathBuf,
    registry: Arc<MeshRegistry>,
) -> AppResult<()> {
    let identity_path = config
        .identity_path
        .clone()
        .unwrap_or_else(default_controller_identity_path);
    let identity = load_or_create(&identity_path)
        .await
        .map_err(|err| AppError::internal(format!("failed to load mesh identity: {err}")))?;
    let secret = identity.secret_key();
    let endpoint_id = secret.public();
    let store = MeshStore::open(state_path)
        .await
        .map_err(|err| AppError::internal(format!("failed to open mesh store: {err}")))?;
    let authorizer = Arc::new(MeshAuthorizer::new(store));
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(secret)
        .relay_mode(RelayMode::Disabled)
        .alpns(vec![ENROLL_ALPN.to_vec(), WORKER_ALPN.to_vec()])
        .bind_addr(config.bind_addr)
        .map_err(|err| AppError::internal(format!("invalid mesh bind address: {err}")))?
        .bind()
        .await
        .map_err(|err| AppError::internal(format!("failed to bind mesh endpoint: {err}")))?;
    tracing::info!(
        endpoint_id = %endpoint_id,
        bind_addr = %config.bind_addr,
        identity_path = %identity_path.display(),
        "mesh controller listening"
    );
    while let Some(incoming) = endpoint.accept().await {
        let registry = Arc::clone(&registry);
        let authorizer = Arc::clone(&authorizer);
        tokio::spawn(async move {
            match incoming.accept() {
                Ok(mut connecting) => {
                    let alpn = match connecting.alpn().await {
                        Ok(alpn) => alpn,
                        Err(err) => {
                            tracing::warn!(error = %err, "mesh worker ALPN negotiation failed");
                            return;
                        }
                    };
                    match connecting.await {
                        Ok(connection) => {
                            if let Err(err) =
                                handle_connection(registry, authorizer, connection, &alpn).await
                            {
                                tracing::warn!(error = %err, "mesh worker connection ended");
                            }
                        }
                        Err(err) => tracing::warn!(error = %err, "mesh worker handshake failed"),
                    }
                }
                Err(err) => tracing::warn!(error = %err, "failed to accept mesh connection"),
            }
        });
    }
    Ok(())
}

async fn handle_connection(
    registry: Arc<MeshRegistry>,
    authorizer: Arc<MeshAuthorizer>,
    connection: iroh::endpoint::Connection,
    alpn: &[u8],
) -> AppResult<()> {
    let endpoint_id = connection.remote_id();
    if alpn == WORKER_ALPN {
        if !authorizer
            .authorize_worker(endpoint_id)
            .await
            .map_err(|err| AppError::internal(format!("mesh authorization failed: {err}")))?
        {
            connection.close(403u32.into(), b"mesh node is not authorized");
            return Err(AppError::upstream("mesh node is not authorized"));
        }
        let (send, mut recv) = connection.accept_bi().await.map_err(|err| {
            AppError::upstream(format!("failed to accept mesh control stream: {err}"))
        })?;
        let first: WorkerToHub = read_control(&mut recv).await?;
        let WorkerToHub::Hello(advertisement) = first else {
            return Err(AppError::bad_request(
                "mesh worker did not start with hello",
            ));
        };
        validate_worker_advertisement(&advertisement)
            .map_err(|err| AppError::bad_request(format!("invalid mesh worker hello: {err}")))?;
        return handle_worker_control(
            registry,
            authorizer,
            connection,
            endpoint_id,
            send,
            recv,
            advertisement,
        )
        .await;
    }
    if alpn != ENROLL_ALPN {
        connection.close(400u32.into(), b"unsupported mesh ALPN");
        return Err(AppError::bad_request("unsupported mesh ALPN"));
    }
    let (mut send, mut recv) = connection
        .accept_bi()
        .await
        .map_err(|err| AppError::upstream(format!("failed to accept enrollment stream: {err}")))?;
    let request: EnrollRequest = read_control(&mut recv).await?;
    validate_enroll_request(&request)
        .map_err(|err| AppError::bad_request(format!("invalid mesh enrollment: {err}")))?;

    let decision = authorizer
        .enroll(&request.join_key, endpoint_id, request.node_name)
        .await
        .map_err(|err| AppError::internal(format!("mesh enrollment failed: {err}")))?;
    match decision {
        JoinKeyDecision::Accepted | JoinKeyDecision::AlreadyEnrolled => {
            write_control(
                &mut send,
                &EnrollResponse::Accepted {
                    protocol_version: PROTOCOL_VERSION,
                },
            )
            .await?;
            tracing::info!(endpoint_id = %endpoint_id, "mesh enrollment accepted");
            Ok(())
        }
        other => {
            write_control(
                &mut send,
                &EnrollResponse::Rejected {
                    reason: format!("{other:?}").to_ascii_lowercase(),
                },
            )
            .await?;
            Err(AppError::bad_request("mesh enrollment rejected"))
        }
    }
}

async fn handle_worker_control(
    registry: Arc<MeshRegistry>,
    authorizer: Arc<MeshAuthorizer>,
    connection: iroh::endpoint::Connection,
    endpoint_id: iroh::EndpointId,
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    advertisement: crate::mesh::protocol::WorkerAdvertisement,
) -> AppResult<()> {
    let session = registry.register(endpoint_id, connection.clone(), advertisement);
    write_control(
        &mut send,
        &HubToWorker::HelloAck {
            protocol_version: crate::mesh::protocol::PROTOCOL_VERSION,
        },
    )
    .await?;
    tracing::info!(
        endpoint_id = %endpoint_id,
        generation = session.generation,
        "mesh worker connected"
    );
    let result: AppResult<()> = async {
        loop {
            match read_control::<_, WorkerToHub>(&mut recv).await {
                Ok(WorkerToHub::Heartbeat(heartbeat)) => {
                    if !authorizer
                        .authorize_worker(endpoint_id)
                        .await
                        .map_err(|err| {
                            AppError::internal(format!("mesh authorization refresh failed: {err}"))
                        })?
                    {
                        return Err(AppError::upstream("mesh node was revoked"));
                    }
                    validate_heartbeat(&heartbeat).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh worker heartbeat: {err}"))
                    })?;
                    session.apply_heartbeat(heartbeat);
                }
                Ok(WorkerToHub::ResourceUpdate(update)) => {
                    validate_resource_advertisement(&update).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh resource update: {err}"))
                    })?;
                    session.update_resource(update);
                }
                Ok(WorkerToHub::ModelCatalogUpdate {
                    resource_id,
                    models,
                    revision,
                }) => {
                    validate_model_catalog(&resource_id, &models).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh model catalog update: {err}"))
                    })?;
                    session.update_model_catalog(&resource_id, models, revision);
                }
                Ok(WorkerToHub::Hello(advertisement)) => {
                    validate_worker_advertisement(&advertisement).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh worker hello: {err}"))
                    })?;
                    for resource in advertisement.resources {
                        session.update_resource(resource);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }
    .await;
    registry.remove_generation(endpoint_id, session.generation);
    connection.close(0u32.into(), b"mesh control stream ended");
    result
}

fn default_controller_identity_path() -> PathBuf {
    PathBuf::from("llmconduit-controller.key")
}
