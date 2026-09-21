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
use crate::mesh::store::{DisabledMeshModelRecord, JoinKeyDecision, MeshStore, StoreError};
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::timeout;

const MAX_CONTROLLER_CONNECTION_TASKS: usize = 256;
const CONNECTION_SETUP_TIMEOUT: Duration = Duration::from_secs(10);
const CONTROL_STREAM_OPEN_TIMEOUT: Duration = Duration::from_secs(10);
const INITIAL_CONTROL_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

type MeshModelAllowlist = BTreeMap<String, BTreeMap<String, Vec<String>>>;

#[derive(Debug, Clone)]
pub(crate) struct MeshAdmin {
    store: MeshStore,
    registry: Arc<MeshRegistry>,
    initialized: Arc<tokio::sync::OnceCell<()>>,
}

impl MeshAdmin {
    pub(crate) fn store(&self) -> &MeshStore {
        &self.store
    }

    pub(crate) fn registry(&self) -> Arc<MeshRegistry> {
        Arc::clone(&self.registry)
    }

    pub(crate) async fn initialize(&self) -> Result<(), StoreError> {
        self.initialized
            .get_or_try_init(|| async {
                self.store.initialize().await?;
                self.reload_disabled_models().await
            })
            .await
            .map(|_| ())
    }

    pub(crate) async fn reload_disabled_models(&self) -> Result<(), StoreError> {
        let disabled = self.store.list_disabled_models().await?;
        self.apply_disabled_models(disabled);
        Ok(())
    }

    pub(crate) fn disable_live_node(&self, endpoint_id: iroh::EndpointId) -> bool {
        self.registry
            .remove_endpoint(endpoint_id, b"mesh node was disabled")
    }

    pub(crate) fn apply_model_disabled(
        &self,
        endpoint_id: iroh::EndpointId,
        resource_id: &str,
        model: &str,
        disabled: bool,
    ) {
        self.registry
            .set_model_disabled(endpoint_id, resource_id, model, disabled);
    }

    fn apply_disabled_models(&self, records: Vec<DisabledMeshModelRecord>) {
        self.registry
            .replace_disabled_models(records.into_iter().filter_map(|record| {
                record
                    .endpoint_id
                    .parse::<iroh::EndpointId>()
                    .ok()
                    .map(|endpoint_id| (endpoint_id, record.resource_id, record.model))
            }));
    }
}

pub(crate) fn spawn_controller(config: &MeshControllerConfig) -> AppResult<Arc<MeshAdmin>> {
    let registry = Arc::new(MeshRegistry::new(Duration::from_secs(
        config.heartbeat_timeout_secs,
    )));
    let state_path = config
        .state_path
        .clone()
        .ok_or_else(|| AppError::bad_request("mesh.controller.state_path is required"))?;
    let store = MeshStore::at_path(&state_path);
    let admin = Arc::new(MeshAdmin {
        store: store.clone(),
        registry: Arc::clone(&registry),
        initialized: Arc::new(tokio::sync::OnceCell::new()),
    });
    let config = config.clone();
    let registry_for_task = Arc::clone(&registry);
    let admin_for_task = Arc::clone(&admin);
    tokio::spawn(async move {
        if let Err(err) = run_controller(config, registry_for_task, admin_for_task).await {
            tracing::error!(error = %err, "mesh controller stopped");
        }
    });
    Ok(admin)
}

async fn run_controller(
    config: MeshControllerConfig,
    registry: Arc<MeshRegistry>,
    admin: Arc<MeshAdmin>,
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
    admin
        .initialize()
        .await
        .map_err(|err| AppError::internal(format!("failed to open mesh store: {err}")))?;
    let authorizer = Arc::new(MeshAuthorizer::new(admin.store.clone()));
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
    let connection_slots = Arc::new(Semaphore::new(MAX_CONTROLLER_CONNECTION_TASKS));
    let model_allowlist = Arc::new(config.model_allowlist.clone());
    if model_allowlist.is_empty() {
        tracing::warn!(
            "mesh controller model_allowlist is empty; workers may enroll but no mesh models will be routable"
        );
    }
    while let Some(incoming) = endpoint.accept().await {
        let Ok(slot) = Arc::clone(&connection_slots).try_acquire_owned() else {
            tracing::warn!(
                limit = MAX_CONTROLLER_CONNECTION_TASKS,
                "dropping mesh connection while controller is overloaded"
            );
            continue;
        };
        let registry = Arc::clone(&registry);
        let authorizer = Arc::clone(&authorizer);
        let model_allowlist = Arc::clone(&model_allowlist);
        tokio::spawn(async move {
            let _slot = slot;
            match incoming.accept() {
                Ok(mut connecting) => {
                    let alpn = match timeout(CONNECTION_SETUP_TIMEOUT, connecting.alpn()).await {
                        Ok(Ok(alpn)) => alpn,
                        Ok(Err(err)) => {
                            tracing::warn!(error = %err, "mesh worker ALPN negotiation failed");
                            return;
                        }
                        Err(_) => {
                            tracing::warn!("mesh worker ALPN negotiation timed out");
                            return;
                        }
                    };
                    match timeout(CONNECTION_SETUP_TIMEOUT, connecting).await {
                        Ok(Ok(connection)) => {
                            if let Err(err) = handle_connection(
                                registry,
                                authorizer,
                                model_allowlist,
                                connection,
                                &alpn,
                            )
                            .await
                            {
                                tracing::warn!(error = %err, "mesh worker connection ended");
                            }
                        }
                        Ok(Err(err)) => {
                            tracing::warn!(error = %err, "mesh worker handshake failed")
                        }
                        Err(_) => tracing::warn!("mesh worker handshake timed out"),
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
    model_allowlist: Arc<MeshModelAllowlist>,
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
        let (send, mut recv) = timeout(CONTROL_STREAM_OPEN_TIMEOUT, connection.accept_bi())
            .await
            .map_err(|_| AppError::upstream("timed out waiting for mesh control stream"))?
            .map_err(|err| {
                AppError::upstream(format!("failed to accept mesh control stream: {err}"))
            })?;
        let first: WorkerToHub = timeout(INITIAL_CONTROL_FRAME_TIMEOUT, read_control(&mut recv))
            .await
            .map_err(|_| AppError::upstream("timed out waiting for mesh worker hello"))??;
        let WorkerToHub::Hello(advertisement) = first else {
            return Err(AppError::bad_request(
                "mesh worker did not start with hello",
            ));
        };
        validate_worker_advertisement(&advertisement)
            .map_err(|err| AppError::bad_request(format!("invalid mesh worker hello: {err}")))?;
        let advertisement =
            filter_worker_advertisement(endpoint_id, model_allowlist.as_ref(), advertisement);
        validate_worker_advertisement(&advertisement)
            .map_err(|err| AppError::bad_request(format!("invalid mesh worker hello: {err}")))?;
        return handle_worker_control(
            registry,
            authorizer,
            model_allowlist,
            connection,
            endpoint_id,
            MeshControlStreams { send, recv },
            advertisement,
        )
        .await;
    }
    if alpn != ENROLL_ALPN {
        connection.close(400u32.into(), b"unsupported mesh ALPN");
        return Err(AppError::bad_request("unsupported mesh ALPN"));
    }
    let (mut send, mut recv) = timeout(CONTROL_STREAM_OPEN_TIMEOUT, connection.accept_bi())
        .await
        .map_err(|_| AppError::upstream("timed out waiting for mesh enrollment stream"))?
        .map_err(|err| AppError::upstream(format!("failed to accept enrollment stream: {err}")))?;
    let request: EnrollRequest = timeout(INITIAL_CONTROL_FRAME_TIMEOUT, read_control(&mut recv))
        .await
        .map_err(|_| AppError::upstream("timed out waiting for mesh enrollment request"))??;
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
    model_allowlist: Arc<MeshModelAllowlist>,
    connection: iroh::endpoint::Connection,
    endpoint_id: iroh::EndpointId,
    mut streams: MeshControlStreams,
    advertisement: crate::mesh::protocol::WorkerAdvertisement,
) -> AppResult<()> {
    let session = registry.register(endpoint_id, connection.clone(), advertisement);
    write_control(
        &mut streams.send,
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
            match read_control::<_, WorkerToHub>(&mut streams.recv).await {
                Ok(WorkerToHub::Heartbeat(heartbeat)) => {
                    ensure_worker_still_authorized(&authorizer, &connection, endpoint_id).await?;
                    validate_heartbeat(&heartbeat).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh worker heartbeat: {err}"))
                    })?;
                    session.apply_heartbeat(heartbeat);
                }
                Ok(WorkerToHub::ResourceUpdate(update)) => {
                    ensure_worker_still_authorized(&authorizer, &connection, endpoint_id).await?;
                    validate_resource_advertisement(&update).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh resource update: {err}"))
                    })?;
                    let update =
                        filter_resource_update(endpoint_id, model_allowlist.as_ref(), update);
                    validate_resource_advertisement(&update).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh resource update: {err}"))
                    })?;
                    let resource_id = update.resource_id.clone();
                    if !session.update_resource(update) {
                        tracing::warn!(
                            endpoint_id = %endpoint_id,
                            resource_id = %resource_id,
                            "mesh worker attempted to add an unapproved resource"
                        );
                    }
                }
                Ok(WorkerToHub::ModelCatalogUpdate {
                    resource_id,
                    models,
                    revision,
                }) => {
                    ensure_worker_still_authorized(&authorizer, &connection, endpoint_id).await?;
                    validate_model_catalog(&resource_id, &models).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh model catalog update: {err}"))
                    })?;
                    let models = filter_model_catalog(
                        endpoint_id,
                        model_allowlist.as_ref(),
                        &resource_id,
                        models,
                    );
                    validate_model_catalog(&resource_id, &models).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh model catalog update: {err}"))
                    })?;
                    session.update_model_catalog(&resource_id, models, revision);
                }
                Ok(WorkerToHub::Hello(advertisement)) => {
                    ensure_worker_still_authorized(&authorizer, &connection, endpoint_id).await?;
                    validate_worker_advertisement(&advertisement).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh worker hello: {err}"))
                    })?;
                    let advertisement = filter_worker_advertisement(
                        endpoint_id,
                        model_allowlist.as_ref(),
                        advertisement,
                    );
                    validate_worker_advertisement(&advertisement).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh worker hello: {err}"))
                    })?;
                    for resource in advertisement.resources {
                        let resource_id = resource.resource_id.clone();
                        if !session.update_resource(resource) {
                            tracing::warn!(
                                endpoint_id = %endpoint_id,
                                resource_id = %resource_id,
                                "mesh worker hello attempted to add an unapproved resource"
                            );
                        }
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

struct MeshControlStreams {
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,
}

async fn ensure_worker_still_authorized(
    authorizer: &MeshAuthorizer,
    connection: &iroh::endpoint::Connection,
    endpoint_id: iroh::EndpointId,
) -> AppResult<()> {
    if authorizer
        .authorize_worker(endpoint_id)
        .await
        .map_err(|err| AppError::internal(format!("mesh authorization refresh failed: {err}")))?
    {
        return Ok(());
    }
    connection.close(403u32.into(), b"mesh node was revoked");
    Err(AppError::upstream("mesh node was revoked"))
}

fn filter_worker_advertisement(
    endpoint_id: iroh::EndpointId,
    allowlist: &MeshModelAllowlist,
    mut advertisement: crate::mesh::protocol::WorkerAdvertisement,
) -> crate::mesh::protocol::WorkerAdvertisement {
    let endpoint_id = endpoint_id.to_string();
    let Some(resources) = allowlist.get(&endpoint_id) else {
        advertisement.resources.clear();
        return advertisement;
    };
    advertisement.resources.retain_mut(|resource| {
        if !resources.contains_key(&resource.resource_id) {
            return false;
        }
        filter_resource_models(resources, resource);
        true
    });
    advertisement
}

fn filter_resource_update(
    endpoint_id: iroh::EndpointId,
    allowlist: &MeshModelAllowlist,
    mut update: crate::mesh::protocol::ResourceAdvertisement,
) -> crate::mesh::protocol::ResourceAdvertisement {
    let endpoint_id = endpoint_id.to_string();
    if let Some(resources) = allowlist.get(&endpoint_id) {
        filter_resource_models(resources, &mut update);
    } else {
        update.models.clear();
    }
    update
}

fn filter_model_catalog(
    endpoint_id: iroh::EndpointId,
    allowlist: &MeshModelAllowlist,
    resource_id: &str,
    models: Vec<crate::mesh::protocol::ModelAdvertisement>,
) -> Vec<crate::mesh::protocol::ModelAdvertisement> {
    let endpoint_id = endpoint_id.to_string();
    let Some(resources) = allowlist.get(&endpoint_id) else {
        return Vec::new();
    };
    let Some(allowed) = resources.get(resource_id) else {
        return Vec::new();
    };
    models
        .into_iter()
        .filter(|model| {
            allowed
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(&model.id))
        })
        .collect()
}

fn filter_resource_models(
    resources: &BTreeMap<String, Vec<String>>,
    resource: &mut crate::mesh::protocol::ResourceAdvertisement,
) {
    let Some(allowed) = resources.get(&resource.resource_id) else {
        resource.models.clear();
        return;
    };
    resource.models.retain(|model| {
        allowed
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(&model.id))
    });
}

fn default_controller_identity_path() -> PathBuf {
    PathBuf::from("llmconduit-controller.key")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AvailabilitySchedule;
    use crate::mesh::protocol::{ModelAdvertisement, ResourceAdvertisement, WorkerAdvertisement};
    use crate::mesh::store::MeshStore;
    use iroh::SecretKey;
    use uuid::Uuid;

    fn advertisement() -> WorkerAdvertisement {
        WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: None,
            agent_version: "test".to_string(),
            resources: vec![
                ResourceAdvertisement {
                    resource_id: "gpu-a".to_string(),
                    models: vec![
                        ModelAdvertisement {
                            id: "allowed-model".to_string(),
                            context_limit: Some(4096),
                        },
                        ModelAdvertisement {
                            id: "surprise-model".to_string(),
                            context_limit: None,
                        },
                    ],
                    availability: AvailabilitySchedule::default(),
                    effective_capacity: 1,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                },
                ResourceAdvertisement {
                    resource_id: "gpu-b".to_string(),
                    models: vec![ModelAdvertisement {
                        id: "other-model".to_string(),
                        context_limit: None,
                    }],
                    availability: AvailabilitySchedule::default(),
                    effective_capacity: 1,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                },
            ],
        }
    }

    fn temp_db(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("llmconduit-{name}-{}.sqlite", Uuid::new_v4()))
    }

    #[test]
    fn controller_model_allowlist_filters_initial_advertisement() {
        let endpoint = SecretKey::generate().public();
        let mut resources = BTreeMap::new();
        resources.insert("gpu-a".to_string(), vec!["ALLOWED-MODEL".to_string()]);
        let allowlist = BTreeMap::from([(endpoint.to_string(), resources)]);

        let filtered = filter_worker_advertisement(endpoint, &allowlist, advertisement());

        assert_eq!(filtered.resources.len(), 1);
        assert_eq!(filtered.resources[0].resource_id, "gpu-a");
        assert_eq!(filtered.resources[0].models.len(), 1);
        assert_eq!(filtered.resources[0].models[0].id, "allowed-model");
    }

    #[test]
    fn controller_model_allowlist_denies_unlisted_endpoint() {
        let endpoint = SecretKey::generate().public();
        let allowlist = BTreeMap::from([(
            SecretKey::generate().public().to_string(),
            BTreeMap::from([("gpu-a".to_string(), vec!["allowed-model".to_string()])]),
        )]);

        let filtered = filter_worker_advertisement(endpoint, &allowlist, advertisement());

        assert!(filtered.resources.is_empty());
    }

    #[test]
    fn controller_model_allowlist_fails_closed_when_empty() {
        let endpoint = SecretKey::generate().public();

        let filtered = filter_worker_advertisement(endpoint, &BTreeMap::new(), advertisement());

        assert!(filtered.resources.is_empty());
    }

    #[test]
    fn controller_model_allowlist_filters_later_catalog_updates() {
        let endpoint = SecretKey::generate().public();
        let allowlist = BTreeMap::from([(
            endpoint.to_string(),
            BTreeMap::from([("gpu-a".to_string(), vec!["allowed-model".to_string()])]),
        )]);
        let models = vec![
            ModelAdvertisement {
                id: "allowed-model".to_string(),
                context_limit: Some(4096),
            },
            ModelAdvertisement {
                id: "surprise-model".to_string(),
                context_limit: None,
            },
        ];

        let filtered = filter_model_catalog(endpoint, &allowlist, "gpu-a", models);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "allowed-model");
    }

    #[tokio::test]
    async fn mesh_admin_initialize_does_not_reload_over_live_model_overrides() {
        let path = temp_db("mesh-admin-once");
        let store = MeshStore::open(&path).await.expect("open store");
        let registry = Arc::new(MeshRegistry::new(Duration::from_secs(30)));
        let endpoint = SecretKey::generate().public();
        registry.register_test(
            endpoint,
            WorkerAdvertisement {
                protocol_version: PROTOCOL_VERSION,
                node_name: None,
                agent_version: "test".into(),
                resources: vec![ResourceAdvertisement {
                    resource_id: "gpu".into(),
                    models: vec![ModelAdvertisement {
                        id: "qwen".into(),
                        context_limit: None,
                    }],
                    availability: AvailabilitySchedule::default(),
                    effective_capacity: 1,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                }],
            },
        );
        let admin = MeshAdmin {
            store: store.clone(),
            registry: Arc::clone(&registry),
            initialized: Arc::new(tokio::sync::OnceCell::new()),
        };
        admin.initialize().await.expect("initial load");
        store
            .set_model_disabled(&endpoint.to_string(), "gpu", "qwen", true, 1)
            .await
            .expect("persist disable");
        admin.apply_model_disabled(endpoint, "gpu", "qwen", true);
        assert!(registry.reserve("qwen").is_none());

        admin
            .initialize()
            .await
            .expect("second initialize is no-op");

        assert!(registry.reserve("qwen").is_none());
        let _ = std::fs::remove_file(path);
    }
}
