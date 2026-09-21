use crate::mesh::store::JoinKeyDecision;
use crate::mesh::store::MeshNodeRecord;
use crate::mesh::store::MeshStore;
use crate::mesh::store::StoreError;
use crate::mesh::store::now_ms;
use iroh::EndpointId;

#[derive(Debug, Clone)]
pub struct MeshAuthorizer {
    store: MeshStore,
}

impl MeshAuthorizer {
    pub fn new(store: MeshStore) -> Self {
        Self { store }
    }

    pub async fn enroll(
        &self,
        join_key: &str,
        endpoint_id: EndpointId,
        node_label: Option<String>,
    ) -> Result<JoinKeyDecision, StoreError> {
        self.store
            .validate_join_key_for_endpoint(
                join_key,
                &endpoint_id.to_string(),
                node_label,
                now_ms(),
            )
            .await
    }

    pub async fn authorize_worker(&self, endpoint_id: EndpointId) -> Result<bool, StoreError> {
        let endpoint_id = endpoint_id.to_string();
        let authorized = self.store.is_node_authorized(&endpoint_id).await?;
        if authorized {
            self.store.touch_node_seen(&endpoint_id, now_ms()).await?;
        }
        Ok(authorized)
    }

    pub async fn revoke_node(&self, endpoint_id: EndpointId) -> Result<bool, StoreError> {
        self.store
            .set_node_enabled(&endpoint_id.to_string(), false)
            .await
    }

    pub async fn enable_node(&self, endpoint_id: EndpointId) -> Result<bool, StoreError> {
        self.store
            .set_node_enabled(&endpoint_id.to_string(), true)
            .await
    }

    pub async fn nodes(&self) -> Result<Vec<MeshNodeRecord>, StoreError> {
        self.store.list_nodes().await
    }
}
