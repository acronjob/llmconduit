use super::{ApiKeySummary, AuthzService};
use crate::dashboard_access::{
    AccessApiKey, AccessBackend, AccessCounts, AccessError, AccessFuture, AccessOperation,
    AccessResult, AccessSummary, ActorSummary, ManagementActor,
};
use axum::http::StatusCode;

impl AccessBackend for AuthzService {
    fn dispatch<'a>(
        &'a self,
        actor: &'a ManagementActor,
        operation: AccessOperation,
    ) -> AccessFuture<'a> {
        let result = self.dispatch_access(actor, operation);
        Box::pin(std::future::ready(result))
    }
}

impl AuthzService {
    fn dispatch_access(
        &self,
        actor: &ManagementActor,
        operation: AccessOperation,
    ) -> Result<AccessResult, AccessError> {
        match operation {
            AccessOperation::Summary => {
                let keys = self.list_keys().map_err(internal)?;
                Ok(AccessResult::Summary(AccessSummary {
                    // The synchronous compatibility store increments its epoch
                    // transactionally but does not yet project it through the
                    // management DTO. Zero explicitly means unavailable here.
                    policy_epoch: 0,
                    actor: actor_summary(actor),
                    counts: AccessCounts {
                        api_keys: keys.len(),
                        ..AccessCounts::default()
                    },
                }))
            }
            AccessOperation::ListApiKeys => Ok(AccessResult::ApiKeys(
                self.list_keys()
                    .map_err(internal)?
                    .into_iter()
                    .map(access_key)
                    .collect(),
            )),
            AccessOperation::RevokeApiKey(id) => {
                if !self.revoke_key(&id).map_err(internal)? {
                    return Err(AccessError::new(
                        StatusCode::NOT_FOUND,
                        "API key not found or already revoked",
                    ));
                }
                Ok(AccessResult::ApiKeys(
                    self.list_keys()
                        .map_err(internal)?
                        .into_iter()
                        .map(access_key)
                        .collect(),
                ))
            }
            unsupported => Err(AccessError::new(
                StatusCode::NOT_IMPLEMENTED,
                format!(
                    "{} is unavailable until the normalized management schema is migrated",
                    operation_name(&unsupported)
                ),
            )),
        }
    }
}

fn internal(message: String) -> AccessError {
    AccessError::new(StatusCode::INTERNAL_SERVER_ERROR, message)
}

fn actor_summary(actor: &ManagementActor) -> ActorSummary {
    match actor {
        ManagementActor::Bootstrap => ActorSummary {
            kind: "bootstrap".to_string(),
            principal_id: None,
            display_name: "Bootstrap administrator".to_string(),
            permissions: Vec::new(),
        },
        ManagementActor::Delegated {
            principal_id,
            permissions,
            ..
        } => ActorSummary {
            kind: "delegated".to_string(),
            principal_id: Some(principal_id.clone()),
            display_name: principal_id.clone(),
            permissions: permissions.to_vec(),
        },
    }
}

fn access_key(key: ApiKeySummary) -> AccessApiKey {
    AccessApiKey {
        id: key.id,
        principal_id: key.principal_id,
        name: key.name,
        prefix: key.prefix,
        enabled: key.enabled,
        created_at: timestamp(key.created_at),
        expires_at: key.expires_at.map(timestamp),
        last_used_at: key.last_used_at.map(timestamp),
    }
}

fn timestamp(seconds: i64) -> String {
    chrono::DateTime::from_timestamp(seconds, 0)
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| seconds.to_string())
}

fn operation_name(operation: &AccessOperation) -> &'static str {
    match operation {
        AccessOperation::Summary => "summary",
        AccessOperation::ListUsers => "user listing",
        AccessOperation::CreateUser(_) => "user creation",
        AccessOperation::ListGroups => "group listing",
        AccessOperation::CreateGroup(_) => "group creation",
        AccessOperation::ListRoles => "role listing",
        AccessOperation::CreateRole(_) => "role creation",
        AccessOperation::ListPolicies => "policy listing",
        AccessOperation::CreatePolicy(_) => "policy creation",
        AccessOperation::ListApiKeys => "API-key listing",
        AccessOperation::CreateApiKey(_) => "API-key creation",
        AccessOperation::RevokeApiKey(_) => "API-key revocation",
        AccessOperation::RotateApiKey(_) => "API-key rotation",
        AccessOperation::ListSessions => "session listing",
        AccessOperation::RevokeSession(_) => "session revocation",
        AccessOperation::Usage => "usage reporting",
        AccessOperation::Audit => "audit reporting",
        AccessOperation::Pricing => "pricing listing",
        AccessOperation::WritePricing(_) => "pricing mutation",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_operations_fail_explicitly() {
        let error = AuthzService::default()
            .dispatch_access(&ManagementActor::Bootstrap, AccessOperation::ListUsers)
            .unwrap_err();
        let response = axum::response::IntoResponse::into_response(error);
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    }
}
