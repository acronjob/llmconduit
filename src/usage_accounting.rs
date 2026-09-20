//! Durable, idempotent per-key usage accounting.
//!
//! The engine owns terminal detection. This module deliberately exposes a single
//! `INSERT OR IGNORE` seam so the engine's terminal CAS may retry safely without
//! ever charging a request twice.

use crate::dashboard_flow::FlowUsage;
use crate::openrouter_pricing::NanoUsd;
use rusqlite::{Connection, OptionalExtension, params};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageCostConfidence {
    Confident,
    Estimated,
    Unavailable,
}

impl UsageCostConfidence {
    fn as_str(self) -> &'static str {
        match self {
            Self::Confident => "confident",
            Self::Estimated => "estimated",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsageRates {
    pub input_per_token: NanoUsd,
    pub output_per_token: NanoUsd,
    pub cached_per_token: Option<NanoUsd>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsageCharge {
    pub nano_usd: i64,
    pub confidence: UsageCostConfidence,
}

/// Compute a persisted integer-nanodollar charge with the same prompt/cache split
/// as the dashboard. Missing cache detail is not silently promoted to confident.
pub fn charge_for_usage(usage: FlowUsage, rates: UsageRates) -> Option<UsageCharge> {
    if usage.prompt < 0 || usage.completion < 0 || usage.total < 0 {
        return None;
    }
    let cached = usage.cached.unwrap_or(0).clamp(0, usage.prompt);
    let uncached = usage.prompt - cached;
    let cached_rate = rates.cached_per_token.unwrap_or(NanoUsd::ZERO);
    let total = i128::from(uncached)
        .checked_mul(i128::from(rates.input_per_token.get()))?
        .checked_add(i128::from(cached).checked_mul(i128::from(cached_rate.get()))?)?
        .checked_add(
            i128::from(usage.completion)
                .checked_mul(i128::from(rates.output_per_token.get()))?,
        )?;
    let nano_usd = i64::try_from(total).ok()?;
    let confidence = if usage.cached == Some(0) || rates.cached_per_token.is_some() {
        UsageCostConfidence::Confident
    } else {
        UsageCostConfidence::Estimated
    };
    Some(UsageCharge {
        nano_usd,
        confidence,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageEvent {
    pub auth_request_id: String,
    pub api_call_id: Option<String>,
    pub key_id: String,
    pub principal_id: String,
    pub endpoint: String,
    pub requested_model: Option<String>,
    pub served_model: Option<String>,
    pub provider: Option<String>,
    pub route: Option<String>,
    pub status: String,
    pub usage: Option<FlowUsage>,
    pub charge: Option<UsageCharge>,
    pub created_at_ms: i64,
}

pub fn migrate_usage_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS auth_usage_events (
            auth_request_id TEXT PRIMARY KEY NOT NULL,
            api_call_id TEXT,
            key_id TEXT NOT NULL,
            principal_id TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            requested_model TEXT,
            served_model TEXT,
            provider TEXT,
            route TEXT,
            status TEXT NOT NULL,
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            total_tokens INTEGER,
            cached_tokens INTEGER,
            reasoning_tokens INTEGER,
            cost_nano_usd INTEGER,
            cost_confidence TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS auth_usage_created_idx
            ON auth_usage_events(created_at_ms);
        CREATE INDEX IF NOT EXISTS auth_usage_key_created_idx
            ON auth_usage_events(key_id, created_at_ms);",
    )
}

/// Returns `true` only for the first insert of an `auth_request_id`.
pub fn record_usage_once(conn: &Connection, event: &UsageEvent) -> rusqlite::Result<bool> {
    validate_event(event)?;
    let usage = event.usage.unwrap_or_default();
    let has_usage = event.usage.is_some();
    let changed = conn.execute(
        "INSERT OR IGNORE INTO auth_usage_events (
            auth_request_id, api_call_id, key_id, principal_id, endpoint,
            requested_model, served_model, provider, route, status,
            prompt_tokens, completion_tokens, total_tokens, cached_tokens,
            reasoning_tokens, cost_nano_usd, cost_confidence, created_at_ms
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                  ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
        params![
            event.auth_request_id,
            event.api_call_id,
            event.key_id,
            event.principal_id,
            event.endpoint,
            event.requested_model,
            event.served_model,
            event.provider,
            event.route,
            event.status,
            has_usage.then_some(usage.prompt),
            has_usage.then_some(usage.completion),
            has_usage.then_some(usage.total),
            has_usage.then_some(usage.cached).flatten(),
            has_usage.then_some(usage.reasoning).flatten(),
            event.charge.map(|charge| charge.nano_usd),
            event
                .charge
                .map(|charge| charge.confidence)
                .unwrap_or(UsageCostConfidence::Unavailable)
                .as_str(),
            event.created_at_ms,
        ],
    )?;
    Ok(changed == 1)
}

fn validate_event(event: &UsageEvent) -> rusqlite::Result<()> {
    let required = [
        event.auth_request_id.as_str(),
        event.key_id.as_str(),
        event.principal_id.as_str(),
        event.endpoint.as_str(),
        event.status.as_str(),
    ];
    if required.iter().any(|value| value.trim().is_empty()) {
        return Err(rusqlite::Error::InvalidParameterName(
            "usage event required field is blank".to_string(),
        ));
    }
    if let Some(usage) = event.usage
        && (usage.prompt < 0
            || usage.completion < 0
            || usage.total < 0
            || usage.cached.is_some_and(|value| value < 0)
            || usage.reasoning.is_some_and(|value| value < 0))
    {
        return Err(rusqlite::Error::InvalidParameterName(
            "usage token counts must be non-negative".to_string(),
        ));
    }
    Ok(())
}

pub fn usage_event_count(conn: &Connection, auth_request_id: &str) -> rusqlite::Result<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM auth_usage_events WHERE auth_request_id = ?1",
        [auth_request_id],
        |row| row.get(0),
    )
}

pub fn recorded_cost(conn: &Connection, auth_request_id: &str) -> rusqlite::Result<Option<i64>> {
    conn.query_row(
        "SELECT cost_nano_usd FROM auth_usage_events WHERE auth_request_id = ?1",
        [auth_request_id],
        |row| row.get(0),
    )
    .optional()
    .map(Option::flatten)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: &str) -> UsageEvent {
        let usage = FlowUsage {
            prompt: 100,
            completion: 20,
            total: 120,
            cached: Some(25),
            reasoning: None,
        };
        UsageEvent {
            auth_request_id: id.to_string(),
            api_call_id: None,
            key_id: "key_1".to_string(),
            principal_id: "usr_1".to_string(),
            endpoint: "responses".to_string(),
            requested_model: Some("model-a".to_string()),
            served_model: Some("model-a".to_string()),
            provider: Some("provider-a".to_string()),
            route: None,
            status: "completed".to_string(),
            usage: Some(usage),
            charge: charge_for_usage(
                usage,
                UsageRates {
                    input_per_token: NanoUsd::new(2).unwrap(),
                    output_per_token: NanoUsd::new(6).unwrap(),
                    cached_per_token: Some(NanoUsd::new(1).unwrap()),
                },
            ),
            created_at_ms: 1,
        }
    }

    #[test]
    fn usage_insert_is_idempotent_by_auth_request_id() {
        let conn = Connection::open_in_memory().unwrap();
        migrate_usage_schema(&conn).unwrap();
        assert!(record_usage_once(&conn, &event("authreq_1")).unwrap());
        assert!(!record_usage_once(&conn, &event("authreq_1")).unwrap());
        assert_eq!(usage_event_count(&conn, "authreq_1").unwrap(), 1);
        assert_eq!(recorded_cost(&conn, "authreq_1").unwrap(), Some(295));
    }

    #[test]
    fn unavailable_usage_stays_null_not_zero() {
        let conn = Connection::open_in_memory().unwrap();
        migrate_usage_schema(&conn).unwrap();
        let mut event = event("authreq_2");
        event.usage = None;
        event.charge = None;
        record_usage_once(&conn, &event).unwrap();
        let row: (Option<i64>, Option<i64>, String) = conn
            .query_row(
                "SELECT total_tokens, cost_nano_usd, cost_confidence FROM auth_usage_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(row, (None, None, "unavailable".to_string()));
    }

    #[test]
    fn missing_cache_breakdown_is_estimated() {
        let usage = FlowUsage {
            prompt: 10,
            completion: 2,
            total: 12,
            cached: None,
            reasoning: None,
        };
        let charge = charge_for_usage(
            usage,
            UsageRates {
                input_per_token: NanoUsd::new(2).unwrap(),
                output_per_token: NanoUsd::new(5).unwrap(),
                cached_per_token: None,
            },
        )
        .unwrap();
        assert_eq!(charge.nano_usd, 30);
        assert_eq!(charge.confidence, UsageCostConfidence::Estimated);
    }
}
