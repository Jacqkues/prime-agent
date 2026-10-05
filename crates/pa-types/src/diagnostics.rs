//! Versioned, opt-in execution traces. Unlike adoption telemetry, these records
//! deliberately contain application content and require operator-only access.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Boundary where bytes or a normalized provider event were actually observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TracePoint {
    HttpRequest,
    HttpResponse,
    DaemonReceive,
    DaemonSend,
    WorkerReceive,
    WorkerSend,
    KernelSend,
    KernelReceive,
    ModelRequest,
    ModelResponse,
    ApplicationKernelSend,
    ApplicationKernelReceive,
}

/// One captured exchange. IDs remain unique across worker/process restarts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionTrace {
    pub version: u32,
    pub id: String,
    pub at_ms: u64,
    pub pid: u32,
    pub point: TracePoint,
    pub active_session_id: Option<String>,
    pub correlation_id: Option<String>,
    pub payload: Value,
    pub truncated: bool,
    pub dropped_total: u64,
}

/// Remove structured credentials before a trace is persisted or served. Text
/// content is intentionally retained; this is not general-purpose secret DLP.
/// JSON encoded inside strings is also sanitized (daemon prompt envelopes).
pub fn redact_trace(value: &mut Value) {
    redact(value, 0);
}

fn redact(value: &mut Value, depth: usize) {
    if depth > 64 {
        *value = Value::String("[depth limit]".into());
        return;
    }
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                let key: String = key
                    .chars()
                    .filter(char::is_ascii_alphanumeric)
                    .flat_map(char::to_lowercase)
                    .collect();
                if matches!(
                    key.as_str(),
                    "authorization"
                        | "proxyauthorization"
                        | "cookie"
                        | "setcookie"
                        | "token"
                        | "accesstoken"
                        | "refreshtoken"
                        | "idtoken"
                        | "password"
                        | "clientsecret"
                        | "workertoken"
                        | "connectiontoken"
                        | "supervisorownertoken"
                        | "xapikey"
                ) || key.ends_with("apikey")
                    || key.ends_with("authtoken")
                {
                    *value = Value::String("[redacted]".into());
                } else {
                    redact(value, depth + 1);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                redact(value, depth + 1);
            }
        }
        Value::String(text) => {
            if text.starts_with('{') || text.starts_with('[') {
                if let Ok(mut encoded) = serde_json::from_str(text) {
                    redact(&mut encoded, depth + 1);
                    *text = encoded.to_string();
                }
            } else if text.starts_with("Bearer ") || text.starts_with("Basic ") {
                *text = "[redacted]".into();
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn credentials_are_redacted_without_erasing_prompts_or_token_usage() {
        let mut value = json!({"api_key":"secret", "headers":{"Authorization":"Bearer secret"},"nested":"{\"workerToken\":\"secret\",\"text\":\"hello\"}","messages":[{"content":"actual prompt"}],"max_tokens":500,"usage":{"output_tokens":42}});
        redact_trace(&mut value);
        assert_eq!(
            value,
            json!({"api_key":"[redacted]", "headers":{"Authorization":"[redacted]"},"nested":"{\"workerToken\":\"[redacted]\",\"text\":\"hello\"}","messages":[{"content":"actual prompt"}],"max_tokens":500,"usage":{"output_tokens":42}})
        );
    }
}
