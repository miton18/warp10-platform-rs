//! HTTP ingress: `POST /api/v0/update` (header `X-Warp10-Token`), `GET /healthz`.
//!
//! Pipeline per request:
//!   token → claims (403 if invalid) → parse lines (400 on error) →
//!   policy.apply(labels) → group by (class, labels) → GTSEncoder (base 0) →
//!   classId/labelsId (palindromic SipHash) → Kafka meta (once per gtsId)
//!   + data messages.

use crate::config::Config;
use crate::encoder::Encoder;
use crate::gts::{parse_line, Datum};
use crate::ids;
use crate::kafka::{MockProducer, Producer, Record};
use crate::policy::{PolicyError, WriteTokenClaims};
use crate::thrift::{KafkaDataMessage, KafkaDataMessageType, Metadata};
use crate::tokens::TokenDirectory;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{header::HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub tokens: TokenDirectory,
    pub producer: Arc<dyn Producer>,
    pub mock: Option<MockProducer>,
    pub class_key: ids::SipKey,
    pub labels_key: ids::SipKey,
    pub data_mac: Option<ids::SipKey>,
    pub meta_mac: Option<ids::SipKey>,
    /// gtsIds for which metadata was already published (dedup, like the
    /// stock ingress' IngressMetadataConsumer).
    pub seen_gts: Arc<Mutex<HashSet<i64>>>,
}

#[derive(Serialize)]
struct UpdateResponse {
    received: usize,
    stored: usize,
    ignored: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<String>,
}

fn err(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, Json(serde_json::json!({"error": msg.into()}))).into_response()
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/api/v0/update", post(update))
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/debug/messages", get(debug_messages))
        .layer(DefaultBodyLimit::max(state.config.max_body_bytes))
        .with_state(state)
}

async fn debug_messages(State(state): State<AppState>) -> Response {
    match &state.mock {
        Some(m) => {
            let recs: Vec<serde_json::Value> = m
                .records()
                .iter()
                .map(|r: &Record| {
                    serde_json::json!({
                        "topic": r.topic,
                        "key": hex::encode(&r.key),
                        "value": hex::encode(&r.value),
                    })
                })
                .collect();
            Json(serde_json::json!({"messages": recs})).into_response()
        }
        None => err(StatusCode::NOT_FOUND, "mock producer not enabled"),
    }
}

async fn update(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let now_ms = now_ms();
    let now_us = now_ms * 1000;

    let token = match headers.get("x-warp10-token").and_then(|v| v.to_str().ok()) {
        Some(t) => t.to_string(),
        None => return err(StatusCode::FORBIDDEN, "Missing token."),
    };
    let claims: WriteTokenClaims = match state.tokens.resolve(&token, now_ms) {
        Ok(c) => c,
        Err(PolicyError::Forbidden(m)) => return err(StatusCode::FORBIDDEN, m),
    };

    let text = match std::str::from_utf8(&body) {
        Ok(t) => t,
        Err(_) => return err(StatusCode::BAD_REQUEST, "body is not UTF-8"),
    };

    // parse all lines
    let mut datums: Vec<Datum> = Vec::new();
    let mut errors: Vec<String> = Vec::new();
    let mut received = 0usize;
    for (i, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        received += 1;
        match parse_line(line, now_us) {
            Ok(mut d) => {
                claims.apply(&mut d.labels);
                datums.push(d);
            }
            Err(e) => errors.push(format!("line {}: {e}", i + 1)),
        }
    }

    // group by (class, labels), preserving first-seen order
    let mut groups: HashMap<(String, BTreeMap<String, String>), Vec<Datum>> = HashMap::new();
    let mut order: Vec<(String, BTreeMap<String, String>)> = Vec::new();
    for d in datums {
        let key = (d.class.clone(), d.labels.clone());
        if !groups.contains_key(&key) {
            order.push(key.clone());
        }
        groups.entry(key).or_default().push(d);
    }

    let mut stored = 0usize;
    for key in order {
        let ds = &groups[&key];
        let (class, labels) = &key;

        // encode (base 0, like the stock ingress)
        let mut enc = Encoder::new(0);
        for d in ds {
            enc.add_value(d.ts, d.location, d.elevation, &d.value);
        }

        let class_id = ids::class_id(&state.class_key, class);
        let labels_id = ids::labels_id(&state.labels_key, labels);
        let gts_id = ids::gts_id(&state.labels_key, class_id, labels_id);
        let kafka_key = ids::fill_gts_ids(class_id, labels_id);

        // metadata message (once per gts), value = key ++ thrift(Metadata)
        let new_gts = {
            let mut seen = state.seen_gts.lock().unwrap();
            seen.insert(gts_id)
        };
        if new_gts {
            let meta = Metadata {
                name: class,
                labels,
                class_id,
                labels_id,
                attributes: None,
                source: Some("warp10-platform-rs"),
                last_activity: None,
            };
            let meta_bytes = meta.serialize();
            let mut value = kafka_key.to_vec();
            value.extend_from_slice(&meta_bytes);
            if let Some(mac) = &state.meta_mac {
                let h = crate::siphash::hash24_palindromic(mac[0], mac[1], &value);
                value.extend_from_slice(&h.to_be_bytes());
            }
            if let Err(e) = state.producer.send(&state.config.meta_topic, &kafka_key, &value) {
                return err(StatusCode::INTERNAL_SERVER_ERROR, format!("meta produce: {e}"));
            }
        }

        // data message: thrift(KafkaDataMessage{STORE, classId, labelsId, data})
        let kdm = KafkaDataMessage {
            class_id,
            labels_id,
            data: Some(enc.get_bytes()),
            indices: None,
            deletion_start: None,
            deletion_end: None,
            msg_type: KafkaDataMessageType::Store,
            deletion_min_age: None,
            metadata: None,
            attributes: None,
        };
        let mut value = kdm.serialize();
        if let Some(mac) = &state.data_mac {
            let h = crate::siphash::hash24_palindromic(mac[0], mac[1], &value);
            value.extend_from_slice(&h.to_be_bytes());
        }
        if let Err(e) = state.producer.send(&state.config.data_topic, &kafka_key, &value) {
            return err(StatusCode::INTERNAL_SERVER_ERROR, format!("data produce: {e}"));
        }
        stored += ds.len();
    }

    if let Err(e) = state.producer.flush() {
        return err(StatusCode::INTERNAL_SERVER_ERROR, format!("flush: {e}"));
    }

    let status = if stored > 0 || errors.is_empty() {
        StatusCode::OK
    } else {
        StatusCode::BAD_REQUEST
    };
    (
        status,
        Json(UpdateResponse { received, stored, ignored: received - stored, errors }),
    )
        .into_response()
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Build the app state. With the `kafka` feature and WARP10_RS_KAFKA=1, uses a
/// real Kafka producer; otherwise the mock.
pub fn build_state(config: Config) -> Result<AppState, String> {
    let tokens = TokenDirectory::load(&config.tokens_file)?;
    let class_key = config.class_key()?;
    let labels_key = config.labels_key()?;
    let data_mac = config.data_mac_key()?;
    let meta_mac = config.meta_mac_key()?;

    #[cfg(feature = "kafka")]
    let (producer, mock): (Arc<dyn Producer>, Option<MockProducer>) = {
        if std::env::var("WARP10_RS_KAFKA").as_deref() == Ok("1") {
            let p = crate::kafka::RdkafkaProducer::new(&config.kafka_brokers)
                .map_err(|e| format!("kafka producer: {e}"))?;
            (Arc::new(p), None)
        } else {
            let m = MockProducer::new();
            (Arc::new(m.clone()), Some(m))
        }
    };
    #[cfg(not(feature = "kafka"))]
    let (producer, mock): (Arc<dyn Producer>, Option<MockProducer>) = {
        let m = MockProducer::new();
        (Arc::new(m.clone()), Some(m))
    };

    Ok(AppState {
        config: Arc::new(config),
        tokens,
        producer,
        mock,
        class_key,
        labels_key,
        data_mac,
        meta_mac,
        seen_gts: Arc::new(Mutex::new(HashSet::new())),
    })
}
