//! End-to-end HTTP test: POST /api/v0/update against the axum app with the
//! mock producer, asserting the Kafka contract (meta + data messages) and the
//! owner/producer policy (forced vs relaxed).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::collections::{BTreeMap, HashMap};
use tower::ServiceExt;
use warp10_platform_rs::config::Config;
use warp10_platform_rs::policy::{
    WriteTokenClaims, ATTR_ALLOW_OWNER, ATTR_ALLOW_PRODUCER,
};
use warp10_platform_rs::server::{app, AppState};
use warp10_platform_rs::tokens::TokenDirectory;

const OWNER_A: &str = "11111111-2222-3333-4444-555555555555";
const OWNER_B: &str = "99999999-8888-7777-6666-555555555555";
const PRODUCER: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

fn test_state() -> AppState {
    let mut tokens = HashMap::new();
    tokens.insert(
        "tok-stock".to_string(),
        WriteTokenClaims {
            owner: OWNER_A.into(),
            producer: PRODUCER.into(),
            app: Some("metrics".into()),
            labels: BTreeMap::new(),
            attributes: vec![],
            expiry: None,
        },
    );
    tokens.insert(
        "tok-relaxed".to_string(),
        WriteTokenClaims {
            owner: OWNER_A.into(),
            producer: PRODUCER.into(),
            app: Some("metrics".into()),
            labels: BTreeMap::new(),
            attributes: vec![ATTR_ALLOW_OWNER.to_string(), ATTR_ALLOW_PRODUCER.to_string()],
            expiry: None,
        },
    );

    let mut cfg = Config::default();
    cfg.data_topic = "data-topic".into();
    cfg.meta_topic = "meta-topic".into();
    cfg.siphash_class_key = "0102030405060708090a0b0c0d0e0f10".into();
    cfg.siphash_labels_key = "1112131415161718191a1b1c1d1e1f20".into();

    let class_key = cfg.class_key().unwrap();
    let labels_key = cfg.labels_key().unwrap();
    let mock = warp10_platform_rs::kafka::MockProducer::new();
    AppState {
        config: std::sync::Arc::new(cfg),
        tokens: TokenDirectory::from_map(tokens),
        producer: std::sync::Arc::new(mock.clone()),
        mock: Some(mock),
        class_key,
        labels_key,
        data_mac: None,
        meta_mac: None,
        seen_gts: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
    }
}

async fn post(state: &AppState, token: Option<&str>, body: &str) -> (StatusCode, String) {
    let app = app(state.clone());
    let mut req = Request::builder().method("POST").uri("/api/v0/update").body(Body::from(body.to_string())).unwrap();
    if let Some(t) = token {
        req.headers_mut().insert("x-warp10-token", t.parse().unwrap());
    }
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 10 * 1024 * 1024).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

#[tokio::test]
async fn healthz() {
    let state = test_state();
    let app = app(state);
    let resp = app.oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn update_requires_token() {
    let state = test_state();
    let (status, _) = post(&state, None, "1700000000000000 cpu{} 42").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = post(&state, Some("nope"), "1700000000000000 cpu{} 42").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn update_stock_forces_owner_and_produces_meta_and_data() {
    let state = test_state();
    let body = format!("1700000000000000 cpu{{.owner={OWNER_B},host=web1}} 42");
    let (status, text) = post(&state, Some("tok-stock"), &body).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert!(text.contains("\"stored\":1"), "{text}");

    let recs = state.mock.as_ref().unwrap().records();
    assert_eq!(recs.len(), 2, "one meta + one data message");
    let meta = recs.iter().find(|r| r.topic == "meta-topic").expect("meta");
    let data = recs.iter().find(|r| r.topic == "data-topic").expect("data");

    // key = 16 bytes classId||labelsId BE, identical for meta and data
    assert_eq!(meta.key.len(), 16);
    assert_eq!(meta.key, data.key);

    // meta value = key ++ thrift(Metadata); labels inside must have the TOKEN's
    // owner (forced), not the client-pushed OWNER_B
    let meta_value = &meta.value;
    assert_eq!(&meta_value[..16], &meta.key[..], "meta value prefixed by key");
    let meta_str = String::from_utf8_lossy(&meta_value[16..]);
    assert!(meta_str.contains(OWNER_A), "meta must contain token owner: {meta_str}");
    assert!(!meta_str.contains(OWNER_B), "client-pushed owner must be overridden: {meta_str}");
    assert!(meta_str.contains(".owner"));
    assert!(meta_str.contains("host"));
    assert!(meta_str.contains("web1"));

    // data value = thrift(KafkaDataMessage) — first field header + classId
    let data_hex = hex::encode(&data.value);
    assert!(data_hex.starts_with("16"), "compact: field 1 (i64) header, got {data_hex}");
}

#[tokio::test]
async fn update_relaxed_token_keeps_valid_client_owner() {
    let state = test_state();
    let body = format!("1700000000000000 mem{{.owner={OWNER_B}}} 7");
    let (status, _) = post(&state, Some("tok-relaxed"), &body).await;
    assert_eq!(status, StatusCode::OK);
    let recs = state.mock.as_ref().unwrap().records();
    let meta = recs.iter().find(|r| r.topic == "meta-topic").unwrap();
    let meta_str = String::from_utf8_lossy(&meta.value[16..]);
    assert!(meta_str.contains(OWNER_B), "relaxed token must keep valid client owner: {meta_str}");
}

#[tokio::test]
async fn update_relaxed_token_rejects_invalid_client_owner() {
    let state = test_state();
    let body = "1700000000000000 mem{.owner=not-a-uuid} 7".to_string();
    let (status, _) = post(&state, Some("tok-relaxed"), &body).await;
    assert_eq!(status, StatusCode::OK);
    let recs = state.mock.as_ref().unwrap().records();
    let meta = recs.iter().find(|r| r.topic == "meta-topic").unwrap();
    let meta_str = String::from_utf8_lossy(&meta.value[16..]);
    assert!(!meta_str.contains("not-a-uuid"));
    assert!(meta_str.contains(OWNER_A));
}

#[tokio::test]
async fn update_bad_line_is_400_when_nothing_stored() {
    let state = test_state();
    let (status, text) = post(&state, Some("tok-stock"), "garbage line").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
}

#[tokio::test]
async fn metadata_dedup_per_gts() {
    let state = test_state();
    // two posts with the same class+labels → only one meta message
    let body = "1700000000000000 cpu{host=web1} 1\n1700000000000001 cpu{host=web1} 2";
    let (status, _) = post(&state, Some("tok-stock"), body).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = post(&state, Some("tok-stock"), "1700000000000002 cpu{host=web1} 3").await;
    assert_eq!(status, StatusCode::OK);
    let recs = state.mock.as_ref().unwrap().records();
    let metas = recs.iter().filter(|r| r.topic == "meta-topic").count();
    let datas = recs.iter().filter(|r| r.topic == "data-topic").count();
    assert_eq!(metas, 1, "meta deduped");
    assert_eq!(datas, 2, "one data message per request");
}
