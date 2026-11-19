//! Unit tests for the line parser and the owner/producer policy
//! (deny-by-default, opt-in relaxation, cross-tenant protection).

use std::collections::{BTreeMap, HashMap};
use warp10_platform_rs::geoxp::to_geo_xp_point;
use warp10_platform_rs::gts::{parse_line, Value};
use warp10_platform_rs::policy::{
    is_valid_uuid, WriteTokenClaims, ATTR_ALLOW_OWNER, ATTR_ALLOW_PRODUCER, OWNER_LABEL, PRODUCER_LABEL,
};
use warp10_platform_rs::tokens::TokenDirectory;

fn claims(attrs: Vec<&str>) -> WriteTokenClaims {
    WriteTokenClaims {
        owner: "11111111-2222-3333-4444-555555555555".into(),
        producer: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into(),
        app: Some("metrics".into()),
        labels: BTreeMap::new(),
        attributes: attrs.into_iter().map(String::from).collect(),
        expiry: None,
    }
}

#[test]
fn uuid_validation() {
    assert!(is_valid_uuid("11111111-2222-3333-4444-555555555555"));
    assert!(is_valid_uuid("11111111222233334444555555555555"));
    assert!(!is_valid_uuid("not-a-uuid"));
    assert!(!is_valid_uuid("11111111-2222-3333-4444-55555555555g"));
    assert!(!is_valid_uuid(""));
}

#[test]
fn policy_stock_forces_token_labels() {
    // No .allow* attributes: token always wins (stock Warp 10 behavior).
    let c = claims(vec![]);
    let mut labels = BTreeMap::new();
    labels.insert(OWNER_LABEL.to_string(), "99999999-8888-7777-6666-555555555555".into());
    labels.insert("host".to_string(), "web1".into());
    c.apply(&mut labels);
    assert_eq!(labels[OWNER_LABEL], "11111111-2222-3333-4444-555555555555");
    assert_eq!(labels[PRODUCER_LABEL], "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    assert_eq!(labels[".app"], "metrics");
    assert_eq!(labels["host"], "web1");
}

#[test]
fn policy_relaxed_keeps_valid_client_owner_only_with_attribute() {
    let mut labels = BTreeMap::new();
    labels.insert(OWNER_LABEL.to_string(), "99999999-8888-7777-6666-555555555555".into());

    // without attribute: token wins
    let c_stock = claims(vec![]);
    let mut l = labels.clone();
    c_stock.apply(&mut l);
    assert_eq!(l[OWNER_LABEL], "11111111-2222-3333-4444-555555555555");

    // with attribute + valid UUID: client value kept
    let c_relaxed = claims(vec![ATTR_ALLOW_OWNER, ATTR_ALLOW_PRODUCER]);
    let mut l = labels.clone();
    c_relaxed.apply(&mut l);
    assert_eq!(l[OWNER_LABEL], "99999999-8888-7777-6666-555555555555");

    // with attribute but garbage value: fall back to token
    let mut l = BTreeMap::new();
    l.insert(OWNER_LABEL.to_string(), "evil".into());
    c_relaxed.apply(&mut l);
    assert_eq!(l[OWNER_LABEL], "11111111-2222-3333-4444-555555555555");
}

#[test]
fn token_directory_403_semantics() {
    let mut m = HashMap::new();
    m.insert("tok".to_string(), claims(vec![]));
    let dir = TokenDirectory::from_map(m);
    assert!(dir.resolve("tok", 1000).is_ok());
    assert!(dir.resolve("unknown", 1000).is_err()); // 403

    // expired token
    let mut m = HashMap::new();
    let mut c = claims(vec![]);
    c.expiry = Some(500);
    m.insert("old".to_string(), c);
    let dir = TokenDirectory::from_map(m);
    assert!(dir.resolve("old", 1000).is_err()); // expired → 403
    assert!(dir.resolve("old", 400).is_ok());

    // invalid owner uuid in token → 403
    let mut m = HashMap::new();
    let mut c = claims(vec![]);
    c.owner = "nope".into();
    m.insert("bad".to_string(), c);
    let dir = TokenDirectory::from_map(m);
    assert!(dir.resolve("bad", 1000).is_err());
}

#[test]
fn parse_line_basic() {
    let d = parse_line("1700000000000000 cpu{host=web1,env=prod} 42", 0).unwrap();
    assert_eq!(d.ts, 1700000000000000);
    assert_eq!(d.class, "cpu");
    assert_eq!(d.labels["host"], "web1");
    assert_eq!(d.labels["env"], "prod");
    assert_eq!(d.value, Value::Long(42));
}

#[test]
fn parse_line_location_elevation_and_types() {
    let d = parse_line("1700000000000000/48.8566:2.3522/35 cpu{} 1.5", 0).unwrap();
    assert_eq!(d.location, to_geo_xp_point(48.8566, 2.3522));
    assert_eq!(d.elevation, 35);
    assert_eq!(d.value, Value::Double(1.5));

    let d = parse_line("1700000000000000// cpu{} T", 0).unwrap();
    assert_eq!(d.value, Value::Bool(true));

    let d = parse_line("1700000000000000 cpu{} 'hello world'", 0).unwrap();
    assert_eq!(d.value, Value::Str("hello world".into()));

    let d = parse_line("now cpu{} -7", 1234567890).unwrap();
    assert_eq!(d.ts, 1234567890);
    assert_eq!(d.value, Value::Long(-7));

    // relative timestamp: -1000 → now - 1000 µs
    let d = parse_line("-1000 cpu{} 1", 5000).unwrap();
    assert_eq!(d.ts, 4000);
}

#[test]
fn parse_line_errors() {
    assert!(parse_line("", 0).is_err());
    assert!(parse_line("garbage", 0).is_err());
    assert!(parse_line("123 cpu{unbalanced 1", 0).is_err());
}
