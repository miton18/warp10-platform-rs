//! Golden vectors generated from the *upstream implementations* (files in
//! tests/golden/, produced by compiling senx/warp10-platform's SipHashInline /
//! GTSEncoder and running GTSHelper's classId/labelsId logic, plus the
//! reference Python thrift library for TCompactProtocol).
//!
//! Regenerate with the upstream Java sources — see README ("Golden vectors").

use std::collections::BTreeMap;
use warp10_platform_rs::encoder::Encoder;
use warp10_platform_rs::geoxp::{NO_ELEVATION, NO_LOCATION};
use warp10_platform_rs::gts::Value;
use warp10_platform_rs::ids;
use warp10_platform_rs::siphash;
use warp10_platform_rs::thrift::{KafkaDataMessage, KafkaDataMessageType, Metadata};

const K0: u64 = 0x0706050403020100;
const K1: u64 = 0x0f0e0d0c0b0a0908;

fn golden(name: &str) -> String {
    let path = format!("tests/golden/{name}");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

#[test]
fn siphash_standard_official_vectors() {
    // from SipHashInlineTest (ported from the siphash24.c reference vectors)
    let expected = [
        0x726fdb47dd0e0e31u64,
        0x74f839c593dc67fd,
        0x0d6c8009d9a94f5a,
        0x85676696d7fb7e2d,
        0xcf2794e0277187b7,
        0x18765564cd99a68d,
        0xcbc9466e58fee3ce,
        0xab0200f58b01d137,
    ];
    for (i, e) in expected.iter().enumerate() {
        let msg: Vec<u8> = (0..i as u8).collect();
        assert_eq!(siphash::hash24(K0, K1, &msg), *e, "len {i}");
    }
}

#[test]
fn siphash_palindromic_golden() {
    // V24/PAL/GETKEY/PALKEY lines produced by upstream SipHashInline.java
    for line in golden("siphash.txt").lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts[0] {
            "V24" => {
                let len: usize = parts[1].split('=').nth(1).unwrap().parse().unwrap();
                let data = hex::decode(parts[2].split('=').nth(1).unwrap()).unwrap();
                let expected = u64::from_str_radix(parts[3].split('=').nth(1).unwrap(), 16).unwrap();
                assert_eq!(data.len(), len);
                assert_eq!(siphash::hash24(K0, K1, &data), expected, "V24 len={len}");
            }
            "PAL" => {
                let len: usize = parts[1].split('=').nth(1).unwrap().parse().unwrap();
                let data = hex::decode(parts[2].split('=').nth(1).unwrap()).unwrap();
                let expected = u64::from_str_radix(parts[3].split('=').nth(1).unwrap(), 16).unwrap();
                assert_eq!(data.len(), len);
                assert_eq!(siphash::hash24_palindromic(K0, K1, &data), expected, "PAL len={len}");
            }
            "PALKEY" => {
                let k0 = u64::from_str_radix(parts[1].split('=').nth(1).unwrap(), 16).unwrap();
                let k1 = u64::from_str_radix(parts[2].split('=').nth(1).unwrap(), 16).unwrap();
                let msg = hex::decode(parts[3].split('=').nth(1).unwrap()).unwrap();
                let expected = u64::from_str_radix(parts[4].split('=').nth(1).unwrap(), 16).unwrap();
                assert_eq!(siphash::hash24_palindromic(k0, k1, &msg), expected, "PALKEY");
            }
            "GETKEY" => {
                let key = hex::decode(parts[1].split('=').nth(1).unwrap()).unwrap();
                let k0 = u64::from_str_radix(parts[2].split('=').nth(1).unwrap(), 16).unwrap();
                let k1 = u64::from_str_radix(parts[3].split('=').nth(1).unwrap(), 16).unwrap();
                let k = siphash::get_key(&key);
                assert_eq!(k, [k0, k1], "GETKEY");
            }
            _ => panic!("unknown line: {line}"),
        }
    }
}

#[test]
fn class_and_labels_ids_golden() {
    let key = [K0, K1];
    for line in golden("ids.txt").lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts[0] {
            "CLASS" => {
                let name = parts[1];
                let expected = u64::from_str_radix(parts[2], 16).unwrap() as i64;
                assert_eq!(ids::class_id(&key, name), expected, "class {name}");
            }
            "LABELS" => {
                let expected = u64::from_str_radix(parts[1], 16).unwrap() as i64;
                // LABELS uses the fixed 4-label map from the generator
                let mut labels = BTreeMap::new();
                labels.insert(".owner".to_string(), "11111111-2222-3333-4444-555555555555".to_string());
                labels.insert(".producer".to_string(), "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".to_string());
                labels.insert(".app".to_string(), "metrics".to_string());
                labels.insert("host".to_string(), "web1".to_string());
                assert_eq!(ids::labels_id(&key, &labels), expected, "LABELS");
            }
            "LABELS2" => {
                let expected = u64::from_str_radix(parts[1], 16).unwrap() as i64;
                let mut l2 = BTreeMap::new();
                l2.insert("b".to_string(), "2".to_string());
                l2.insert("a".to_string(), "1".to_string());
                assert_eq!(ids::labels_id(&key, &l2), expected, "LABELS2");
            }
            "LABELS_EMPTY" => {
                let expected = u64::from_str_radix(parts[1], 16).unwrap() as i64;
                assert_eq!(ids::labels_id(&key, &BTreeMap::new()), expected, "LABELS_EMPTY");
            }
            _ => panic!("unknown line: {line}"),
        }
    }
}

#[test]
fn fill_gts_ids_big_endian() {
    let k = ids::fill_gts_ids(0x0122334455667788, -42);
    assert_eq!(hex::encode(k), "0122334455667788ffffffffffffffd6");
}

fn enc_add(e: &mut Encoder, ts: i64, loc: i64, elev: i64, v: Value) {
    e.add_value(ts, loc, elev, &v);
}

#[test]
fn encoder_golden_vectors() {
    // ENC1..ENC5 produced by compiling upstream GTSEncoder.java; the inputs are
    // documented in tests/golden/encoder.txt comments upstream, reproduced here.
    let mut e1 = Encoder::new(0);
    enc_add(&mut e1, 1700000000000000, NO_LOCATION, NO_ELEVATION, Value::Long(42));
    let mut e2 = Encoder::new(1000);
    enc_add(&mut e2, 1000, NO_LOCATION, NO_ELEVATION, Value::Long(42));
    enc_add(&mut e2, 1001, NO_LOCATION, NO_ELEVATION, Value::Long(43));
    enc_add(&mut e2, 1002, 0x0145014501450145, 100, Value::Long(43));
    enc_add(&mut e2, 2000, 0x0145014501450146, 100, Value::Double(1.5));
    enc_add(&mut e2, 3000, NO_LOCATION, NO_ELEVATION, Value::Str("hello".into()));
    enc_add(&mut e2, 4000, NO_LOCATION, NO_ELEVATION, Value::Bool(true));
    enc_add(&mut e2, 5000, NO_LOCATION, NO_ELEVATION, Value::Long(-7));
    let mut e3 = Encoder::new(5);
    enc_add(&mut e3, 5, NO_LOCATION, NO_ELEVATION, Value::Long(1));
    enc_add(&mut e3, 1i64 << 50, NO_LOCATION, NO_ELEVATION, Value::Long(2));
    let mut e4 = Encoder::new(10);
    enc_add(&mut e4, 10, NO_LOCATION, NO_ELEVATION, Value::Str("abc".into()));
    enc_add(&mut e4, 11, NO_LOCATION, NO_ELEVATION, Value::Str("abc".into()));
    enc_add(&mut e4, 12, NO_LOCATION, NO_ELEVATION, Value::Delete);
    let mut e5 = Encoder::new(0);
    enc_add(&mut e5, 1, NO_LOCATION, NO_ELEVATION, Value::Long(1i64 << 50));
    enc_add(&mut e5, 2, NO_LOCATION, NO_ELEVATION, Value::Double(2.5));
    enc_add(&mut e5, 3, NO_LOCATION, NO_ELEVATION, Value::Double(2.5));

    let mut expected: BTreeMap<String, String> = BTreeMap::new();
    for line in golden("encoder.txt").lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        expected.insert(parts[0].to_string(), parts[2].to_string());
    }
    for (name, enc) in [("ENC1", &e1), ("ENC2", &e2), ("ENC3", &e3), ("ENC4", &e4), ("ENC5", &e5)] {
        assert_eq!(hex::encode(enc.get_bytes()), expected[name], "{name}");
    }
}

#[test]
fn thrift_compact_golden() {
    // produced with the reference Python thrift library (TCompactProtocol)
    let mut attrs = BTreeMap::new();
    attrs.insert("fdb.tenant.prefix".to_string(), "abc".to_string());
    let kdm = KafkaDataMessage {
        class_id: 0x0122334455667788,
        labels_id: -42,
        data: Some(&[1, 2, 3]),
        indices: None,
        deletion_start: None,
        deletion_end: None,
        msg_type: KafkaDataMessageType::Store,
        deletion_min_age: None,
        metadata: None,
        attributes: Some(&attrs),
    };
    assert_eq!(
        hex::encode(kdm.serialize()),
        "1690deb3d68ad199a2021653180301020345023b0188116664622e74656e616e742e7072656669780361626300"
    );

    let mut labels = BTreeMap::new();
    labels.insert(".owner".to_string(), "1111".to_string());
    labels.insert("host".to_string(), "web1".to_string());
    let md = Metadata {
        name: "cpu",
        labels: &labels,
        class_id: 0x0122334455667788,
        labels_id: -42,
        attributes: None,
        source: None,
        last_activity: None,
    };
    assert_eq!(
        hex::encode(md.serialize()),
        "18036370751b0288062e6f776e6572043131313104686f737404776562311690deb3d68ad199a202165300"
    );
}
