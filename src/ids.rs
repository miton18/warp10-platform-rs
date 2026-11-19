//! GTS id computation, ported from `io.warp10.continuum.gts.GTSHelper`
//! (classId / labelsId / gtsId / fillGTSIds).

use crate::siphash::hash24_palindromic;
use std::collections::BTreeMap;

pub type SipKey = [u64; 2];

/// classId = palindromic SipHash of the UTF-8 class name.
pub fn class_id(key: &SipKey, name: &str) -> i64 {
    hash24_palindromic(key[0], key[1], name.as_bytes()) as i64
}

/// labelsId = palindromic SipHash of the big-endian concatenation of the
/// per-label palindromic hashes (name hash, value hash), sorted ascending.
pub fn labels_id(key: &SipKey, labels: &BTreeMap<String, String>) -> i64 {
    let mut hashes: Vec<(i64, i64)> = labels
        .iter()
        .map(|(k, v)| {
            (
                hash24_palindromic(key[0], key[1], k.as_bytes()) as i64,
                hash24_palindromic(key[0], key[1], v.as_bytes()) as i64,
            )
        })
        .collect();
    hashes.sort();
    let mut buf = Vec::with_capacity(hashes.len() * 16);
    for (k, v) in &hashes {
        buf.extend_from_slice(&k.to_be_bytes());
        buf.extend_from_slice(&v.to_be_bytes());
    }
    hash24_palindromic(key[0], key[1], &buf) as i64
}

/// gtsId = palindromic SipHash of "GTS:" + classId(BE) + ":" + labelsId(BE).
pub fn gts_id(key: &SipKey, class_id: i64, labels_id: i64) -> i64 {
    let mut buf = Vec::with_capacity(21);
    buf.extend_from_slice(b"GTS:");
    buf.extend_from_slice(&class_id.to_be_bytes());
    buf.push(b':');
    buf.extend_from_slice(&labels_id.to_be_bytes());
    hash24_palindromic(key[0], key[1], &buf) as i64
}

/// 16-byte Kafka key: classId(BE) || labelsId(BE), as `GTSHelper.fillGTSIds`.
pub fn fill_gts_ids(class_id: i64, labels_id: i64) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&class_id.to_be_bytes());
    out[8..].copy_from_slice(&labels_id.to_be_bytes());
    out
}
