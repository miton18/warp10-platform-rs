//! The owner/producer policy — the heart of the experiment.
//!
//! Stock Warp 10 behavior (StandaloneIngressHandler): the labels `.producer`,
//! `.owner` and `.app` are *always forced from the write token*, overriding
//! whatever the client pushed (GTSHelper: `labels.putAll(extraLabels)`).
//! A token without owner/producer is rejected with 403 "Invalid token".
//!
//! This crate keeps that as the **default** and adds an *opt-in* relaxation:
//! a token carrying the attribute `.allowowner` (resp. `.allowproducer`,
//! `.allowapp`) may let the client-pushed label through, but only if it is a
//! syntactically valid UUID. Anything else falls back to the token value.
//! Deny-by-default: no attribute → token always wins, exactly like the stock
//! ingress.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const PRODUCER_LABEL: &str = ".producer";
pub const OWNER_LABEL: &str = ".owner";
pub const APPLICATION_LABEL: &str = ".app";

pub const ATTR_ALLOW_OWNER: &str = ".allowowner";
pub const ATTR_ALLOW_PRODUCER: &str = ".allowproducer";
pub const ATTR_ALLOW_APP: &str = ".allowapp";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteTokenClaims {
    /// UUID of the owner the token writes for (required, like a WRITE token).
    pub owner: String,
    /// UUID of the producer (required).
    pub producer: String,
    /// Application name (optional; stock removes .app when absent).
    #[serde(default)]
    pub app: Option<String>,
    /// Extra labels forced onto every GTS (write token "labels" map).
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Token attributes (capabilities), e.g. ".allowowner".
    #[serde(default)]
    pub attributes: Vec<String>,
    /// Expiry in ms since epoch; absent = never expires.
    #[serde(default)]
    pub expiry: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PolicyError {
    /// 403 — token itself is invalid (missing owner/producer or expired)
    Forbidden(String),
}

/// A UUID as Warp 10 sees it: 36-char canonical form or 32 hex chars.
pub fn is_valid_uuid(s: &str) -> bool {
    let hexes: String = s.chars().filter(|c| *c != '-').collect();
    if hexes.len() != 32 || !hexes.chars().all(|c| c.is_ascii_hexdigit()) {
        return false;
    }
    if s.len() == 36 {
        let b = s.as_bytes();
        return b[8] == b'-' && b[13] == b'-' && b[18] == b'-' && b[23] == b'-';
    }
    s.len() == 32
}

impl WriteTokenClaims {
    pub fn check_valid(&self, now_ms: i64) -> Result<(), PolicyError> {
        if !is_valid_uuid(&self.owner) || !is_valid_uuid(&self.producer) {
            return Err(PolicyError::Forbidden("Invalid token.".into()));
        }
        if let Some(exp) = self.expiry {
            if exp > 0 && exp < now_ms {
                return Err(PolicyError::Forbidden("Token expired.".into()));
            }
        }
        Ok(())
    }

    fn allows(&self, attr: &str) -> bool {
        self.attributes.iter().any(|a| a == attr)
    }

    /// Apply the token to the labels of one datum (client labels in `labels`).
    ///
    /// Mutates `labels` in place: token labels are applied first, then
    /// .producer/.owner/.app are forced (stock) or conditionally overridden
    /// (relaxed mode, opt-in via token attributes).
    pub fn apply(&self, labels: &mut BTreeMap<String, String>) {
        for (k, v) in &self.labels {
            labels.insert(k.clone(), v.clone());
        }
        apply_one(labels, PRODUCER_LABEL, &self.producer, self.allows(ATTR_ALLOW_PRODUCER));
        apply_one(labels, OWNER_LABEL, &self.owner, self.allows(ATTR_ALLOW_OWNER));
        // stock: .app is forced if the token has one, removed otherwise
        match &self.app {
            Some(app) => apply_one(labels, APPLICATION_LABEL, app, self.allows(ATTR_ALLOW_APP)),
            None => {
                if self.allows(ATTR_ALLOW_APP) {
                    if let Some(v) = labels.get(APPLICATION_LABEL).cloned() {
                        if is_valid_uuid(&v) || !v.starts_with('.') {
                            // keep client .app only if it does not look like a reserved label
                            let _ = v;
                        } else {
                            labels.remove(APPLICATION_LABEL);
                        }
                    } else {
                        labels.remove(APPLICATION_LABEL);
                    }
                } else {
                    labels.remove(APPLICATION_LABEL);
                }
            }
        }
    }
}

fn apply_one(labels: &mut BTreeMap<String, String>, key: &str, token_value: &str, allowed: bool) {
    if allowed {
        if let Some(client) = labels.get(key) {
            if is_valid_uuid(client) {
                return; // keep the client-provided, valid UUID
            }
        }
    }
    labels.insert(key.to_string(), token_value.to_string());
}
