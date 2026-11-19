//! Configuration: JSON file + env overrides (WARP10_RS_*).

use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Listen address of the HTTP ingress.
    pub listen: String,
    /// JSON token directory (token string -> WriteTokenClaims).
    pub tokens_file: PathBuf,
    /// 16-byte SipHash key for class ids (hex) — `warp.siphash.class`.
    pub siphash_class_key: String,
    /// 16-byte SipHash key for labels ids (hex) — `warp.siphash.labels`.
    pub siphash_labels_key: String,
    pub data_topic: String,
    pub meta_topic: String,
    /// Kafka brokers (only used with the `kafka` feature).
    pub kafka_brokers: String,
    /// Max request body size in bytes.
    pub max_body_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen: "127.0.0.1:8080".to_string(),
            tokens_file: PathBuf::from("tokens.json"),
            siphash_class_key: "00".repeat(16),
            siphash_labels_key: "00".repeat(16),
            data_topic: "warp10.data".to_string(),
            meta_topic: "warp10.meta".to_string(),
            kafka_brokers: "localhost:9092".to_string(),
            max_body_bytes: 32 * 1024 * 1024,
        }
    }
}

fn env_str(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

impl Config {
    pub fn load(path: Option<&PathBuf>) -> Result<Self, String> {
        let mut cfg = match path {
            Some(p) => {
                let content = std::fs::read_to_string(p)
                    .map_err(|e| format!("read {}: {e}", p.display()))?;
                serde_json::from_str(&content).map_err(|e| format!("parse {}: {e}", p.display()))?
            }
            None => Config::default(),
        };
        // env overrides
        if let Some(v) = env_str("WARP10_RS_LISTEN") {
            cfg.listen = v;
        }
        if let Some(v) = env_str("WARP10_RS_TOKENS_FILE") {
            cfg.tokens_file = PathBuf::from(v);
        }
        if let Some(v) = env_str("WARP10_RS_SIPHASH_CLASS_KEY") {
            cfg.siphash_class_key = v;
        }
        if let Some(v) = env_str("WARP10_RS_SIPHASH_LABELS_KEY") {
            cfg.siphash_labels_key = v;
        }
        if let Some(v) = env_str("WARP10_RS_DATA_TOPIC") {
            cfg.data_topic = v;
        }
        if let Some(v) = env_str("WARP10_RS_META_TOPIC") {
            cfg.meta_topic = v;
        }
        if let Some(v) = env_str("WARP10_RS_KAFKA_BROKERS") {
            cfg.kafka_brokers = v;
        }
        Ok(cfg)
    }

    pub fn listen_addr(&self) -> Result<SocketAddr, String> {
        self.listen.parse().map_err(|e| format!("bad listen addr '{}': {e}", self.listen))
    }

    pub fn class_key(&self) -> Result<[u64; 2], String> {
        let b = hex::decode(&self.siphash_class_key).map_err(|e| format!("bad siphash class key: {e}"))?;
        if b.len() != 16 {
            return Err("siphash class key must be 16 bytes (32 hex chars)".into());
        }
        Ok(crate::siphash::get_key(&b))
    }

    pub fn labels_key(&self) -> Result<[u64; 2], String> {
        let b = hex::decode(&self.siphash_labels_key).map_err(|e| format!("bad siphash labels key: {e}"))?;
        if b.len() != 16 {
            return Err("siphash labels key must be 16 bytes (32 hex chars)".into());
        }
        Ok(crate::siphash::get_key(&b))
    }
}
