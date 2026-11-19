//! Local write-token directory.
//!
//! Real Warp 10 tokens are AES-encrypted; decrypting them is out of scope for
//! this experiment. Instead the ingress loads a JSON directory mapping the
//! *token string presented in the `X-Warp10-Token` header* to its claims
//! (owner/producer/app/attributes/expiry). In production this lookup would be
//! an `AuthenticationPlugin` equivalent (JWT, macaroon, ...) resolving the
//! credential to a WriteToken — see the upstream `AuthenticationPlugin`
//! interface (PR #274).

use crate::policy::{PolicyError, WriteTokenClaims};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
struct TokenFile {
    tokens: HashMap<String, WriteTokenClaims>,
}

#[derive(Debug, Clone)]
pub struct TokenDirectory {
    tokens: Arc<HashMap<String, WriteTokenClaims>>,
}

impl TokenDirectory {
    pub fn load(path: &Path) -> Result<Self, String> {
        let content = fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
        let tf: TokenFile =
            serde_json::from_str(&content).map_err(|e| format!("parse {}: {e}", path.display()))?;
        Ok(TokenDirectory { tokens: Arc::new(tf.tokens) })
    }

    pub fn empty() -> Self {
        TokenDirectory { tokens: Arc::new(HashMap::new()) }
    }

    pub fn from_map(tokens: HashMap<String, WriteTokenClaims>) -> Self {
        TokenDirectory { tokens: Arc::new(tokens) }
    }

    /// Resolve a token string to its claims, checking validity (403 semantics).
    pub fn resolve(&self, token: &str, now_ms: i64) -> Result<WriteTokenClaims, PolicyError> {
        match self.tokens.get(token) {
            None => Err(PolicyError::Forbidden("Invalid token.".into())),
            Some(claims) => {
                claims.check_valid(now_ms)?;
                Ok(claims.clone())
            }
        }
    }
}
