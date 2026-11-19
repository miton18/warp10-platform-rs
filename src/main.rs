//! warp10-platform-rs — experimental Rust ingress for Warp 10.
//!
//! Usage: warp10-platform-rs [--config config.json]
//! Env overrides: WARP10_RS_LISTEN, WARP10_RS_TOKENS_FILE,
//! WARP10_RS_SIPHASH_CLASS_KEY, WARP10_RS_SIPHASH_LABELS_KEY,
//! WARP10_RS_DATA_TOPIC, WARP10_RS_META_TOPIC, WARP10_RS_KAFKA_BROKERS,
//! WARP10_RS_KAFKA=1 (with the `kafka` feature).

use std::path::PathBuf;
use warp10_platform_rs::{config, server};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let config_path = args
        .iter()
        .position(|a| a == "--config")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from);

    let config = match config::Config::load(config_path.as_ref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("config error: {e}");
            std::process::exit(2);
        }
    };

    let state = match server::build_state(config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("init error: {e}");
            std::process::exit(2);
        }
    };

    let addr = match state.config.listen_addr() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("config error: {e}");
            std::process::exit(2);
        }
    };

    let app = server::app(state);
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    println!("warp10-platform-rs listening on http://{addr} (POST /api/v0/update, GET /healthz, GET /debug/messages)");
    axum::serve(listener, app).await.expect("serve");
}
