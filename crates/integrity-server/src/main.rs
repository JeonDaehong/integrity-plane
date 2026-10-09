//! `integrity-server`: runs the gateway from a TOML configuration file.
//!
//! Usage: `integrity-server <config.toml>`

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use integrity_index::PersistentStore;
use integrity_server::config::Config;
use integrity_server::fileio::ObjectStoreIo;
use integrity_server::store::Registry;
use integrity_server::{Gateway, router};
use integrity_txn::TxnLog;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: integrity-server <config.toml>");
        return ExitCode::from(2);
    };
    match run(&path).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("integrity-server: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_toml_with_env(&std::fs::read_to_string(path)?, std::env::vars())?;
    std::fs::create_dir_all(&config.control_store.path)?;
    let store = PersistentStore::open(config.control_store.path.join("indexes.redb"))?;
    let log = TxnLog::open(config.control_store.path.join("txn.redb"))?;
    let registry = Registry::open(
        config.control_store.path.join("registry.redb"),
        &config.constraints,
    )?;
    let io = Arc::new(ObjectStoreIo::new(
        config.storage.clone(),
        tokio::runtime::Handle::current(),
    ));
    let gateway = Gateway::new(
        &config.upstream.catalog_uri,
        Duration::from_secs(config.upstream.timeout_secs),
        io,
        store,
        log,
        config.limits.max_inline_validation_bytes,
        registry,
    )?
    .with_prefix(config.upstream.prefix.as_deref())
    .with_admin_token(config.server.admin_token.clone())
    .with_redact_keys(config.errors.redact_keys);
    if let Err(e) = gateway.recover_on_start().await {
        tracing::warn!("recovery pending until upstream answers: {e}");
    }
    let listener = tokio::net::TcpListener::bind(&config.server.bind).await?;
    tracing::info!("listening on {}", config.server.bind);
    axum::serve(listener, router(Arc::new(gateway))).await?;
    Ok(())
}
