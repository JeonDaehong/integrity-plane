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
use integrity_txn::{LogOptions, TxnLog};

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: integrity-server <config.toml>");
        return ExitCode::from(2);
    };
    match path.as_str() {
        "--version" | "-V" => {
            println!("integrity-server {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        "--help" | "-h" => {
            println!(
                "usage: integrity-server <config.toml>\n\
                 Configuration reference: deploy/integrity.example.toml"
            );
            return ExitCode::SUCCESS;
        }
        _ => {}
    }
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
    let log = TxnLog::open_with(
        config.control_store.path.join("txn.redb"),
        LogOptions {
            retention: config.txn_log.retention,
            chunk_writes: config.txn_log.chunk_writes,
            ..LogOptions::default()
        },
    )?;
    let registry = Registry::open(
        config.control_store.path.join("registry.redb"),
        &config.constraints,
    )?;
    let io = Arc::new(ObjectStoreIo::new(
        config.storage.clone(),
        tokio::runtime::Handle::current(),
    ));
    let client_header = config
        .limits
        .client_header
        .as_deref()
        .map(axum::http::HeaderName::try_from)
        .transpose()
        .map_err(|e| format!("limits.client_header: {e}"))?;
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
    .with_api_tokens(config.server.tokens()?)
    .with_redact_keys(config.errors.redact_keys)
    .with_scan_memory(config.limits.scan_memory)
    .with_request_limits(
        config.limits.max_queued_commits,
        config.limits.commits_per_client,
        client_header,
    )
    .with_upstream_auth(config.upstream.auth.clone())
    .with_catch_up(config.catch_up)
    .with_fence(config.ha.fence == integrity_server::config::Fence::MultiTable);
    let gateway = match &config.signing {
        Some(s) => {
            let timeout = Duration::from_secs(config.upstream.timeout_secs);
            let signer = integrity_server::signing::Signer::from_config(s, timeout).await?;
            tracing::info!("signing certificates with key {}", signer.key_id());
            gateway.with_signer(Some(signer), s.require)
        }
        None => gateway,
    };
    if let Err(e) = gateway.recover_on_start().await {
        tracing::warn!("recovery pending until upstream answers: {e}");
    }
    let listener = tokio::net::TcpListener::bind(&config.server.bind).await?;
    tracing::info!("listening on {}", config.server.bind);
    axum::serve(
        listener,
        router(Arc::new(gateway)).into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}
