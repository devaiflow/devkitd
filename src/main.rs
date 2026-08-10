use devkitd::config::AppConfig;
use devkitd::core::{CommandService, Executor, JobRegistry, PluginRegistry};
use devkitd::transport::{require_bearer_token, DevkitdHandler};
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use std::future::IntoFuture;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

const MCP_TOKEN_ENV: &str = "DEVKITD_MCP_TOKEN";

/// Only a loopback bind is safe to run without a token — devkitd executes
/// privileged scripts on the host. Accepts IP literals (the common case)
/// and the "localhost" hostname; anything else is treated as non-loopback,
/// which is the safe default when we can't prove otherwise.
fn is_loopback_bind(bind_address: &str) -> bool {
    if let Ok(addr) = bind_address.parse::<std::net::SocketAddr>() {
        return addr.ip().is_loopback();
    }
    bind_address
        .rsplit_once(':')
        .map(|(host, _port)| host == "localhost")
        .unwrap_or(false)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    tracing::info!("Service starting");

    let config = AppConfig::load();
    tracing::info!(
        plugins_dir = ?config.paths.plugins_dir,
        scripts_dir = ?config.paths.scripts_dir,
        timeout_seconds = config.execution.timeout_seconds,
        max_parallel = config.execution.max_parallel,
        job_retention_seconds = config.execution.job_retention_seconds,
        bind_address = %config.server.bind_address,
        allowed_hosts = ?config.server.allowed_hosts,
        "Config loaded"
    );

    // Token lives only in the environment, never in devkitd.toml — a
    // config file is far more likely to end up committed or copied
    // around than a per-host env var.
    let mcp_token = std::env::var(MCP_TOKEN_ENV).ok().filter(|t| !t.is_empty());

    if mcp_token.is_none() && !is_loopback_bind(&config.server.bind_address) {
        tracing::error!(
            bind_address = %config.server.bind_address,
            "Refusing to start: non-loopback bind requires {} (devkitd runs privileged scripts)",
            MCP_TOKEN_ENV
        );
        return Err(Error::new(
            ErrorKind::Other,
            format!("non-loopback bind requires {}", MCP_TOKEN_ENV),
        ));
    }

    let registry = Arc::new(PluginRegistry::load(&config.paths.plugins_dir));
    let executor = Arc::new(Executor::new(&config.execution, &config.paths.scripts_dir));
    let jobs = Arc::new(JobRegistry::new(Duration::from_secs(config.execution.job_retention_seconds)));
    let service = CommandService::new(registry, executor, jobs);

    // Stateless: every tools/call returns a job_id immediately (fire and
    // track), so a dropped HTTP connection never orphans a running job —
    // the JobRegistry outlives the request and is polled/cancelled through
    // job-status/job-cancel instead.
    let mcp_config = StreamableHttpServerConfig::default()
        .with_stateful_mode(false)
        .with_allowed_hosts(config.server.allowed_hosts.clone());
    let http_service = StreamableHttpService::new(
        move || Ok(DevkitdHandler::new(service.clone())),
        Arc::new(NeverSessionManager::default()),
        mcp_config,
    );

    let mut router = axum::Router::new().nest_service("/mcp", http_service);
    if let Some(token) = mcp_token {
        tracing::info!("Bearer token auth enabled");
        router = require_bearer_token(router, Arc::from(token));
    } else {
        tracing::warn!("No {} set — MCP endpoint has no auth (only safe on loopback)", MCP_TOKEN_ENV);
    }

    let listener = tokio::net::TcpListener::bind(&config.server.bind_address).await?;
    tracing::info!(bind_address = %config.server.bind_address, "MCP server listening on /mcp");

    tokio::select! {
        result = axum::serve(listener, router).into_future() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "MCP server exited with error");
                return Err(Error::new(ErrorKind::Other, e));
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down");
        }
    }

    tracing::info!("Shutting down");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_v4_and_v6_and_localhost_are_loopback() {
        assert!(is_loopback_bind("127.0.0.1:9000"));
        assert!(is_loopback_bind("[::1]:9000"));
        assert!(is_loopback_bind("localhost:9000"));
    }

    #[test]
    fn non_loopback_addresses_are_rejected() {
        assert!(!is_loopback_bind("0.0.0.0:9000"));
        assert!(!is_loopback_bind("192.168.1.5:9000"));
        assert!(!is_loopback_bind("example.com:9000"));
    }
}
