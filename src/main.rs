use devkitd::config::AppConfig;
use devkitd::executor::Executor;
use devkitd::handler::Handler;
use devkitd::plugins::load_plugins;
use devkitd::transport::Broker;
use std::io::Error;
use tracing_subscriber::EnvFilter;

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
        "Config loaded"
    );

    let plugins = load_plugins(&config.paths.plugins_dir);
    tracing::debug!(plugin_count = plugins.len(), "Plugins loaded");

    let executor = Executor::new(&config.execution, &config.paths.scripts_dir);

    let (listener, sender) = Broker::new(&config.server.bind_address).await?;
    let (handler, parser_tx) = Handler::new(plugins, sender, executor);

    tracing::info!("Components initialized, running transport and handler");
    tokio::select! {
        _ = listener.run(parser_tx) => {
            tracing::warn!("Broker stopped unexpectedly");
        }
        _ = handler.run() => {
            tracing::warn!("Handler stopped unexpectedly");
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down");
        }
    }
    tracing::info!("Shutting down");
    Ok(())
}
