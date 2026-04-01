use devkitd::config::AppConfig;
use devkitd::executor::Executor;
use devkitd::handler::Handler;
use devkitd::plugins::load_plugins;
use devkitd::transport::Broker;
use std::io::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("Service Starting");

    let config = AppConfig::load();
    println!("[main] Config loaded: plugins_dir={:?}, scripts_dir={:?}, timeout={}s, max_parallel={}",
        config.paths.plugins_dir,
        config.paths.scripts_dir,
        config.execution.timeout_seconds,
        config.execution.max_parallel
    );

    let plugins = load_plugins(&config.paths.plugins_dir);
    println!("{:#?}", plugins);

    let executor = Executor::new(&config.execution, &config.paths.scripts_dir);

    let (listener, sender) = Broker::new("127.0.0.1:9000").await?;
    let (handler, parser_tx) = Handler::new(plugins, sender, executor);

    println!("Components initialized. Running transport and handler...");
    tokio::select! {
        _ = listener.run(parser_tx) => {
            println!("Broker stopped unexpectedly");
        }
        _ = handler.run() => {
                println!("Handler stopped unexpectedly");
            }
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down...");
        }
    }
    println!("Shutting down...");
    Ok(())
}
