use crate::executor::{ExecutionResult, Executor};
use crate::plugins::PluginConfig;
use async_trait::async_trait;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc;

#[async_trait]
pub trait MessageSender: Send + Sync + 'static {
    async fn send(&self, channel: &str, payload: Vec<u8>) -> Result<(), std::io::Error>;
}

#[derive(Deserialize, Serialize)]
pub struct Message {
    pub message_id: String,
    pub reply_to: String,
    pub command: String,
    pub args: IndexMap<String, String>,
}

pub struct Handler<S: MessageSender> {
    pub plugins: HashMap<String, PluginConfig>,
    receiver: mpsc::Receiver<Message>,
    outbound_sender: S,
    executor: Executor,
}

impl<S: MessageSender> Handler<S> {
    pub fn new(plugins: HashMap<String, PluginConfig>, sender: S, executor: Executor) -> (Self, mpsc::Sender<Message>) {
        let (message_tx, message_rx) = mpsc::channel(100);
        let handler = Handler {
            plugins,
            receiver: message_rx,
            outbound_sender: sender,
            executor,
        };
        (handler, message_tx)
    }

    pub async fn run(mut self) {
        println!("Handler is starting...");
        while let Some(msg) = self.receiver.recv().await {
            println!(
                "Received command: {:?} | message_id: {:?}",
                msg.command, msg.message_id
            );
            let plugin = match self.plugins.get(&msg.command) {
                Some(p) => p,
                None => {
                    println!("Unknown command: {}", msg.command);
                    let error_payload = format!(
                        "{{\"status\":\"error\", \"message\":\"Unknown command: {}\", \"id\":\"{}\"}}",
                        msg.command, msg.message_id
                    );
                    let _ = self.outbound_sender
                        .send(&msg.reply_to, error_payload.into_bytes())
                        .await;
                    continue;
                }
            };

            let (valid_args, missing_args) = Self::check_required_args(plugin, &msg.args);
            if !missing_args.is_empty() {
                println!("Missing required args for {}: {:?}", msg.command, missing_args);
                let error_payload = format!(
                    "{{\"status\":\"error\", \"message\":\"missing required argument: {}\", \"id\":\"{}\"}}",
                    missing_args.join(", "),
                    msg.message_id
                );
                let _ = self.outbound_sender
                    .send(&msg.reply_to, error_payload.into_bytes())
                    .await;
                continue;
            }

            println!("Executing plugin: {} {:?}", plugin.executable, valid_args);
            let result = self.executor.execute(&plugin.executable, &valid_args).await;
            let response_payload = Self::build_response(&msg.message_id, &result);

            if let Err(e) = self
                .outbound_sender
                .send(&msg.reply_to, response_payload.into_bytes())
                .await
            {
                println!("Error sending callback to {}: {}", msg.reply_to, e);
            }
        }
    }

    fn build_response(message_id: &str, result: &ExecutionResult) -> String {
        let status = if result.exit_code == 0 { "ok" } else { "error" };
        let stdout_escaped = result.stdout.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n").replace('\r', "\\r").replace('\t', "\\t");
        let stderr_escaped = result.stderr.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n").replace('\r', "\\r").replace('\t', "\\t");

        format!(
            "{{\"status\":\"{}\", \"id\":\"{}\", \"stdout\":\"{}\", \"stderr\":\"{}\", \"exit_code\":{}}}",
            status, message_id, stdout_escaped, stderr_escaped, result.exit_code
        )
    }

    fn check_required_args(
        plugin: &PluginConfig,
        args: &IndexMap<String, String>,
    ) -> (Vec<String>, Vec<String>) {
        let mut missing_args = Vec::new();
        let mut valid_args = Vec::new();

        for (arg_name, value) in args {
            match plugin.args_mapping.get(arg_name) {
                Some(arg_config) => {
                    valid_args.push(arg_config.flag.clone());
                    valid_args.push(value.clone());
                }
                None => {
                    missing_args.push(arg_name.clone());
                }
            }
        }

        (valid_args, missing_args)
    }
}
