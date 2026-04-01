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

            if msg.command == "list-plugins" {
                let payload = self.build_list_plugins_response(&msg.message_id);
                let _ = self.outbound_sender
                    .send(&msg.reply_to, payload.into_bytes())
                    .await;
                continue;
            }

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

    fn build_list_plugins_response(&self, message_id: &str) -> String {
        let plugins: Vec<serde_json::Value> = self
            .plugins
            .values()
            .map(|plugin| {
                let args: Vec<serde_json::Value> = plugin
                    .args_mapping
                    .iter()
                    .map(|(name, config)| {
                        serde_json::json!({
                            "name": name,
                            "required": config.required
                        })
                    })
                    .collect();

                serde_json::json!({
                    "name": plugin.name,
                    "description": plugin.description,
                    "args": args
                })
            })
            .collect();

        serde_json::json!({
            "status": "ok",
            "id": message_id,
            "plugins": plugins
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::ArgConfig;

    struct MockSender;

    #[async_trait]
    impl MessageSender for MockSender {
        async fn send(&self, _channel: &str, _payload: Vec<u8>) -> Result<(), std::io::Error> {
            Ok(())
        }
    }

    fn create_test_plugin(name: &str, description: Option<&str>, args: Vec<(&str, &str, bool)>) -> PluginConfig {
        let mut args_mapping = HashMap::new();
        for (arg_name, flag, required) in args {
            args_mapping.insert(
                arg_name.to_string(),
                ArgConfig {
                    flag: flag.to_string(),
                    required,
                },
            );
        }
        PluginConfig {
            name: name.to_string(),
            executable: format!("{}.sh", name),
            description: description.map(String::from),
            args_mapping,
        }
    }

    #[test]
    fn empty_plugins_returns_zero_count() {
        let plugins: HashMap<String, PluginConfig> = HashMap::new();
        let (handler, _tx) = Handler::new(plugins, MockSender, Executor::new(
            &crate::config::ExecutionConfig { timeout_seconds: 30, max_parallel: 5 },
            std::path::Path::new("./scripts"),
        ));

        let result = handler.build_list_plugins_response("test-msg");
        let json: serde_json::Value = serde_json::from_str(&result).expect("valid JSON");

        assert_eq!(json["status"], "ok");
        assert_eq!(json["id"], "test-msg");
        assert_eq!(json["plugins"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn single_plugin_returns_correct_structure() {
        let mut plugins = HashMap::new();
        let plugin = create_test_plugin(
            "test-plugin",
            Some("A test plugin"),
            vec![("id", "--id", true), ("msg", "--msg", false)],
        );
        plugins.insert(plugin.name.clone(), plugin);

        let (handler, _tx) = Handler::new(plugins, MockSender, Executor::new(
            &crate::config::ExecutionConfig { timeout_seconds: 30, max_parallel: 5 },
            std::path::Path::new("./scripts"),
        ));

        let result = handler.build_list_plugins_response("msg-1");
        let json: serde_json::Value = serde_json::from_str(&result).expect("valid JSON");

        assert_eq!(json["status"], "ok");

        let plugin_entry = &json["plugins"][0];
        assert_eq!(plugin_entry["name"], "test-plugin");
        assert_eq!(plugin_entry["description"], "A test plugin");
        assert_eq!(plugin_entry["args"].as_array().unwrap().len(), 2);
        assert!(plugin_entry.get("executable").is_none());

        let arg = &plugin_entry["args"][0];
        assert!(arg.get("name").is_some());
        assert!(arg.get("required").is_some());
        assert!(arg.get("flag").is_none());
    }

    #[test]
    fn null_description_when_missing() {
        let mut plugins = HashMap::new();
        let plugin = create_test_plugin("no-desc", None, vec![]);
        plugins.insert(plugin.name.clone(), plugin);

        let (handler, _tx) = Handler::new(plugins, MockSender, Executor::new(
            &crate::config::ExecutionConfig { timeout_seconds: 30, max_parallel: 5 },
            std::path::Path::new("./scripts"),
        ));

        let result = handler.build_list_plugins_response("msg-2");
        let json: serde_json::Value = serde_json::from_str(&result).expect("valid JSON");

        assert!(json["plugins"][0]["description"].is_null());
    }

    #[test]
    fn special_characters_in_description_produce_valid_json() {
        let mut plugins = HashMap::new();
        let plugin = create_test_plugin("special", Some("Has \"quotes\" and \\backslash"), vec![]);
        plugins.insert(plugin.name.clone(), plugin);

        let (handler, _tx) = Handler::new(plugins, MockSender, Executor::new(
            &crate::config::ExecutionConfig { timeout_seconds: 30, max_parallel: 5 },
            std::path::Path::new("./scripts"),
        ));

        let result = handler.build_list_plugins_response("msg-3");
        let parse_result = serde_json::from_str::<serde_json::Value>(&result);
        assert!(parse_result.is_ok(), "Should produce valid JSON: {:?}", result);
    }
}
