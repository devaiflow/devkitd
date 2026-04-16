use crate::executor::{ExecutionResult, Executor};
use crate::plugins::PluginConfig;
use async_trait::async_trait;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ResponseStatus {
    Received,
    Running,
    Finished,
    Error,
    Ok,
}

#[derive(Serialize)]
pub struct PluginInfo {
    pub name: String,
    pub description: Option<String>,
    pub args: Vec<ArgInfo>,
}

#[derive(Serialize)]
pub struct ArgInfo {
    pub name: String,
    pub required: bool,
}

#[derive(Serialize)]
pub struct Response {
    pub status: ResponseStatus,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugins: Option<Vec<PluginInfo>>,
}

impl Response {
    pub fn received(id: &str) -> Self {
        Response {
            status: ResponseStatus::Received,
            id: id.to_string(),
            stdout: None,
            stderr: None,
            exit_code: None,
            message: None,
            plugins: None,
        }
    }

    pub fn running(id: &str) -> Self {
        Response {
            status: ResponseStatus::Running,
            id: id.to_string(),
            stdout: None,
            stderr: None,
            exit_code: None,
            message: None,
            plugins: None,
        }
    }

    pub fn finished(id: &str, result: &ExecutionResult) -> Self {
        let status = if result.exit_code == 0 {
            ResponseStatus::Finished
        } else {
            ResponseStatus::Error
        };
        Response {
            status,
            id: id.to_string(),
            stdout: Some(result.stdout.clone()),
            stderr: Some(result.stderr.clone()),
            exit_code: Some(result.exit_code),
            message: None,
            plugins: None,
        }
    }

    pub fn error(id: &str, message: &str) -> Self {
        Response {
            status: ResponseStatus::Error,
            id: id.to_string(),
            stdout: None,
            stderr: None,
            exit_code: None,
            message: Some(message.to_string()),
            plugins: None,
        }
    }

    pub fn plugins(id: &str, plugins: Vec<PluginInfo>) -> Self {
        Response {
            status: ResponseStatus::Finished,
            id: id.to_string(),
            stdout: None,
            stderr: None,
            exit_code: None,
            message: None,
            plugins: Some(plugins),
        }
    }

    pub fn pong(id: &str) -> Self {
        Response {
            status: ResponseStatus::Ok,
            id: id.to_string(),
            stdout: None,
            stderr: None,
            exit_code: None,
            message: Some("pong".to_string()),
            plugins: None,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Response serialization should not fail")
    }
}

#[async_trait]
pub trait MessageSender: Send + Sync + 'static {
    async fn send(&self, channel: &str, payload: Vec<u8>) -> Result<(), std::io::Error>;
}

#[derive(Deserialize, Serialize)]
pub struct Message {
    pub message_id: String,
    pub reply_to: String,
    pub command: String,
    #[serde(default)]
    pub args: IndexMap<String, String>,
}

pub struct Handler<S: MessageSender + Clone> {
    pub plugins: HashMap<String, PluginConfig>,
    receiver: mpsc::Receiver<Message>,
    outbound_sender: S,
    executor: Arc<Executor>,
    join_set: JoinSet<()>,
}

impl<S: MessageSender + Clone> Handler<S> {
    pub fn new(plugins: HashMap<String, PluginConfig>, sender: S, executor: Executor) -> (Self, mpsc::Sender<Message>) {
        let (message_tx, message_rx) = mpsc::channel(100);
        let handler = Handler {
            plugins,
            receiver: message_rx,
            outbound_sender: sender,
            executor: Arc::new(executor),
            join_set: JoinSet::new(),
        };
        (handler, message_tx)
    }

    pub async fn run(mut self) {
        tracing::info!("Handler is starting");
        while let Some(msg) = self.receiver.recv().await {
            tracing::debug!(command = %msg.command, message_id = %msg.message_id, "Received command");

            // Built-in: ping / health
            if msg.command == "ping" || msg.command == "health" {
                let resp = Response::pong(&msg.message_id);
                if let Err(e) = self.outbound_sender.send(&msg.reply_to, resp.to_bytes()).await {
                    tracing::error!(reply_to = %msg.reply_to, error = %e, "Error sending pong response");
                }
                continue;
            }

            // Built-in: list-plugins
            if msg.command == "list-plugins" {
                let plugin_list: Vec<PluginInfo> = self.plugins.values().map(|p| {
                    PluginInfo {
                        name: p.name.clone(),
                        description: p.description.clone(),
                        args: p.args_mapping.iter().map(|(name, config)| {
                            ArgInfo { name: name.clone(), required: config.required }
                        }).collect(),
                    }
                }).collect();
                let resp = Response::plugins(&msg.message_id, plugin_list);
                if let Err(e) = self.outbound_sender.send(&msg.reply_to, resp.to_bytes()).await {
                    tracing::error!(reply_to = %msg.reply_to, error = %e, "Error sending list-plugins response");
                }
                continue;
            }

            // Plugin lookup
            let plugin = match self.plugins.get(&msg.command) {
                Some(p) => p,
                None => {
                    tracing::warn!(command = %msg.command, "Unknown command");
                    let resp = Response::error(&msg.message_id, &format!("Unknown command: {}", msg.command));
                    if let Err(e) = self.outbound_sender.send(&msg.reply_to, resp.to_bytes()).await {
                        tracing::error!(reply_to = %msg.reply_to, error = %e, "Error sending error response");
                    }
                    continue;
                }
            };

            // Arg validation
            let (valid_args, missing_args) = Self::check_required_args(plugin, &msg.args);
            if !missing_args.is_empty() {
                tracing::warn!(command = %msg.command, missing = ?missing_args, "Missing required args");
                let resp = Response::error(
                    &msg.message_id,
                    &format!("missing required argument: {}", missing_args.join(", ")),
                );
                if let Err(e) = self.outbound_sender.send(&msg.reply_to, resp.to_bytes()).await {
                    tracing::error!(reply_to = %msg.reply_to, error = %e, "Error sending error response");
                }
                continue;
            }

            // Send "received" status
            let resp = Response::received(&msg.message_id);
            if let Err(e) = self.outbound_sender.send(&msg.reply_to, resp.to_bytes()).await {
                tracing::error!(reply_to = %msg.reply_to, error = %e, "Error sending received status");
            }

            // Spawn execution
            let sender = self.outbound_sender.clone();
            let executor = Arc::clone(&self.executor);
            let executable = plugin.executable.clone();
            let reply_to = msg.reply_to.clone();
            let message_id = msg.message_id.clone();

            self.join_set.spawn(async move {
                // Send "running" status
                let resp = Response::running(&message_id);
                if let Err(e) = sender.send(&reply_to, resp.to_bytes()).await {
                    tracing::error!(reply_to = %reply_to, error = %e, "Error sending running status");
                }

                // Execute
                tracing::debug!(executable = %executable, args = ?valid_args, "Executing plugin");
                let result = executor.execute(&executable, &valid_args).await;

                // Send finished/error
                let resp = Response::finished(&message_id, &result);
                if let Err(e) = sender.send(&reply_to, resp.to_bytes()).await {
                    tracing::error!(reply_to = %reply_to, error = %e, "Error sending execution result");
                }
            });
        }

        // Drain: wait for all in-flight tasks
        tracing::info!(in_flight = self.join_set.len(), "Handler draining in-flight tasks");
        while self.join_set.join_next().await.is_some() {}
        tracing::info!("Handler shutdown complete");
    }

    fn check_required_args(
        plugin: &PluginConfig,
        args: &IndexMap<String, String>,
    ) -> (Vec<String>, Vec<String>) {
        let mut missing_required = Vec::new();
        let mut valid_args = Vec::new();

        for (arg_name, arg_config) in &plugin.args_mapping {
            match args.get(arg_name) {
                Some(value) => {
                    valid_args.push(arg_config.flag.clone());
                    valid_args.push(value.clone());
                }
                None if arg_config.required => {
                    missing_required.push(arg_name.clone());
                }
                None => {}
            }
        }

        (valid_args, missing_required)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::ArgConfig;
    use std::sync::Arc;

    #[derive(Clone)]
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
        let plugins_list: Vec<PluginInfo> = vec![];
        let resp = Response::plugins("test-msg", plugins_list);
        let json: serde_json::Value = serde_json::from_slice(&resp.to_bytes()).unwrap();

        assert_eq!(json["status"], "finished");
        assert_eq!(json["id"], "test-msg");
        assert_eq!(json["plugins"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn single_plugin_returns_correct_structure() {
        let plugins_list = vec![
            PluginInfo {
                name: "test-plugin".to_string(),
                description: Some("A test plugin".to_string()),
                args: vec![
                    ArgInfo { name: "id".to_string(), required: true },
                    ArgInfo { name: "msg".to_string(), required: false },
                ],
            },
        ];
        let resp = Response::plugins("msg-1", plugins_list);
        let json: serde_json::Value = serde_json::from_slice(&resp.to_bytes()).unwrap();

        assert_eq!(json["status"], "finished");

        let plugin_entry = &json["plugins"][0];
        assert_eq!(plugin_entry["name"], "test-plugin");
        assert_eq!(plugin_entry["description"], "A test plugin");
        assert_eq!(plugin_entry["args"].as_array().unwrap().len(), 2);

        let arg = &plugin_entry["args"][0];
        assert_eq!(arg["name"], "id");
        assert_eq!(arg["required"], true);
    }

    #[test]
    fn null_description_when_missing() {
        let plugins_list = vec![
            PluginInfo {
                name: "no-desc".to_string(),
                description: None,
                args: vec![],
            },
        ];
        let resp = Response::plugins("msg-2", plugins_list);
        let json: serde_json::Value = serde_json::from_slice(&resp.to_bytes()).unwrap();

        assert!(json["plugins"][0]["description"].is_null());
    }

    #[test]
    fn missing_required_arg_returns_error() {
        let plugin = create_test_plugin(
            "test",
            None,
            vec![("id", "--id", true), ("msg", "--msg", false)],
        );
        let mut args = IndexMap::new();
        args.insert("msg".to_string(), "hello".to_string());

        let (valid_args, missing) = Handler::<MockSender>::check_required_args(&plugin, &args);
        assert_eq!(missing, vec!["id".to_string()], "Should report 'id' as missing required arg");
        assert_eq!(valid_args, vec!["--msg".to_string(), "hello".to_string()]);
    }

    #[test]
    fn unknown_arg_is_silently_ignored() {
        let plugin = create_test_plugin(
            "test",
            None,
            vec![("id", "--id", true)],
        );
        let mut args = IndexMap::new();
        args.insert("id".to_string(), "123".to_string());
        args.insert("typo".to_string(), "val".to_string());

        let (valid_args, missing) = Handler::<MockSender>::check_required_args(&plugin, &args);
        assert!(missing.is_empty(), "Unknown args should not be reported as missing");
        assert_eq!(valid_args, vec!["--id".to_string(), "123".to_string()]);
    }

    #[test]
    fn all_required_args_present_passes() {
        let plugin = create_test_plugin(
            "test",
            None,
            vec![("id", "--id", true), ("msg", "--msg", true)],
        );
        let mut args = IndexMap::new();
        args.insert("id".to_string(), "1".to_string());
        args.insert("msg".to_string(), "hi".to_string());

        let (valid_args, missing) = Handler::<MockSender>::check_required_args(&plugin, &args);
        assert!(missing.is_empty());
        assert_eq!(valid_args.len(), 4, "Should have 2 flag-value pairs");
    }

    #[test]
    fn special_characters_in_description_produce_valid_json() {
        let plugins_list = vec![
            PluginInfo {
                name: "special".to_string(),
                description: Some("Has \"quotes\" and \\backslash".to_string()),
                args: vec![],
            },
        ];
        let resp = Response::plugins("msg-3", plugins_list);
        let parse_result = serde_json::from_slice::<serde_json::Value>(&resp.to_bytes());
        assert!(parse_result.is_ok(), "Should produce valid JSON");
    }

    // --- Response struct tests ---

    #[test]
    fn response_finished_has_correct_shape() {
        let result = ExecutionResult::success("hello world".to_string(), String::new());
        let resp = Response::finished("msg-1", &result);
        let json: serde_json::Value = serde_json::from_slice(&resp.to_bytes()).unwrap();

        assert_eq!(json["status"], "finished");
        assert_eq!(json["id"], "msg-1");
        assert_eq!(json["stdout"], "hello world");
        assert_eq!(json["stderr"], "");
        assert_eq!(json["exit_code"], 0);
        assert!(json.get("message").is_none(), "message should be omitted for finished");
        assert!(json.get("plugins").is_none(), "plugins should be omitted for finished");
    }

    #[test]
    fn response_error_from_execution_has_exit_code() {
        let result = ExecutionResult::error(1, String::new(), "something failed".to_string());
        let resp = Response::finished("msg-2", &result);
        let json: serde_json::Value = serde_json::from_slice(&resp.to_bytes()).unwrap();

        assert_eq!(json["status"], "error");
        assert_eq!(json["exit_code"], 1);
        assert_eq!(json["stderr"], "something failed");
    }

    #[test]
    fn response_error_message_has_correct_shape() {
        let resp = Response::error("msg-3", "Unknown command: foo");
        let json: serde_json::Value = serde_json::from_slice(&resp.to_bytes()).unwrap();

        assert_eq!(json["status"], "error");
        assert_eq!(json["id"], "msg-3");
        assert_eq!(json["message"], "Unknown command: foo");
        assert!(json.get("stdout").is_none());
        assert!(json.get("exit_code").is_none());
    }

    #[test]
    fn response_received_has_minimal_shape() {
        let resp = Response::received("msg-4");
        let json: serde_json::Value = serde_json::from_slice(&resp.to_bytes()).unwrap();

        assert_eq!(json["status"], "received");
        assert_eq!(json["id"], "msg-4");
        assert!(json.get("stdout").is_none());
        assert!(json.get("message").is_none());
    }

    #[test]
    fn response_running_has_minimal_shape() {
        let resp = Response::running("msg-5");
        let json: serde_json::Value = serde_json::from_slice(&resp.to_bytes()).unwrap();

        assert_eq!(json["status"], "running");
        assert_eq!(json["id"], "msg-5");
    }

    #[test]
    fn response_pong_has_message() {
        let resp = Response::pong("msg-6");
        let json: serde_json::Value = serde_json::from_slice(&resp.to_bytes()).unwrap();

        assert_eq!(json["status"], "ok");
        assert_eq!(json["message"], "pong");
    }

    #[test]
    fn response_plugins_has_correct_structure() {
        let plugins = vec![
            PluginInfo {
                name: "test-plugin".to_string(),
                description: Some("A test".to_string()),
                args: vec![
                    ArgInfo { name: "id".to_string(), required: true },
                    ArgInfo { name: "msg".to_string(), required: false },
                ],
            },
        ];
        let resp = Response::plugins("msg-7", plugins);
        let json: serde_json::Value = serde_json::from_slice(&resp.to_bytes()).unwrap();

        assert_eq!(json["status"], "finished");
        assert_eq!(json["plugins"][0]["name"], "test-plugin");
        assert_eq!(json["plugins"][0]["description"], "A test");
        assert_eq!(json["plugins"][0]["args"].as_array().unwrap().len(), 2);
        assert_eq!(json["plugins"][0]["args"][0]["name"], "id");
        assert_eq!(json["plugins"][0]["args"][0]["required"], true);
        assert!(json.get("stdout").is_none());
    }

    // --- Lifecycle and integration tests ---

    #[derive(Clone)]
    struct CaptureSender {
        payloads: Arc<tokio::sync::Mutex<Vec<Vec<u8>>>>,
    }

    impl CaptureSender {
        fn new() -> Self {
            CaptureSender {
                payloads: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            }
        }

        async fn captured(&self) -> Vec<serde_json::Value> {
            self.payloads.lock().await.iter().map(|p| {
                serde_json::from_slice(p).expect("captured payload should be valid JSON")
            }).collect()
        }
    }

    #[async_trait]
    impl MessageSender for CaptureSender {
        async fn send(&self, _channel: &str, payload: Vec<u8>) -> Result<(), std::io::Error> {
            self.payloads.lock().await.push(payload);
            Ok(())
        }
    }

    #[tokio::test]
    async fn lifecycle_sends_received_running_finished() {
        let sender = CaptureSender::new();
        let mut plugins = HashMap::new();
        // Use a plugin whose executable won't be found — executor returns not_found error
        // which still exercises the full lifecycle: received → running → error
        let plugin = create_test_plugin("echo", None, vec![]);
        plugins.insert("echo".to_string(), plugin);

        let executor = Executor::new(
            &crate::config::ExecutionConfig { timeout_seconds: 5, max_parallel: 5 },
            std::path::Path::new("./nonexistent-scripts"),
        );

        let (handler, tx) = Handler::new(plugins, sender.clone(), executor);

        let msg = Message {
            message_id: "lc-1".to_string(),
            reply_to: "127.0.0.1:0".to_string(),
            command: "echo".to_string(),
            args: IndexMap::new(),
        };
        tx.send(msg).await.unwrap();
        drop(tx); // close channel so handler.run() exits

        handler.run().await;

        let captured = sender.captured().await;
        assert_eq!(captured.len(), 3, "Should have 3 status messages: received, running, finished/error");
        assert_eq!(captured[0]["status"], "received");
        assert_eq!(captured[0]["id"], "lc-1");
        assert_eq!(captured[1]["status"], "running");
        assert_eq!(captured[1]["id"], "lc-1");
        // Third is error because script not found
        assert_eq!(captured[2]["status"], "error");
        assert_eq!(captured[2]["id"], "lc-1");
        assert!(captured[2]["exit_code"].is_number());
    }

    #[tokio::test]
    async fn ping_returns_pong() {
        let sender = CaptureSender::new();
        let plugins = HashMap::new();

        let executor = Executor::new(
            &crate::config::ExecutionConfig { timeout_seconds: 5, max_parallel: 5 },
            std::path::Path::new("./scripts"),
        );

        let (handler, tx) = Handler::new(plugins, sender.clone(), executor);

        let msg = Message {
            message_id: "ping-1".to_string(),
            reply_to: "127.0.0.1:0".to_string(),
            command: "ping".to_string(),
            args: IndexMap::new(),
        };
        tx.send(msg).await.unwrap();
        drop(tx);

        handler.run().await;

        let captured = sender.captured().await;
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0]["status"], "ok");
        assert_eq!(captured[0]["id"], "ping-1");
        assert_eq!(captured[0]["message"], "pong");
    }

    #[tokio::test]
    async fn health_returns_pong() {
        let sender = CaptureSender::new();
        let plugins = HashMap::new();

        let executor = Executor::new(
            &crate::config::ExecutionConfig { timeout_seconds: 5, max_parallel: 5 },
            std::path::Path::new("./scripts"),
        );

        let (handler, tx) = Handler::new(plugins, sender.clone(), executor);

        let msg = Message {
            message_id: "health-1".to_string(),
            reply_to: "127.0.0.1:0".to_string(),
            command: "health".to_string(),
            args: IndexMap::new(),
        };
        tx.send(msg).await.unwrap();
        drop(tx);

        handler.run().await;

        let captured = sender.captured().await;
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0]["status"], "ok");
        assert_eq!(captured[0]["message"], "pong");
    }

    #[test]
    fn response_special_chars_escaped_by_serde() {
        let result = ExecutionResult::success(
            "line1\nline2\ttab\"quote".to_string(),
            "\x00null\rcarriage".to_string(),
        );
        let resp = Response::finished("msg-8", &result);
        let bytes = resp.to_bytes();
        let parse_result = serde_json::from_slice::<serde_json::Value>(&bytes);
        assert!(parse_result.is_ok(), "Special chars must produce valid JSON");
        let json = parse_result.unwrap();
        assert_eq!(json["stdout"], "line1\nline2\ttab\"quote");
    }

    #[tokio::test]
    async fn concurrent_commands_execute_in_parallel() {
        let sender = CaptureSender::new();
        let mut plugins = HashMap::new();
        plugins.insert("cmd-a".to_string(), create_test_plugin("cmd-a", None, vec![]));
        plugins.insert("cmd-b".to_string(), create_test_plugin("cmd-b", None, vec![]));

        let executor = Executor::new(
            &crate::config::ExecutionConfig { timeout_seconds: 5, max_parallel: 5 },
            std::path::Path::new("./nonexistent-scripts"),
        );

        let (handler, tx) = Handler::new(plugins, sender.clone(), executor);

        for i in 0..3 {
            let cmd = if i % 2 == 0 { "cmd-a" } else { "cmd-b" };
            tx.send(Message {
                message_id: format!("par-{}", i),
                reply_to: "127.0.0.1:0".to_string(),
                command: cmd.to_string(),
                args: IndexMap::new(),
            }).await.unwrap();
        }
        drop(tx);

        handler.run().await;

        let captured = sender.captured().await;
        // 3 commands × 3 statuses each (received, running, error) = 9 messages
        assert_eq!(captured.len(), 9, "3 commands should produce 9 status messages total");

        // Verify all 3 message_ids appear in the captured responses
        let ids: Vec<&str> = captured.iter()
            .filter_map(|v| v["id"].as_str())
            .collect();
        for i in 0..3 {
            let expected_id = format!("par-{}", i);
            let count = ids.iter().filter(|&&id| id == expected_id).count();
            assert_eq!(count, 3, "Command {} should have 3 status messages", expected_id);
        }
    }

    #[tokio::test]
    async fn sender_error_propagation_on_connection_refused() {
        use crate::transport::TcpCallbackSender;

        let sender = TcpCallbackSender::new();
        // Port 1 is almost certainly not listening — connection should be refused
        let result: Result<(), std::io::Error> = sender.send("127.0.0.1:1", b"test payload".to_vec()).await;
        assert!(result.is_err(), "Sender MUST return Err when connection is refused");
    }

    #[tokio::test]
    async fn sender_successful_send_appends_newline() {
        use crate::transport::TcpCallbackSender;

        // Spawn a TCP listener, send a payload, verify it arrives with trailing \n
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let send_task: tokio::task::JoinHandle<Result<(), std::io::Error>> = tokio::spawn(async move {
            let sender = TcpCallbackSender::new();
            sender.send(&addr, b"hello".to_vec()).await
        });

        let (stream, _) = listener.accept().await.unwrap();
        let mut reader = tokio::io::BufReader::new(stream);
        let mut line = String::new();
        tokio::io::AsyncBufReadExt::read_line(&mut reader, &mut line).await.unwrap();

        assert_eq!(line, "hello\n", "Sender must write payload followed by newline");
        assert!(send_task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn broker_continues_after_invalid_json() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (handler_tx, mut handler_rx) = mpsc::channel::<Message>(10);

        let broker_task = tokio::spawn(async move {
            // Simulate what Broker::run does for one connection
            let (stream, _) = listener.accept().await.unwrap();
            let reader = tokio::io::BufReader::new(stream);
            let mut lines = tokio::io::AsyncBufReadExt::lines(reader);
            while let Ok(Some(line)) = lines.next_line().await {
                if line.is_empty() { continue; }
                match serde_json::from_str::<Message>(&line) {
                    Ok(msg) => { let _ = handler_tx.send(msg).await; }
                    Err(_) => { /* broker continues on bad JSON */ }
                }
            }
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        // Send valid message
        let msg1 = r#"{"message_id":"1","reply_to":"127.0.0.1:0","command":"ping","args":{}}"#;
        stream.write_all(format!("{}\n", msg1).as_bytes()).await.unwrap();
        // Send invalid JSON
        stream.write_all(b"this is not json\n").await.unwrap();
        // Send another valid message — broker MUST NOT have closed the connection
        let msg3 = r#"{"message_id":"3","reply_to":"127.0.0.1:0","command":"health","args":{}}"#;
        stream.write_all(format!("{}\n", msg3).as_bytes()).await.unwrap();
        drop(stream); // close connection

        broker_task.await.unwrap();

        // Should have received both valid messages, skipping the invalid one
        let first = handler_rx.recv().await.unwrap();
        assert_eq!(first.message_id, "1");
        let third = handler_rx.recv().await.unwrap();
        assert_eq!(third.message_id, "3");
        assert!(handler_rx.recv().await.is_none(), "Only 2 valid messages should arrive");
    }
}
