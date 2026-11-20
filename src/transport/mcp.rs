use crate::core::CommandService;
use crate::core::job_registry::JobState;
use crate::core::plugins::PluginConfig;
use indexmap::IndexMap;
use rmcp::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, ContentBlock, ErrorData, Implementation,
    ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use serde_json::{Map, Value, json};
use std::sync::Arc;
use uuid::Uuid;

const JOB_STATUS_TOOL: &str = "job-status";
const JOB_CANCEL_TOOL: &str = "job-cancel";

#[derive(Clone)]
pub struct DevkitdHandler {
    service: CommandService,
}

impl DevkitdHandler {
    pub fn new(service: CommandService) -> Self {
        DevkitdHandler { service }
    }

    async fn handle_plugin_call(
        &self,
        command: &str,
        args: &IndexMap<String, String>,
    ) -> Result<CallToolResult, ErrorData> {
        match self.service.start(command, args).await {
            Ok(job_id) => Ok(CallToolResult::success(vec![ContentBlock::text(
                json!({ "job_id": job_id.to_string() }).to_string(),
            )])),
            Err(reason) => Ok(CallToolResult::error(vec![ContentBlock::text(
                reason.message(),
            )])),
        }
    }

    async fn handle_job_status(
        &self,
        args: &IndexMap<String, String>,
    ) -> Result<CallToolResult, ErrorData> {
        let Some(job_id) = parse_job_id(args) else {
            return Ok(CallToolResult::error(vec![ContentBlock::text(
                "missing or invalid job_id",
            )]));
        };
        match self.service.jobs().get(job_id).await {
            Some(state) => Ok(CallToolResult::success(vec![ContentBlock::text(
                job_state_to_json(&state).to_string(),
            )])),
            None => Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                "unknown job_id: {}",
                job_id
            ))])),
        }
    }

    async fn handle_job_cancel(
        &self,
        args: &IndexMap<String, String>,
    ) -> Result<CallToolResult, ErrorData> {
        let Some(job_id) = parse_job_id(args) else {
            return Ok(CallToolResult::error(vec![ContentBlock::text(
                "missing or invalid job_id",
            )]));
        };
        if self.service.jobs().cancel(job_id).await {
            Ok(CallToolResult::success(vec![ContentBlock::text(
                json!({ "cancelled": true }).to_string(),
            )]))
        } else {
            Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                "unknown job_id: {}",
                job_id
            ))]))
        }
    }

    /// Live reload: full rescan every call. Cheap at plugin-directory scale,
    /// and correct where a directory-mtime shortcut would miss in-place
    /// edits to an existing plugin file. Split out from the `ServerHandler`
    /// trait method so it's callable in tests without constructing a
    /// `RequestContext`.
    async fn build_tool_list(&self) -> Vec<Tool> {
        self.service.registry().reload().await;
        let plugins = self.service.registry().list().await;
        let mut tools: Vec<Tool> = plugins.iter().map(plugin_to_tool).collect();
        tools.push(job_status_tool());
        tools.push(job_cancel_tool());
        tools
    }
}

impl ServerHandler for DevkitdHandler {
    fn get_info(&self) -> ServerInfo {
        let mut server_info = Implementation::default();
        server_info.name = "devkitd".to_string();
        server_info.version = env!("CARGO_PKG_VERSION").to_string();

        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder()
            .enable_tools()
            .enable_tool_list_changed()
            .build();
        info.server_info = server_info;
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult::with_all_items(
            self.build_tool_list().await,
        ))
    }

    /// Fire-and-track: every call (plugin or built-in) returns immediately.
    /// Plugin calls hand back a `job_id`; poll it with `job-status` and stop
    /// it with `job-cancel`. Blocking here would leave a caller with no way
    /// to cancel a stateless HTTP call once the connection drops (see
    /// PLANREF Fase 2 -> 2B pivot) — nothing in devkitd's original design
    /// was ever blocking, so this restores that shape instead of patching
    /// around it.
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let command = request.name.to_string();
        let args = json_object_to_args(request.arguments.unwrap_or_default());

        match command.as_str() {
            JOB_STATUS_TOOL => self.handle_job_status(&args).await,
            JOB_CANCEL_TOOL => self.handle_job_cancel(&args).await,
            _ => self.handle_plugin_call(&command, &args).await,
        }
    }
}

fn parse_job_id(args: &IndexMap<String, String>) -> Option<Uuid> {
    args.get("job_id").and_then(|s| Uuid::parse_str(s).ok())
}

fn job_state_to_json(state: &JobState) -> Value {
    match state {
        JobState::Received => json!({ "state": "received" }),
        JobState::Running => json!({ "state": "running" }),
        JobState::Cancelled => json!({ "state": "cancelled" }),
        JobState::Done(result) => json!({
            "state": "done",
            "stdout": result.stdout,
            "stderr": result.stderr,
            "exit_code": result.exit_code,
        }),
    }
}

fn job_id_schema() -> Map<String, Value> {
    let mut properties = Map::new();
    properties.insert(
        "job_id".to_string(),
        json!({ "type": "string", "description": "job_id returned by a previous tool call" }),
    );

    let mut schema = Map::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert("required".to_string(), json!(["job_id"]));
    schema
}

fn job_status_tool() -> Tool {
    Tool::new(
        JOB_STATUS_TOOL,
        "Get the status of a job started by a previous tool call. Returns state \
         (received/running/done/cancelled) and, once done, stdout/stderr/exit_code.",
        Arc::new(job_id_schema()),
    )
}

fn job_cancel_tool() -> Tool {
    Tool::new(
        JOB_CANCEL_TOOL,
        "Cancel a job started by a previous tool call, whether pending or running.",
        Arc::new(job_id_schema()),
    )
}

/// Build an MCP `Tool` from a plugin's arg definitions. All args are typed
/// as JSON strings — devkitd plugins map every arg to a CLI flag value.
fn plugin_to_tool(plugin: &PluginConfig) -> Tool {
    let mut properties = Map::new();
    let mut required = Vec::new();

    for (arg_name, arg_config) in &plugin.args_mapping {
        properties.insert(
            arg_name.clone(),
            json!({
                "type": "string",
                "description": format!("Maps to flag {}", arg_config.flag),
            }),
        );
        if arg_config.required {
            required.push(Value::String(arg_name.clone()));
        }
    }

    let mut schema = Map::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert("required".to_string(), Value::Array(required));

    Tool::new(
        plugin.name.clone(),
        plugin.description.clone().unwrap_or_default(),
        Arc::new(schema),
    )
}

/// Convert MCP call arguments to devkitd's flat string-arg map. Non-scalar
/// values (objects/arrays) are JSON-encoded rather than rejected — callers
/// like flowspec may pass structured values through templating.
fn json_object_to_args(obj: rmcp::model::JsonObject) -> IndexMap<String, String> {
    obj.into_iter()
        .map(|(k, v)| {
            let s = match v {
                Value::String(s) => s,
                Value::Null => String::new(),
                other => other.to_string(),
            };
            (k, s)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExecutionConfig;
    use crate::core::process_test_lock;
    use crate::core::{Executor, JobRegistry, PluginRegistry};
    use std::collections::HashMap;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    fn write_plugin_and_script(dir: &std::path::Path, plugin: &PluginConfig, script_body: &str) {
        let scripts_dir = dir.join("scripts");
        std::fs::create_dir_all(&scripts_dir).unwrap();
        let script_path = scripts_dir.join(&plugin.executable);
        std::fs::write(&script_path, script_body).unwrap();
        let mut perms = std::fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script_path, perms).unwrap();

        let plugins_dir = dir.join("plugins");
        std::fs::create_dir_all(&plugins_dir).unwrap();
        let plugin_path = plugins_dir.join(format!("{}.json", plugin.name));
        std::fs::write(&plugin_path, serde_json::to_string(plugin).unwrap()).unwrap();
    }

    fn make_handler(dir: &std::path::Path) -> DevkitdHandler {
        let registry = Arc::new(PluginRegistry::load(&dir.join("plugins")));
        let executor = Arc::new(Executor::new(
            &ExecutionConfig {
                timeout_seconds: 5,
                max_parallel: 5,
                kill_grace_seconds: 1,
                job_retention_seconds: 900,
            },
            &dir.join("scripts"),
        ));
        let jobs = Arc::new(JobRegistry::default());
        DevkitdHandler::new(CommandService::new(registry, executor, jobs))
    }

    fn result_text(result: &CallToolResult) -> String {
        match result.content.first() {
            Some(ContentBlock::Text(t)) => t.text.clone(),
            other => panic!("expected a text content block, got {:?}", other),
        }
    }

    fn args_with(job_id: &str) -> IndexMap<String, String> {
        let mut args = IndexMap::new();
        args.insert("job_id".to_string(), job_id.to_string());
        args
    }

    async fn start_echo_job(handler: &DevkitdHandler, dir: &std::path::Path) -> Uuid {
        let plugin = PluginConfig {
            name: "echo".to_string(),
            executable: "echo.sh".to_string(),
            description: None,
            args_mapping: HashMap::new(),
            timeout_seconds: None,
        };
        write_plugin_and_script(dir, &plugin, "#!/bin/sh\necho hi\nexit 0\n");
        handler.service.registry().reload().await;

        let result = handler
            .handle_plugin_call("echo", &IndexMap::new())
            .await
            .unwrap();
        assert_ne!(result.is_error, Some(true));
        let job_id: Value = serde_json::from_str(&result_text(&result)).unwrap();
        Uuid::parse_str(job_id["job_id"].as_str().unwrap()).unwrap()
    }

    async fn wait_for_terminal(handler: &DevkitdHandler, job_id: Uuid) -> JobState {
        loop {
            match handler.service.jobs().get(job_id).await {
                Some(JobState::Received) | Some(JobState::Running) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Some(state) => return state,
                None => panic!("job disappeared before reaching a terminal state"),
            }
        }
    }

    #[tokio::test]
    async fn handle_plugin_call_unknown_command_is_an_error_result() {
        let dir = std::env::temp_dir().join(format!("devkitd-mcp-unknown-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("plugins")).unwrap();
        let handler = make_handler(&dir);

        let result = handler
            .handle_plugin_call("nope", &IndexMap::new())
            .await
            .unwrap();
        assert_eq!(result.is_error, Some(true));
        assert!(result_text(&result).contains("Unknown command"));
    }

    #[tokio::test]
    async fn handle_plugin_call_starts_a_job_and_it_reaches_done() {
        let _guard = process_test_lock().lock().await;
        let dir = std::env::temp_dir().join(format!("devkitd-mcp-ok-{}", std::process::id()));
        let handler = make_handler(&dir);

        let job_id = start_echo_job(&handler, &dir).await;
        match wait_for_terminal(&handler, job_id).await {
            JobState::Done(result) => assert_eq!(result.exit_code, 0),
            other => panic!("expected Done, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn handle_job_status_unknown_id_is_an_error_result() {
        let dir =
            std::env::temp_dir().join(format!("devkitd-mcp-status-unknown-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("plugins")).unwrap();
        let handler = make_handler(&dir);

        let result = handler
            .handle_job_status(&args_with(&Uuid::new_v4().to_string()))
            .await
            .unwrap();
        assert_eq!(result.is_error, Some(true));
        assert!(result_text(&result).contains("unknown job_id"));
    }

    #[tokio::test]
    async fn handle_job_status_missing_arg_is_an_error_result() {
        let dir =
            std::env::temp_dir().join(format!("devkitd-mcp-status-missing-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("plugins")).unwrap();
        let handler = make_handler(&dir);

        let result = handler.handle_job_status(&IndexMap::new()).await.unwrap();
        assert_eq!(result.is_error, Some(true));
        assert!(result_text(&result).contains("missing or invalid job_id"));
    }

    #[tokio::test]
    async fn handle_job_status_known_job_reports_done_with_output() {
        let _guard = process_test_lock().lock().await;
        let dir =
            std::env::temp_dir().join(format!("devkitd-mcp-status-ok-{}", std::process::id()));
        let handler = make_handler(&dir);

        let job_id = start_echo_job(&handler, &dir).await;
        wait_for_terminal(&handler, job_id).await;

        let result = handler
            .handle_job_status(&args_with(&job_id.to_string()))
            .await
            .unwrap();
        assert_ne!(result.is_error, Some(true));
        let body: Value = serde_json::from_str(&result_text(&result)).unwrap();
        assert_eq!(body["state"], "done");
        assert_eq!(body["exit_code"], 0);
        assert_eq!(body["stdout"].as_str().unwrap().trim(), "hi");
    }

    #[tokio::test]
    async fn handle_job_cancel_unknown_id_is_an_error_result() {
        let dir =
            std::env::temp_dir().join(format!("devkitd-mcp-cancel-unknown-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("plugins")).unwrap();
        let handler = make_handler(&dir);

        let result = handler
            .handle_job_cancel(&args_with(&Uuid::new_v4().to_string()))
            .await
            .unwrap();
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn handle_job_cancel_known_job_cancels_it() {
        let _guard = process_test_lock().lock().await;
        let dir =
            std::env::temp_dir().join(format!("devkitd-mcp-cancel-ok-{}", std::process::id()));
        let plugin = PluginConfig {
            name: "sleep".to_string(),
            executable: "sleep.sh".to_string(),
            description: None,
            args_mapping: HashMap::new(),
            timeout_seconds: None,
        };
        write_plugin_and_script(&dir, &plugin, "#!/bin/sh\nsleep 10\n");
        let handler = make_handler(&dir);
        handler.service.registry().reload().await;

        let result = handler
            .handle_plugin_call("sleep", &IndexMap::new())
            .await
            .unwrap();
        let job_id: Value = serde_json::from_str(&result_text(&result)).unwrap();
        let job_id = Uuid::parse_str(job_id["job_id"].as_str().unwrap()).unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
        let cancel_result = handler
            .handle_job_cancel(&args_with(&job_id.to_string()))
            .await
            .unwrap();
        assert_ne!(cancel_result.is_error, Some(true));

        let state = wait_for_terminal(&handler, job_id).await;
        assert!(
            matches!(state, JobState::Cancelled),
            "expected Cancelled, got {:?}",
            state
        );
    }

    #[test]
    fn parse_job_id_rejects_missing_and_malformed_values() {
        assert!(parse_job_id(&IndexMap::new()).is_none());

        let mut args = IndexMap::new();
        args.insert("job_id".to_string(), "not-a-uuid".to_string());
        assert!(parse_job_id(&args).is_none());
    }

    #[test]
    fn parse_job_id_accepts_a_valid_uuid() {
        let id = Uuid::new_v4();
        assert_eq!(parse_job_id(&args_with(&id.to_string())), Some(id));
    }

    #[test]
    fn job_state_to_json_shapes_each_variant() {
        assert_eq!(
            job_state_to_json(&JobState::Received),
            json!({ "state": "received" })
        );
        assert_eq!(
            job_state_to_json(&JobState::Running),
            json!({ "state": "running" })
        );
        assert_eq!(
            job_state_to_json(&JobState::Cancelled),
            json!({ "state": "cancelled" })
        );

        let done = JobState::Done(crate::core::executor::ExecutionResult::success(
            "out".into(),
            "err".into(),
        ));
        assert_eq!(
            job_state_to_json(&done),
            json!({ "state": "done", "stdout": "out", "stderr": "err", "exit_code": 0 })
        );
    }

    #[tokio::test]
    async fn build_tool_list_includes_plugins_and_builtin_job_tools() {
        let dir = std::env::temp_dir().join(format!("devkitd-mcp-list-{}", std::process::id()));
        let plugin = PluginConfig {
            name: "echo".to_string(),
            executable: "echo.sh".to_string(),
            description: None,
            args_mapping: HashMap::new(),
            timeout_seconds: None,
        };
        write_plugin_and_script(&dir, &plugin, "#!/bin/sh\nexit 0\n");
        let handler = make_handler(&dir);

        let tools = handler.build_tool_list().await;
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();

        assert!(names.contains(&"echo"));
        assert!(names.contains(&JOB_STATUS_TOOL));
        assert!(names.contains(&JOB_CANCEL_TOOL));
    }

    #[tokio::test]
    async fn build_tool_list_picks_up_plugins_added_after_construction() {
        let dir = std::env::temp_dir().join(format!("devkitd-mcp-reload-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("plugins")).unwrap();
        let handler = make_handler(&dir);
        assert_eq!(
            handler.build_tool_list().await.len(),
            2,
            "only the two builtin job tools before any plugin exists"
        );

        let plugin = PluginConfig {
            name: "late".to_string(),
            executable: "late.sh".to_string(),
            description: None,
            args_mapping: HashMap::new(),
            timeout_seconds: None,
        };
        write_plugin_and_script(&dir, &plugin, "#!/bin/sh\nexit 0\n");

        let tools = handler.build_tool_list().await;
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert!(
            names.contains(&"late"),
            "newly added plugin should be picked up without restart"
        );
    }
}
