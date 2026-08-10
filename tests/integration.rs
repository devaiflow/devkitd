//! End-to-end tests: a real axum + rmcp Streamable HTTP server, bound to an
//! ephemeral loopback port, driven by a real rmcp client (or, for the auth
//! test, a raw TCP connection). See PLANREF.md Fase 5.

use devkitd::config::ExecutionConfig;
use devkitd::core::plugins::PluginConfig;
use devkitd::core::{CommandService, Executor, JobRegistry, PluginRegistry};
use devkitd::transport::{require_bearer_token, DevkitdHandler};
use rmcp::model::{CallToolRequestParams, ContentBlock};
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::ServiceExt;
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

fn write_plugin_and_script(dir: &Path, plugin: &PluginConfig, script_body: &str) {
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

fn test_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("devkitd-e2e-{}-{}", label, std::process::id()));
    std::fs::create_dir_all(dir.join("plugins")).unwrap();
    dir
}

/// Boots a real server (same wiring as main.rs) on an ephemeral loopback
/// port and returns its address. The server task is detached; it dies with
/// the test process.
async fn spawn_server(dir: &Path, token: Option<&str>) -> SocketAddr {
    spawn_server_with_timeout(dir, token, 5).await
}

/// Same as `spawn_server` but with a custom `[execution] timeout_seconds`
/// default — used to prove a per-call `_timeout_seconds` extends past it.
async fn spawn_server_with_timeout(dir: &Path, token: Option<&str>, timeout_seconds: u64) -> SocketAddr {
    let registry = Arc::new(PluginRegistry::load(&dir.join("plugins")));
    let executor = Arc::new(Executor::new(
        &ExecutionConfig { timeout_seconds, max_parallel: 5, kill_grace_seconds: 1, job_retention_seconds: 900 },
        &dir.join("scripts"),
    ));
    let jobs = Arc::new(JobRegistry::default());
    let service = CommandService::new(registry, executor, jobs);

    let mcp_config = StreamableHttpServerConfig::default().with_stateful_mode(false);
    let http_service = StreamableHttpService::new(
        move || Ok(DevkitdHandler::new(service.clone())),
        Arc::new(NeverSessionManager::default()),
        mcp_config,
    );

    let mut router = axum::Router::new().nest_service("/mcp", http_service);
    if let Some(token) = token {
        router = require_bearer_token(router, Arc::from(token));
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    addr
}

fn result_text(result: &rmcp::model::CallToolResult) -> String {
    match result.content.first() {
        Some(ContentBlock::Text(t)) => t.text.clone(),
        other => panic!("expected a text content block, got {:?}", other),
    }
}

async fn poll_job_status(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
    job_id: &str,
) -> Value {
    loop {
        let result = client
            .call_tool(
                CallToolRequestParams::new("job-status")
                    .with_arguments(json!({ "job_id": job_id }).as_object().unwrap().clone()),
            )
            .await
            .unwrap();
        let body: Value = serde_json::from_str(&result_text(&result)).unwrap();
        match body["state"].as_str().unwrap() {
            "received" | "running" => tokio::time::sleep(Duration::from_millis(50)).await,
            _ => return body,
        }
    }
}

#[tokio::test]
async fn tools_list_call_echo_and_poll_to_done() {
    let dir = test_dir("echo");
    let plugin = PluginConfig {
        name: "echo".to_string(),
        executable: "echo.sh".to_string(),
        description: None,
        args_mapping: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "id".to_string(),
                devkitd::core::plugins::ArgConfig { flag: "--id".to_string(), required: true },
            );
            m
        },
        timeout_seconds: None,
    };
    write_plugin_and_script(&dir, &plugin, "#!/bin/sh\necho \"got $2\"\nexit 0\n");

    let addr = spawn_server(&dir, None).await;
    let transport = StreamableHttpClientTransport::from_uri(format!("http://{}/mcp", addr));
    let client = ().serve(transport).await.unwrap();

    let tools = client.list_all_tools().await.unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    assert!(names.contains(&"echo"));
    assert!(names.contains(&"job-status"));
    assert!(names.contains(&"job-cancel"));

    let call_result = client
        .call_tool(
            CallToolRequestParams::new("echo")
                .with_arguments(json!({ "id": "hello" }).as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_ne!(call_result.is_error, Some(true));
    let job_id = serde_json::from_str::<Value>(&result_text(&call_result)).unwrap()["job_id"]
        .as_str()
        .unwrap()
        .to_string();

    let status = poll_job_status(&client, &job_id).await;
    assert_eq!(status["state"], "done");
    assert_eq!(status["exit_code"], 0);
    assert!(status["stdout"].as_str().unwrap().contains("got hello"));
}

/// Cancels a job mid-flight and verifies the whole process group actually
/// dies: the script forks a child that touches a marker file 3s in, we
/// cancel at 200ms, then wait past the 3s mark and assert the marker was
/// never created.
#[tokio::test]
async fn job_cancel_kills_the_process_group() {
    let dir = test_dir("cancel");
    let marker = dir.join("child-done");
    let plugin = PluginConfig {
        name: "spawn-child".to_string(),
        executable: "spawn-child.sh".to_string(),
        description: None,
        args_mapping: std::collections::HashMap::new(),
        timeout_seconds: None,
    };
    write_plugin_and_script(
        &dir,
        &plugin,
        &format!("#!/bin/sh\n(sleep 3 && touch {}) &\nsleep 10\n", marker.display()),
    );

    let addr = spawn_server(&dir, None).await;
    let transport = StreamableHttpClientTransport::from_uri(format!("http://{}/mcp", addr));
    let client = ().serve(transport).await.unwrap();

    let call_result = client
        .call_tool(CallToolRequestParams::new("spawn-child"))
        .await
        .unwrap();
    let job_id = serde_json::from_str::<Value>(&result_text(&call_result)).unwrap()["job_id"]
        .as_str()
        .unwrap()
        .to_string();

    tokio::time::sleep(Duration::from_millis(200)).await;
    let cancel_result = client
        .call_tool(
            CallToolRequestParams::new("job-cancel")
                .with_arguments(json!({ "job_id": job_id }).as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_ne!(cancel_result.is_error, Some(true));

    let status = poll_job_status(&client, &job_id).await;
    assert_eq!(status["state"], "cancelled");

    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(!marker.exists(), "child process should have died with its parent's process group");
}

/// A missing/unknown job_id is a tool-level error, not a transport failure.
#[tokio::test]
async fn job_status_unknown_id_is_a_tool_error() {
    let dir = test_dir("unknown-status");
    let addr = spawn_server(&dir, None).await;
    let transport = StreamableHttpClientTransport::from_uri(format!("http://{}/mcp", addr));
    let client = ().serve(transport).await.unwrap();

    let result = client
        .call_tool(
            CallToolRequestParams::new("job-status")
                .with_arguments(json!({ "job_id": uuid::Uuid::new_v4().to_string() }).as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(true));
}

/// Requests without the configured bearer token are rejected before they
/// ever reach the MCP handler; the same token is accepted. Driven over a
/// raw TCP connection (not the rmcp client) since we only need to observe
/// the HTTP status line the auth middleware produces.
#[tokio::test]
async fn requests_without_the_configured_token_are_rejected() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = test_dir("auth");
    let addr = spawn_server(&dir, Some("s3cr3t")).await;

    async fn post_mcp(addr: SocketAddr, auth_header: Option<&str>) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let body = "{}";
        let mut request = format!(
            "POST /mcp HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            addr,
            body.len()
        );
        if let Some(h) = auth_header {
            request.push_str(&format!("Authorization: {}\r\n", h));
        }
        request.push_str("\r\n");
        request.push_str(body);

        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response.lines().next().unwrap_or_default().to_string()
    }

    let no_token = post_mcp(addr, None).await;
    assert!(no_token.contains("401"), "expected 401 without a token, got: {}", no_token);

    let wrong_token = post_mcp(addr, Some("Bearer nope")).await;
    assert!(wrong_token.contains("401"), "expected 401 with the wrong token, got: {}", wrong_token);

    let right_token = post_mcp(addr, Some("Bearer s3cr3t")).await;
    assert!(!right_token.contains("401"), "expected the request to pass auth, got: {}", right_token);
}

/// A per-call `_timeout_seconds` is authoritative: it lets the caller
/// extend past the server's `[execution] timeout_seconds` default. The
/// server default here is 1s; the script sleeps 2s; the caller passes
/// `_timeout_seconds: 5`. Pre-fix this would have been capped to 1s and
/// the script killed (`done` / exit_code -2). Post-fix it completes with
/// exit 0.
#[tokio::test]
async fn per_call_timeout_extends_past_server_default() {
    let dir = test_dir("timeout-extend");
    let plugin = PluginConfig {
        name: "slow".to_string(),
        executable: "slow.sh".to_string(),
        description: None,
        args_mapping: std::collections::HashMap::new(),
        timeout_seconds: None,
    };
    write_plugin_and_script(&dir, &plugin, "#!/bin/sh\nsleep 2\nexit 0\n");

    let addr = spawn_server_with_timeout(&dir, None, 1).await;
    let transport = StreamableHttpClientTransport::from_uri(format!("http://{}/mcp", addr));
    let client = ().serve(transport).await.unwrap();

    let call_result = client
        .call_tool(
            CallToolRequestParams::new("slow")
                .with_arguments(json!({ "_timeout_seconds": "5" }).as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_ne!(call_result.is_error, Some(true));
    let job_id = serde_json::from_str::<Value>(&result_text(&call_result)).unwrap()["job_id"]
        .as_str()
        .unwrap()
        .to_string();

    let status = poll_job_status(&client, &job_id).await;
    assert_eq!(status["state"], "done", "expected done, got: {}", status);
    assert_eq!(status["exit_code"], 0, "script should have completed, not be killed at the 1s default");
}
