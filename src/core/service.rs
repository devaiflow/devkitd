use crate::core::executor::{ExecOutcome, ExecutionResult, Executor};
use crate::core::job_registry::{JobId, JobState};
use crate::core::plugins::PluginConfig;
use crate::core::{JobRegistry, PluginRegistry};
use indexmap::IndexMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Reserved arg name for a per-call timeout override. Stripped before flag
/// mapping so it never leaks into the script's argv. Not exposed in MCP
/// tool schemas — only programmatic callers (e.g. flowspec) that know about
/// it can use it; agents filling tool args from a schema never see it.
pub const TIMEOUT_OVERRIDE_ARG: &str = "_timeout_seconds";

/// Why a command was rejected before a job was ever created. Distinct from
/// job failure (which is a terminal `JobState::Done` with a nonzero exit
/// code) — a rejection means nothing ran at all.
pub enum RejectReason {
    UnknownCommand(String),
    MissingArgs(Vec<String>),
}

impl RejectReason {
    pub fn message(&self) -> String {
        match self {
            RejectReason::UnknownCommand(cmd) => format!("Unknown command: {}", cmd),
            RejectReason::MissingArgs(missing) => {
                format!("missing required argument: {}", missing.join(", "))
            }
        }
    }
}

#[derive(Clone)]
pub struct CommandService {
    registry: Arc<PluginRegistry>,
    executor: Arc<Executor>,
    jobs: Arc<JobRegistry>,
}

impl CommandService {
    pub fn new(
        registry: Arc<PluginRegistry>,
        executor: Arc<Executor>,
        jobs: Arc<JobRegistry>,
    ) -> Self {
        CommandService {
            registry,
            executor,
            jobs,
        }
    }

    pub fn registry(&self) -> &Arc<PluginRegistry> {
        &self.registry
    }

    pub fn jobs(&self) -> &Arc<JobRegistry> {
        &self.jobs
    }

    /// Validates and starts a plugin command in the background, returning
    /// its job id immediately. Validation (unknown command / missing
    /// required args) happens synchronously and rejects without creating a
    /// job. Poll `jobs().get(job_id)` for progress; `jobs().cancel(job_id)`
    /// to stop it.
    pub async fn start(
        &self,
        command: &str,
        args: &IndexMap<String, String>,
    ) -> Result<JobId, RejectReason> {
        let plugin = self
            .registry
            .get(command)
            .await
            .ok_or_else(|| RejectReason::UnknownCommand(command.to_string()))?;

        let (valid_args, missing_args) = Self::check_required_args(&plugin, args);
        if !missing_args.is_empty() {
            return Err(RejectReason::MissingArgs(missing_args));
        }

        let timeout_override = args
            .get(TIMEOUT_OVERRIDE_ARG)
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .or(plugin.timeout_seconds.map(Duration::from_secs));

        let ct = CancellationToken::new();
        let job_id = self.jobs.insert(command.to_string(), ct.clone()).await;

        let executor = Arc::clone(&self.executor);
        let executable = plugin.executable.clone();
        let jobs = Arc::clone(&self.jobs);

        tokio::spawn(async move {
            jobs.set_state(job_id, JobState::Running).await;

            let outcome = executor
                .execute(&executable, &valid_args, timeout_override, ct)
                .await;

            let state = match outcome {
                ExecOutcome::Completed(result) => JobState::Done(result),
                ExecOutcome::TimedOut => JobState::Done(ExecutionResult::timed_out()),
                ExecOutcome::Cancelled => JobState::Cancelled,
            };
            jobs.set_state(job_id, state).await;
        });

        Ok(job_id)
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
    use crate::config::ExecutionConfig;
    use crate::core::plugins::ArgConfig;
    use crate::core::process_test_lock;
    use std::collections::HashMap;
    use std::os::unix::fs::PermissionsExt;

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

    fn make_service(dir: &std::path::Path) -> CommandService {
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
        CommandService::new(registry, executor, Arc::new(JobRegistry::default()))
    }

    async fn wait_for_terminal(service: &CommandService, job_id: JobId) -> JobState {
        loop {
            match service.jobs().get(job_id).await {
                Some(JobState::Received) | Some(JobState::Running) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Some(state) => return state,
                None => panic!("job disappeared before reaching a terminal state"),
            }
        }
    }

    #[tokio::test]
    async fn unknown_command_is_rejected_without_creating_a_job() {
        let dir = std::env::temp_dir().join(format!("devkitd-svc-unknown-{}", std::process::id()));
        std::fs::create_dir_all(&dir.join("plugins")).unwrap();
        let service = make_service(&dir);

        let result = service.start("nope", &IndexMap::new()).await;
        assert!(matches!(result, Err(RejectReason::UnknownCommand(_))));
    }

    #[tokio::test]
    async fn missing_required_arg_is_rejected() {
        let dir = std::env::temp_dir().join(format!("devkitd-svc-missing-{}", std::process::id()));
        let mut args_mapping = HashMap::new();
        args_mapping.insert(
            "id".to_string(),
            ArgConfig {
                flag: "--id".to_string(),
                required: true,
            },
        );
        let plugin = PluginConfig {
            name: "test".to_string(),
            executable: "test.sh".to_string(),
            description: None,
            args_mapping,
            timeout_seconds: None,
        };
        write_plugin_and_script(&dir, &plugin, "#!/bin/sh\nexit 0\n");
        let service = make_service(&dir);

        let result = service.start("test", &IndexMap::new()).await;
        assert!(matches!(result, Err(RejectReason::MissingArgs(_))));
    }

    #[tokio::test]
    async fn successful_command_reaches_done() {
        let _guard = process_test_lock().lock().await;
        let dir = std::env::temp_dir().join(format!("devkitd-svc-ok-{}", std::process::id()));
        let plugin = PluginConfig {
            name: "echo".to_string(),
            executable: "echo.sh".to_string(),
            description: None,
            args_mapping: HashMap::new(),
            timeout_seconds: None,
        };
        write_plugin_and_script(&dir, &plugin, "#!/bin/sh\necho hi\nexit 0\n");
        let service = make_service(&dir);

        let job_id = service.start("echo", &IndexMap::new()).await.ok().unwrap();
        match wait_for_terminal(&service, job_id).await {
            JobState::Done(result) => assert_eq!(result.exit_code, 0),
            other => panic!("expected Done, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn timeout_override_arg_is_stripped_before_flag_mapping() {
        let _guard = process_test_lock().lock().await;
        let dir = std::env::temp_dir().join(format!("devkitd-svc-timeout-{}", std::process::id()));
        let plugin = PluginConfig {
            name: "argdump".to_string(),
            executable: "argdump.sh".to_string(),
            description: None,
            args_mapping: HashMap::new(),
            timeout_seconds: None,
        };
        // Script ignores its args; test only needs it to run fast and succeed.
        write_plugin_and_script(&dir, &plugin, "#!/bin/sh\necho \"$@\"\nexit 0\n");
        let service = make_service(&dir);

        let mut args = IndexMap::new();
        args.insert(TIMEOUT_OVERRIDE_ARG.to_string(), "2".to_string());

        let job_id = service.start("argdump", &args).await.ok().unwrap();
        let result = match wait_for_terminal(&service, job_id).await {
            JobState::Done(result) => result,
            other => panic!("expected Done, got {:?}", other),
        };
        assert_eq!(result.exit_code, 0);
        assert!(
            !result.stdout.contains(TIMEOUT_OVERRIDE_ARG),
            "_timeout_seconds must not leak into script argv"
        );
    }

    #[tokio::test]
    async fn cancellation_reports_cancelled_not_done() {
        let _guard = process_test_lock().lock().await;
        let dir = std::env::temp_dir().join(format!("devkitd-svc-cancel-{}", std::process::id()));
        let plugin = PluginConfig {
            name: "sleep".to_string(),
            executable: "sleep.sh".to_string(),
            description: None,
            args_mapping: HashMap::new(),
            timeout_seconds: None,
        };
        write_plugin_and_script(&dir, &plugin, "#!/bin/sh\nsleep 10\n");
        let service = make_service(&dir);

        let job_id = service.start("sleep", &IndexMap::new()).await.ok().unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(service.jobs().cancel(job_id).await);

        let state = wait_for_terminal(&service, job_id).await;
        assert!(
            matches!(state, JobState::Cancelled),
            "expected Cancelled, got {:?}",
            state
        );
    }

    #[tokio::test]
    async fn cancel_unknown_job_id_returns_false() {
        let dir =
            std::env::temp_dir().join(format!("devkitd-svc-cancel-unknown-{}", std::process::id()));
        std::fs::create_dir_all(&dir.join("plugins")).unwrap();
        let service = make_service(&dir);

        assert!(!service.jobs().cancel(uuid::Uuid::new_v4()).await);
    }
}
