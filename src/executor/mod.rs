use crate::config::ExecutionConfig;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

impl ExecutionResult {
    pub fn success(stdout: String, stderr: String) -> Self {
        ExecutionResult {
            stdout,
            stderr,
            exit_code: 0,
        }
    }

    pub fn error(exit_code: i32, stdout: String, stderr: String) -> Self {
        ExecutionResult {
            stdout,
            stderr,
            exit_code,
        }
    }

    pub fn not_found(stderr: String) -> Self {
        ExecutionResult {
            stdout: String::new(),
            stderr,
            exit_code: -1,
        }
    }

    pub fn timed_out() -> Self {
        ExecutionResult {
            stdout: String::new(),
            stderr: "execution timed out".to_string(),
            exit_code: -2,
        }
    }
}

pub struct Executor {
    scripts_dir: PathBuf,
    semaphore: Arc<Semaphore>,
    timeout: Duration,
}

impl Executor {
    pub fn new(config: &ExecutionConfig, scripts_dir: &Path) -> Self {
        Executor {
            scripts_dir: scripts_dir.to_path_buf(),
            semaphore: Arc::new(Semaphore::new(config.max_parallel)),
            timeout: Duration::from_secs(config.timeout_seconds),
        }
    }

    pub async fn execute(&self, executable: &str, args: &[String]) -> ExecutionResult {
        let _permit = match self.semaphore.acquire().await {
            Ok(p) => p,
            Err(_) => {
                return ExecutionResult::error(-1, String::new(), "failed to acquire execution slot".to_string());
            }
        };

        let script_path = self.scripts_dir.join(executable);

        if !script_path.exists() {
            return ExecutionResult::not_found(format!("script not found: {}", script_path.display()));
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(metadata) = script_path.metadata() {
                let perms = metadata.permissions();
                if perms.mode() & 0o111 == 0 {
                    return ExecutionResult::error(-1, String::new(), format!("permission denied: {}", script_path.display()));
                }
            }
        }

        let mut cmd = Command::new(&script_path);
        cmd.args(args);

        match tokio::time::timeout(self.timeout, cmd.output()).await {
            Ok(Ok(output)) => {
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                let exit_code = output.status.code().unwrap_or(-1);

                if exit_code == 0 {
                    ExecutionResult::success(stdout, stderr)
                } else {
                    ExecutionResult::error(exit_code, stdout, stderr)
                }
            }
            Ok(Err(e)) => ExecutionResult::error(-1, String::new(), e.to_string()),
            Err(_) => {
                eprintln!("[executor] Script timed out: {}", script_path.display());
                ExecutionResult::timed_out()
            }
        }
    }
}
