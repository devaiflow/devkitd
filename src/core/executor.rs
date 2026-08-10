use crate::config::ExecutionConfig;
use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

impl ExecutionResult {
    pub fn success(stdout: String, stderr: String) -> Self {
        ExecutionResult { stdout, stderr, exit_code: 0 }
    }

    pub fn error(exit_code: i32, stdout: String, stderr: String) -> Self {
        ExecutionResult { stdout, stderr, exit_code }
    }

    pub fn not_found(stderr: String) -> Self {
        ExecutionResult { stdout: String::new(), stderr, exit_code: -1 }
    }

    pub fn timed_out() -> Self {
        ExecutionResult {
            stdout: String::new(),
            stderr: "execution timed out".to_string(),
            exit_code: -2,
        }
    }

    pub fn cancelled() -> Self {
        ExecutionResult {
            stdout: String::new(),
            stderr: "execution cancelled".to_string(),
            exit_code: -3,
        }
    }
}

/// Outcome of a single execution attempt. Timeout and cancellation are
/// distinguished from a normal completion so the caller (CommandService)
/// can report them as distinct lifecycle events.
#[derive(Debug)]
pub enum ExecOutcome {
    Completed(ExecutionResult),
    TimedOut,
    Cancelled,
}

#[derive(Clone)]
pub struct Executor {
    scripts_dir: PathBuf,
    semaphore: Arc<Semaphore>,
    /// Default per-call timeout when neither a per-call `_timeout_seconds`
    /// nor a per-plugin `timeout_seconds` is supplied. A caller-supplied
    /// override is authoritative — it can extend past this default (the
    /// flowspec phase4 contract treats the step's `timeout:` as the real
    /// bound, with deadline-driven `job-cancel` as the backstop). There is
    /// no server-side hard ceiling; if one is ever needed, add an
    /// `absolute_max_timeout_seconds` knob rather than repurposing this
    /// field to silently cap callers.
    max_timeout: Duration,
    kill_grace: Duration,
}

impl Executor {
    pub fn new(config: &ExecutionConfig, scripts_dir: &Path) -> Self {
        Executor {
            scripts_dir: scripts_dir.to_path_buf(),
            semaphore: Arc::new(Semaphore::new(config.max_parallel)),
            max_timeout: Duration::from_secs(config.timeout_seconds),
            kill_grace: Duration::from_secs(config.kill_grace_seconds),
        }
    }

    /// Execute a plugin's script. `timeout_override` (e.g. a caller-supplied
    /// `_timeout_seconds`) is authoritative when present — it does *not*
    /// get capped by `max_timeout`, which is only the default when no
    /// override is given. Callers can extend past the default; the
    /// deadline-driven `job-cancel` is the backstop for a wedged call.
    pub async fn execute(
        &self,
        executable: &str,
        args: &[String],
        timeout_override: Option<Duration>,
        ct: CancellationToken,
    ) -> ExecOutcome {
        let _permit = match self.semaphore.acquire().await {
            Ok(p) => p,
            Err(_) => {
                return ExecOutcome::Completed(ExecutionResult::error(
                    -1,
                    String::new(),
                    "failed to acquire execution slot".to_string(),
                ));
            }
        };

        let script_path = self.scripts_dir.join(executable);

        if !script_path.exists() {
            return ExecOutcome::Completed(ExecutionResult::not_found(format!(
                "script not found: {}",
                script_path.display()
            )));
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(metadata) = script_path.metadata() {
                let perms = metadata.permissions();
                if perms.mode() & 0o111 == 0 {
                    return ExecOutcome::Completed(ExecutionResult::error(
                        -1,
                        String::new(),
                        format!("permission denied: {}", script_path.display()),
                    ));
                }
            }
        }

        let effective_timeout = match timeout_override {
            Some(t) => t,
            None => self.max_timeout,
        };

        let mut cmd = Command::new(&script_path);
        cmd.args(args);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        #[cfg(unix)]
        cmd.process_group(0);

        let child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                return ExecOutcome::Completed(ExecutionResult::error(
                    -1,
                    String::new(),
                    e.to_string(),
                ));
            }
        };

        // process_group(0) makes the child its own group leader, so its pid
        // is also its pgid — killing the group reaches any children the
        // script itself spawns (agent scripts commonly do).
        let pgid = child.id().map(|id| id as i32);

        let fut = child.wait_with_output();
        tokio::pin!(fut);

        tokio::select! {
            res = &mut fut => {
                match res {
                    Ok(output) => {
                        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                        let exit_code = output.status.code().unwrap_or(-1);
                        let result = if exit_code == 0 {
                            ExecutionResult::success(stdout, stderr)
                        } else {
                            ExecutionResult::error(exit_code, stdout, stderr)
                        };
                        ExecOutcome::Completed(result)
                    }
                    Err(e) => ExecOutcome::Completed(ExecutionResult::error(-1, String::new(), e.to_string())),
                }
            }
            _ = tokio::time::sleep(effective_timeout) => {
                tracing::error!(script = %script_path.display(), "Script timed out");
                if let Some(pgid) = pgid {
                    kill_and_drain(pgid, fut.as_mut(), self.kill_grace).await;
                }
                ExecOutcome::TimedOut
            }
            _ = ct.cancelled() => {
                tracing::info!(script = %script_path.display(), "Execution cancelled");
                if let Some(pgid) = pgid {
                    kill_and_drain(pgid, fut.as_mut(), self.kill_grace).await;
                }
                ExecOutcome::Cancelled
            }
        }
    }
}

/// Send SIGTERM to the process group and give it `grace` to exit (still
/// draining stdout/stderr via `fut`, so a handler that flushes final output
/// on SIGTERM is captured). If it hasn't exited by then, SIGKILL and wait
/// for the actual exit so no process is left orphaned.
async fn kill_and_drain<F>(pgid: i32, mut fut: Pin<&mut F>, grace: Duration)
where
    F: Future<Output = std::io::Result<std::process::Output>>,
{
    let _ = killpg(Pid::from_raw(pgid), Signal::SIGTERM);

    tokio::select! {
        _ = &mut fut => return,
        _ = tokio::time::sleep(grace) => {
            let _ = killpg(Pid::from_raw(pgid), Signal::SIGKILL);
        }
    }

    let _ = fut.await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExecutionConfig;
    use crate::core::process_test_lock;
    use std::os::unix::fs::PermissionsExt;

    fn write_script(dir: &Path, name: &str, body: &str) {
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).unwrap();
    }

    fn test_config(timeout_seconds: u64) -> ExecutionConfig {
        ExecutionConfig {
            timeout_seconds,
            max_parallel: 5,
            kill_grace_seconds: 1,
            job_retention_seconds: 900,
        }
    }

    #[tokio::test]
    async fn success_exit_zero() {
        let _guard = process_test_lock().lock().await;
        let dir = std::env::temp_dir().join(format!("devkitd-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        write_script(&dir, "ok.sh", "#!/bin/sh\necho hello\nexit 0\n");

        let executor = Executor::new(&test_config(5), &dir);
        let outcome = executor.execute("ok.sh", &[], None, CancellationToken::new()).await;

        match outcome {
            ExecOutcome::Completed(r) => {
                assert_eq!(r.exit_code, 0);
                assert_eq!(r.stdout.trim(), "hello");
            }
            _ => panic!("expected Completed"),
        }
    }

    #[tokio::test]
    async fn not_found_script() {
        let _guard = process_test_lock().lock().await;
        let dir = std::env::temp_dir().join(format!("devkitd-test-nf-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let executor = Executor::new(&test_config(5), &dir);
        let outcome = executor.execute("missing.sh", &[], None, CancellationToken::new()).await;

        match outcome {
            ExecOutcome::Completed(r) => assert_eq!(r.exit_code, -1),
            _ => panic!("expected Completed(not_found)"),
        }
    }

    /// A script that spawns a child sleeping far past the timeout. If group
    /// kill worked, the child dies too — verified by the child failing to
    /// write its "done" marker file after the parent is killed.
    #[tokio::test]
    async fn timeout_kills_process_group_including_children() {
        let _guard = process_test_lock().lock().await;
        let dir = std::env::temp_dir().join(format!("devkitd-test-grp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let marker = dir.join("child-done");
        let _ = std::fs::remove_file(&marker);

        write_script(
            &dir,
            "spawn-child.sh",
            &format!(
                "#!/bin/sh\n(sleep 3 && touch {}) &\nsleep 10\n",
                marker.display()
            ),
        );

        let executor = Executor::new(&test_config(1), &dir);
        let outcome = executor.execute("spawn-child.sh", &[], None, CancellationToken::new()).await;

        assert!(matches!(outcome, ExecOutcome::TimedOut));

        // Give the (now-dead) group a moment past when the child *would*
        // have written the marker if it had survived the kill.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!marker.exists(), "child process should have been killed with its parent's group");
    }

    #[tokio::test]
    async fn cancellation_kills_running_process() {
        let _guard = process_test_lock().lock().await;
        let dir = std::env::temp_dir().join(format!("devkitd-test-cancel-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        write_script(&dir, "sleep.sh", "#!/bin/sh\nsleep 10\n");

        let executor = Executor::new(&test_config(30), &dir);
        let ct = CancellationToken::new();
        let ct_clone = ct.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            ct_clone.cancel();
        });

        let start = std::time::Instant::now();
        let outcome = executor.execute("sleep.sh", &[], None, ct).await;

        assert!(matches!(outcome, ExecOutcome::Cancelled));
        assert!(start.elapsed() < Duration::from_secs(5), "should return promptly on cancellation, not wait for the 30s timeout");
    }

    /// A per-call `_timeout_seconds` override is authoritative — it can
    /// extend past the configured `[execution] timeout_seconds` default.
    /// The script sleeps 2s; the global default is 1s; the override asks
    /// for 3s. The script must reach `Completed` (exit 0) and take ~2s,
    /// not be killed at the 1s default.
    #[tokio::test]
    async fn per_call_timeout_override_extends_past_global_default() {
        let _guard = process_test_lock().lock().await;
        let dir = std::env::temp_dir().join(format!("devkitd-test-ext-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        write_script(&dir, "sleep.sh", "#!/bin/sh\nsleep 2\nexit 0\n");

        let executor = Executor::new(&test_config(1), &dir);
        let start = std::time::Instant::now();
        let outcome = executor
            .execute("sleep.sh", &[], Some(Duration::from_secs(3)), CancellationToken::new())
            .await;

        match outcome {
            ExecOutcome::Completed(r) => assert_eq!(r.exit_code, 0),
            other => panic!("expected Completed, got {:?} — override must extend past default", other),
        }
        assert!(
            start.elapsed() >= Duration::from_secs(2),
            "script should have run to completion (~2s), not be cut at the 1s default: {:?}",
            start.elapsed()
        );
        assert!(
            start.elapsed() < Duration::from_secs(3),
            "script should not have hit the 3s override: {:?}",
            start.elapsed()
        );
    }
}
