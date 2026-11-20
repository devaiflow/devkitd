use crate::core::executor::ExecutionResult;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub type JobId = Uuid;

/// Default retention used by [`JobRegistry::default`] (the `Default` impl,
/// test/fixture path). Production wiring passes the configured
/// `job_retention_seconds` (default 24h) via [`JobRegistry::new`].
const DEFAULT_RETENTION: Duration = Duration::from_secs(15 * 60);

#[derive(Clone, Debug)]
pub enum JobState {
    Received,
    Running,
    Done(ExecutionResult),
    Cancelled,
}

impl JobState {
    fn is_terminal(&self) -> bool {
        matches!(self, JobState::Done(_) | JobState::Cancelled)
    }
}

struct JobRecord {
    state: JobState,
    cancel_token: CancellationToken,
    #[allow(dead_code)]
    command: String,
    #[allow(dead_code)]
    started_at: Instant,
    terminal_since: Option<Instant>,
}

#[derive(Clone)]
pub struct JobRegistry {
    jobs: Arc<Mutex<HashMap<JobId, JobRecord>>>,
    retention: Duration,
}

impl JobRegistry {
    /// Production constructor — `retention` is how long a terminal job
    /// stays queryable before the next insert purges it. Comes from
    /// `[execution] job_retention_seconds` (default 24h).
    pub fn new(retention: Duration) -> Self {
        JobRegistry {
            jobs: Arc::new(Mutex::new(HashMap::new())),
            retention,
        }
    }

    /// Registers a new job in `Received` state and returns its id.
    /// Opportunistically purges jobs that reached a terminal state more
    /// than `retention` ago.
    pub async fn insert(&self, command: String, cancel_token: CancellationToken) -> JobId {
        let job_id = Uuid::new_v4();
        let mut jobs = self.jobs.lock().await;
        let retention = self.retention;
        jobs.retain(|_, r| r.terminal_since.is_none_or(|t| t.elapsed() < retention));
        jobs.insert(
            job_id,
            JobRecord {
                state: JobState::Received,
                cancel_token,
                command,
                started_at: Instant::now(),
                terminal_since: None,
            },
        );
        job_id
    }

    pub async fn set_state(&self, job_id: JobId, state: JobState) {
        if let Some(record) = self.jobs.lock().await.get_mut(&job_id) {
            if state.is_terminal() {
                record.terminal_since = Some(Instant::now());
            }
            record.state = state;
        }
    }

    pub async fn get(&self, job_id: JobId) -> Option<JobState> {
        self.jobs.lock().await.get(&job_id).map(|r| r.state.clone())
    }

    /// Cancels the job's token if it exists. Returns false if `job_id` is
    /// unknown. Cancelling an already-terminal job is a harmless no-op that
    /// still reports success — the caller only needs to know the id was
    /// real.
    pub async fn cancel(&self, job_id: JobId) -> bool {
        match self.jobs.lock().await.get(&job_id) {
            Some(record) => {
                record.cancel_token.cancel();
                true
            }
            None => false,
        }
    }
}

impl Default for JobRegistry {
    /// Test/fixture default — keeps the previous 15-min retention so tests
    /// that don't care about retention behaviour read identically to before
    /// it became configurable. Production wiring uses [`JobRegistry::new`]
    /// with the configured `job_retention_seconds` (default 24h).
    fn default() -> Self {
        JobRegistry::new(DEFAULT_RETENTION)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unknown_job_returns_none() {
        let jobs = JobRegistry::default();
        assert!(jobs.get(Uuid::new_v4()).await.is_none());
    }

    #[tokio::test]
    async fn insert_then_get_reflects_state_transitions() {
        let jobs = JobRegistry::default();
        let id = jobs
            .insert("echo".to_string(), CancellationToken::new())
            .await;
        assert!(matches!(jobs.get(id).await, Some(JobState::Received)));

        jobs.set_state(id, JobState::Running).await;
        assert!(matches!(jobs.get(id).await, Some(JobState::Running)));

        jobs.set_state(
            id,
            JobState::Done(ExecutionResult::success("hi".into(), "".into())),
        )
        .await;
        assert!(matches!(jobs.get(id).await, Some(JobState::Done(_))));
    }

    #[tokio::test]
    async fn cancel_fires_token_and_reports_known() {
        let jobs = JobRegistry::default();
        let ct = CancellationToken::new();
        let id = jobs.insert("sleep".to_string(), ct.clone()).await;

        assert!(jobs.cancel(id).await);
        assert!(ct.is_cancelled());
    }

    #[tokio::test]
    async fn cancel_unknown_job_returns_false() {
        let jobs = JobRegistry::default();
        assert!(!jobs.cancel(Uuid::new_v4()).await);
    }

    /// A terminal job is purged on the next insert once it has been
    /// terminal longer than the registry's retention window. With
    /// retention = 0, anything terminal is immediately eligible for purge.
    #[tokio::test]
    async fn retention_zero_purges_terminal_job_on_next_insert() {
        let jobs = JobRegistry::new(Duration::ZERO);
        let id = jobs
            .insert("echo".to_string(), CancellationToken::new())
            .await;
        jobs.set_state(
            id,
            JobState::Done(ExecutionResult::success("hi".into(), "".into())),
        )
        .await;

        let _other = jobs
            .insert("echo2".to_string(), CancellationToken::new())
            .await;
        assert!(
            jobs.get(id).await.is_none(),
            "terminal job should be purged on insert with retention 0"
        );
    }

    /// A terminal job within the retention window is kept across inserts.
    #[tokio::test]
    async fn retention_window_keeps_terminal_job_across_inserts() {
        let jobs = JobRegistry::new(Duration::from_secs(24 * 60 * 60));
        let id = jobs
            .insert("echo".to_string(), CancellationToken::new())
            .await;
        jobs.set_state(
            id,
            JobState::Done(ExecutionResult::success("hi".into(), "".into())),
        )
        .await;

        let _other = jobs
            .insert("echo2".to_string(), CancellationToken::new())
            .await;
        assert!(
            matches!(jobs.get(id).await, Some(JobState::Done(_))),
            "terminal job within retention window should survive"
        );
    }
}
