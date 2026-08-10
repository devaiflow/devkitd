pub mod executor;
pub mod job_registry;
pub mod plugins;
pub mod service;

pub use executor::Executor;
pub use job_registry::{JobId, JobRegistry, JobState};
pub use plugins::PluginRegistry;
pub use service::{CommandService, RejectReason};

/// Shared across executor and service test modules. Real processes get
/// spawned and process-group-signalled in these tests; run concurrently,
/// two tests' pgids can collide via kernel pid/pgid reuse (one test's kill
/// hits another's freshly spawned process). Serialize them.
#[cfg(test)]
pub(crate) fn process_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
