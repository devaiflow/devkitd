use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone)]
pub struct PathConfig {
    #[serde(default = "default_plugins_dir")]
    pub plugins_dir: PathBuf,
    #[serde(default = "default_scripts_dir")]
    pub scripts_dir: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExecutionConfig {
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default = "default_max_parallel")]
    pub max_parallel: usize,
    #[serde(default = "default_kill_grace_seconds")]
    pub kill_grace_seconds: u64,
    /// How long a finished (done/cancelled) job stays queryable via
    /// `job-status` before being purged from the in-memory registry. No
    /// background sweeper — purge is opportunistic on every new insert.
    /// Default 24h to make re-attach effectively infallible short of a
    /// devkitd restart (matches the flowspec phase4 contract). Operators
    /// with tighter memory budgets can lower it per-deploy.
    #[serde(default = "default_job_retention_seconds")]
    pub job_retention_seconds: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: String,
    /// Hostnames/`host:port` authorities accepted in the inbound `Host`
    /// header (rmcp's built-in DNS-rebinding guard). Defaults match rmcp's
    /// own default (loopback only) — override to reach devkitd from a
    /// Docker container (e.g. add `host.docker.internal`) or another host
    /// on the LAN.
    #[serde(default = "default_allowed_hosts")]
    pub allowed_hosts: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    #[serde(default)]
    pub paths: PathConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub server: ServerConfig,
}

fn default_plugins_dir() -> PathBuf {
    PathBuf::from("./plugins")
}

fn default_scripts_dir() -> PathBuf {
    PathBuf::from("./scripts")
}

fn default_bind_address() -> String {
    "127.0.0.1:9000".to_string()
}

fn default_allowed_hosts() -> Vec<String> {
    vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ]
}

fn default_timeout_seconds() -> u64 {
    600
}

fn default_max_parallel() -> usize {
    5
}

fn default_kill_grace_seconds() -> u64 {
    5
}

fn default_job_retention_seconds() -> u64 {
    24 * 60 * 60
}

impl Default for PathConfig {
    fn default() -> Self {
        PathConfig {
            plugins_dir: default_plugins_dir(),
            scripts_dir: default_scripts_dir(),
        }
    }
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        ExecutionConfig {
            timeout_seconds: default_timeout_seconds(),
            max_parallel: default_max_parallel(),
            kill_grace_seconds: default_kill_grace_seconds(),
            job_retention_seconds: default_job_retention_seconds(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            bind_address: default_bind_address(),
            allowed_hosts: default_allowed_hosts(),
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            paths: PathConfig::default(),
            execution: ExecutionConfig::default(),
            server: ServerConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn load() -> Self {
        if let Some(config) = Self::load_from_os_path() {
            return config;
        }

        if let Some(config) = Self::load_from_project_local() {
            return config;
        }

        tracing::info!("No config file found, using built-in defaults");
        Self::default()
    }

    fn load_from_os_path() -> Option<Self> {
        let proj_dirs = directories::ProjectDirs::from("com", "devkitd", "devkitd")?;
        let config_path = proj_dirs.config_dir().join("config.toml");

        if config_path.exists() {
            match Self::parse_file(&config_path) {
                Ok(config) => {
                    tracing::info!(path = %config_path.display(), "Config loaded");
                    return Some(config);
                }
                Err(e) => {
                    tracing::warn!(path = %config_path.display(), error = %e, "Error parsing config, falling back");
                }
            }
        }
        None
    }

    fn load_from_project_local() -> Option<Self> {
        let config_path = PathBuf::from("./devkitd.toml");

        if config_path.exists() {
            match Self::parse_file(&config_path) {
                Ok(config) => {
                    tracing::info!(path = %config_path.display(), "Config loaded");
                    return Some(config);
                }
                Err(e) => {
                    tracing::warn!(path = %config_path.display(), error = %e, "Error parsing config, falling back");
                }
            }
        }
        None
    }

    fn parse_file(path: &std::path::Path) -> Result<Self, String> {
        let content = fs::read_to_string(path).map_err(|e| e.to_string())?;
        let config: AppConfig = toml::from_str(&content).map_err(|e| e.to_string())?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_allowed_hosts_is_loopback_only() {
        assert_eq!(
            ServerConfig::default().allowed_hosts,
            vec![
                "localhost".to_string(),
                "127.0.0.1".to_string(),
                "::1".to_string()
            ]
        );
    }

    #[test]
    fn allowed_hosts_omitted_from_toml_falls_back_to_default() {
        let config: AppConfig =
            toml::from_str("[server]\nbind_address = \"127.0.0.1:9000\"\n").unwrap();
        assert_eq!(
            config.server.allowed_hosts,
            ServerConfig::default().allowed_hosts
        );
    }

    #[test]
    fn allowed_hosts_can_be_overridden_for_docker() {
        let config: AppConfig = toml::from_str(
            "[server]\nbind_address = \"0.0.0.0:9000\"\nallowed_hosts = [\"localhost\", \"host.docker.internal\"]\n",
        )
        .unwrap();
        assert_eq!(
            config.server.allowed_hosts,
            vec!["localhost".to_string(), "host.docker.internal".to_string()]
        );
    }

    #[test]
    fn default_job_retention_is_24h() {
        assert_eq!(
            ExecutionConfig::default().job_retention_seconds,
            24 * 60 * 60
        );
    }

    #[test]
    fn job_retention_omitted_from_toml_falls_back_to_24h() {
        let config: AppConfig = toml::from_str("[execution]\ntimeout_seconds = 60\n").unwrap();
        assert_eq!(config.execution.job_retention_seconds, 24 * 60 * 60);
    }

    #[test]
    fn job_retention_can_be_overridden() {
        let config: AppConfig =
            toml::from_str("[execution]\njob_retention_seconds = 1800\n").unwrap();
        assert_eq!(config.execution.job_retention_seconds, 1800);
    }
}
