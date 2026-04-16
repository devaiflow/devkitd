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
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: String,
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

fn default_timeout_seconds() -> u64 {
    300
}

fn default_max_parallel() -> usize {
    5
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
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            bind_address: default_bind_address(),
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
