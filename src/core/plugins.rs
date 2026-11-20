use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use tokio::sync::RwLock;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ArgConfig {
    pub flag: String,
    #[serde(default)]
    pub required: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PluginConfig {
    pub name: String,
    pub executable: String,
    pub description: Option<String>,
    #[serde(default)]
    pub args_mapping: HashMap<String, ArgConfig>,
    /// Per-plugin timeout override (seconds). Capped by the global config max.
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
}

fn scan_plugins(dir_path: &std::path::Path) -> HashMap<String, PluginConfig> {
    let mut plugins = HashMap::new();

    if !dir_path.exists() || !dir_path.is_dir() {
        tracing::warn!(path = %dir_path.display(), "Plugin directory does not exist or is not a directory");
        return plugins;
    }

    if let Ok(entries) = fs::read_dir(dir_path) {
        for entry in entries.flatten() {
            let file_path = entry.path();
            if !file_path.is_file()
                || file_path.extension().and_then(|s| s.to_str()) != Some("json")
            {
                continue;
            }
            let Ok(content) = fs::read_to_string(&file_path) else {
                tracing::error!(path = ?file_path, "Error reading plugin file");
                continue;
            };
            match serde_json::from_str::<PluginConfig>(&content) {
                Ok(config) => {
                    tracing::info!(name = %config.name, args = config.args_mapping.len(), "Loaded plugin");
                    plugins.insert(config.name.clone(), config);
                }
                Err(e) => {
                    tracing::error!(path = ?file_path, error = %e, "Error parsing plugin file");
                }
            }
        }
    }
    plugins
}

fn hash_plugins(plugins: &HashMap<String, PluginConfig>) -> u64 {
    let mut names: Vec<&String> = plugins.keys().collect();
    names.sort();
    let mut hasher = DefaultHasher::new();
    for name in names {
        name.hash(&mut hasher);
        if let Some(p) = plugins.get(name) {
            p.executable.hash(&mut hasher);
            p.description.hash(&mut hasher);
            p.timeout_seconds.hash(&mut hasher);
            let mut arg_names: Vec<&String> = p.args_mapping.keys().collect();
            arg_names.sort();
            for a in arg_names {
                a.hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

/// Live registry of plugins backed by the plugins directory on disk.
/// `reload()` performs a full rescan (cheap at the expected scale) and
/// reports whether the resulting plugin set actually changed, so callers
/// can emit `tools/list_changed` only when it matters.
pub struct PluginRegistry {
    dir: PathBuf,
    state: RwLock<(HashMap<String, PluginConfig>, u64)>,
}

impl PluginRegistry {
    pub fn load(dir: &std::path::Path) -> Self {
        let plugins = scan_plugins(dir);
        let hash = hash_plugins(&plugins);
        PluginRegistry {
            dir: dir.to_path_buf(),
            state: RwLock::new((plugins, hash)),
        }
    }

    /// Rescan the plugins directory. Returns true if the plugin set changed.
    pub async fn reload(&self) -> bool {
        let plugins = scan_plugins(&self.dir);
        let hash = hash_plugins(&plugins);
        let mut state = self.state.write().await;
        let changed = state.1 != hash;
        *state = (plugins, hash);
        changed
    }

    pub async fn get(&self, name: &str) -> Option<PluginConfig> {
        self.state.read().await.0.get(name).cloned()
    }

    pub async fn list(&self) -> Vec<PluginConfig> {
        self.state.read().await.0.values().cloned().collect()
    }
}
