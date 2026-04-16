use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Deserialize, Clone)]
pub struct ArgConfig {
    pub flag: String,
    #[serde(default)]
    pub required: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PluginConfig {
    pub name: String,
    pub executable: String,
    pub description: Option<String>,
    #[serde(default)]
    pub args_mapping: HashMap<String, ArgConfig>,
}

pub fn load_plugins(dir_path: &std::path::Path) -> HashMap<String, PluginConfig> {
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
                    let required: Vec<&String> = config.args_mapping.iter()
                        .filter(|(_, c)| c.required)
                        .map(|(name, _)| name)
                        .collect();
                    let optional: Vec<&String> = config.args_mapping.iter()
                        .filter(|(_, c)| !c.required)
                        .map(|(name, _)| name)
                        .collect();
                    tracing::info!(
                        name = %config.name,
                        args = config.args_mapping.len(),
                        ?required,
                        ?optional,
                        "Loaded plugin",
                    );
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
