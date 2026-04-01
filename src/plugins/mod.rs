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
        println!(
            "Plugin directory does not exist or is not a directory: {}",
            dir_path.display()
        );
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
                println!("Error reading file: {:?}", file_path);
                continue;
            };
            match serde_json::from_str::<PluginConfig>(&content) {
                Ok(config) => {
                    println!("Loaded plugin: {}", config.name);
                    plugins.insert(config.name.clone(), config);
                }
                Err(e) => {
                    println!("Error parsing file {:?}: {}", file_path, e);
                }
            }
        }
    }
    plugins
}
