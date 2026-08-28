//! File path helpers for file-backed table scans.

use std::path::Path;

pub(super) fn table_name(location: &str) -> String {
    let file_name = Path::new(location)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");
    let normalized = file_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_owned();
    if normalized.is_empty() {
        "file".to_owned()
    } else {
        normalized
    }
}

pub(super) fn file_extension(path: &str, fallback_file_type: &str) -> String {
    Path::new(path)
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or(fallback_file_type)
        .to_string()
}

pub(super) fn normalize_split_location(
    root_location: &str,
    is_directory: bool,
    path: String,
) -> String {
    if Path::new(&path).is_absolute() || path.contains("://") {
        return path;
    }
    let absolute_candidate = format!("/{path}");
    if Path::new(&absolute_candidate).exists() {
        return absolute_candidate;
    }
    if !is_directory {
        if let Some(parent) = Path::new(root_location).parent() {
            return parent.join(path).to_string_lossy().into_owned();
        }
    }
    Path::new(root_location)
        .join(path)
        .to_string_lossy()
        .into_owned()
}
