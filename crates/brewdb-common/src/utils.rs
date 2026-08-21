//! Shared utility helpers.

use std::path::Path;

pub fn normalize_file_path(path: &str) -> String {
    let file_name = Path::new(path)
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

#[cfg(test)]
mod tests {
    use super::normalize_file_path;

    #[test]
    fn normalize_file_path_normalizes_file_name() {
        assert_eq!(
            normalize_file_path("/tmp/source-file.csv"),
            "source_file_csv"
        );
        assert_eq!(normalize_file_path("/tmp/___"), "file");
    }
}
