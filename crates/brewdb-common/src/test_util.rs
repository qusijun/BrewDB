use std::fs;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use uuid::Uuid;

pub fn temp_root() -> PathBuf {
    PathBuf::from("/tmp")
}

pub struct TestDir {
    path: PathBuf,
}

impl TestDir {
    pub fn new(prefix: &str) -> Self {
        Self::with_prefix(prefix)
    }

    pub fn with_prefix(prefix: &str) -> Self {
        let path = temp_root().join(format!("{prefix}-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).expect("test directory must be created");
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn to_path_buf(&self) -> PathBuf {
        self.path.clone()
    }
}

impl Deref for TestDir {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub struct TestFile {
    path: PathBuf,
}

impl TestFile {
    pub fn new(prefix: &str, extension: &str) -> Self {
        Self {
            path: temp_root().join(format!("{prefix}-{}.{}", Uuid::new_v4(), extension)),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn write_parquet_file(path: &Path, batch: RecordBatch) {
    let file = fs::File::create(path).expect("parquet test file must be created");
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), None).expect("parquet writer must be created");
    writer
        .write(&batch)
        .expect("record batch must be written to parquet test file");
    writer
        .close()
        .expect("parquet test file writer must be closed");
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{TestDir, TestFile, write_parquet_file};

    #[test]
    fn test_dir_removes_directory_when_dropped() {
        let path = {
            let dir = TestDir::new("brewdb-common-test-dir");
            let path = dir.to_path_buf();
            std::fs::write(dir.join("marker"), "ok").unwrap();
            assert!(path.exists());
            path
        };

        assert!(!path.exists(), "{} should be cleaned up", path.display());
    }

    #[test]
    fn test_file_removes_file_when_dropped() {
        let path = {
            let file = TestFile::new("brewdb-common-test-file", "txt");
            let path = file.path().to_path_buf();
            std::fs::write(file.path(), "ok").unwrap();
            assert!(path.exists());
            path
        };

        assert!(!path.exists(), "{} should be cleaned up", path.display());
    }

    #[test]
    fn write_parquet_file_creates_a_file() {
        let file = TestFile::new("brewdb-common-test-parquet-file", "parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();

        write_parquet_file(file.path(), batch);

        assert!(file.path().exists());
        assert!(std::fs::metadata(file.path()).unwrap().len() > 0);
    }
}
