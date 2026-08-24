//! Storage table scan split descriptors.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataFileFormat {
    Parquet,
    Vortex,
    Unknown(String),
}

impl DataFileFormat {
    pub fn from_path(path: &str) -> Self {
        match path.rsplit('.').next() {
            Some("parquet") => Self::Parquet,
            Some("vortex") => Self::Vortex,
            Some(extension) if extension != path => Self::Unknown(extension.to_owned()),
            _ => Self::Unknown(String::new()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataFileDescriptor {
    pub path: String,
    pub file_name: String,
    pub file_format: DataFileFormat,
    pub file_size: Option<u64>,
    pub row_count: Option<u64>,
    pub schema_id: Option<i64>,
    pub serialized_metadata: Option<Vec<u8>>,
    pub min_sequence_number: Option<i64>,
    pub max_sequence_number: Option<i64>,
    pub delete_row_count: Option<i64>,
    pub first_row_id: Option<i64>,
    pub external_path: Option<String>,
    pub write_columns: Option<Vec<String>>,
    pub extra_files: Vec<String>,
}

impl DataFileDescriptor {
    pub fn new(path: impl Into<String>) -> Self {
        let path = path.into();
        let file_name = path
            .rsplit('/')
            .next()
            .filter(|name| !name.is_empty())
            .unwrap_or(&path)
            .to_owned();
        Self {
            file_format: DataFileFormat::from_path(&path),
            file_name,
            path,
            file_size: None,
            row_count: None,
            schema_id: None,
            serialized_metadata: None,
            min_sequence_number: None,
            max_sequence_number: None,
            delete_row_count: None,
            first_row_id: None,
            external_path: None,
            write_columns: None,
            extra_files: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeletionFileDescriptor {
    pub data_file_ordinal: u32,
    pub path: String,
    pub offset: i64,
    pub length: i64,
    pub row_count: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowRangeDescriptor {
    pub from: i64,
    pub to: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketDescriptor {
    pub bucket: i32,
    pub total_buckets: Option<i32>,
    pub path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionDescriptor {
    pub serialized_binary_row: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableScanSplit {
    pub split_id: String,
    pub table_name: String,
    pub ordinal: u32,
    pub locations: Vec<String>,
    pub snapshot_id: Option<i64>,
    pub partition: Option<PartitionDescriptor>,
    pub bucket: Option<BucketDescriptor>,
    pub data_files: Vec<DataFileDescriptor>,
    pub deletion_files: Vec<DeletionFileDescriptor>,
    pub row_ranges: Vec<RowRangeDescriptor>,
    pub properties: BTreeMap<String, String>,
}

impl TableScanSplit {
    pub fn new(table_name: impl Into<String>, ordinal: u32) -> Self {
        let table_name = table_name.into();
        Self {
            split_id: format!("{table_name}#{ordinal}"),
            table_name,
            ordinal,
            locations: Vec::new(),
            snapshot_id: None,
            partition: None,
            bucket: None,
            data_files: Vec::new(),
            deletion_files: Vec::new(),
            row_ranges: Vec::new(),
            properties: BTreeMap::new(),
        }
    }

    pub fn with_locations(mut self, locations: impl IntoIterator<Item = String>) -> Self {
        self.locations = locations.into_iter().collect();
        self
    }

    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    pub fn with_partition(mut self, partition: PartitionDescriptor) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn with_bucket(mut self, bucket: BucketDescriptor) -> Self {
        self.bucket = Some(bucket);
        self
    }

    pub fn with_data_files(mut self, data_files: Vec<DataFileDescriptor>) -> Self {
        self.data_files = data_files;
        self
    }

    pub fn with_deletion_files(mut self, deletion_files: Vec<DeletionFileDescriptor>) -> Self {
        self.deletion_files = deletion_files;
        self
    }

    pub fn with_row_ranges(mut self, row_ranges: Vec<RowRangeDescriptor>) -> Self {
        self.row_ranges = row_ranges;
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableScanSplitGroup {
    pub splits: Vec<TableScanSplit>,
}

impl TableScanSplitGroup {
    pub fn new(splits: Vec<TableScanSplit>) -> Self {
        Self { splits }
    }

    pub fn is_empty(&self) -> bool {
        self.splits.is_empty()
    }

    pub fn len(&self) -> usize {
        self.splits.len()
    }

    pub fn for_table(&self, table_name: &str) -> Self {
        let mut seen_split_ids = BTreeSet::new();
        Self::new(
            self.splits
                .iter()
                .filter(|split| split.table_name == table_name)
                .filter(|split| seen_split_ids.insert(split.split_id.clone()))
                .cloned()
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{TableScanSplit, TableScanSplitGroup};

    #[test]
    fn for_table_deduplicates_repeated_scan_splits() {
        let group = TableScanSplitGroup::new(vec![
            TableScanSplit::new("orders", 0),
            TableScanSplit::new("orders", 0),
            TableScanSplit::new("orders", 1),
            TableScanSplit::new("customers", 0),
        ]);

        let splits = group.for_table("orders");

        assert_eq!(splits.splits.len(), 2);
        assert_eq!(splits.splits[0].split_id, "orders#0");
        assert_eq!(splits.splits[1].split_id, "orders#1");
    }
}
