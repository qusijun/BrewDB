//! Storage table scan split descriptors.

use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableScanSplit {
    pub split_id: String,
    pub table_name: String,
    pub ordinal: u32,
    pub locations: Vec<String>,
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
        Self::new(
            self.splits
                .iter()
                .filter(|split| split.table_name == table_name)
                .cloned()
                .collect(),
        )
    }
}
