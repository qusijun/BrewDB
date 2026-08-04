//! Shared schema types used across catalog, planning, and execution boundaries.

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Double,
    Binary,
    Date,
    Time {
        precision: u32,
    },
    Timestamp {
        precision: u32,
        with_time_zone: bool,
    },
    Decimal {
        precision: u32,
        scale: u32,
    },
    String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl ColumnSchema {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
        }
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn new(columns: Vec<ColumnSchema>) -> Self {
        Self { columns }
    }
}
