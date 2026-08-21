//! Shared column metadata types.

use arrow::datatypes::Field as ArrowField;

use crate::common::{datatype::DataType, errors::CommonError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnField {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl ColumnField {
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

    pub fn to_arrow_field(&self) -> Result<ArrowField, CommonError> {
        Ok(ArrowField::new(
            &self.name,
            self.data_type.to_arrow_data_type()?,
            self.nullable,
        ))
    }

    pub fn from_arrow_field(field: &ArrowField) -> Result<Self, CommonError> {
        Ok(Self {
            name: field.name().to_owned(),
            data_type: DataType::from_arrow_data_type(field.data_type())?,
            nullable: field.is_nullable(),
        })
    }
}
