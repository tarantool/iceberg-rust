//! This module contains a variety of extensions, including different extra functions and methods,
//! for types and primitives defined in the rest, "origin" part of the crate.
//! It should help while migrating onto newer versions of the libraries group.
use std::sync;

use datafusion::arrow::datatypes;
use iceberg::table;

impl crate::IcebergTableProvider {
    /// Creates a new instance of [`crate::IcebergTableProvider`] from its parts.
    /// Passed schema should match the one used while creating the table,
    /// and the catalog should be the one that table was created in.
    /// We don't guarantee correct work of the system if those requirements are not met.
    /// 
    /// Current implementation lacks a native way to create a [`crate::IcebergTableProvider`]
    /// bound to the specific [`iceberg::Catalog`] that table is originated from.
    /// However, [`crate::IcebergTableProvider`] has an optional field `catalog` that must be populated
    /// for [`datafusion::catalog::TableProvider`] method `insert_into` to work correctly.
    pub fn new_from_parts(
        table: table::Table,
        schema: datatypes::SchemaRef,
        catalog: sync::Arc<dyn iceberg::Catalog>,
    ) -> Self {
        Self {
            table,
            snapshot_id: None,
            schema,
            catalog: Some(catalog),
        }
    }
}
