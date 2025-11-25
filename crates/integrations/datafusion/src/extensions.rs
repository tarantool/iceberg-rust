//! This module contains a variety of extensions, including different extra functions and methods,
//! for types and primitives defined in the rest, "origin" part if the crate.
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
    /// Current code state doesn't provide any "original" way to create a [`crate::IcebergTableProvider`] 
    /// bound with [`iceberg::Catalog`] implementation the related table was created within.
    /// But [`crate::IcebergTableProvider`] has a dedicated field (`Option<_>`), and it should have 
    /// `Some(_)` value for [`datafusion::catalog::TableProvider`] method `insert_into` to work properly.
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
