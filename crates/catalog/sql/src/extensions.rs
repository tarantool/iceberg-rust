//! This module contains a variety of extensions, including different extra functions and methods,
//! for types and primitives defined in the rest, "origin" part of the crate.
//! It should help while migrating onto newer versions of the libraries group.

use iceberg::{Catalog, table};

use crate::constants::*;

impl crate::SqlCatalog {
    /// Applies changes provided with given commit to the related table inside this catalog.
    pub(crate) async fn apply_table_update(
        &self,
        commit: iceberg::TableCommit,
    ) -> iceberg::Result<table::Table> {
        let identifier = commit.identifier();

        let current_table = self.load_table(identifier).await?;

        // Apply TableCommit to get staged table.
        let staged_table = commit.apply(current_table)?;

        let metadata_location = staged_table.metadata_location_result()?;

        // Write table metadata to the new location.
        staged_table
            .metadata()
            .write_to(staged_table.file_io(), metadata_location)
            .await?;

        let identifier = staged_table.identifier();

        self.execute(
            &format!(
                "UPDATE {CATALOG_TABLE_NAME}
                 SET {CATALOG_FIELD_METADATA_LOCATION_PROP} = ?
                 WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                  AND {CATALOG_FIELD_TABLE_NAME} = ?
                  AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                  AND (
                    {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}'
                    OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                )"
            ),
            vec![
                Some(metadata_location),
                Some(&self.name),
                Some(identifier.name()),
                Some(&identifier.namespace().join(".")),
            ],
            None,
        )
        .await?;

        Ok(staged_table)
    }
}
