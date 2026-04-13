// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::spec::{MAIN_BRANCH, SnapshotReference, SnapshotRetention};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// Transaction action that rolls back the table to a specific snapshot.
#[derive(Default)]
pub struct RollbackToSnapshotAction {
    snapshot_id: Option<i64>,
    rollback_schema: bool,
}

impl RollbackToSnapshotAction {
    /// Creates a new [`RollbackToSnapshotAction`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the target snapshot_id for this action.
    pub fn set_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Updates the table's current schema to match the schema of the target snapshot.
    pub fn with_schema_rollback(mut self) -> Self {
        self.rollback_schema = true;
        self
    }
}

#[async_trait]
impl TransactionAction for RollbackToSnapshotAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let Some(snapshot_id) = self.snapshot_id else {
            return Err(Error::new(ErrorKind::DataInvalid, "snapshot id is not set"));
        };

        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "snapshot with id {} does not exist in the table",
                        snapshot_id
                    ),
                )
            })?;

        let reference =
            SnapshotReference::new(snapshot_id, SnapshotRetention::branch(None, None, None));

        let mut updates = vec![TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference,
        }];

        let mut requirements = vec![
            TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: table.metadata().current_snapshot_id(),
            },
        ];

        let current_schema_id = table.metadata().current_schema_id();
        if self.rollback_schema
            && let Some(snapshot_schema_id) = snapshot.schema_id()
            && current_schema_id != snapshot_schema_id
        {
            updates.push(TableUpdate::SetCurrentSchema {
                schema_id: snapshot_schema_id,
            });
            requirements.push(TableRequirement::CurrentSchemaIdMatch { current_schema_id });
        }

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock};

    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_array::{RecordBatch, record_batch};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use itertools::Itertools;
    use uuid::Uuid;

    use crate::arrow::schema_to_arrow_schema;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, NestedField,
        PrimitiveType, Schema as IcebergSchema, SnapshotReference, SnapshotRetention, Struct, Type,
    };
    use crate::table::Table;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{AddColumn, ApplyTransactionAction, Transaction, TransactionAction};
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
    use crate::{
        Catalog, Error, NamespaceIdent, TableCreation, TableIdent, TableRequirement, TableUpdate,
    };

    static FILE_NAME_GENERATOR: LazyLock<DefaultFileNameGenerator> = LazyLock::new(|| {
        DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet)
    });

    async fn apply_write_and_commit(
        tx: Transaction,
        table: &Table,
        batch: RecordBatch,
    ) -> Result<Transaction, Error> {
        let iceberg_schema = table.metadata().current_schema();
        let arrow_schema = schema_to_arrow_schema(iceberg_schema)?;
        let batch = batch.with_schema(Arc::new(arrow_schema))?;

        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
        let parquet_writer_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator.clone(),
            FILE_NAME_GENERATOR.clone(),
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
        let mut data_file_writer = data_file_writer_builder.build(None).await?;
        data_file_writer.write(batch).await?;
        let data_file = data_file_writer.close().await?;

        let append_action = tx.fast_append().add_data_files(data_file);
        append_action.apply(tx)
    }

    async fn write_and_commit(
        table: &Table,
        catalog: &dyn Catalog,
        batch: RecordBatch,
    ) -> Result<Table, Error> {
        let tx = Transaction::new(table);
        let tx = apply_write_and_commit(tx, table, batch).await?;
        tx.commit(catalog).await
    }

    async fn get_batches(table: &Table) -> Vec<RecordBatch> {
        let batch_stream = table
            .scan()
            .select_all()
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap();
        batch_stream.try_collect().await.unwrap()
    }

    async fn create_test_table(catalog: &dyn Catalog) -> Table {
        let namespace_ident = NamespaceIdent::new(format!("ns-{}", Uuid::new_v4()));
        let table_ident =
            TableIdent::new(namespace_ident.clone(), format!("table-{}", Uuid::new_v4()));

        let schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(0, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let table_creation = TableCreation::builder()
            .name(table_ident.name.clone())
            .schema(schema)
            .build();

        catalog
            .create_namespace(&namespace_ident, HashMap::new())
            .await
            .unwrap();

        catalog
            .create_table(&namespace_ident, table_creation)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_rollback_to_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = create_test_table(&catalog).await;

        let get_id_columns = |batches: &[RecordBatch]| {
            batches
                .iter()
                .flat_map(|b| {
                    b.columns()
                        .iter()
                        .flat_map(|c| c.as_primitive::<Int32Type>().values())
                        .copied()
                })
                .sorted()
                .collect::<Vec<_>>()
        };

        let insert_batch = record_batch!(("id", Int32, [1, 2])).unwrap();
        let table = write_and_commit(&table, &catalog, insert_batch)
            .await
            .unwrap();

        let snapshot_id_1 = table.metadata().current_snapshot_id().unwrap();
        let batch_1 = get_batches(&table).await;
        let ids = get_id_columns(&batch_1);
        assert_eq!(ids, [1, 2]);

        let insert_batch = record_batch!(("id", Int32, [3, 4])).unwrap();
        let table = write_and_commit(&table, &catalog, insert_batch)
            .await
            .unwrap();

        let snapshot_id_2 = table.metadata().current_snapshot_id().unwrap();
        let batch_2 = get_batches(&table).await;
        let ids = get_id_columns(&batch_2);
        assert_eq!(ids, [1, 2, 3, 4]);

        let tx = Transaction::new(&table);
        let action = tx
            .rollback_to_snapshot()
            .set_snapshot_id(snapshot_id_1)
            .apply(tx)
            .unwrap();
        let table = action.commit(&catalog).await.unwrap();
        assert_eq!(table.metadata().current_snapshot_id(), Some(snapshot_id_1));

        let batch_after_rollback = get_batches(&table).await;
        let ids = get_id_columns(&batch_after_rollback);
        assert_eq!(ids, [1, 2]);

        let insert_batch = record_batch!(("id", Int32, [5, 6])).unwrap();
        let table = write_and_commit(&table, &catalog, insert_batch)
            .await
            .unwrap();

        let snapshot_id_3 = table.metadata().current_snapshot_id().unwrap();
        assert_ne!(snapshot_id_3, snapshot_id_2);

        let batch_3 = get_batches(&table).await;
        let ids = get_id_columns(&batch_3);
        assert_eq!(ids, [1, 2, 5, 6]);

        let tx = Transaction::new(&table);
        let action = tx
            .rollback_to_snapshot()
            .set_snapshot_id(snapshot_id_2)
            .apply(tx)
            .unwrap();
        let table = action.commit(&catalog).await.unwrap();
        assert_eq!(table.metadata().current_snapshot_id(), Some(snapshot_id_2));

        let batch_after_rollback = get_batches(&table).await;
        let ids = get_id_columns(&batch_after_rollback);
        assert_eq!(ids, [1, 2, 3, 4]);

        let tx = Transaction::new(&table);
        let action = tx
            .rollback_to_snapshot()
            .set_snapshot_id(snapshot_id_3)
            .apply(tx)
            .unwrap();
        let table = action.commit(&catalog).await.unwrap();
        assert_eq!(table.metadata().current_snapshot_id(), Some(snapshot_id_3));

        let batch_after_rollback = get_batches(&table).await;
        let ids = get_id_columns(&batch_after_rollback);
        assert_eq!(ids, [1, 2, 5, 6]);
    }

    struct SchemaRollbackTestSetup {
        table: Table,
        snapshot_id_before_schema_change: i64,
        initial_schema_id: i32,
    }

    async fn create_table_with_modified_schema(catalog: &dyn Catalog) -> SchemaRollbackTestSetup {
        let table = create_test_table(catalog).await;

        let insert_batch = record_batch!(("id", Int32, [1, 2])).unwrap();
        let table = write_and_commit(&table, catalog, insert_batch)
            .await
            .unwrap();

        let snapshot_id_before_schema_change = table.metadata().current_snapshot_id().unwrap();
        let initial_schema_id = table.metadata().current_schema_id();

        let tx = Transaction::new(&table);
        let add_column = AddColumn::optional("new_column", Type::Primitive(PrimitiveType::Int));
        let action = tx.update_schema().add_column(add_column);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog).await.unwrap();

        let new_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("new_column", DataType::Int32, true),
        ]);
        let insert_batch =
            record_batch!(("id", Int32, [3, 4]), ("new_column", Int32, [100, 200])).unwrap();
        let insert_batch = insert_batch.with_schema(Arc::new(new_schema)).unwrap();
        let table = write_and_commit(&table, catalog, insert_batch)
            .await
            .unwrap();

        assert_eq!(table.metadata().current_schema_id(), initial_schema_id + 1);
        let current_schema = table.metadata().current_schema();
        assert!(current_schema.field_by_name("new_column").is_some());

        SchemaRollbackTestSetup {
            table,
            snapshot_id_before_schema_change,
            initial_schema_id,
        }
    }

    #[tokio::test]
    async fn test_rollback_to_snapshot_with_schema_update() {
        let catalog = new_memory_catalog().await;
        let setup = create_table_with_modified_schema(&catalog).await;

        let tx = Transaction::new(&setup.table);
        let action = tx
            .rollback_to_snapshot()
            .set_snapshot_id(setup.snapshot_id_before_schema_change)
            .apply(tx)
            .unwrap();
        let table = action.commit(&catalog).await.unwrap();

        assert_eq!(
            table.metadata().current_snapshot_id(),
            Some(setup.snapshot_id_before_schema_change)
        );

        let current_schema = table.metadata().current_schema();
        assert_eq!(
            table.metadata().current_schema_id(),
            setup.initial_schema_id + 1,
        );
        assert!(current_schema.field_by_name("new_column").is_some());
    }

    #[tokio::test]
    async fn test_rollback_to_snapshot_with_schema_rollback() {
        let catalog = new_memory_catalog().await;
        let setup = create_table_with_modified_schema(&catalog).await;

        let tx = Transaction::new(&setup.table);
        let action = tx
            .rollback_to_snapshot()
            .set_snapshot_id(setup.snapshot_id_before_schema_change)
            .with_schema_rollback()
            .apply(tx)
            .unwrap();

        let table = action.commit(&catalog).await.unwrap();
        assert_eq!(
            table.metadata().current_snapshot_id(),
            Some(setup.snapshot_id_before_schema_change)
        );
        assert_eq!(
            table.metadata().current_schema_id(),
            setup.initial_schema_id
        );
        let current_schema = table.metadata().current_schema();
        assert!(current_schema.field_by_name("new_column").is_none());
    }

    async fn insert_data(catalog: &dyn Catalog, table_ident: &TableIdent) -> Table {
        let table = catalog.load_table(table_ident).await.unwrap();
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("test/{}.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(vec![data_file])
            .apply(tx)
            .unwrap();

        tx.commit(catalog).await.unwrap()
    }

    #[tokio::test]
    async fn test_rollback_to_snapshot_build() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = insert_data(&catalog, table.identifier()).await;
        let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        let tx = Transaction::new(&table);
        let action = tx.rollback_to_snapshot().set_snapshot_id(snapshot_id);
        assert_eq!(action.snapshot_id, Some(snapshot_id));

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        let reference =
            SnapshotReference::new(snapshot_id, SnapshotRetention::branch(None, None, None));

        assert_eq!(updates, vec![TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference,
        }],);

        assert_eq!(requirements, vec![
            TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: table.metadata().current_snapshot_id(),
            },
        ])
    }
}
