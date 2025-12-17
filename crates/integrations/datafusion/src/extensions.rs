//! This module contains a variety of extensions, including different extra functions and methods,
//! for types and primitives defined in the rest, "origin" part of the crate.
//! It should help while migrating onto newer versions of the libraries group.
use std::sync;
use std::sync::atomic;

use datafusion::arrow::datatypes;
use datafusion::error;
use iceberg::table;

use crate::physical_plan::commit;

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
        snapshot_id: Option<i64>,
        schema: datatypes::SchemaRef,
        catalog: sync::Arc<dyn iceberg::Catalog>,
    ) -> Self {
        Self {
            table,
            snapshot_id,
            schema,
            catalog: Some(catalog),
            shared_snapshot_id: None,
        }
    }

    /// Returns table to be used in operations.
    /// If there is no catalog provider, returns a clone of the table stored within.
    /// If there is some catalog provider, gets either explicitly specified snapshot id value or set shared value.
    /// If has needed snapshot id and the table stored within has such snapshot, retuns clone of that table.
    /// Otherwise loads table from the catalog and returns it.
    pub(crate) async fn table_to_use(&self) -> error::Result<table::Table> {
        let Some(ref catalog) = self.catalog else {
            return Ok(self.table.clone());
        };

        let has_needed_snapshot = self
            .snapshot_id
            .or_else(|| {
                self.shared_snapshot_id
                    .as_ref()
                    .and_then(|ssid| ssid.get_value(None))
            })
            .and_then(|snapshot_id| self.table.metadata().snapshot_by_id(snapshot_id))
            .is_some();

        if has_needed_snapshot {
            Ok(self.table.clone())
        } else {
            catalog
                .load_table(self.table.identifier())
                .await
                .map_err(|e| error::DataFusionError::Internal(e.to_string()))
        }
    }

    /// Sets moderator for snapshot id and returns it. If a moderator has already been set, it is reset.
    /// When set, moderator is updated with new snapshot id value after each full (run to completion) insert_into() execution.
    pub fn set_snapshot_id_moderator(&mut self) -> SnapshotIdModerator {
        let shared = SharedSnapshotId::new(self.snapshot_id);
        let moderator = SnapshotIdModerator::new(shared.clone());
        self.shared_snapshot_id = Some(shared);
        moderator
    }
}

impl commit::IcebergCommitExec {
    /// Sets shared snapshot id container.
    pub(crate) fn with_shared_snapshot_id(mut self, shared: SharedSnapshotId) -> Self {
        self.shared_snapshot_id = Some(shared);
        self
    }

    /// Updates snapshot id for moderator with a new value.
    pub(crate) fn update_shared_snapshot_id(
        shared: Option<SharedSnapshotId>,
        new_table: &table::Table,
    ) {
        if let (Some(moderator), Some(value)) = (shared, new_table.metadata().current_snapshot_id())
        {
            moderator.set_value(value);
        }
    }
}

/// Type representing shared snapshot id value.
#[derive(Clone, Debug)]
pub(crate) struct SharedSnapshotId(sync::Arc<atomic::AtomicI64>);

impl SharedSnapshotId {
    const UNSET_ID: i64 = i64::MIN; // SnapshotProducer::generate_unique_snapshot_id() doesn't generate negative ids.

    /// Creates new [`SnapshotIdWatcher`] instance.
    pub(crate) fn new(initial: Option<i64>) -> Self {
        Self(sync::Arc::new(atomic::AtomicI64::new(
            initial.unwrap_or(Self::UNSET_ID),
        )))
    }

    /// Returns contained snapshot id value, if it differs from the previous.
    /// When previous is `None`, returns snapshot id value if it was set.
    pub(crate) fn get_value(&self, previous_id_value: Option<i64>) -> Option<i64> {
        let received = self.0.load(atomic::Ordering::Relaxed);
        let previous = previous_id_value.unwrap_or(Self::UNSET_ID);
        (previous != received).then_some(received)
    }

    /// Sets new value for snapshot id; returns `true` if it differs from the old one, `false` otherwise.
    pub(crate) fn set_value(&self, new_id_value: i64) -> bool {
        let old_value = self.0.swap(new_id_value, atomic::Ordering::Relaxed);
        old_value != new_id_value
    }
}

/// Type representing shared snapshot id value with control over its changes.
#[derive(Clone, Debug)]
pub struct SnapshotIdModerator {
    shared: SharedSnapshotId,
    previous: i64,
}

impl SnapshotIdModerator {
    /// Creates new instance of [`SnapshotIdModerator`].
    pub(crate) fn new(shared: SharedSnapshotId) -> Self {
        let previous = shared.0.load(atomic::Ordering::Relaxed);
        Self { shared, previous }
    }

    /// Returns `Some(_)` with new snapshot id, if there is any. Otherwise returns `None`.
    pub fn get_new_value(&self) -> Option<i64> {
        self.shared.get_value(Some(self.previous))
    }

    /// Sets new value for snapshot id; returns `true` if it differs from the old one, `false` otherwise.
    pub fn set_new_value(&self, new_id_value: i64) -> bool {
        self.shared.set_value(new_id_value)
    }

    /// Marks change of snapshot id to given value as acknowledged, so that consecutive calls to `new_snapshot_id` will return `None` until new change.
    pub fn acknowledge(&mut self, value_to_acknowledge: i64) {
        self.previous = value_to_acknowledge;
    }
}
