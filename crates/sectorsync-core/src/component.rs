//! Custom component registry and station-local blob storage.

use std::collections::HashMap;

use crate::ids::{ComponentId, EntityHandle};

/// Storage strategy declared by a registered component.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComponentStorageKind {
    /// SectorSync stores opaque component bytes in a sparse station-local column.
    SparseBlob,
    /// Component data lives outside SectorSync; the registry only documents it.
    External,
}

/// Synchronization behavior declared by a registered component.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComponentSyncMode {
    /// Component is never replicated by SectorSync.
    NotReplicated,
    /// Component is replicated as delta when dirty.
    Delta,
    /// Component is sent as a snapshot when selected by policy.
    Snapshot,
    /// Component changes are represented by events.
    EventOnly,
}

/// Migration behavior declared by a registered component.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComponentMigrationMode {
    /// Copy component bytes during owner handoff.
    Copy,
    /// Drop component bytes during owner handoff.
    Drop,
    /// External system owns migration.
    External,
}

/// Registered component descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentDescriptor {
    /// Component id used in hot-path records.
    pub id: ComponentId,
    /// Stable debug name.
    pub name: &'static str,
    /// Storage strategy.
    pub storage: ComponentStorageKind,
    /// Synchronization strategy.
    pub sync: ComponentSyncMode,
    /// Migration strategy.
    pub migration: ComponentMigrationMode,
    /// Maximum accepted blob size in bytes for SectorSync-owned storage.
    pub max_bytes: usize,
}

impl ComponentDescriptor {
    /// Creates a sparse blob descriptor.
    pub const fn sparse_blob(
        id: ComponentId,
        name: &'static str,
        sync: ComponentSyncMode,
        migration: ComponentMigrationMode,
        max_bytes: usize,
    ) -> Self {
        Self {
            id,
            name,
            storage: ComponentStorageKind::SparseBlob,
            sync,
            migration,
            max_bytes,
        }
    }
}

/// Component registry error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ComponentRegistryError {
    /// Component id is already registered.
    DuplicateId(ComponentId),
    /// Component name is already registered.
    DuplicateName(&'static str),
}

impl core::fmt::Display for ComponentRegistryError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::DuplicateId(id) => write!(f, "duplicate component id {}", id.get()),
            Self::DuplicateName(name) => write!(f, "duplicate component name {name}"),
        }
    }
}

impl std::error::Error for ComponentRegistryError {}

/// Dense component descriptor registry.
#[derive(Clone, Debug, Default)]
pub struct ComponentRegistry {
    descriptors: Vec<Option<ComponentDescriptor>>,
}

impl ComponentRegistry {
    /// Registers a component descriptor.
    pub fn register(
        &mut self,
        descriptor: ComponentDescriptor,
    ) -> Result<(), ComponentRegistryError> {
        if self.get(descriptor.id).is_some() {
            return Err(ComponentRegistryError::DuplicateId(descriptor.id));
        }
        if self.iter().any(|existing| existing.name == descriptor.name) {
            return Err(ComponentRegistryError::DuplicateName(descriptor.name));
        }

        let index = usize::from(descriptor.id.get());
        if self.descriptors.len() <= index {
            self.descriptors.resize(index + 1, None);
        }
        self.descriptors[index] = Some(descriptor);
        Ok(())
    }

    /// Gets a descriptor by component id.
    pub fn get(&self, id: ComponentId) -> Option<&ComponentDescriptor> {
        self.descriptors
            .get(usize::from(id.get()))
            .and_then(Option::as_ref)
    }

    /// Iterates over descriptors.
    pub fn iter(&self) -> impl Iterator<Item = &ComponentDescriptor> {
        self.descriptors.iter().filter_map(Option::as_ref)
    }

    /// Number of registered descriptors.
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Returns whether no descriptors are registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Opaque component blob stored in a station-local component column.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ComponentBlob {
    /// Monotonic version selected by the writer.
    pub version: u64,
    /// Dirty flag used by replication planners.
    pub dirty: bool,
    /// Opaque bytes.
    pub bytes: Vec<u8>,
}

/// Component storage error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComponentStoreError {
    /// Descriptor does not use SectorSync-owned blob storage.
    NotBlobStorage(ComponentId),
    /// Blob exceeds descriptor limit.
    BlobTooLarge {
        /// Component id.
        component_id: ComponentId,
        /// Blob size in bytes.
        actual: usize,
        /// Maximum allowed size in bytes.
        max: usize,
    },
}

impl core::fmt::Display for ComponentStoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NotBlobStorage(id) => {
                write!(f, "component {} is not SectorSync blob storage", id.get())
            }
            Self::BlobTooLarge {
                component_id,
                actual,
                max,
            } => write!(
                f,
                "component {} blob has {} bytes, max {}",
                component_id.get(),
                actual,
                max
            ),
        }
    }
}

impl std::error::Error for ComponentStoreError {}

#[derive(Clone, Debug, Default)]
struct ComponentColumn {
    values: HashMap<EntityHandle, ComponentBlob>,
}

/// Station-local sparse component blob store.
#[derive(Clone, Debug, Default)]
pub struct ComponentStore {
    columns: Vec<Option<ComponentColumn>>,
}

impl ComponentStore {
    /// Writes an opaque component blob.
    pub fn set_blob(
        &mut self,
        descriptor: &ComponentDescriptor,
        entity: EntityHandle,
        version: u64,
        bytes: Vec<u8>,
    ) -> Result<(), ComponentStoreError> {
        if descriptor.storage != ComponentStorageKind::SparseBlob {
            return Err(ComponentStoreError::NotBlobStorage(descriptor.id));
        }
        if bytes.len() > descriptor.max_bytes {
            return Err(ComponentStoreError::BlobTooLarge {
                component_id: descriptor.id,
                actual: bytes.len(),
                max: descriptor.max_bytes,
            });
        }

        let column = self.column_mut(descriptor.id);
        column.values.insert(
            entity,
            ComponentBlob {
                version,
                dirty: true,
                bytes,
            },
        );
        Ok(())
    }

    /// Gets an opaque component blob.
    pub fn get_blob(
        &self,
        component_id: ComponentId,
        entity: EntityHandle,
    ) -> Option<&ComponentBlob> {
        self.columns
            .get(usize::from(component_id.get()))
            .and_then(Option::as_ref)
            .and_then(|column| column.values.get(&entity))
    }

    /// Gets a mutable opaque component blob.
    pub fn get_blob_mut(
        &mut self,
        component_id: ComponentId,
        entity: EntityHandle,
    ) -> Option<&mut ComponentBlob> {
        self.columns
            .get_mut(usize::from(component_id.get()))
            .and_then(Option::as_mut)
            .and_then(|column| column.values.get_mut(&entity))
    }

    /// Clears dirty flags for all components on one entity.
    pub fn clear_dirty_for_entity(&mut self, entity: EntityHandle) -> usize {
        let mut cleared = 0;
        for column in self.columns.iter_mut().filter_map(Option::as_mut) {
            if let Some(blob) = column.values.get_mut(&entity) {
                if blob.dirty {
                    blob.dirty = false;
                    cleared += 1;
                }
            }
        }
        cleared
    }

    /// Removes all component blobs for an entity and returns the removed values.
    pub fn remove_entity(&mut self, entity: EntityHandle) -> Vec<(ComponentId, ComponentBlob)> {
        let mut removed = Vec::new();
        for (index, column) in self.columns.iter_mut().enumerate() {
            let Some(column) = column else {
                continue;
            };
            if let Some(blob) = column.values.remove(&entity) {
                removed.push((ComponentId::new(index as u16), blob));
            }
        }
        removed
    }

    /// Copies migratable component blobs from one entity handle to another.
    pub fn copy_for_migration(
        &mut self,
        registry: &ComponentRegistry,
        source: EntityHandle,
        target: EntityHandle,
    ) -> usize {
        let mut copied = 0;
        for descriptor in registry.iter() {
            if descriptor.migration != ComponentMigrationMode::Copy {
                continue;
            }
            let Some(blob) = self.get_blob(descriptor.id, source).cloned() else {
                continue;
            };
            self.column_mut(descriptor.id).values.insert(target, blob);
            copied += 1;
        }
        copied
    }

    /// Returns number of component blobs stored in all columns.
    pub fn blob_count(&self) -> usize {
        self.columns
            .iter()
            .filter_map(Option::as_ref)
            .map(|column| column.values.len())
            .sum()
    }

    fn column_mut(&mut self, component_id: ComponentId) -> &mut ComponentColumn {
        let index = usize::from(component_id.get());
        if self.columns.len() <= index {
            self.columns.resize_with(index + 1, || None);
        }
        self.columns[index].get_or_insert_with(ComponentColumn::default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_rejects_duplicate_ids_and_names() {
        let mut registry = ComponentRegistry::default();
        let descriptor = ComponentDescriptor::sparse_blob(
            ComponentId::new(1),
            "health",
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            16,
        );

        registry
            .register(descriptor.clone())
            .expect("first registration should work");
        assert_eq!(
            registry
                .register(descriptor.clone())
                .expect_err("duplicate id"),
            ComponentRegistryError::DuplicateId(ComponentId::new(1))
        );
        assert_eq!(
            registry
                .register(ComponentDescriptor::sparse_blob(
                    ComponentId::new(2),
                    "health",
                    ComponentSyncMode::Delta,
                    ComponentMigrationMode::Copy,
                    16,
                ))
                .expect_err("duplicate name"),
            ComponentRegistryError::DuplicateName("health")
        );
    }

    #[test]
    fn component_store_sets_clears_and_migrates_blobs() {
        let descriptor = ComponentDescriptor::sparse_blob(
            ComponentId::new(1),
            "health",
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            16,
        );
        let mut registry = ComponentRegistry::default();
        registry
            .register(descriptor.clone())
            .expect("descriptor should register");
        let mut store = ComponentStore::default();
        let source = EntityHandle::new(1, 0);
        let target = EntityHandle::new(2, 0);

        store
            .set_blob(&descriptor, source, 7, vec![1, 2, 3])
            .expect("blob should fit");
        assert!(
            store
                .get_blob(ComponentId::new(1), source)
                .expect("blob")
                .dirty
        );
        assert_eq!(store.clear_dirty_for_entity(source), 1);
        assert!(
            !store
                .get_blob(ComponentId::new(1), source)
                .expect("blob")
                .dirty
        );

        assert_eq!(store.copy_for_migration(&registry, source, target), 1);
        assert_eq!(
            store
                .get_blob(ComponentId::new(1), target)
                .expect("target blob")
                .bytes,
            vec![1, 2, 3]
        );
    }
}
