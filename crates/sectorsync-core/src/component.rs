//! Custom component registry and station-local blob storage.

use std::collections::HashMap;

use crate::ids::{ComponentId, EntityHandle};
use crate::spatial::Vec3;

/// Component codec error used by built-in codecs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComponentCodecError {
    /// Input length did not match codec expectation.
    ExpectedBytes {
        /// Expected byte count.
        expected: usize,
        /// Actual byte count.
        actual: usize,
    },
}

impl core::fmt::Display for ComponentCodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::ExpectedBytes { expected, actual } => {
                write!(f, "expected {expected} bytes, got {actual}")
            }
        }
    }
}

impl std::error::Error for ComponentCodecError {}

/// Typed component codec. Embedders can implement this for their own compact
/// schema and bit-packing formats.
pub trait ComponentCodec<T> {
    /// Encodes `value` into `out`.
    fn encode(&self, value: &T, out: &mut Vec<u8>) -> Result<(), ComponentCodecError>;

    /// Decodes a value from bytes.
    fn decode(&self, input: &[u8]) -> Result<T, ComponentCodecError>;

    /// Fixed encoded size when known.
    fn fixed_size(&self) -> Option<usize> {
        None
    }
}

/// Little-endian `u32` codec.
#[derive(Clone, Copy, Debug, Default)]
pub struct U32LeCodec;

impl ComponentCodec<u32> for U32LeCodec {
    fn encode(&self, value: &u32, out: &mut Vec<u8>) -> Result<(), ComponentCodecError> {
        out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<u32, ComponentCodecError> {
        let bytes = exact_array::<4>(input)?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn fixed_size(&self) -> Option<usize> {
        Some(4)
    }
}

/// Little-endian `f32` codec.
#[derive(Clone, Copy, Debug, Default)]
pub struct F32LeCodec;

impl ComponentCodec<f32> for F32LeCodec {
    fn encode(&self, value: &f32, out: &mut Vec<u8>) -> Result<(), ComponentCodecError> {
        out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<f32, ComponentCodecError> {
        let bytes = exact_array::<4>(input)?;
        Ok(f32::from_le_bytes(bytes))
    }

    fn fixed_size(&self) -> Option<usize> {
        Some(4)
    }
}

/// Little-endian `Vec3` codec.
#[derive(Clone, Copy, Debug, Default)]
pub struct Vec3LeCodec;

impl ComponentCodec<Vec3> for Vec3LeCodec {
    fn encode(&self, value: &Vec3, out: &mut Vec<u8>) -> Result<(), ComponentCodecError> {
        out.extend_from_slice(&value.x.to_le_bytes());
        out.extend_from_slice(&value.y.to_le_bytes());
        out.extend_from_slice(&value.z.to_le_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<Vec3, ComponentCodecError> {
        if input.len() != 12 {
            return Err(ComponentCodecError::ExpectedBytes {
                expected: 12,
                actual: input.len(),
            });
        }
        let x = f32::from_le_bytes(input[0..4].try_into().expect("slice length checked"));
        let y = f32::from_le_bytes(input[4..8].try_into().expect("slice length checked"));
        let z = f32::from_le_bytes(input[8..12].try_into().expect("slice length checked"));
        Ok(Vec3 { x, y, z })
    }

    fn fixed_size(&self) -> Option<usize> {
        Some(12)
    }
}

fn exact_array<const N: usize>(input: &[u8]) -> Result<[u8; N], ComponentCodecError> {
    if input.len() != N {
        return Err(ComponentCodecError::ExpectedBytes {
            expected: N,
            actual: input.len(),
        });
    }
    let mut out = [0_u8; N];
    out.copy_from_slice(input);
    Ok(out)
}

/// Storage strategy declared by a registered component.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComponentStorageKind {
    /// `SectorSync` stores opaque component bytes in a sparse station-local column.
    SparseBlob,
    /// Component data lives outside `SectorSync`; the registry only documents it.
    External,
}

/// Synchronization behavior declared by a registered component.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComponentSyncMode {
    /// Component is never replicated by `SectorSync`.
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
    /// Maximum accepted blob size in bytes for `SectorSync`-owned storage.
    pub max_bytes: usize,
    /// Stable schema hash selected by the embedding application.
    pub schema_hash: u64,
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
            schema_hash: 0,
        }
    }

    /// Attaches a stable schema hash to this descriptor.
    #[must_use]
    pub const fn with_schema_hash(mut self, schema_hash: u64) -> Self {
        self.schema_hash = schema_hash;
        self
    }
}

/// Typed component schema descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentSchema {
    /// Component descriptor.
    pub descriptor: ComponentDescriptor,
    /// Fixed encoded size when known.
    pub fixed_size: Option<usize>,
}

impl ComponentSchema {
    /// Creates a typed schema from a descriptor and codec.
    pub fn new<T, C: ComponentCodec<T>>(descriptor: ComponentDescriptor, codec: &C) -> Self {
        Self {
            descriptor,
            fixed_size: codec.fixed_size(),
        }
    }
}

/// Fixed field type used by generated component schema helpers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComponentFieldType {
    /// Unsigned 8-bit integer.
    U8,
    /// Unsigned 16-bit little-endian integer.
    U16,
    /// Unsigned 32-bit little-endian integer.
    U32,
    /// Unsigned 64-bit little-endian integer.
    U64,
    /// Signed 32-bit little-endian integer.
    I32,
    /// 32-bit little-endian floating point value.
    F32,
    /// Three little-endian `f32` values.
    Vec3,
    /// Opaque bytes with a maximum generated layout size.
    Bytes {
        /// Maximum byte count reserved in the generated layout.
        max_len: usize,
    },
}

impl ComponentFieldType {
    /// Maximum encoded size in bytes.
    pub const fn max_size(self) -> usize {
        match self {
            Self::U8 => 1,
            Self::U16 => 2,
            Self::U32 | Self::I32 | Self::F32 => 4,
            Self::U64 => 8,
            Self::Vec3 => 12,
            Self::Bytes { max_len } => max_len,
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::U8 => 1,
            Self::U16 => 2,
            Self::U32 => 3,
            Self::U64 => 4,
            Self::I32 => 5,
            Self::F32 => 6,
            Self::Vec3 => 7,
            Self::Bytes { .. } => 8,
        }
    }
}

/// Field descriptor emitted by external schema generators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ComponentFieldDescriptor {
    /// Stable field name.
    pub name: &'static str,
    /// Encoded field type.
    pub ty: ComponentFieldType,
    /// Byte offset inside the generated component blob.
    pub offset: usize,
}

impl ComponentFieldDescriptor {
    /// Creates a generated field descriptor.
    pub const fn new(name: &'static str, ty: ComponentFieldType, offset: usize) -> Self {
        Self { name, ty, offset }
    }

    /// Returns the exclusive end offset.
    pub const fn end_offset(self) -> usize {
        self.offset.saturating_add(self.ty.max_size())
    }
}

/// Component schema shape emitted by an external generator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GeneratedComponentSchema {
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
    /// Maximum accepted blob size in bytes.
    pub max_bytes: usize,
    /// Generated field layout.
    pub fields: &'static [ComponentFieldDescriptor],
}

impl GeneratedComponentSchema {
    /// Creates a generated component schema.
    pub const fn new(
        id: ComponentId,
        name: &'static str,
        storage: ComponentStorageKind,
        sync: ComponentSyncMode,
        migration: ComponentMigrationMode,
        max_bytes: usize,
        fields: &'static [ComponentFieldDescriptor],
    ) -> Self {
        Self {
            id,
            name,
            storage,
            sync,
            migration,
            max_bytes,
            fields,
        }
    }

    /// Validates generated layout invariants.
    pub fn validate(&self) -> Result<(), ComponentSchemaError> {
        for (index, field) in self.fields.iter().enumerate() {
            if field.end_offset() > self.max_bytes {
                return Err(ComponentSchemaError::FieldOutOfBounds {
                    name: field.name,
                    offset: field.offset,
                    size: field.ty.max_size(),
                    max_bytes: self.max_bytes,
                });
            }

            for earlier in &self.fields[..index] {
                if earlier.name == field.name {
                    return Err(ComponentSchemaError::DuplicateFieldName(field.name));
                }
                if ranges_overlap(
                    earlier.offset,
                    earlier.end_offset(),
                    field.offset,
                    field.end_offset(),
                ) {
                    return Err(ComponentSchemaError::FieldOverlap {
                        left: earlier.name,
                        right: field.name,
                    });
                }
            }
        }
        Ok(())
    }

    /// Returns the generated schema hash.
    pub fn schema_hash(&self) -> u64 {
        let mut hash = FNV_OFFSET;
        hash = hash_u64(hash, self.id.get().into());
        hash = hash_str(hash, self.name);
        hash = hash_u8(hash, storage_tag(self.storage));
        hash = hash_u8(hash, sync_tag(self.sync));
        hash = hash_u8(hash, migration_tag(self.migration));
        hash = hash_u64(hash, self.max_bytes as u64);
        for field in self.fields {
            hash = hash_str(hash, field.name);
            hash = hash_u8(hash, field.ty.tag());
            hash = hash_u64(hash, field.ty.max_size() as u64);
            hash = hash_u64(hash, field.offset as u64);
        }
        hash
    }

    /// Returns the maximum generated fixed size.
    pub fn fixed_size(&self) -> Option<usize> {
        self.fields
            .iter()
            .map(|field| field.end_offset())
            .max()
            .or(Some(0))
    }

    /// Builds a component descriptor with the generated schema hash attached.
    pub fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor {
            id: self.id,
            name: self.name,
            storage: self.storage,
            sync: self.sync,
            migration: self.migration,
            max_bytes: self.max_bytes,
            schema_hash: self.schema_hash(),
        }
    }

    /// Builds a typed component schema wrapper.
    pub fn component_schema(&self) -> ComponentSchema {
        ComponentSchema {
            descriptor: self.descriptor(),
            fixed_size: self.fixed_size(),
        }
    }
}

/// Generated component schema validation error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComponentSchemaError {
    /// Field name was repeated.
    DuplicateFieldName(&'static str),
    /// Field range exceeded `max_bytes`.
    FieldOutOfBounds {
        /// Field name.
        name: &'static str,
        /// Field offset.
        offset: usize,
        /// Field size.
        size: usize,
        /// Maximum component bytes.
        max_bytes: usize,
    },
    /// Two field byte ranges overlap.
    FieldOverlap {
        /// Earlier field.
        left: &'static str,
        /// Later field.
        right: &'static str,
    },
}

impl core::fmt::Display for ComponentSchemaError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::DuplicateFieldName(name) => write!(f, "duplicate component field {name}"),
            Self::FieldOutOfBounds {
                name,
                offset,
                size,
                max_bytes,
            } => write!(
                f,
                "component field {name} at {offset} with size {size} exceeds max bytes {max_bytes}"
            ),
            Self::FieldOverlap { left, right } => {
                write!(f, "component fields {left} and {right} overlap")
            }
        }
    }
}

impl std::error::Error for ComponentSchemaError {}

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

/// Error produced while registering a generated schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GeneratedSchemaRegistrationError {
    /// Generated schema validation failed.
    Schema(ComponentSchemaError),
    /// Component registry rejected the generated descriptor.
    Registry(ComponentRegistryError),
}

impl core::fmt::Display for GeneratedSchemaRegistrationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Schema(error) => write!(f, "{error}"),
            Self::Registry(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for GeneratedSchemaRegistrationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Schema(error) => Some(error),
            Self::Registry(error) => Some(error),
        }
    }
}

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

    /// Validates and registers a generated component schema.
    pub fn register_generated_schema(
        &mut self,
        schema: &GeneratedComponentSchema,
    ) -> Result<ComponentSchema, GeneratedSchemaRegistrationError> {
        schema
            .validate()
            .map_err(GeneratedSchemaRegistrationError::Schema)?;
        let component_schema = schema.component_schema();
        self.register(component_schema.descriptor.clone())
            .map_err(GeneratedSchemaRegistrationError::Registry)?;
        Ok(component_schema)
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

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn hash_u8(hash: u64, value: u8) -> u64 {
    (hash ^ u64::from(value)).wrapping_mul(FNV_PRIME)
}

fn hash_u64(mut hash: u64, value: u64) -> u64 {
    for byte in value.to_le_bytes() {
        hash = hash_u8(hash, byte);
    }
    hash
}

fn hash_str(mut hash: u64, value: &str) -> u64 {
    for byte in value.bytes() {
        hash = hash_u8(hash, byte);
    }
    hash_u8(hash, 0)
}

fn storage_tag(storage: ComponentStorageKind) -> u8 {
    match storage {
        ComponentStorageKind::SparseBlob => 1,
        ComponentStorageKind::External => 2,
    }
}

fn sync_tag(sync: ComponentSyncMode) -> u8 {
    match sync {
        ComponentSyncMode::NotReplicated => 0,
        ComponentSyncMode::Delta => 1,
        ComponentSyncMode::Snapshot => 2,
        ComponentSyncMode::EventOnly => 3,
    }
}

fn migration_tag(migration: ComponentMigrationMode) -> u8 {
    match migration {
        ComponentMigrationMode::Copy => 1,
        ComponentMigrationMode::Drop => 2,
        ComponentMigrationMode::External => 3,
    }
}

fn ranges_overlap(
    left_start: usize,
    left_end: usize,
    right_start: usize,
    right_end: usize,
) -> bool {
    left_start < right_end && right_start < left_end
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

/// Caller-owned reusable storage for typed component encoding.
#[derive(Clone, Debug, Default)]
pub struct ComponentEncodeScratch {
    bytes: Vec<u8>,
}

impl ComponentEncodeScratch {
    /// Creates empty encoding scratch.
    pub const fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    /// Creates encoding scratch with an initial byte capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
        }
    }

    /// Retained byte capacity available to subsequent encodes.
    pub fn retained_capacity(&self) -> usize {
        self.bytes.capacity()
    }
}

/// Component storage error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComponentStoreError {
    /// Descriptor does not use `SectorSync`-owned blob storage.
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
    /// Codec failed while encoding or decoding.
    Codec(ComponentCodecError),
    /// Component blob does not exist.
    MissingBlob(ComponentId),
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
            Self::Codec(error) => write!(f, "{error}"),
            Self::MissingBlob(id) => write!(f, "component {} blob is missing", id.get()),
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
    /// Reserves sparse blob entries for one component column.
    pub fn reserve_component(&mut self, component_id: ComponentId, additional_entities: usize) {
        self.column_mut(component_id)
            .values
            .reserve(additional_entities);
    }

    /// Component column slots currently retained without another allocation.
    pub fn column_slots_capacity(&self) -> usize {
        self.columns.capacity()
    }

    /// Sparse entity entries retained for one component without another rehash.
    pub fn component_capacity(&self, component_id: ComponentId) -> usize {
        self.columns
            .get(usize::from(component_id.get()))
            .and_then(Option::as_ref)
            .map_or(0, |column| column.values.capacity())
    }

    /// Writes an opaque component blob.
    pub fn set_blob(
        &mut self,
        descriptor: &ComponentDescriptor,
        entity: EntityHandle,
        version: u64,
        bytes: Vec<u8>,
    ) -> Result<(), ComponentStoreError> {
        validate_blob_write(descriptor, bytes.len())?;

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

    /// Copies an opaque component value into retained blob storage.
    ///
    /// Existing blob byte capacity is reused when sufficient. Validation runs
    /// before mutation, so failed writes leave the previous value unchanged.
    pub fn set_blob_from_slice(
        &mut self,
        descriptor: &ComponentDescriptor,
        entity: EntityHandle,
        version: u64,
        bytes: &[u8],
    ) -> Result<(), ComponentStoreError> {
        validate_blob_write(descriptor, bytes.len())?;
        let column = self.column_mut(descriptor.id);
        if let Some(blob) = column.values.get_mut(&entity) {
            blob.bytes.clear();
            blob.bytes.extend_from_slice(bytes);
            blob.version = version;
            blob.dirty = true;
        } else {
            column.values.insert(
                entity,
                ComponentBlob {
                    version,
                    dirty: true,
                    bytes: bytes.to_vec(),
                },
            );
        }
        Ok(())
    }

    /// Encodes and writes a typed component value using `codec`.
    pub fn set_typed<T, C: ComponentCodec<T>>(
        &mut self,
        descriptor: &ComponentDescriptor,
        entity: EntityHandle,
        version: u64,
        codec: &C,
        value: &T,
    ) -> Result<(), ComponentStoreError> {
        let mut bytes = Vec::with_capacity(codec.fixed_size().unwrap_or(0));
        codec
            .encode(value, &mut bytes)
            .map_err(ComponentStoreError::Codec)?;
        self.set_blob(descriptor, entity, version, bytes)
    }

    /// Encodes a typed value through caller-owned scratch and copies it into
    /// retained blob storage.
    pub fn set_typed_with_scratch<T, C: ComponentCodec<T>>(
        &mut self,
        descriptor: &ComponentDescriptor,
        entity: EntityHandle,
        version: u64,
        codec: &C,
        value: &T,
        scratch: &mut ComponentEncodeScratch,
    ) -> Result<(), ComponentStoreError> {
        scratch.bytes.clear();
        if let Some(size) = codec.fixed_size() {
            scratch.bytes.reserve(size);
        }
        codec
            .encode(value, &mut scratch.bytes)
            .map_err(ComponentStoreError::Codec)?;
        self.set_blob_from_slice(descriptor, entity, version, &scratch.bytes)
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

    /// Decodes a typed component value using `codec`.
    pub fn get_typed<T, C: ComponentCodec<T>>(
        &self,
        component_id: ComponentId,
        entity: EntityHandle,
        codec: &C,
    ) -> Result<T, ComponentStoreError> {
        let blob = self
            .get_blob(component_id, entity)
            .ok_or(ComponentStoreError::MissingBlob(component_id))?;
        codec
            .decode(&blob.bytes)
            .map_err(ComponentStoreError::Codec)
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
            if let Some(blob) = column.values.get_mut(&entity)
                && blob.dirty
            {
                blob.dirty = false;
                cleared += 1;
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
                let component_id = u16::try_from(index)
                    .expect("component columns are indexed by u16 component ids");
                removed.push((ComponentId::new(component_id), blob));
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

fn validate_blob_write(
    descriptor: &ComponentDescriptor,
    bytes: usize,
) -> Result<(), ComponentStoreError> {
    if descriptor.storage != ComponentStorageKind::SparseBlob {
        return Err(ComponentStoreError::NotBlobStorage(descriptor.id));
    }
    if bytes > descriptor.max_bytes {
        return Err(ComponentStoreError::BlobTooLarge {
            component_id: descriptor.id,
            actual: bytes,
            max: descriptor.max_bytes,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_column_capacity_is_explicit_and_observable() {
        let component_id = ComponentId::new(3);
        let mut store = ComponentStore::default();
        store.reserve_component(component_id, 16);

        assert!(store.column_slots_capacity() >= 4);
        assert!(store.component_capacity(component_id) >= 16);

        let descriptor = ComponentDescriptor::sparse_blob(
            component_id,
            "reserved",
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            4,
        );
        let handle = EntityHandle::new(1, 0);
        store
            .set_blob(&descriptor, handle, 1, vec![1, 2, 3, 4])
            .expect("reserved component should write");
        assert_eq!(
            store
                .get_blob(component_id, handle)
                .map(|blob| blob.bytes.as_slice()),
            Some(&[1, 2, 3, 4][..])
        );
    }

    #[test]
    fn slice_updates_reuse_blob_storage_and_reject_oversized_values_atomically() {
        let component_id = ComponentId::new(4);
        let descriptor = ComponentDescriptor::sparse_blob(
            component_id,
            "state",
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            8,
        );
        let handle = EntityHandle::new(1, 0);
        let mut store = ComponentStore::default();
        store
            .set_blob(&descriptor, handle, 1, vec![0; 8])
            .expect("initial blob should fit");
        let retained_bytes = store
            .get_blob(component_id, handle)
            .expect("blob exists")
            .bytes
            .as_ptr();

        store
            .set_blob_from_slice(&descriptor, handle, 2, &[1, 2, 3, 4])
            .expect("slice update should fit");
        let blob = store.get_blob(component_id, handle).expect("blob exists");
        assert_eq!(blob.bytes, [1, 2, 3, 4]);
        assert_eq!(blob.bytes.as_ptr(), retained_bytes);
        assert_eq!(blob.version, 2);
        assert!(blob.dirty);

        assert_eq!(
            store
                .set_blob_from_slice(&descriptor, handle, 3, &[9; 9])
                .expect_err("oversized update should fail"),
            ComponentStoreError::BlobTooLarge {
                component_id,
                actual: 9,
                max: 8,
            }
        );
        let blob = store.get_blob(component_id, handle).expect("blob remains");
        assert_eq!(blob.bytes, [1, 2, 3, 4]);
        assert_eq!(blob.version, 2);
    }

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

    #[test]
    fn typed_component_codec_roundtrips_values() {
        let descriptor = ComponentDescriptor::sparse_blob(
            ComponentId::new(3),
            "velocity",
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            12,
        )
        .with_schema_hash(0xABCD);
        let schema = ComponentSchema::new(descriptor.clone(), &Vec3LeCodec);
        assert_eq!(schema.fixed_size, Some(12));
        assert_eq!(schema.descriptor.schema_hash, 0xABCD);

        let mut store = ComponentStore::default();
        let entity = EntityHandle::new(7, 0);
        let value = Vec3::new(1.0, 2.0, 3.5);

        store
            .set_typed(&descriptor, entity, 1, &Vec3LeCodec, &value)
            .expect("typed set should work");
        let decoded = store
            .get_typed(ComponentId::new(3), entity, &Vec3LeCodec)
            .expect("typed get should work");
        assert_eq!(decoded, value);
    }

    #[test]
    fn typed_component_scratch_reuses_encoding_and_blob_capacity() {
        let component_id = ComponentId::new(5);
        let descriptor = ComponentDescriptor::sparse_blob(
            component_id,
            "score",
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            4,
        );
        let entity = EntityHandle::new(7, 0);
        let mut store = ComponentStore::default();
        let mut scratch = ComponentEncodeScratch::new();

        store
            .set_typed_with_scratch(&descriptor, entity, 1, &U32LeCodec, &10, &mut scratch)
            .expect("initial typed write should work");
        let retained_scratch = scratch.retained_capacity();
        let retained_blob = store
            .get_blob(component_id, entity)
            .expect("blob exists")
            .bytes
            .as_ptr();
        store
            .set_typed_with_scratch(&descriptor, entity, 2, &U32LeCodec, &20, &mut scratch)
            .expect("repeated typed write should work");

        assert_eq!(scratch.retained_capacity(), retained_scratch);
        assert_eq!(
            store
                .get_blob(component_id, entity)
                .expect("blob exists")
                .bytes
                .as_ptr(),
            retained_blob
        );
        assert_eq!(
            store
                .get_typed(component_id, entity, &U32LeCodec)
                .expect("typed value decodes"),
            20
        );
    }

    #[test]
    fn generated_schema_builds_descriptor_and_registers() {
        const FIELDS: &[ComponentFieldDescriptor] = &[
            ComponentFieldDescriptor::new("position", ComponentFieldType::Vec3, 0),
            ComponentFieldDescriptor::new("health", ComponentFieldType::U32, 12),
        ];
        let generated = GeneratedComponentSchema::new(
            ComponentId::new(8),
            "unit_state",
            ComponentStorageKind::SparseBlob,
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            16,
            FIELDS,
        );

        generated.validate().expect("schema should be valid");
        assert_eq!(generated.fixed_size(), Some(16));
        assert_ne!(generated.schema_hash(), 0);

        let descriptor = generated.descriptor();
        assert_eq!(descriptor.id, ComponentId::new(8));
        assert_eq!(descriptor.schema_hash, generated.schema_hash());

        let mut registry = ComponentRegistry::default();
        let schema = registry
            .register_generated_schema(&generated)
            .expect("generated schema should register");
        assert_eq!(schema.fixed_size, Some(16));
        assert_eq!(
            registry
                .get(ComponentId::new(8))
                .expect("registered descriptor")
                .schema_hash,
            generated.schema_hash()
        );
    }

    #[test]
    fn generated_schema_validation_rejects_bad_layouts() {
        const DUP_FIELDS: &[ComponentFieldDescriptor] = &[
            ComponentFieldDescriptor::new("x", ComponentFieldType::U32, 0),
            ComponentFieldDescriptor::new("x", ComponentFieldType::U32, 4),
        ];
        const OVERLAP_FIELDS: &[ComponentFieldDescriptor] = &[
            ComponentFieldDescriptor::new("left", ComponentFieldType::U32, 0),
            ComponentFieldDescriptor::new("right", ComponentFieldType::U32, 2),
        ];
        const OOB_FIELDS: &[ComponentFieldDescriptor] = &[ComponentFieldDescriptor::new(
            "wide",
            ComponentFieldType::U64,
            4,
        )];

        let duplicate = GeneratedComponentSchema::new(
            ComponentId::new(1),
            "duplicate",
            ComponentStorageKind::SparseBlob,
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            8,
            DUP_FIELDS,
        );
        assert_eq!(
            duplicate.validate().expect_err("duplicate should fail"),
            ComponentSchemaError::DuplicateFieldName("x")
        );

        let overlap = GeneratedComponentSchema::new(
            ComponentId::new(2),
            "overlap",
            ComponentStorageKind::SparseBlob,
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            8,
            OVERLAP_FIELDS,
        );
        assert_eq!(
            overlap.validate().expect_err("overlap should fail"),
            ComponentSchemaError::FieldOverlap {
                left: "left",
                right: "right"
            }
        );

        let out_of_bounds = GeneratedComponentSchema::new(
            ComponentId::new(3),
            "oob",
            ComponentStorageKind::SparseBlob,
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            8,
            OOB_FIELDS,
        );
        assert_eq!(
            out_of_bounds
                .validate()
                .expect_err("out of bounds should fail"),
            ComponentSchemaError::FieldOutOfBounds {
                name: "wide",
                offset: 4,
                size: 8,
                max_bytes: 8
            }
        );
    }
}
