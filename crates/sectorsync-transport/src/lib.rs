//! Transport traits and fake transport support for SectorSync.

#![forbid(unsafe_code)]

/// Returns the crate name for smoke tests and examples.
pub const fn crate_name() -> &'static str {
    "sectorsync-transport"
}
