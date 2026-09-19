//! Generic physical storage mechanisms shared by Tine persistence domains.
//!
//! The dependency direction is `src-tauri -> tine-core -> tine-storage`.
//! This crate owns physical storage mechanisms; `tine-core` owns policy,
//! authority, validation, and domain interpretation. SQLite is a disposable
//! projection of the Direct Files graph: the Markdown/Org files remain
//! authoritative and can rebuild it. Consequently, this crate never depends on
//! `tine-core`, `lsdoc`, Tauri, or UI crates.
//!
//! Since 0.25.0 the crate is the Direct Files durability and projection crate
//! only. The Managed Storage spine (oplog batches, local journals, sealed
//! accepted-history indexes, the frontier-stamped SQLite database and its
//! file set) was deleted in the compact-projection campaign's P1 packet after
//! Tine removed Managed Storage (Tine ADR 0066); it lives in git history up to
//! v0.24.0 and nothing here reads or writes its formats.
//!
//! SQLite implementation modules remain private. Consumers use [`sqlite`],
//! the deliberately curated physical-storage boundary that does not expose a
//! raw SQLite connection or schema-construction details.
//!
//! Every constant that describes bytes already on disk is exported from
//! [`formats`] and **only** from there, so a release or pin receipt has exactly
//! one thing to quote and a reader cannot reach a format constant by a path the
//! receipt does not cover. On-disk format versions are deliberately independent
//! of this crate's semver; see that module for the rule and the manifest.
//! `formats::tests::no_format_constant_has_a_second_export_path` enforces the
//! single-path rule against this file.

pub mod api_surface;
mod content_digest;
mod filesystem;
pub mod formats;
mod package_store;
mod sqlite_graph_projection;
mod sqlite_materialization;

/// Curated physical SQLite API for the disposable Direct Files projection.
///
/// This facade exposes typed DTOs, errors, bounded reads, the query snapshot,
/// and the connection-owning projection database. It intentionally excludes
/// raw DDL, direct connection access, and lower-level implementation helpers.
/// Persistent-format constants are not here either: they live in [`formats`],
/// which owns every value a reader must agree with a writer about.
pub mod sqlite {
    pub use crate::sqlite_graph_projection::{
        PhysicalGraphProjectionDatabase, PhysicalGraphProjectionSourceDelta,
        PhysicalGraphProjectionSourceRevision, PhysicalProjectionQueryCancellation,
        PhysicalProjectionQueryReader, PhysicalProjectionQuerySnapshot, PhysicalQueryValue,
    };
    pub use crate::sqlite_materialization::{
        query_page_result_estimated_bytes, query_result_estimated_bytes,
        ApplyChangeInstrumentation, MaterializationError, PhysicalAliasDeclaration, PhysicalBlock,
        PhysicalEntityId, PhysicalGraphProjectionChange, PhysicalPage,
        PhysicalPagePortablePathClaim, PhysicalPlanning, PhysicalProperty, PhysicalPropertyAtom,
        PhysicalReference, PhysicalReferencePosting, PhysicalReferenceTarget, PhysicalTag,
        PhysicalTask, SqliteGraphProjectionRead, MAX_MATERIALIZATION_QUERY_BYTES,
        MAX_MATERIALIZATION_QUERY_ROWS, MAX_MATERIALIZATION_READ_BYTES,
    };
}

pub use content_digest::ContentDigest;
pub use filesystem::{sync_dir_required, DurableDirectoryPublication, FilesystemError};
pub use package_store::{
    publish_package_noclobber, recover_package_store, retire_package, PackageFile,
    PackagePublishOutcome, PackageStoreError,
};
