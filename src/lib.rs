//! Generic physical storage mechanisms shared by Tine persistence domains.
//!
//! The dependency direction is `src-tauri -> tine-core -> tine-storage`.
//! This crate owns physical storage mechanisms; `tine-core` owns policy,
//! authority, validation, and domain interpretation. SQLite is a disposable
//! projection: the oplog/archive remains authoritative and can rebuild it.
//! Consequently, this crate never depends on `tine-core`, `lsdoc`, Tauri, or
//! UI crates.
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
mod digest_sealed;
mod durable_batch;
mod filesystem;
pub mod formats;
mod local_journal;
mod local_journal_v2;
mod managed_layout;
mod sealed_accepted_index_impl;
mod sqlite_database;
mod sqlite_fileset;
mod sqlite_frontier;
mod sqlite_graph_projection;
mod sqlite_materialization;

/// Curated physical SQLite API for the disposable projection.
///
/// This facade exposes typed DTOs, errors, bounded reads, instrumentation,
/// physical file-set/candidate publication, and the connection-owning database
/// wrapper. It intentionally excludes raw DDL, direct connection access, and
/// lower-level production implementation helpers. Persistent-format constants
/// are not here either: they live in [`formats`], which owns every value a
/// reader must agree with a writer about.
pub mod sqlite {
    pub use crate::sqlite_database::{PhysicalSqliteDatabase, PhysicalWriteInstrumentation};
    pub use crate::sqlite_fileset::{
        PhysicalFileCheckpoint, PhysicalSqliteCheckpoint, SqliteFileSet, SqliteFileSetError,
        SqliteForensicPathMapping,
    };
    pub use crate::sqlite_frontier::{
        ApplyDisposition, ApplyFault, ApplyResult, FrontierError, PhysicalAcceptedBatch,
        PhysicalApplyRequest, PhysicalCheckpointFrontierRoot, PhysicalCheckpointGenerationAnchor,
        PhysicalCheckpointGenerationBinding, PhysicalClaim, PhysicalFrontierDocument,
        PhysicalFrontierRoot, PreflightDisposition, StoredBatch, StoredFrontier,
    };
    pub use crate::sqlite_graph_projection::{
        PhysicalGraphProjectionDatabase, PhysicalGraphProjectionSourceDelta,
        PhysicalGraphProjectionSourceRevision,
    };
    pub use crate::sqlite_materialization::{
        ApplyChangeInstrumentation, MaterializationError, PhysicalAliasDeclaration, PhysicalBlock,
        PhysicalBlockHomeClaim, PhysicalBlockHomeClaimRow, PhysicalBlockPropertyCandidateRow,
        PhysicalBlockReferenceCountRow, PhysicalBlockReferrerCandidateRow, PhysicalBlockRow,
        PhysicalBlockStructureRow, PhysicalEntityId, PhysicalFuzzyCandidatePageRow,
        PhysicalGraphProjectionChange, PhysicalIdentityRecord, PhysicalIdentityRecordRow,
        PhysicalLogseqUuidIntroduction, PhysicalLogseqUuidIntroductionRow,
        PhysicalMaterializationChange, PhysicalNavigationAliasRow, PhysicalNavigationPageRow,
        PhysicalNavigationReferenceNameRow, PhysicalPage, PhysicalPageInventoryRow,
        PhysicalPagePortablePathClaim, PhysicalPageReferrerCandidateRow, PhysicalPageRow,
        PhysicalPlainTextCandidatePageRow, PhysicalProperty, PhysicalPropertyFacetRow,
        PhysicalPropertyRow, PhysicalReference, PhysicalReferencePosting, PhysicalReferenceTarget,
        PhysicalReferrerRow, PhysicalSearchHit, PhysicalSearchIndexBuildStep,
        PhysicalSearchIndexStatus, PhysicalTagRow, PhysicalTask, PhysicalTaskCandidateBlockRow,
        PhysicalTaskCandidateLocatorRow, PhysicalTaskCandidatePageRow, PhysicalTaskRow,
        PhysicalTerminalConstructionBatch, PhysicalTerminalMaterializationChunk,
        PhysicalTerminalProjectionStamp, SqliteGraphProjectionRead, SqliteMaterializedRead,
        MAX_MATERIALIZATION_QUERY_BYTES, MAX_MATERIALIZATION_QUERY_ROWS,
        MAX_MATERIALIZATION_READ_BYTES,
    };

    #[cfg(feature = "test-support")]
    pub use crate::sqlite_fileset::physical_checkpoint_interior_ranges_for_test;

    #[cfg(feature = "test-support")]
    pub use crate::sqlite_materialization::{
        apply_change as apply_materialization_change_for_test,
        initialize_schema as initialize_materialization_schema_for_test,
    };
}

/// Canonical logical and physical formats for checkpoint-generation accepted
/// history indexes.
///
/// The module is intentionally independent of Tine's engine policy and of any
/// particular filesystem layout. Both the engine and SQLite compose the same
/// reader/writer with their own content-addressed object store.
pub mod sealed_accepted_index {
    pub use crate::sealed_accepted_index_impl::{
        accepted_causal_record_digest, authenticated_map_empty_digest,
        authenticated_map_node_digest, authenticated_map_priority,
        authenticated_map_priority_order, authenticated_map_root, causal_clock_counter_digest,
        AcceptedEvidenceBindingV2, AcceptedSequenceChildV2, AcceptedSequenceEntryV2,
        AcceptedSequenceNodeV2, AcceptedSequenceRootV2, AcceptedStatusRecordV2,
        AuthenticatedMapLinkV1, AuthenticatedMapRootV1, CausalTipRecordV2,
        SealedAcceptedCausalClockEntryV2, SealedAcceptedCausalRecordV2,
        SealedAcceptedEvidenceDecoder, SealedAcceptedIndexError, SealedAcceptedIndexObjectStore,
        SealedAcceptedIndexRead, SealedAcceptedIndexReader, SealedAcceptedIndexRootsV2,
        SealedAcceptedIndexWriter, SealedAcceptedMembershipProofV2, SealedAcceptedObjectKind,
        SealedAuthenticatedMapNodeV2, MAX_ACCEPTED_INDEX_DEPTH,
    };
}

pub use content_digest::ContentDigest;
pub use digest_sealed::{DigestSealedError, DigestSealedPayload};
pub use durable_batch::{
    BatchCausalDot, BatchError, CausalPeerId, DurableBatchContract, LineageDigest,
    ObjectDescriptor, ObjectKind, OperationBatch, OperationObject, SemanticEffectDigest,
};
pub use filesystem::{
    ensure_directory_nofollow, nonblocking_lock_is_contended, open_dir_nofollow,
    open_existing_dir_nofollow, open_file_nofollow, publish_immutable_exact,
    publish_immutable_exact_single_writer, read_optional_regular, read_required_regular,
    require_regular_entry, sync_dir_required, CompletedExactImmutablePublicationBatch,
    DurableDirectoryPublication, ExactImmutablePublicationBatch, FilesystemError,
    StagedExactImmutablePublication,
};
pub use local_journal::{
    LocalJournalAppend, LocalJournalError, LocalJournalFrame, LocalJournalPayloadKind,
    LocalJournalRecovery, LocalJournalSegment, LocalJournalStats,
};
pub use local_journal_v2::{
    LocalJournalAppendError, LocalJournalSegmentV2, LocalJournalSegmentV2Selection,
};
