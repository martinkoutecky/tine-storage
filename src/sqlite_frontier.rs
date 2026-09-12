//! Physical authenticated accepted-prefix storage for the disposable SQLite projection.
//!
//! This module deliberately knows only bounded canonical key bytes supplied by the domain
//! owner, digests, counters, and canonical payload bytes. It owns the SQLite representation and the
//! single transaction that advances accepted history, authenticated indexes, materialized rows,
//! and the terminal frontier row.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;

use rusqlite::{params, Connection, OptionalExtension as _, Transaction, TransactionBehavior};

use crate::sealed_accepted_index_impl::{
    accepted_causal_record_digest, authenticated_map_empty_digest, authenticated_map_node_digest,
    authenticated_map_priority_order, causal_clock_counter_digest, AuthenticatedMapKey,
    AuthenticatedMapLinkV1, SealedAcceptedCausalClockEntryV2, SealedAcceptedCausalRecordV2,
    SealedAcceptedIndexError, SealedAcceptedIndexRead, MAX_AUTHENTICATED_MAP_KEY_BYTES,
};
use crate::sqlite_materialization::{
    self, ApplyChangeInstrumentation, MaterializationError, PhysicalMaterializationChange,
};
use crate::ContentDigest;

pub const SQLITE_APPLICATION_ID: u32 = 0x5449_4e45;
pub const SQLITE_SCHEMA_VERSION: u32 = 28;
const MAX_AUTHENTICATED_MAP_DEPTH: usize = 256;

pub const META_DDL: &str = "CREATE TABLE meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    workspace_id BLOB NOT NULL CHECK (length(workspace_id) = 16),
    lineage_digest BLOB NOT NULL CHECK (length(lineage_digest) = 32),
    oplog_protocol_version INTEGER NOT NULL,
    operation_schema_version INTEGER NOT NULL,
    object_envelope_schema_version INTEGER NOT NULL,
    manifest_encoding_version INTEGER NOT NULL,
    managed_entity_set_version INTEGER NOT NULL
) STRICT";
pub const FRONTIER_DDL: &str = "CREATE TABLE frontier (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    frontier_root BLOB NOT NULL,
    frontier_root_digest BLOB NOT NULL CHECK (length(frontier_root_digest) = 32),
    applied_batch_count INTEGER NOT NULL CHECK (applied_batch_count >= 0)
) STRICT";
/// The document-map key bound is written literally into the DDL below, which
/// SQLite compares as canonical SQL text. Keep the two in step.
const _: () = assert!(MAX_AUTHENTICATED_MAP_KEY_BYTES == 48);

pub const FRONTIER_DOCUMENTS_DDL: &str = "CREATE TABLE frontier_documents (
    document_id BLOB PRIMARY KEY CHECK (length(document_id) BETWEEN 1 AND 48),
    dependencies BLOB NOT NULL,
    dependencies_digest BLOB NOT NULL CHECK (length(dependencies_digest) = 32),
    left_document_id BLOB,
    left_digest BLOB,
    right_document_id BLOB,
    right_digest BLOB,
    node_digest BLOB NOT NULL CHECK (length(node_digest) = 32),
    CHECK ((left_document_id IS NULL AND left_digest IS NULL)
        OR (length(left_document_id) BETWEEN 1 AND 48 AND length(left_digest) = 32)),
    CHECK ((right_document_id IS NULL AND right_digest IS NULL)
        OR (length(right_document_id) BETWEEN 1 AND 48 AND length(right_digest) = 32))
) STRICT";
pub const CAUSAL_CLOCK_NODES_DDL: &str = "CREATE TABLE causal_clock_nodes (
    node_digest BLOB PRIMARY KEY CHECK (length(node_digest) = 32),
    peer_id BLOB NOT NULL CHECK (length(peer_id) = 16),
    counter INTEGER NOT NULL CHECK (counter > 0),
    value_digest BLOB NOT NULL CHECK (length(value_digest) = 32),
    left_peer_id BLOB,
    left_digest BLOB,
    right_peer_id BLOB,
    right_digest BLOB,
    CHECK ((left_peer_id IS NULL AND left_digest IS NULL)
        OR (length(left_peer_id) = 16 AND length(left_digest) = 32)),
    CHECK ((right_peer_id IS NULL AND right_digest IS NULL)
        OR (length(right_peer_id) = 16 AND length(right_digest) = 32))
) STRICT";
pub const ACCEPTED_BATCH_NODES_DDL: &str = "CREATE TABLE accepted_batch_nodes (
    node_digest BLOB PRIMARY KEY CHECK (length(node_digest) = 32),
    batch_id BLOB NOT NULL CHECK (length(batch_id) = 16),
    value_digest BLOB NOT NULL CHECK (length(value_digest) = 32),
    left_batch_id BLOB,
    left_digest BLOB,
    right_batch_id BLOB,
    right_digest BLOB,
    CHECK ((left_batch_id IS NULL AND left_digest IS NULL)
        OR (length(left_batch_id) = 16 AND length(left_digest) = 32)),
    CHECK ((right_batch_id IS NULL AND right_digest IS NULL)
        OR (length(right_batch_id) = 16 AND length(right_digest) = 32))
) STRICT";
pub const APPLIED_BATCHES_DDL: &str = "CREATE TABLE applied_batches (
    sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
    batch_id BLOB NOT NULL CHECK (length(batch_id) = 16),
    manifest_digest BLOB NOT NULL CHECK (length(manifest_digest) = 32),
    semantic_effect BLOB NOT NULL,
    semantic_effect_digest BLOB NOT NULL CHECK (length(semantic_effect_digest) = 32),
    dependency_frontier BLOB NOT NULL,
    dependency_frontier_digest BLOB NOT NULL
        CHECK (length(dependency_frontier_digest) = 32),
    prior_frontier_root BLOB NOT NULL,
    prior_frontier_root_digest BLOB NOT NULL
        CHECK (length(prior_frontier_root_digest) = 32),
    post_frontier_root BLOB NOT NULL,
    post_frontier_root_digest BLOB NOT NULL
        CHECK (length(post_frontier_root_digest) = 32),
    affected_documents BLOB NOT NULL,
    affected_documents_digest BLOB NOT NULL
        CHECK (length(affected_documents_digest) = 32),
    causal_dependency_heads BLOB NOT NULL,
    causal_peer_id BLOB NOT NULL CHECK (length(causal_peer_id) = 16),
    causal_counter INTEGER NOT NULL CHECK (causal_counter > 0),
    causal_clock_root_key BLOB NOT NULL CHECK (length(causal_clock_root_key) = 16),
    causal_clock_root_digest BLOB NOT NULL CHECK (length(causal_clock_root_digest) = 32),
    acceptance_sequence INTEGER NOT NULL CHECK (acceptance_sequence > 0),
    retained_bytes INTEGER NOT NULL CHECK (retained_bytes >= 0)
) STRICT";
pub const CHECKPOINT_GENERATION_ANCHOR_DDL: &str = "CREATE TABLE checkpoint_generation_anchor (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    generation_id BLOB NOT NULL CHECK (length(generation_id) = 16),
    predecessor_generation_id BLOB,
    full_anchor_generation_id BLOB NOT NULL CHECK (length(full_anchor_generation_id) = 16),
    covered_count INTEGER NOT NULL CHECK (covered_count >= 0),
    covered_document_count INTEGER NOT NULL CHECK (covered_document_count >= 0),
    covered_block_count INTEGER NOT NULL CHECK (covered_block_count >= 0),
    covered_retained_bytes_total INTEGER NOT NULL CHECK (covered_retained_bytes_total >= 0),
    covered_semantic_capsules_root_digest BLOB NOT NULL
        CHECK (length(covered_semantic_capsules_root_digest) = 32),
    covered_batch_root_key BLOB,
    covered_batch_root_digest BLOB NOT NULL CHECK (length(covered_batch_root_digest) = 32),
    covered_status_root_key BLOB,
    covered_status_root_digest BLOB NOT NULL CHECK (length(covered_status_root_digest) = 32),
    covered_sequence_root_digest BLOB,
    covered_sequence_height INTEGER NOT NULL
        CHECK (covered_sequence_height >= 0 AND covered_sequence_height <= 255),
    covered_causal_tip_root_key BLOB,
    covered_causal_tip_root_digest BLOB NOT NULL
        CHECK (length(covered_causal_tip_root_digest) = 32),
    covered_head_facts_root_digest BLOB NOT NULL
        CHECK (length(covered_head_facts_root_digest) = 32),
    current_projection_payload_pins_root_digest BLOB NOT NULL
        CHECK (length(current_projection_payload_pins_root_digest) = 32),
    nonlinear_state_root_digest BLOB NOT NULL CHECK (length(nonlinear_state_root_digest) = 32),
    retention_pins_root_digest BLOB NOT NULL CHECK (length(retention_pins_root_digest) = 32),
    checkpoint_frontier_root BLOB NOT NULL,
    checkpoint_frontier_root_digest BLOB NOT NULL
        CHECK (length(checkpoint_frontier_root_digest) = 32),
    terminal_batch_id BLOB,
    terminal_evidence_digest BLOB,
    materialization_frontier_root_digest BLOB NOT NULL
        CHECK (length(materialization_frontier_root_digest) = 32),
    CHECK (predecessor_generation_id IS NULL OR length(predecessor_generation_id) = 16),
    CHECK ((covered_batch_root_key IS NULL AND covered_count = 0)
        OR length(covered_batch_root_key) = 16),
    CHECK ((covered_status_root_key IS NULL AND covered_count = 0)
        OR length(covered_status_root_key) = 16),
    CHECK ((covered_sequence_root_digest IS NULL AND covered_count = 0)
        OR length(covered_sequence_root_digest) = 32),
    CHECK ((covered_causal_tip_root_key IS NULL AND covered_count = 0)
        OR length(covered_causal_tip_root_key) = 16),
    CHECK ((terminal_batch_id IS NULL AND terminal_evidence_digest IS NULL AND covered_count = 0)
        OR (length(terminal_batch_id) = 16 AND length(terminal_evidence_digest) = 32))
) STRICT";
pub const BATCH_ID_INDEX_DDL: &str =
    "CREATE UNIQUE INDEX applied_batches_batch_id_uq ON applied_batches(batch_id)";
pub const ACCEPTANCE_SEQUENCE_INDEX_DDL: &str = "CREATE UNIQUE INDEX \
    applied_batches_acceptance_sequence_uq ON applied_batches(acceptance_sequence)";

const EXPECTED_TABLES: [&str; 48] = [
    "accepted_batch_nodes",
    "applied_batches",
    "block_home_claims",
    "block_path_refs",
    "block_own_refs",
    "query_block_results",
    "query_page_order",
    "query_page_results",
    "query_projection_state",
    "block_planning",
    "blocks",
    "block_text",
    "causal_clock_nodes",
    "checkpoint_generation_anchor",
    "frontier",
    "frontier_documents",
    "logseq_uuid_introductions",
    "materialization_batches",
    "materialization_stamp",
    "meta",
    "page_name_identity_records",
    "page_portable_path_claims",
    "pages",
    "page_text",
    "portable_path_identity_records",
    "properties",
    "property_atoms",
    "reference_alias_bindings",
    "reference_alias_declarations",
    "reference_postings",
    "refs",
    "search_fts",
    "search_fts_build",
    "search_fts_config",
    "search_fts_content",
    "search_fts_data",
    "search_fts_docsize",
    "search_fts_idx",
    "search_fts_outbox",
    "search_fts_owners",
    "search_substring_fts",
    "search_substring_fts_config",
    "search_substring_fts_content",
    "search_substring_fts_data",
    "search_substring_fts_docsize",
    "search_substring_fts_idx",
    "tags",
    "tasks",
];
const EXPECTED_INDEXES: [&str; 37] = [
    "applied_batches_acceptance_sequence_uq",
    "applied_batches_batch_id_uq",
    "block_path_refs_lookup_idx",
    "block_path_refs_page_idx",
    "block_planning_deadline_day_idx",
    "block_planning_deadline_idx",
    "block_planning_priority_idx",
    "block_planning_scheduled_day_idx",
    "block_planning_scheduled_idx",
    "blocks_logseq_uuid_idx",
    "blocks_page_order_idx",
    "blocks_parent_page_idx",
    "page_portable_path_claims_key_idx",
    "pages_home_document_id_idx",
    "pages_name_idx",
    "pages_journal_day_idx",
    "pages_name_key_idx",
    "pages_path_idx",
    "properties_lookup_idx",
    "properties_page_idx",
    "property_atoms_day_idx",
    "property_atoms_key_idx",
    "property_atoms_num_idx",
    "property_atoms_page_idx",
    "reference_alias_bindings_normalized_alias_idx",
    "reference_alias_declarations_source_idx",
    "reference_postings_normalized_name_idx",
    "reference_postings_raw_uuid_idx",
    "reference_postings_source_idx",
    "references_source_idx",
    "references_target_idx",
    "search_fts_owners_page_idx",
    "tags_lookup_idx",
    "tags_page_idx",
    "tasks_deadline_idx",
    "tasks_marker_idx",
    "tasks_page_idx",
];

const META_COLUMNS: &[&str] = &[
    "singleton",
    "workspace_id",
    "lineage_digest",
    "oplog_protocol_version",
    "operation_schema_version",
    "object_envelope_schema_version",
    "manifest_encoding_version",
    "managed_entity_set_version",
];
const FRONTIER_COLUMNS: &[&str] = &[
    "singleton",
    "frontier_root",
    "frontier_root_digest",
    "applied_batch_count",
];
const FRONTIER_DOCUMENT_COLUMNS: &[&str] = &[
    "document_id",
    "dependencies",
    "dependencies_digest",
    "left_document_id",
    "left_digest",
    "right_document_id",
    "right_digest",
    "node_digest",
];
const CAUSAL_CLOCK_NODE_COLUMNS: &[&str] = &[
    "node_digest",
    "peer_id",
    "counter",
    "value_digest",
    "left_peer_id",
    "left_digest",
    "right_peer_id",
    "right_digest",
];
const ACCEPTED_BATCH_NODE_COLUMNS: &[&str] = &[
    "node_digest",
    "batch_id",
    "value_digest",
    "left_batch_id",
    "left_digest",
    "right_batch_id",
    "right_digest",
];
const APPLIED_BATCH_COLUMNS: &[&str] = &[
    "sequence",
    "batch_id",
    "manifest_digest",
    "semantic_effect",
    "semantic_effect_digest",
    "dependency_frontier",
    "dependency_frontier_digest",
    "prior_frontier_root",
    "prior_frontier_root_digest",
    "post_frontier_root",
    "post_frontier_root_digest",
    "affected_documents",
    "affected_documents_digest",
    "causal_dependency_heads",
    "causal_peer_id",
    "causal_counter",
    "causal_clock_root_key",
    "causal_clock_root_digest",
    "acceptance_sequence",
    "retained_bytes",
];
const CHECKPOINT_GENERATION_ANCHOR_COLUMNS: &[&str] = &[
    "singleton",
    "generation_id",
    "predecessor_generation_id",
    "full_anchor_generation_id",
    "covered_count",
    "covered_document_count",
    "covered_block_count",
    "covered_retained_bytes_total",
    "covered_semantic_capsules_root_digest",
    "covered_batch_root_key",
    "covered_batch_root_digest",
    "covered_status_root_key",
    "covered_status_root_digest",
    "covered_sequence_root_digest",
    "covered_sequence_height",
    "covered_causal_tip_root_key",
    "covered_causal_tip_root_digest",
    "covered_head_facts_root_digest",
    "current_projection_payload_pins_root_digest",
    "nonlinear_state_root_digest",
    "retention_pins_root_digest",
    "checkpoint_frontier_root",
    "checkpoint_frontier_root_digest",
    "terminal_batch_id",
    "terminal_evidence_digest",
    "materialization_frontier_root_digest",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhysicalClaim {
    pub workspace_id: [u8; 16],
    pub lineage_digest: ContentDigest,
    pub oplog_protocol_version: u32,
    pub operation_schema_version: u32,
    pub object_envelope_schema_version: u32,
    pub manifest_encoding_version: u32,
    pub managed_entity_set_version: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalFrontierRoot {
    pub canonical_bytes: Vec<u8>,
    pub acceptance_sequence: u64,
    pub document_count: u64,
    /// Full document-map key bytes. Document identity is domain-owned and
    /// variable-width; batch identity below stays a fixed 16-byte UUID.
    pub document_map_root_key: Option<AuthenticatedMapKey>,
    pub document_map_root_digest: ContentDigest,
    pub batch_map_root_key: Option<[u8; 16]>,
    pub batch_map_root_digest: ContentDigest,
    pub state_digest: ContentDigest,
}

impl PhysicalFrontierRoot {
    pub fn digest(&self) -> ContentDigest {
        ContentDigest::of(&self.canonical_bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalCheckpointGenerationBinding {
    pub generation_id: [u8; 16],
    pub predecessor_generation_id: Option<[u8; 16]>,
    pub full_anchor_generation_id: [u8; 16],
    pub covered_count: u64,
    pub covered_document_count: u64,
    pub covered_block_count: u64,
    pub covered_retained_bytes_total: u64,
    pub covered_semantic_capsules_root_digest: ContentDigest,
    pub covered_batch_root_key: Option<[u8; 16]>,
    pub covered_batch_root_digest: ContentDigest,
    pub covered_status_root_key: Option<[u8; 16]>,
    pub covered_status_root_digest: ContentDigest,
    pub covered_sequence_root_digest: Option<ContentDigest>,
    pub covered_sequence_height: u8,
    pub covered_causal_tip_root_key: Option<[u8; 16]>,
    pub covered_causal_tip_root_digest: ContentDigest,
    pub covered_head_facts_root_digest: ContentDigest,
    pub current_projection_payload_pins_root_digest: ContentDigest,
    pub nonlinear_state_root_digest: ContentDigest,
    pub retention_pins_root_digest: ContentDigest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalCheckpointGenerationAnchor {
    pub generation: PhysicalCheckpointGenerationBinding,
    pub checkpoint_frontier_root: Vec<u8>,
    pub terminal_batch_id: Option<[u8; 16]>,
    pub terminal_evidence_digest: Option<ContentDigest>,
    pub materialization_frontier_root_digest: ContentDigest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalCheckpointFrontierRoot {
    pub canonical_bytes: Vec<u8>,
    pub acceptance_sequence: u64,
    pub document_count: u64,
    pub document_overlay_count: u64,
    pub retained_bytes_total: u64,
    pub document_map_root_key: Option<AuthenticatedMapKey>,
    pub document_map_root_digest: ContentDigest,
    pub batch_map_root_key: Option<[u8; 16]>,
    pub batch_map_root_digest: ContentDigest,
    pub batch_map_count: u64,
    pub status_map_root_key: Option<[u8; 16]>,
    pub status_map_root_digest: ContentDigest,
    pub status_map_count: u64,
    pub sequence_root_digest: Option<ContentDigest>,
    pub sequence_height: u8,
    pub sequence_count: u64,
    pub generation: PhysicalCheckpointGenerationBinding,
    pub state_digest: ContentDigest,
}

impl PhysicalCheckpointFrontierRoot {
    pub fn digest(&self) -> ContentDigest {
        ContentDigest::of(&self.canonical_bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalFrontierDocument {
    /// The domain owner's complete, lossless document key bytes.
    pub document_key: AuthenticatedMapKey,
    pub canonical_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalAcceptedBatch {
    pub batch_id: [u8; 16],
    pub manifest_digest: ContentDigest,
    pub event_binding_digest: ContentDigest,
    pub semantic_effect: Vec<u8>,
    pub semantic_effect_digest: ContentDigest,
    pub dependency_frontier: Vec<u8>,
    pub prior_frontier_root: PhysicalFrontierRoot,
    pub post_frontier_root: PhysicalFrontierRoot,
    pub affected_documents: Vec<PhysicalFrontierDocument>,
    pub affected_documents_bytes: Vec<u8>,
    pub causal_dependency_heads: Vec<[u8; 16]>,
    pub causal_dependency_heads_bytes: Vec<u8>,
    pub causal_peer_id: [u8; 16],
    pub causal_counter: u64,
    pub acceptance_sequence: u64,
    pub retained_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ApplyFault {
    #[default]
    None,
    ReturnAfterInsert,
    ReturnAfterMaterialization,
    AbortAfterInsert,
}

#[derive(Clone, Debug)]
pub struct PhysicalApplyRequest {
    pub batch: PhysicalAcceptedBatch,
    pub materialization: Option<PhysicalMaterializationChange>,
    pub materialization_input_digest: Option<ContentDigest>,
    pub fault: ApplyFault,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyDisposition {
    Applied,
    Duplicate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyResult {
    pub disposition: ApplyDisposition,
    pub materialization: ApplyChangeInstrumentation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreflightDisposition {
    New,
    Duplicate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredFrontier {
    pub canonical_bytes: Vec<u8>,
    pub digest: ContentDigest,
    pub applied_batch_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredBatch {
    pub sequence: i64,
    pub batch_id: [u8; 16],
    pub manifest_digest: Vec<u8>,
    pub semantic_effect: Vec<u8>,
    pub semantic_effect_digest: Vec<u8>,
    pub dependency_frontier: Vec<u8>,
    pub dependency_frontier_digest: Vec<u8>,
    pub prior_frontier_root: Vec<u8>,
    pub prior_frontier_root_digest: Vec<u8>,
    pub post_frontier_root: Vec<u8>,
    pub post_frontier_root_digest: Vec<u8>,
    pub affected_documents: Vec<u8>,
    pub affected_documents_digest: Vec<u8>,
    pub causal_dependency_heads: Vec<u8>,
    pub causal_peer_id: Vec<u8>,
    pub causal_counter: i64,
    pub causal_clock_root_key: Vec<u8>,
    pub causal_clock_root_digest: Vec<u8>,
    pub acceptance_sequence: i64,
    pub retained_bytes: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FrontierError {
    Sqlite(String),
    Materialization(MaterializationError),
    SealedAcceptedIndex(SealedAcceptedIndexError),
    Schema(String),
    ClaimBytes {
        field: &'static str,
        expected: Vec<u8>,
        found: Vec<u8>,
    },
    ClaimVersion {
        field: &'static str,
        expected: i64,
        found: i64,
    },
    Corrupt(String),
    InvalidInput(String),
    MissingDependency([u8; 16]),
    AcceptanceOrder {
        expected: u64,
        found: u64,
    },
    FrontierRegression,
    BatchCollision([u8; 16]),
    MaterializationCollision([u8; 16]),
    /// A batch whose id the anchored frontier already contains inside its
    /// SEALED covered history was offered again.
    ///
    /// This is deliberately its own refusal rather than a `Duplicate`
    /// disposition. Deciding it were a duplicate means answering "is the
    /// re-delivery equal to what we accepted?", and on covered history the
    /// exact `applied_batches` row that `stored_matches_request` compares --
    /// `semantic_effect` included -- is gone; the sealed causal record carries
    /// the batch id, manifest fingerprint, event-binding digest, causal dot and
    /// canonical clock, and nothing else. A commitment-based equivalence over
    /// the binding digest may well be the right contract, but that is a trust
    /// boundary change, not an implementation detail, so storage refuses and
    /// names the case instead of guessing an equivalence forward.
    ///
    /// Safety does not depend on that decision: a covered id can never be
    /// re-inserted, because its acceptance sequence is at or below the covered
    /// floor and apply admits only `current + 1`.
    CoveredBatchRedelivery([u8; 16]),
    InjectedFailure,
}

impl fmt::Display for FrontierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(formatter, "SQLite frontier error: {error}"),
            Self::Materialization(error) => error.fmt(formatter),
            Self::SealedAcceptedIndex(error) => error.fmt(formatter),
            Self::Schema(error) => write!(formatter, "SQLite frontier schema mismatch: {error}"),
            Self::ClaimBytes { field, .. } => write!(formatter, "SQLite claim {field} mismatch"),
            Self::ClaimVersion {
                field,
                expected,
                found,
            } => {
                write!(
                    formatter,
                    "SQLite claim {field} mismatch: expected {expected}, found {found}"
                )
            }
            Self::Corrupt(error) => write!(formatter, "corrupt SQLite frontier: {error}"),
            Self::InvalidInput(error) => {
                write!(formatter, "invalid physical frontier input: {error}")
            }
            Self::MissingDependency(id) => write!(formatter, "missing dependency {}", HexId(id)),
            Self::AcceptanceOrder { expected, found } => {
                write!(
                    formatter,
                    "acceptance sequence {found} cannot apply before {expected}"
                )
            }
            Self::FrontierRegression => formatter.write_str("accepted frontier regression"),
            Self::BatchCollision(id) => write!(formatter, "batch {} collides", HexId(id)),
            Self::CoveredBatchRedelivery(id) => write!(
                formatter,
                "batch {} was already accepted into sealed covered history",
                HexId(id)
            ),
            Self::MaterializationCollision(id) => {
                write!(
                    formatter,
                    "materialization for batch {} collides",
                    HexId(id)
                )
            }
            Self::InjectedFailure => formatter.write_str("injected SQLite transaction failure"),
        }
    }
}

impl std::error::Error for FrontierError {}

impl From<rusqlite::Error> for FrontierError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value.to_string())
    }
}

impl From<MaterializationError> for FrontierError {
    fn from(value: MaterializationError) -> Self {
        Self::Materialization(value)
    }
}

impl From<SealedAcceptedIndexError> for FrontierError {
    fn from(value: SealedAcceptedIndexError) -> Self {
        Self::SealedAcceptedIndex(value)
    }
}

struct HexId<'a>(&'a [u8; 16]);
impl fmt::Display for HexId<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

pub fn initialize_schema(
    connection: &Connection,
    claim: PhysicalClaim,
    empty_frontier: &[u8],
    parse_config_hash: ContentDigest,
) -> Result<(), FrontierError> {
    // Schema state is disposable, but it must still be all-or-nothing. Apart
    // from avoiding one FULL/NORMAL commit per DDL statement, this means a
    // failed candidate never leaves a partially shaped database that looks
    // meaningful to later diagnostics or recovery.
    let transaction = connection.unchecked_transaction()?;
    transaction.execute_batch(&format!(
        "PRAGMA application_id = {SQLITE_APPLICATION_ID};
         PRAGMA user_version = {SQLITE_SCHEMA_VERSION};
         {META_DDL}; {FRONTIER_DDL}; {FRONTIER_DOCUMENTS_DDL};
         {CAUSAL_CLOCK_NODES_DDL}; {ACCEPTED_BATCH_NODES_DDL};
         {APPLIED_BATCHES_DDL}; {CHECKPOINT_GENERATION_ANCHOR_DDL};
         {BATCH_ID_INDEX_DDL}; {ACCEPTANCE_SEQUENCE_INDEX_DDL};"
    ))?;
    let empty_digest = ContentDigest::of(empty_frontier);
    sqlite_materialization::initialize_schema(&transaction, empty_digest, parse_config_hash)?;
    transaction.execute(
        "INSERT INTO meta (
             singleton, workspace_id, lineage_digest, oplog_protocol_version,
             operation_schema_version, object_envelope_schema_version,
             manifest_encoding_version, managed_entity_set_version
         ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            claim.workspace_id.as_slice(),
            claim.lineage_digest.as_bytes().as_slice(),
            i64::from(claim.oplog_protocol_version),
            i64::from(claim.operation_schema_version),
            i64::from(claim.object_envelope_schema_version),
            i64::from(claim.manifest_encoding_version),
            i64::from(claim.managed_entity_set_version),
        ],
    )?;
    transaction.execute(
        "INSERT INTO frontier (
             singleton, frontier_root, frontier_root_digest, applied_batch_count
         ) VALUES (1, ?1, ?2, 0)",
        params![empty_frontier, empty_digest.as_bytes().as_slice()],
    )?;
    validate_schema_and_claim(&transaction, claim)?;
    transaction.commit()?;
    Ok(())
}

pub fn initialize_checkpoint_candidate_schema(
    connection: &Connection,
    claim: PhysicalClaim,
    root: &PhysicalCheckpointFrontierRoot,
    anchor: &PhysicalCheckpointGenerationAnchor,
    parse_config_hash: ContentDigest,
) -> Result<(), FrontierError> {
    validate_checkpoint_anchor_input(root, anchor)?;
    let transaction = connection.unchecked_transaction()?;
    transaction.execute_batch(&format!(
        "PRAGMA application_id = {SQLITE_APPLICATION_ID};
         PRAGMA user_version = {SQLITE_SCHEMA_VERSION};
         {META_DDL}; {FRONTIER_DDL}; {FRONTIER_DOCUMENTS_DDL};
         {CAUSAL_CLOCK_NODES_DDL}; {ACCEPTED_BATCH_NODES_DDL};
         {APPLIED_BATCHES_DDL}; {CHECKPOINT_GENERATION_ANCHOR_DDL};
         {BATCH_ID_INDEX_DDL}; {ACCEPTANCE_SEQUENCE_INDEX_DDL};"
    ))?;
    sqlite_materialization::initialize_schema(&transaction, root.digest(), parse_config_hash)?;
    transaction.execute(
        "UPDATE materialization_stamp
         SET acceptance_sequence = ?1, frontier_root_digest = ?2
         WHERE singleton = 1",
        params![
            sqlite_i64(root.acceptance_sequence, "acceptance sequence")?,
            anchor
                .materialization_frontier_root_digest
                .as_bytes()
                .as_slice(),
        ],
    )?;
    transaction.execute(
        "INSERT INTO meta (
             singleton, workspace_id, lineage_digest, oplog_protocol_version,
             operation_schema_version, object_envelope_schema_version,
             manifest_encoding_version, managed_entity_set_version
         ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            claim.workspace_id.as_slice(),
            claim.lineage_digest.as_bytes().as_slice(),
            i64::from(claim.oplog_protocol_version),
            i64::from(claim.operation_schema_version),
            i64::from(claim.object_envelope_schema_version),
            i64::from(claim.manifest_encoding_version),
            i64::from(claim.managed_entity_set_version),
        ],
    )?;
    transaction.execute(
        "INSERT INTO frontier (
             singleton, frontier_root, frontier_root_digest, applied_batch_count
         ) VALUES (1, ?1, ?2, ?3)",
        params![
            root.canonical_bytes.as_slice(),
            root.digest().as_bytes().as_slice(),
            sqlite_i64(root.acceptance_sequence, "acceptance sequence")?,
        ],
    )?;
    let generation = &anchor.generation;
    transaction.execute(
        "INSERT INTO checkpoint_generation_anchor (
             singleton, generation_id, predecessor_generation_id,
             full_anchor_generation_id, covered_count, covered_document_count,
             covered_block_count, covered_retained_bytes_total,
             covered_semantic_capsules_root_digest, covered_batch_root_key,
             covered_batch_root_digest, covered_status_root_key,
             covered_status_root_digest, covered_sequence_root_digest,
             covered_sequence_height, covered_causal_tip_root_key,
             covered_causal_tip_root_digest, covered_head_facts_root_digest,
             current_projection_payload_pins_root_digest,
             nonlinear_state_root_digest, retention_pins_root_digest,
             checkpoint_frontier_root, checkpoint_frontier_root_digest,
             terminal_batch_id, terminal_evidence_digest,
             materialization_frontier_root_digest
         ) VALUES (
             1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
             ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25
         )",
        params![
            generation.generation_id.as_slice(),
            generation
                .predecessor_generation_id
                .as_ref()
                .map(|value| value.as_slice()),
            generation.full_anchor_generation_id.as_slice(),
            sqlite_i64(generation.covered_count, "covered count")?,
            sqlite_i64(generation.covered_document_count, "covered document count")?,
            sqlite_i64(generation.covered_block_count, "covered block count")?,
            sqlite_i64(
                generation.covered_retained_bytes_total,
                "covered retained bytes"
            )?,
            generation
                .covered_semantic_capsules_root_digest
                .as_bytes()
                .as_slice(),
            generation
                .covered_batch_root_key
                .as_ref()
                .map(|value| value.as_slice()),
            generation.covered_batch_root_digest.as_bytes().as_slice(),
            generation
                .covered_status_root_key
                .as_ref()
                .map(|value| value.as_slice()),
            generation.covered_status_root_digest.as_bytes().as_slice(),
            generation
                .covered_sequence_root_digest
                .as_ref()
                .map(|value| value.as_bytes().as_slice()),
            i64::from(generation.covered_sequence_height),
            generation
                .covered_causal_tip_root_key
                .as_ref()
                .map(|value| value.as_slice()),
            generation
                .covered_causal_tip_root_digest
                .as_bytes()
                .as_slice(),
            generation
                .covered_head_facts_root_digest
                .as_bytes()
                .as_slice(),
            generation
                .current_projection_payload_pins_root_digest
                .as_bytes()
                .as_slice(),
            generation.nonlinear_state_root_digest.as_bytes().as_slice(),
            generation.retention_pins_root_digest.as_bytes().as_slice(),
            anchor.checkpoint_frontier_root.as_slice(),
            ContentDigest::of(&anchor.checkpoint_frontier_root)
                .as_bytes()
                .as_slice(),
            anchor
                .terminal_batch_id
                .as_ref()
                .map(|value| value.as_slice()),
            anchor
                .terminal_evidence_digest
                .as_ref()
                .map(|value| value.as_bytes().as_slice()),
            anchor
                .materialization_frontier_root_digest
                .as_bytes()
                .as_slice(),
        ],
    )?;
    validate_schema_and_claim(&transaction, claim)?;
    transaction.commit()?;
    Ok(())
}

fn sqlite_i64(value: u64, field: &str) -> Result<i64, FrontierError> {
    i64::try_from(value).map_err(|_| FrontierError::InvalidInput(format!("{field} exceeds SQLite")))
}

fn validate_checkpoint_anchor_input(
    root: &PhysicalCheckpointFrontierRoot,
    anchor: &PhysicalCheckpointGenerationAnchor,
) -> Result<(), FrontierError> {
    // Shape first: the field-by-field comparison below can be satisfied by a
    // root that is internally inconsistent -- in particular an overlay that
    // claims to be empty while naming a root the candidate cannot walk. The
    // empty-overlay precondition is only worth anything if it is checked at
    // INSTALLATION, not just on the reads that come later.
    validate_checkpoint_root_shape(root)?;
    let generation = &anchor.generation;
    let count = generation.covered_count;
    let terminal_pair_is_valid = if count == 0 {
        anchor.terminal_batch_id.is_none() && anchor.terminal_evidence_digest.is_none()
    } else {
        anchor.terminal_batch_id.is_some() && anchor.terminal_evidence_digest.is_some()
    };
    if root.generation != *generation
        || root.acceptance_sequence != count
        || root.document_count != generation.covered_document_count
        || root.document_overlay_count != 0
        || root.retained_bytes_total != generation.covered_retained_bytes_total
        || root.batch_map_root_key != generation.covered_batch_root_key
        || root.batch_map_root_digest != generation.covered_batch_root_digest
        || root.batch_map_count != count
        || root.status_map_root_key != generation.covered_status_root_key
        || root.status_map_root_digest != generation.covered_status_root_digest
        || root.status_map_count != count
        || root.sequence_root_digest != generation.covered_sequence_root_digest
        || root.sequence_height != generation.covered_sequence_height
        || root.sequence_count != count
        || anchor.checkpoint_frontier_root != root.canonical_bytes
        || anchor.materialization_frontier_root_digest != root.digest()
        || !terminal_pair_is_valid
    {
        return Err(FrontierError::InvalidInput(
            "checkpoint-generation anchor does not match its checkpoint frontier".into(),
        ));
    }
    Ok(())
}

pub fn validate_schema_and_claim(
    connection: &Connection,
    claim: PhysicalClaim,
) -> Result<(), FrontierError> {
    let application_id: u32 =
        connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
    if application_id != SQLITE_APPLICATION_ID {
        return Err(FrontierError::Schema(format!(
            "application_id {application_id:#x} != {SQLITE_APPLICATION_ID:#x}"
        )));
    }
    let user_version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if user_version != SQLITE_SCHEMA_VERSION {
        return Err(FrontierError::Schema(format!(
            "unsupported SQLite frontier user_version {user_version}; current is {SQLITE_SCHEMA_VERSION}"
        )));
    }
    let journal_mode: String = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(FrontierError::Schema(format!(
            "journal_mode {journal_mode:?} is not WAL"
        )));
    }
    let tables = schema_names(connection, "table")?;
    let expected_tables = EXPECTED_TABLES
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    if tables != expected_tables {
        return Err(FrontierError::Schema(format!(
            "unexpected SQLite schema tables: {tables:?}"
        )));
    }
    let indexes = schema_names(connection, "index")?;
    let expected_indexes = EXPECTED_INDEXES
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    if indexes != expected_indexes {
        return Err(FrontierError::Schema(format!(
            "unexpected SQLite schema indexes: {indexes:?}"
        )));
    }
    for (table, columns) in [
        ("meta", META_COLUMNS),
        ("frontier", FRONTIER_COLUMNS),
        ("frontier_documents", FRONTIER_DOCUMENT_COLUMNS),
        ("causal_clock_nodes", CAUSAL_CLOCK_NODE_COLUMNS),
        ("accepted_batch_nodes", ACCEPTED_BATCH_NODE_COLUMNS),
        ("applied_batches", APPLIED_BATCH_COLUMNS),
    ] {
        validate_table_columns(connection, table, columns)?;
    }
    validate_table_columns(
        connection,
        "checkpoint_generation_anchor",
        CHECKPOINT_GENERATION_ANCHOR_COLUMNS,
    )?;
    for (kind, name, ddl) in [
        ("table", "meta", META_DDL),
        ("table", "frontier", FRONTIER_DDL),
        ("table", "frontier_documents", FRONTIER_DOCUMENTS_DDL),
        ("table", "causal_clock_nodes", CAUSAL_CLOCK_NODES_DDL),
        ("table", "accepted_batch_nodes", ACCEPTED_BATCH_NODES_DDL),
        ("table", "applied_batches", APPLIED_BATCHES_DDL),
        ("index", "applied_batches_batch_id_uq", BATCH_ID_INDEX_DDL),
        (
            "index",
            "applied_batches_acceptance_sequence_uq",
            ACCEPTANCE_SEQUENCE_INDEX_DDL,
        ),
    ] {
        validate_schema_sql(connection, kind, name, ddl)?;
    }
    validate_schema_sql(
        connection,
        "table",
        "checkpoint_generation_anchor",
        CHECKPOINT_GENERATION_ANCHOR_DDL,
    )?;
    sqlite_materialization::validate_schema(connection)?;
    let stored: (Vec<u8>, Vec<u8>, i64, i64, i64, i64, i64) = connection.query_row(
        "SELECT workspace_id, lineage_digest, oplog_protocol_version,
                operation_schema_version, object_envelope_schema_version,
                manifest_encoding_version, managed_entity_set_version
         FROM meta WHERE singleton = 1",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        },
    )?;
    compare_claim_bytes("workspace_id", claim.workspace_id.as_slice(), &stored.0)?;
    compare_claim_bytes("lineage_digest", claim.lineage_digest.as_bytes(), &stored.1)?;
    for (field, found, expected) in [
        (
            "oplog_protocol_version",
            stored.2,
            i64::from(claim.oplog_protocol_version),
        ),
        (
            "operation_schema_version",
            stored.3,
            i64::from(claim.operation_schema_version),
        ),
        (
            "object_envelope_schema_version",
            stored.4,
            i64::from(claim.object_envelope_schema_version),
        ),
        (
            "manifest_encoding_version",
            stored.5,
            i64::from(claim.manifest_encoding_version),
        ),
        (
            "managed_entity_set_version",
            stored.6,
            i64::from(claim.managed_entity_set_version),
        ),
    ] {
        if found != expected {
            return Err(FrontierError::ClaimVersion {
                field,
                expected,
                found,
            });
        }
    }
    let counts: (i64, i64, i64) = connection.query_row(
        "SELECT (SELECT COUNT(*) FROM meta), (SELECT COUNT(*) FROM frontier),
                (SELECT COUNT(*) FROM checkpoint_generation_anchor)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if counts.0 != 1 || counts.1 != 1 || !(0..=1).contains(&counts.2) {
        return Err(FrontierError::Corrupt(
            "meta/frontier/checkpoint singleton cardinality is invalid".into(),
        ));
    }
    if counts.2 == 1 {
        validate_checkpoint_generation_anchor(connection)?;
    }
    Ok(())
}

fn validate_checkpoint_generation_anchor(connection: &Connection) -> Result<(), FrontierError> {
    let stored: (
        i64,
        Vec<u8>,
        Vec<u8>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        i64,
        Vec<u8>,
    ) = connection
        .query_row(
            "SELECT covered_count, checkpoint_frontier_root,
                    checkpoint_frontier_root_digest, terminal_batch_id,
                    terminal_evidence_digest, covered_sequence_height,
                    materialization_frontier_root_digest
             FROM checkpoint_generation_anchor WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .map_err(|error| {
            FrontierError::Corrupt(format!(
                "checkpoint-generation anchor singleton is missing or malformed: {error}"
            ))
        })?;
    let covered_count = u64::try_from(stored.0)
        .map_err(|_| FrontierError::Corrupt("covered count is negative".into()))?;
    if stored.2.as_slice() != ContentDigest::of(&stored.1).as_bytes() || stored.6 != stored.2 {
        return Err(FrontierError::Corrupt(
            "checkpoint frontier digest does not match its canonical bytes".into(),
        ));
    }
    let terminal_pair_is_valid = if covered_count == 0 {
        stored.3.is_none() && stored.4.is_none()
    } else {
        stored.3.as_ref().is_some_and(|value| value.len() == 16)
            && stored.4.as_ref().is_some_and(|value| value.len() == 32)
    };
    if !terminal_pair_is_valid || !(0..=255).contains(&stored.5) {
        return Err(FrontierError::Corrupt(
            "checkpoint-generation anchor terminal evidence is invalid".into(),
        ));
    }
    let (frontier_bytes, frontier_digest, applied_count): (Vec<u8>, Vec<u8>, i64) = connection
        .query_row(
            "SELECT frontier_root, frontier_root_digest, applied_batch_count
             FROM frontier WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
    // The anchor is a FLOOR, not a freeze.
    //
    // This equality used to be unconditional, which was right while an anchored
    // candidate could never advance: nothing could be applied to one, so its
    // active frontier had to be exactly the anchored root. Once a tail batch can
    // be applied, that same check refuses the healthy database it just produced
    // -- apply advances the active frontier's bytes, digest and count, so the
    // very next open reported `Corrupt`. A one-way door: the candidate accepted
    // work and could then never be opened again.
    //
    // A refusal has to name an in-scope failure, and "the frontier moved forward"
    // is not one; it is the path working. What the anchor legitimately proves is
    // that this database has not gone BACKWARDS below its covered floor. The
    // remaining protections are unchanged and are the ones that actually detect
    // damage: `read_frontier` rejects a frontier whose digest does not match its
    // own bytes, the anchor's own digest/bytes binding is checked above, and the
    // materialization stamp must still agree with the active frontier below.
    // The frontier row must at minimum hash to its own digest. `read_frontier`
    // checks this, but `validate_schema_and_claim` calls THIS function directly
    // and never goes through `read_frontier` -- so relying on that was a gap, and
    // an unrelated-but-self-consistent root at an advanced count opened cleanly.
    if ContentDigest::of(&frontier_bytes).as_bytes() != frontier_digest.as_slice() {
        return Err(FrontierError::Corrupt(
            "active frontier digest does not hash its own bytes".into(),
        ));
    }
    if applied_count < stored.0 {
        return Err(FrontierError::Corrupt(
            "active frontier is behind its generation anchor".into(),
        ));
    }
    if applied_count == stored.0 {
        if frontier_bytes != stored.1 || frontier_digest != stored.2 {
            return Err(FrontierError::Corrupt(
                "unadvanced active frontier is misbound to its generation anchor".into(),
            ));
        }
    } else {
        validate_anchored_tail_extends_to_frontier(
            connection,
            stored.0,
            &stored.1,
            &stored.2,
            applied_count,
            &frontier_bytes,
            &frontier_digest,
        )?;
    }
    let (materialized_count, materialized_frontier): (i64, Vec<u8>) = connection.query_row(
        "SELECT acceptance_sequence, frontier_root_digest
         FROM materialization_stamp WHERE singleton = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    // The stamp is a floor too, for the same reason and with the same hazard.
    //
    // At candidate build the stamp is written to exactly the anchored root, so
    // equality was true then. It stops being true the moment a tail batch is
    // applied without a materialization change: apply only advances the stamp
    // when `request.materialization` is present (`apply_change_inner`), and an
    // unmaterialized accepted batch is an ordinary production shape -- Tine
    // builds `PhysicalApplyRequest.materialization` from an `Option`. An
    // ordinary database opens fine in that state; an anchored one refused, so
    // the second unmaterialized tail batch would have been a one-way door of
    // exactly the same class as the frontier check above.
    //
    // The in-scope failure this still names: a stamp AHEAD of the active
    // frontier means the graph projection carries rows for batches the frontier
    // never accepted -- the shape a partially rolled back or externally
    // truncated file leaves behind (crash/power loss, torn write, disk error),
    // and the one that would let a query answer from a batch that was undone.
    // A stamp BELOW the covered floor means materialization rows inside the
    // sealed region are missing, which the candidate was built to carry. Both
    // are damage; neither is the healthy path moving forward. Per D-3 the
    // caller treats this disposable cache as rebuildable rather than
    // authoritative.
    if materialized_count > applied_count {
        return Err(FrontierError::Corrupt(
            "materialization stamp is ahead of the active checkpoint frontier".into(),
        ));
    }
    if materialized_count < stored.0 {
        return Err(FrontierError::Corrupt(
            "materialization stamp is behind its generation anchor".into(),
        ));
    }
    // A LAGGING stamp is still bound to something: the frontier at the sequence
    // it claims. Leaving the lag interval unchecked was the same mistake as
    // leaving the advanced frontier unchecked -- a zeroed digest on a lagging
    // stamp opened cleanly. At the floor it must name the anchor's
    // materialization root; above the floor, the post-root of the hot batch at
    // that exact sequence.
    let expected_materialized = if materialized_count == applied_count {
        frontier_digest.clone()
    } else if materialized_count == stored.0 {
        stored.6.clone()
    } else {
        connection
            .query_row(
                "SELECT post_frontier_root_digest FROM applied_batches WHERE sequence = ?1",
                params![materialized_count],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .ok_or_else(|| {
                FrontierError::Corrupt(
                    "materialization stamp names a sequence with no accepted batch".into(),
                )
            })?
    };
    if materialized_frontier != expected_materialized {
        return Err(FrontierError::Corrupt(
            "materialization stamp is misbound to the frontier at its own sequence".into(),
        ));
    }
    // Projection evidence may not claim more than the stamp does. A torn stamp
    // page after a materialized tail leaves exactly this shape: receipts and
    // rows for a batch the stamp no longer admits.
    let latest_receipt: Option<i64> = connection.query_row(
        "SELECT MAX(acceptance_sequence) FROM materialization_batches",
        [],
        |row| row.get(0),
    )?;
    if let Some(latest) = latest_receipt {
        if latest > materialized_count {
            return Err(FrontierError::Corrupt(
                "materialization receipts are ahead of the materialization stamp".into(),
            ));
        }
    }
    Ok(())
}

/// Prove that the active frontier is an EXTENSION of the immutable anchor, not
/// merely a larger number than it.
///
/// Relaxing the anchor equality to `count >= floor` made the healthy forward
/// move legal -- and made everything else legal too: at an advanced count,
/// neither the frontier bytes nor the digest were compared with anything at
/// all, so a self-consistent but wholly unrelated root passed reopen. Verified
/// by probe, not by reading.
///
/// What binds them is the retained hot tail. Each `applied_batches` row carries
/// the prior and post frontier roots of one transition, so the tail is a chain:
/// it must start at the anchored root, be contiguous, and end at the active
/// frontier. Cost is O(tail), which is bounded by the tail and not by the
/// covered history (I-14).
///
/// Per D-3 the SQLite side is a disposable cache: a failure here routes the
/// caller to rebuild, and must never be repaired by trusting the rows.
fn validate_anchored_tail_extends_to_frontier(
    connection: &Connection,
    covered_count: i64,
    anchored_bytes: &[u8],
    anchored_digest: &[u8],
    applied_count: i64,
    frontier_bytes: &[u8],
    frontier_digest: &[u8],
) -> Result<(), FrontierError> {
    let mut statement = connection.prepare(
        "SELECT sequence, prior_frontier_root, prior_frontier_root_digest,
                post_frontier_root, post_frontier_root_digest
         FROM applied_batches ORDER BY sequence",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, Vec<u8>>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let expected_len = usize::try_from(applied_count.saturating_sub(covered_count))
        .map_err(|_| FrontierError::Corrupt("anchored tail length is invalid".into()))?;
    if rows.len() != expected_len {
        return Err(FrontierError::Corrupt(
            "anchored tail does not hold exactly the batches above the covered floor".into(),
        ));
    }
    let mut previous_bytes = anchored_bytes;
    let mut previous_digest = anchored_digest;
    for (offset, (sequence, prior, prior_digest, post, post_digest)) in rows.iter().enumerate() {
        let expected_sequence = covered_count
            .checked_add(1 + offset as i64)
            .ok_or_else(|| FrontierError::Corrupt("anchored tail sequence overflowed".into()))?;
        if *sequence != expected_sequence {
            return Err(FrontierError::Corrupt(
                "anchored tail is not contiguous above the covered floor".into(),
            ));
        }
        if prior.as_slice() != previous_bytes || prior_digest.as_slice() != previous_digest {
            return Err(FrontierError::Corrupt(
                "anchored tail does not chain back to its generation anchor".into(),
            ));
        }
        if ContentDigest::of(post).as_bytes() != post_digest.as_slice() {
            return Err(FrontierError::Corrupt(
                "anchored tail post-root digest does not hash its own bytes".into(),
            ));
        }
        previous_bytes = post;
        previous_digest = post_digest;
    }
    if previous_bytes != frontier_bytes || previous_digest != frontier_digest {
        return Err(FrontierError::Corrupt(
            "active frontier is not the end of the anchored tail".into(),
        ));
    }
    Ok(())
}

fn schema_names(connection: &Connection, kind: &str) -> Result<BTreeSet<String>, FrontierError> {
    let mut statement = connection
        .prepare("SELECT name FROM sqlite_schema WHERE type = ?1 AND name NOT LIKE 'sqlite_%'")?;
    let names = statement
        .query_map([kind], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    Ok(names)
}

fn compare_claim_bytes(
    field: &'static str,
    expected: &[u8],
    found: &[u8],
) -> Result<(), FrontierError> {
    if found != expected {
        return Err(FrontierError::ClaimBytes {
            field,
            expected: expected.to_vec(),
            found: found.to_vec(),
        });
    }
    Ok(())
}

fn validate_table_columns(
    connection: &Connection,
    table: &str,
    expected: &[&str],
) -> Result<(), FrontierError> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns: Vec<String> = statement
        .query_map([], |row| row.get(1))?
        .collect::<Result<_, _>>()?;
    if columns != expected {
        return Err(FrontierError::Schema(format!(
            "{table} columns {columns:?} != {expected:?}"
        )));
    }
    Ok(())
}

fn validate_schema_sql(
    connection: &Connection,
    kind: &str,
    name: &str,
    expected: &str,
) -> Result<(), FrontierError> {
    let found: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type = ?1 AND name = ?2",
        params![kind, name],
        |row| row.get(0),
    )?;
    if canonical_sql(&found) != canonical_sql(expected) {
        return Err(FrontierError::Schema(format!(
            "{kind} {name} does not match canonical DDL"
        )));
    }
    Ok(())
}

fn canonical_sql(sql: &str) -> String {
    sql.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn read_frontier(connection: &Connection) -> Result<StoredFrontier, FrontierError> {
    let (canonical_bytes, digest, count): (Vec<u8>, Vec<u8>, i64) = connection.query_row(
        "SELECT frontier_root, frontier_root_digest, applied_batch_count
         FROM frontier WHERE singleton = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let found = decode_digest(&digest)?;
    let expected = ContentDigest::of(&canonical_bytes);
    if found != expected {
        return Err(FrontierError::Corrupt(
            "frontier-root digest does not match frontier-root bytes".into(),
        ));
    }
    let applied_batch_count = u64::try_from(count)
        .map_err(|_| FrontierError::Corrupt("applied batch count is negative".into()))?;
    Ok(StoredFrontier {
        canonical_bytes,
        digest: found,
        applied_batch_count,
    })
}

pub fn load_batch(
    connection: &Connection,
    batch_id: [u8; 16],
) -> Result<Option<StoredBatch>, FrontierError> {
    connection
        .query_row(
            "SELECT sequence, batch_id, manifest_digest, semantic_effect,
                semantic_effect_digest, dependency_frontier, dependency_frontier_digest,
                prior_frontier_root, prior_frontier_root_digest, post_frontier_root,
                post_frontier_root_digest, affected_documents, affected_documents_digest,
                causal_dependency_heads, causal_peer_id, causal_counter, causal_clock_root_key,
                causal_clock_root_digest, acceptance_sequence, retained_bytes
         FROM applied_batches WHERE batch_id = ?1",
            [batch_id.as_slice()],
            stored_batch_from_row,
        )
        .optional()
        .map_err(Into::into)
}

pub fn load_batch_at_sequence(
    connection: &Connection,
    sequence: i64,
) -> Result<Option<StoredBatch>, FrontierError> {
    connection
        .query_row(
            "SELECT sequence, batch_id, manifest_digest, semantic_effect,
                semantic_effect_digest, dependency_frontier, dependency_frontier_digest,
                prior_frontier_root, prior_frontier_root_digest, post_frontier_root,
                post_frontier_root_digest, affected_documents, affected_documents_digest,
                causal_dependency_heads, causal_peer_id, causal_counter, causal_clock_root_key,
                causal_clock_root_digest, acceptance_sequence, retained_bytes
         FROM applied_batches WHERE sequence = ?1",
            [sequence],
            stored_batch_from_row,
        )
        .optional()
        .map_err(Into::into)
}

pub fn load_all_batches(connection: &Connection) -> Result<Vec<StoredBatch>, FrontierError> {
    let mut statement = connection.prepare(
        "SELECT sequence, batch_id, manifest_digest, semantic_effect,
                semantic_effect_digest, dependency_frontier, dependency_frontier_digest,
                prior_frontier_root, prior_frontier_root_digest, post_frontier_root,
                post_frontier_root_digest, affected_documents, affected_documents_digest,
                causal_dependency_heads, causal_peer_id, causal_counter, causal_clock_root_key,
                causal_clock_root_digest, acceptance_sequence, retained_bytes
         FROM applied_batches ORDER BY sequence",
    )?;
    let batches = statement
        .query_map([], stored_batch_from_row)?
        .collect::<Result<_, _>>()?;
    Ok(batches)
}

pub fn diagnostic_row_counts(connection: &Connection) -> Result<(u64, u64), FrontierError> {
    let (batches, documents): (i64, i64) = connection.query_row(
        "SELECT (SELECT COUNT(*) FROM applied_batches),
                (SELECT COUNT(*) FROM frontier_documents)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok((
        u64::try_from(batches)
            .map_err(|_| FrontierError::Corrupt("accepted row count is negative".into()))?,
        u64::try_from(documents).map_err(|_| {
            FrontierError::Corrupt("frontier document row count is negative".into())
        })?,
    ))
}

pub fn semantic_projection_digest(connection: &Connection) -> Result<ContentDigest, FrontierError> {
    let mut statement = connection.prepare(
        "SELECT batch_id, manifest_digest, semantic_effect, semantic_effect_digest,
                dependency_frontier FROM applied_batches ORDER BY batch_id",
    )?;
    let mut rows = statement.query([])?;
    let mut bytes = b"tine/sqlite-frontier/semantic-projection/v1\0".to_vec();
    while let Some(row) = rows.next()? {
        for index in 0..5 {
            let value: Vec<u8> = row.get(index)?;
            bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
            bytes.extend_from_slice(&value);
        }
    }
    let frontier = read_frontier(connection)?.canonical_bytes;
    bytes.extend_from_slice(&(frontier.len() as u64).to_be_bytes());
    bytes.extend_from_slice(&frontier);
    Ok(ContentDigest::of(&bytes))
}

pub fn stored_semantic_effects(connection: &Connection) -> Result<Vec<Vec<u8>>, FrontierError> {
    let mut statement =
        connection.prepare("SELECT semantic_effect FROM applied_batches ORDER BY sequence")?;
    let effects = statement
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    Ok(effects)
}

fn stored_batch_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredBatch> {
    let id: Vec<u8> = row.get(1)?;
    let batch_id = id.as_slice().try_into().map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            16,
            rusqlite::types::Type::Blob,
            "invalid batch ID length".into(),
        )
    })?;
    Ok(StoredBatch {
        sequence: row.get(0)?,
        batch_id,
        manifest_digest: row.get(2)?,
        semantic_effect: row.get(3)?,
        semantic_effect_digest: row.get(4)?,
        dependency_frontier: row.get(5)?,
        dependency_frontier_digest: row.get(6)?,
        prior_frontier_root: row.get(7)?,
        prior_frontier_root_digest: row.get(8)?,
        post_frontier_root: row.get(9)?,
        post_frontier_root_digest: row.get(10)?,
        affected_documents: row.get(11)?,
        affected_documents_digest: row.get(12)?,
        causal_dependency_heads: row.get(13)?,
        causal_peer_id: row.get(14)?,
        causal_counter: row.get(15)?,
        causal_clock_root_key: row.get(16)?,
        causal_clock_root_digest: row.get(17)?,
        acceptance_sequence: row.get(18)?,
        retained_bytes: row.get(19)?,
    })
}

fn decode_digest(bytes: &[u8]) -> Result<ContentDigest, FrontierError> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| FrontierError::Corrupt("invalid digest length".into()))?;
    Ok(ContentDigest::from_bytes(bytes))
}

fn decode_id(bytes: &[u8], what: &str) -> Result<[u8; 16], FrontierError> {
    bytes
        .try_into()
        .map_err(|_| FrontierError::Corrupt(format!("invalid {what} length")))
}

fn decode_key(bytes: &[u8], what: &str) -> Result<AuthenticatedMapKey, FrontierError> {
    AuthenticatedMapKey::new(bytes)
        .map_err(|_| FrontierError::Corrupt(format!("invalid {what} length")))
}

/// Recover a fixed-width identity from a general authenticated-map key.
///
/// The batch, status and causal maps keep 16-byte UUID identities; only the
/// document map is variable-width. A shared node that reaches one of those maps
/// with a wider key is corrupt, not a wider identity.
fn key_as_id(key: AuthenticatedMapKey, what: &str) -> Result<[u8; 16], FrontierError> {
    decode_id(key.as_slice(), what)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MapLink {
    key: AuthenticatedMapKey,
    digest: ContentDigest,
}

fn accepted_batch_causal_record_digest(
    batch: &PhysicalAcceptedBatch,
    clock_root: &MapLink,
) -> ContentDigest {
    accepted_causal_record_digest(
        batch.batch_id,
        batch.manifest_digest,
        batch.event_binding_digest,
        batch.causal_peer_id,
        batch.causal_counter,
        Some(AuthenticatedMapLinkV1 {
            key: clock_root.key,
            digest: clock_root.digest,
        }),
    )
}

fn valid_map_children(
    key: AuthenticatedMapKey,
    left: Option<&MapLink>,
    right: Option<&MapLink>,
) -> bool {
    left.is_none_or(|child| {
        child.key < key && authenticated_map_priority_order(key, child.key).is_lt()
    }) && right.is_none_or(|child| {
        child.key > key && authenticated_map_priority_order(key, child.key).is_lt()
    })
}

fn decode_map_link(
    key: Option<Vec<u8>>,
    digest: Option<Vec<u8>>,
) -> Result<Option<MapLink>, FrontierError> {
    match (key, digest) {
        (None, None) => Ok(None),
        (Some(key), Some(digest)) => Ok(Some(MapLink {
            key: decode_key(&key, "authenticated map key")?,
            digest: decode_digest(&digest)?,
        })),
        _ => Err(FrontierError::Corrupt(
            "authenticated map child is incomplete".into(),
        )),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClockNode {
    peer: [u8; 16],
    counter: u64,
    left: Option<MapLink>,
    right: Option<MapLink>,
}

#[derive(Clone, Debug)]
struct BatchMapNode {
    batch_id: [u8; 16],
    value_digest: ContentDigest,
    left: Option<MapLink>,
    right: Option<MapLink>,
}

fn ensure_depth(depth: usize, operation: &str) -> Result<(), FrontierError> {
    if depth > MAX_AUTHENTICATED_MAP_DEPTH {
        return Err(FrontierError::Corrupt(format!(
            "{operation} exceeds its bounded depth"
        )));
    }
    Ok(())
}

fn load_clock_node(
    connection: &Connection,
    expected: &MapLink,
) -> Result<ClockNode, FrontierError> {
    let stored = connection
        .query_row(
            "SELECT peer_id, counter, value_digest, left_peer_id, left_digest,
                right_peer_id, right_digest FROM causal_clock_nodes WHERE node_digest = ?1",
            [expected.digest.as_bytes().as_slice()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                    row.get::<_, Option<Vec<u8>>>(4)?,
                    row.get::<_, Option<Vec<u8>>>(5)?,
                    row.get::<_, Option<Vec<u8>>>(6)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| {
            FrontierError::Corrupt(format!(
                "authenticated causal clock node {} is missing",
                expected.digest
            ))
        })?;
    let peer = decode_id(&stored.0, "causal peer ID")?;
    let counter = u64::try_from(stored.1)
        .map_err(|_| FrontierError::Corrupt("causal clock counter is invalid".into()))?;
    let value_digest = decode_digest(&stored.2)?;
    let left = decode_map_link(stored.3, stored.4)?;
    let right = decode_map_link(stored.5, stored.6)?;
    let node = ClockNode {
        peer,
        counter,
        left,
        right,
    };
    let peer_key = AuthenticatedMapKey::from(peer);
    let computed = authenticated_map_node_digest(
        peer_key,
        value_digest,
        node.left.as_ref().map(|child| (child.key, child.digest)),
        node.right.as_ref().map(|child| (child.key, child.digest)),
    );
    if expected.key != peer_key
        || counter == 0
        || value_digest != causal_clock_counter_digest(peer, counter)
        || !valid_map_children(peer_key, node.left.as_ref(), node.right.as_ref())
        || computed != expected.digest
    {
        return Err(FrontierError::Corrupt(
            "authenticated causal clock node is misbound".into(),
        ));
    }
    Ok(node)
}

fn load_batch_map_node(
    connection: &Connection,
    expected: &MapLink,
) -> Result<BatchMapNode, FrontierError> {
    load_hot_batch_map_node(connection, expected)?.ok_or_else(|| {
        FrontierError::Corrupt(format!(
            "authenticated accepted-batch node {} is missing",
            expected.digest
        ))
    })
}

fn load_hot_batch_map_node(
    connection: &Connection,
    expected: &MapLink,
) -> Result<Option<BatchMapNode>, FrontierError> {
    let Some(stored) = connection
        .query_row(
            "SELECT batch_id, value_digest, left_batch_id, left_digest,
                right_batch_id, right_digest FROM accepted_batch_nodes WHERE node_digest = ?1",
            [expected.digest.as_bytes().as_slice()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                    row.get::<_, Option<Vec<u8>>>(4)?,
                    row.get::<_, Option<Vec<u8>>>(5)?,
                ))
            },
        )
        .optional()?
    else {
        return Ok(None);
    };
    let batch_id = decode_id(&stored.0, "batch ID")?;
    let node = BatchMapNode {
        batch_id,
        value_digest: decode_digest(&stored.1)?,
        left: decode_map_link(stored.2, stored.3)?,
        right: decode_map_link(stored.4, stored.5)?,
    };
    let batch_key = AuthenticatedMapKey::from(batch_id);
    let computed = authenticated_map_node_digest(
        batch_key,
        node.value_digest,
        node.left.as_ref().map(|child| (child.key, child.digest)),
        node.right.as_ref().map(|child| (child.key, child.digest)),
    );
    if expected.key != batch_key
        || !valid_map_children(batch_key, node.left.as_ref(), node.right.as_ref())
        || computed != expected.digest
    {
        return Err(FrontierError::Corrupt(
            "authenticated accepted-batch node is misbound".into(),
        ));
    }
    Ok(Some(node))
}

fn batch_map_value(
    connection: &Connection,
    sealed: Option<&dyn SealedAcceptedIndexRead>,
    root: &PhysicalFrontierRoot,
    batch_id: [u8; 16],
) -> Result<Option<ContentDigest>, FrontierError> {
    let Some(key) = root.batch_map_root_key else {
        if root.acceptance_sequence == 0 {
            return Ok(None);
        }
        return Err(FrontierError::Corrupt(
            "nonempty frontier has no authenticated batch-map root".into(),
        ));
    };
    let mut current = Some(MapLink {
        key: key.into(),
        digest: root.batch_map_root_digest,
    });
    let mut depth = 0;
    while let Some(link) = current {
        ensure_depth(depth, "accepted batch lookup")?;
        let node = load_batch_map_node_for_apply(connection, sealed, &link)?;
        match batch_id.cmp(&node.batch_id) {
            Ordering::Equal => return Ok(Some(node.value_digest)),
            Ordering::Less => current = node.left,
            Ordering::Greater => current = node.right,
        }
        depth += 1;
    }
    Ok(None)
}

/// Rebuild a covered batch's causal clock as hot `causal_clock_nodes` rows and
/// prove the result against the root the sealed record commits to.
///
/// `expected` is passed in rather than recomputed here on purpose. Both sides
/// derived from the same entry vector would move together under any
/// perturbation, which is the shape of a check that cannot fail; taking the
/// expected root as an argument is what lets
/// `a_perturbed_covered_clock_cannot_pass_its_canonical_root` perturb exactly
/// one side and see this refuse.
fn materialize_covered_causal_clock(
    connection: &Connection,
    batch_id: &[u8; 16],
    entries: &[SealedAcceptedCausalClockEntryV2],
    expected: Option<AuthenticatedMapLinkV1>,
) -> Result<MapLink, FrontierError> {
    let mut link = None;
    for entry in entries {
        link = Some(upsert_causal_clock(
            connection,
            link,
            entry.peer_id,
            entry.counter,
            0,
        )?);
    }
    let link = link.ok_or_else(|| {
        FrontierError::Corrupt(format!(
            "covered accepted batch {} has an empty causal clock",
            HexId(batch_id)
        ))
    })?;
    let expected = expected.ok_or_else(|| {
        FrontierError::Corrupt(format!(
            "covered accepted batch {} derived an empty canonical clock root",
            HexId(batch_id)
        ))
    })?;
    if link.key != expected.key || link.digest != expected.digest {
        return Err(FrontierError::Corrupt(format!(
            "covered accepted batch {} rebuilt a causal clock that differs from its canonical \
             root",
            HexId(batch_id)
        )));
    }
    Ok(link)
}

fn validate_checkpoint_root_shape(
    root: &PhysicalCheckpointFrontierRoot,
) -> Result<(), FrontierError> {
    let count = root.acceptance_sequence;
    let roots_match_count = |key: Option<[u8; 16]>, digest: ContentDigest| {
        if count == 0 {
            key.is_none() && digest == authenticated_map_empty_digest()
        } else {
            key.is_some()
        }
    };
    if root.batch_map_count != count
        || root.status_map_count != count
        || root.sequence_count != count
        || root.generation.covered_count > count
        || root.document_overlay_count > root.document_count
        // The generation-relative document overlay is empty exactly when it has
        // no root. The anchor separately requires the overlay to be empty AT
        // the cutover (`validate_checkpoint_anchor_input`), and those two
        // together are what make covered document nodes unreachable: apply
        // seeds its document descent from `document_map_root_key` and walks
        // `frontier_documents`, which has no sealed seam, so an anchored
        // candidate that started with an overlay root would fault on a node
        // that is not there. Starting from `None` makes every overlay node a
        // tail apply writes hot by construction.
        || (root.document_overlay_count == 0)
            != (root.document_map_root_key.is_none()
                && root.document_map_root_digest == authenticated_map_empty_digest())
        || !roots_match_count(root.batch_map_root_key, root.batch_map_root_digest)
        || !roots_match_count(root.status_map_root_key, root.status_map_root_digest)
        || (count == 0) != root.sequence_root_digest.is_none()
    {
        return Err(FrontierError::Corrupt(
            "checkpoint frontier root/count shape is inconsistent".into(),
        ));
    }
    Ok(())
}

fn load_composite_batch_map_node(
    connection: &Connection,
    sealed: &dyn SealedAcceptedIndexRead,
    expected: &MapLink,
) -> Result<BatchMapNode, FrontierError> {
    if let Some(node) = load_hot_batch_map_node(connection, expected)? {
        return Ok(node);
    }
    let sealed = sealed.sealed_map_node(AuthenticatedMapLinkV1 {
        key: expected.key,
        digest: expected.digest,
    })?;
    Ok(BatchMapNode {
        batch_id: key_as_id(sealed.key, "sealed accepted-batch key")?,
        value_digest: sealed.value_digest,
        left: sealed.left.map(|child| MapLink {
            key: child.key,
            digest: child.digest,
        }),
        right: sealed.right.map(|child| MapLink {
            key: child.key,
            digest: child.digest,
        }),
    })
}

/// Resolve one accepted-batch node for a path that may or may not be anchored.
///
/// `None` is the LIVE database: every node is a row, and this is exactly the hot
/// read it has always been, so the live apply path keeps its current cost and
/// behaviour. `Some` is an anchored candidate, whose covered nodes left SQLite at
/// the generation cutover and survive only as sealed accepted-index objects.
///
/// Falling back is sound rather than merely convenient: SQLite and sealed map
/// nodes are the SAME tree, not two encodings that happen to agree.
/// `sqlite_frontier` and the sealed writer both call
/// `authenticated_map_node_digest` and `authenticated_map_priority` from
/// `sealed_accepted_index_impl`, so a node has one identity on both sides.
fn load_batch_map_node_for_apply(
    connection: &Connection,
    sealed: Option<&dyn SealedAcceptedIndexRead>,
    expected: &MapLink,
) -> Result<BatchMapNode, FrontierError> {
    match sealed {
        Some(sealed) => load_composite_batch_map_node(connection, sealed, expected),
        None => load_batch_map_node(connection, expected),
    }
}

fn batch_map_value_checkpoint(
    connection: &Connection,
    root: &PhysicalCheckpointFrontierRoot,
    sealed: &dyn SealedAcceptedIndexRead,
    batch_id: [u8; 16],
) -> Result<Option<ContentDigest>, FrontierError> {
    validate_checkpoint_root_shape(root)?;
    let Some(key) = root.batch_map_root_key else {
        return Ok(None);
    };
    let mut current = Some(MapLink {
        key: key.into(),
        digest: root.batch_map_root_digest,
    });
    let mut depth = 0;
    while let Some(link) = current {
        ensure_depth(depth, "checkpoint accepted batch lookup")?;
        let node = load_composite_batch_map_node(connection, sealed, &link)?;
        match batch_id.cmp(&node.batch_id) {
            Ordering::Equal => return Ok(Some(node.value_digest)),
            Ordering::Less => current = node.left,
            Ordering::Greater => current = node.right,
        }
        depth += 1;
    }
    Ok(None)
}

fn validate_stored_batch_physical(record: &StoredBatch) -> Result<(), FrontierError> {
    if record.sequence <= 0
        || record.acceptance_sequence != record.sequence
        || record.retained_bytes < 0
        || record.causal_counter <= 0
    {
        return Err(FrontierError::Corrupt(
            "stored accepted sequence or retained-byte count is invalid".into(),
        ));
    }
    for (label, bytes, digest) in [
        (
            "dependency-frontier",
            record.dependency_frontier.as_slice(),
            record.dependency_frontier_digest.as_slice(),
        ),
        (
            "prior-frontier-root",
            record.prior_frontier_root.as_slice(),
            record.prior_frontier_root_digest.as_slice(),
        ),
        (
            "post-frontier-root",
            record.post_frontier_root.as_slice(),
            record.post_frontier_root_digest.as_slice(),
        ),
        (
            "affected-documents",
            record.affected_documents.as_slice(),
            record.affected_documents_digest.as_slice(),
        ),
    ] {
        if digest != ContentDigest::of(bytes).as_bytes() {
            return Err(FrontierError::Corrupt(format!(
                "stored batch {} {label} digest mismatch",
                HexId(&record.batch_id)
            )));
        }
    }
    decode_digest(&record.manifest_digest)?;
    if decode_digest(&record.semantic_effect_digest)? != ContentDigest::of(&record.semantic_effect)
    {
        return Err(FrontierError::Corrupt(format!(
            "stored batch {} semantic-effect digest mismatch",
            HexId(&record.batch_id)
        )));
    }
    decode_id(&record.causal_peer_id, "causal peer ID")?;
    decode_id(&record.causal_clock_root_key, "causal clock root key")?;
    decode_digest(&record.causal_clock_root_digest)?;
    Ok(())
}

/// Resolve one accepted batch, hot or covered.
///
/// `sealed` is `None` on a live database, where every accepted batch has a row
/// and a missing one is corruption. On an anchored candidate it is `Some`, and a
/// batch below the generation cutover legitimately has NO row: it is answered by
/// its sealed causal record instead. That record carries the whole
/// `canonical_causal_clock`, which is why a covered ancestor needs neither a
/// `semantic_effect` (read only on the `Hot` branch, by
/// `validate_stored_batch_physical`) nor any `causal_clock_nodes` row.
fn authenticated_batch_record(
    connection: &Connection,
    sealed: Option<&dyn SealedAcceptedIndexRead>,
    root: &PhysicalFrontierRoot,
    batch_id: [u8; 16],
    expected_value: Option<ContentDigest>,
) -> Result<Option<CheckpointAuthenticatedBatchRecord>, FrontierError> {
    let Some(value) = batch_map_value(connection, sealed, root, batch_id)? else {
        return Ok(None);
    };
    if expected_value.is_some_and(|expected| expected != value) {
        return Err(FrontierError::Corrupt(format!(
            "accepted batch {} differs from its authenticated causal record",
            HexId(&batch_id)
        )));
    }
    if let Some(record) = load_batch(connection, batch_id)? {
        validate_stored_batch_physical(&record)?;
        let authenticated = CheckpointAuthenticatedBatchRecord::Hot(record);
        let (peer, counter) = authenticated.causal_dot()?;
        if authenticated.clock_counter(connection, peer)? != Some(counter) {
            return Err(FrontierError::Corrupt(format!(
                "accepted batch {} causal dot is absent from its authenticated clock",
                HexId(&batch_id)
            )));
        }
        return Ok(Some(authenticated));
    }
    let Some(sealed) = sealed else {
        return Err(FrontierError::Corrupt(format!(
            "authenticated accepted batch {} is missing its exact record",
            HexId(&batch_id)
        )));
    };
    Ok(Some(CheckpointAuthenticatedBatchRecord::Covered(
        sealed.sealed_causal_record(batch_id, value)?,
    )))
}

pub fn contains_batch(
    connection: &Connection,
    root: &PhysicalFrontierRoot,
    batch_id: [u8; 16],
) -> Result<bool, FrontierError> {
    authenticated_batch_record(connection, None, root, batch_id, None)
        .map(|record| record.is_some())
}

pub fn authenticate_batch(
    connection: &Connection,
    root: &PhysicalFrontierRoot,
    batch_id: [u8; 16],
    causal_record_digest: ContentDigest,
) -> Result<bool, FrontierError> {
    authenticated_batch_record(connection, None, root, batch_id, Some(causal_record_digest))
        .map(|record| record.is_some())
}

enum CheckpointAuthenticatedBatchRecord {
    Hot(StoredBatch),
    Covered(SealedAcceptedCausalRecordV2),
}

impl CheckpointAuthenticatedBatchRecord {
    fn causal_dot(&self) -> Result<([u8; 16], u64), FrontierError> {
        match self {
            Self::Hot(record) => Ok((
                decode_id(&record.causal_peer_id, "causal peer ID")?,
                u64::try_from(record.causal_counter).map_err(|_| {
                    FrontierError::Corrupt("stored causal counter is invalid".into())
                })?,
            )),
            Self::Covered(record) => Ok((record.causal_peer_id, record.causal_counter)),
        }
    }

    /// This record's causal clock as a hot map link the frontier can union.
    ///
    /// `Hot` already has one. `Covered` does not and cannot: its clock nodes
    /// left SQLite at the generation cutover. It carries the whole clock as a
    /// sorted `(peer, counter)` vector instead, so the nodes are rebuilt here.
    ///
    /// Rebuilding is cheap and BOUNDED BY PEER COUNT, not by history -- which is
    /// why this is materialized rather than making `union_clocks` composite.
    /// A composite union would thread the reader through six recursive load
    /// sites to avoid work that is already small.
    ///
    /// The rebuild is CHECKED, not trusted: `clock_root()` derives the canonical
    /// root from the same vector by a different construction (a Cartesian build
    /// over sorted entries, rather than repeated treap insertion). They must
    /// agree, and they can only agree if the rebuilt tree is the same tree --
    /// node priority folds the key alone, so shape is a function of the key set,
    /// not of insertion order. A silent divergence here would corrupt causal
    /// ordering, which is the one thing no later check would catch.
    fn clock_link(&self, connection: &Connection) -> Result<MapLink, FrontierError> {
        match self {
            Self::Hot(record) => Ok(MapLink {
                key: decode_id(&record.causal_clock_root_key, "causal clock root key")?.into(),
                digest: decode_digest(&record.causal_clock_root_digest)?,
            }),
            Self::Covered(record) => materialize_covered_causal_clock(
                connection,
                &record.batch_id,
                &record.canonical_causal_clock,
                record
                    .clock_root()
                    .map_err(FrontierError::SealedAcceptedIndex)?
                    .root,
            ),
        }
    }

    fn clock_counter(
        &self,
        connection: &Connection,
        peer: [u8; 16],
    ) -> Result<Option<u64>, FrontierError> {
        match self {
            Self::Hot(record) => {
                let clock = MapLink {
                    key: decode_id(&record.causal_clock_root_key, "causal clock root key")?.into(),
                    digest: decode_digest(&record.causal_clock_root_digest)?,
                };
                causal_clock_lookup(connection, Some(clock), peer)
            }
            Self::Covered(record) => Ok(record
                .canonical_causal_clock
                .binary_search_by_key(&peer, |entry| entry.peer_id)
                .ok()
                .map(|index| record.canonical_causal_clock[index].counter)),
        }
    }
}

fn authenticated_checkpoint_batch_record(
    connection: &Connection,
    root: &PhysicalCheckpointFrontierRoot,
    sealed: &dyn SealedAcceptedIndexRead,
    batch_id: [u8; 16],
    expected_value: Option<ContentDigest>,
) -> Result<Option<CheckpointAuthenticatedBatchRecord>, FrontierError> {
    let Some(value) = batch_map_value_checkpoint(connection, root, sealed, batch_id)? else {
        return Ok(None);
    };
    if expected_value.is_some_and(|expected| expected != value) {
        return Err(FrontierError::Corrupt(format!(
            "accepted batch {} differs from its authenticated causal record",
            HexId(&batch_id)
        )));
    }
    if let Some(record) = load_batch(connection, batch_id)? {
        validate_stored_batch_physical(&record)?;
        let authenticated = CheckpointAuthenticatedBatchRecord::Hot(record);
        let (peer, counter) = authenticated.causal_dot()?;
        if authenticated.clock_counter(connection, peer)? != Some(counter) {
            return Err(FrontierError::Corrupt(format!(
                "accepted batch {} causal dot is absent from its authenticated hot clock",
                HexId(&batch_id)
            )));
        }
        return Ok(Some(authenticated));
    }
    let record = sealed.sealed_causal_record(batch_id, value)?;
    Ok(Some(CheckpointAuthenticatedBatchRecord::Covered(record)))
}

pub fn contains_checkpoint_batch(
    connection: &Connection,
    root: &PhysicalCheckpointFrontierRoot,
    sealed: &dyn SealedAcceptedIndexRead,
    batch_id: [u8; 16],
) -> Result<bool, FrontierError> {
    authenticated_checkpoint_batch_record(connection, root, sealed, batch_id, None)
        .map(|record| record.is_some())
}

pub fn authenticate_checkpoint_batch(
    connection: &Connection,
    root: &PhysicalCheckpointFrontierRoot,
    sealed: &dyn SealedAcceptedIndexRead,
    batch_id: [u8; 16],
    causal_record_digest: ContentDigest,
) -> Result<bool, FrontierError> {
    authenticated_checkpoint_batch_record(
        connection,
        root,
        sealed,
        batch_id,
        Some(causal_record_digest),
    )
    .map(|record| record.is_some())
}

pub fn checkpoint_batch_descends_from(
    connection: &Connection,
    root: &PhysicalCheckpointFrontierRoot,
    sealed: &dyn SealedAcceptedIndexRead,
    descendant: [u8; 16],
    ancestor: [u8; 16],
) -> Result<bool, FrontierError> {
    let descendant =
        authenticated_checkpoint_batch_record(connection, root, sealed, descendant, None)?
            .ok_or_else(|| {
                FrontierError::Corrupt(
                    "descendant batch is absent from the checkpoint authenticated accepted map"
                        .into(),
                )
            })?;
    let Some(ancestor) =
        authenticated_checkpoint_batch_record(connection, root, sealed, ancestor, None)?
    else {
        return Ok(false);
    };
    let (peer, counter) = ancestor.causal_dot()?;
    Ok(descendant
        .clock_counter(connection, peer)?
        .is_some_and(|found| found >= counter))
}

fn causal_clock_lookup(
    connection: &Connection,
    mut current: Option<MapLink>,
    peer: [u8; 16],
) -> Result<Option<u64>, FrontierError> {
    let mut depth = 0;
    while let Some(link) = current {
        ensure_depth(depth, "causal clock lookup")?;
        let node = load_clock_node(connection, &link)?;
        match peer.cmp(&node.peer) {
            Ordering::Equal => return Ok(Some(node.counter)),
            Ordering::Less => current = node.left,
            Ordering::Greater => current = node.right,
        }
        depth += 1;
    }
    Ok(None)
}

pub fn batch_descends_from(
    connection: &Connection,
    root: &PhysicalFrontierRoot,
    descendant: [u8; 16],
    ancestor: [u8; 16],
) -> Result<bool, FrontierError> {
    let descendant = authenticated_batch_record(connection, None, root, descendant, None)?
        .ok_or_else(|| {
            FrontierError::Corrupt(
                "descendant batch is absent from the authenticated accepted map".into(),
            )
        })?;
    let Some(ancestor) = authenticated_batch_record(connection, None, root, ancestor, None)? else {
        return Ok(false);
    };
    let (peer, counter) = ancestor.causal_dot()?;
    Ok(descendant
        .clock_counter(connection, peer)?
        .is_some_and(|found| found >= counter))
}

#[derive(Clone)]
struct FrontierMapNode {
    document_key: AuthenticatedMapKey,
    encoded: Vec<u8>,
    value_digest: ContentDigest,
    left: Option<MapLink>,
    right: Option<MapLink>,
    node_digest: ContentDigest,
}

impl FrontierMapNode {
    fn recompute_digest(&self) -> ContentDigest {
        authenticated_map_node_digest(
            self.document_key,
            self.value_digest,
            self.left.as_ref().map(|child| (child.key, child.digest)),
            self.right.as_ref().map(|child| (child.key, child.digest)),
        )
    }

    fn as_link(&self) -> MapLink {
        MapLink {
            key: self.document_key,
            digest: self.node_digest,
        }
    }
}

fn load_frontier_map_node(
    connection: &Connection,
    document_key: AuthenticatedMapKey,
    expected_digest: Option<ContentDigest>,
) -> Result<Option<FrontierMapNode>, FrontierError> {
    type StoredRow = (
        Vec<u8>,
        Vec<u8>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Vec<u8>,
    );
    let found: Option<StoredRow> = connection
        .query_row(
            "SELECT dependencies, dependencies_digest, left_document_id, left_digest,
                right_document_id, right_digest, node_digest
         FROM frontier_documents WHERE document_id = ?1",
            [document_key.as_slice()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((encoded, value_digest, left_id, left_digest, right_id, right_digest, node_digest)) =
        found
    else {
        return Ok(None);
    };
    let value_digest = decode_digest(&value_digest)?;
    if value_digest != ContentDigest::of(&encoded) {
        return Err(FrontierError::Corrupt(format!(
            "frontier document {document_key} digest mismatch"
        )));
    }
    let left = decode_map_link(left_id, left_digest)?;
    let right = decode_map_link(right_id, right_digest)?;
    if left.as_ref().is_some_and(|child| child.key >= document_key)
        || right
            .as_ref()
            .is_some_and(|child| child.key <= document_key)
    {
        return Err(FrontierError::Corrupt(
            "frontier map child ordering is invalid".into(),
        ));
    }
    let mut node = FrontierMapNode {
        document_key,
        encoded,
        value_digest,
        left,
        right,
        node_digest: decode_digest(&node_digest)?,
    };
    let computed = node.recompute_digest();
    if node.node_digest != computed || expected_digest.is_some_and(|expected| expected != computed)
    {
        return Err(FrontierError::Corrupt(format!(
            "frontier document {document_key} is not authenticated by its map root"
        )));
    }
    node.node_digest = computed;
    Ok(Some(node))
}

pub fn frontier_document(
    connection: &Connection,
    root: &PhysicalFrontierRoot,
    document_key: AuthenticatedMapKey,
) -> Result<Option<Vec<u8>>, FrontierError> {
    let mut current = match root.document_map_root_key {
        Some(key) => Some(MapLink {
            key,
            digest: root.document_map_root_digest,
        }),
        // Lazy genesis authenticates baseline dependencies in the immutable
        // pack. SQLite contains only the later mutable overlay, so the map may
        // be empty while the logical document count is nonzero.
        None => None,
    };
    let mut depth = 0;
    while let Some(link) = current {
        ensure_depth(depth, "frontier map lookup")?;
        let node =
            load_frontier_map_node(connection, link.key, Some(link.digest))?.ok_or_else(|| {
                FrontierError::Corrupt(format!(
                    "authenticated frontier node {} is missing",
                    link.key
                ))
            })?;
        match document_key.cmp(&node.document_key) {
            Ordering::Equal => return Ok(Some(node.encoded)),
            Ordering::Less => current = node.left,
            Ordering::Greater => current = node.right,
        }
        depth += 1;
    }
    Ok(None)
}

pub fn read_frontier_documents(
    connection: &Connection,
    root: &PhysicalFrontierRoot,
) -> Result<Vec<PhysicalFrontierDocument>, FrontierError> {
    let mut pending = root
        .document_map_root_key
        .map(|key| MapLink {
            key,
            digest: root.document_map_root_digest,
        })
        .into_iter()
        .collect::<Vec<_>>();
    let mut documents = Vec::with_capacity(
        usize::try_from(root.document_count)
            .unwrap_or(1_000_000)
            .min(1_000_000),
    );
    while let Some(link) = pending.pop() {
        let node =
            load_frontier_map_node(connection, link.key, Some(link.digest))?.ok_or_else(|| {
                FrontierError::Corrupt(format!(
                    "authenticated frontier node {} is missing",
                    link.key
                ))
            })?;
        if let Some(right) = node.right.clone() {
            pending.push(right);
        }
        documents.push(PhysicalFrontierDocument {
            document_key: node.document_key,
            canonical_bytes: node.encoded,
        });
        if let Some(left) = node.left {
            pending.push(left);
        }
    }
    documents.sort_unstable_by_key(|document| document.document_key);
    // This is the authenticated SQLite overlay. Its row count equals the
    // logical total only for a frontier without an external genesis baseline.
    Ok(documents)
}

fn store_frontier_map_node(
    transaction: &Connection,
    node: &FrontierMapNode,
) -> Result<(), FrontierError> {
    if node.node_digest != node.recompute_digest() {
        return Err(FrontierError::Corrupt(
            "frontier map node digest is stale".into(),
        ));
    }
    transaction.execute(
        "INSERT INTO frontier_documents (
             document_id, dependencies, dependencies_digest, left_document_id, left_digest,
             right_document_id, right_digest, node_digest
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(document_id) DO UPDATE SET
             dependencies = excluded.dependencies,
             dependencies_digest = excluded.dependencies_digest,
             left_document_id = excluded.left_document_id,
             left_digest = excluded.left_digest,
             right_document_id = excluded.right_document_id,
             right_digest = excluded.right_digest,
             node_digest = excluded.node_digest",
        params![
            node.document_key.as_slice(),
            &node.encoded,
            node.value_digest.as_bytes().as_slice(),
            node.left.as_ref().map(|child| child.key.as_slice()),
            node.left
                .as_ref()
                .map(|child| child.digest.as_bytes().as_slice()),
            node.right.as_ref().map(|child| child.key.as_slice()),
            node.right
                .as_ref()
                .map(|child| child.digest.as_bytes().as_slice()),
            node.node_digest.as_bytes().as_slice(),
        ],
    )?;
    Ok(())
}

fn upsert_frontier_map(
    transaction: &Connection,
    root: Option<MapLink>,
    document: &PhysicalFrontierDocument,
    depth: usize,
) -> Result<(MapLink, bool), FrontierError> {
    ensure_depth(depth, "frontier map update")?;
    let Some(root) = root else {
        let value_digest = ContentDigest::of(&document.canonical_bytes);
        let mut node = FrontierMapNode {
            document_key: document.document_key,
            encoded: document.canonical_bytes.clone(),
            value_digest,
            left: None,
            right: None,
            node_digest: authenticated_map_empty_digest(),
        };
        node.node_digest = node.recompute_digest();
        store_frontier_map_node(transaction, &node)?;
        return Ok((node.as_link(), true));
    };
    let mut node =
        load_frontier_map_node(transaction, root.key, Some(root.digest))?.ok_or_else(|| {
            FrontierError::Corrupt(format!(
                "authenticated frontier node {} is missing",
                root.key
            ))
        })?;
    let inserted;
    match document.document_key.cmp(&node.document_key) {
        Ordering::Equal => {
            node.encoded = document.canonical_bytes.clone();
            node.value_digest = ContentDigest::of(&node.encoded);
            inserted = false;
        }
        Ordering::Less => {
            let (left, was_inserted) =
                upsert_frontier_map(transaction, node.left.take(), document, depth + 1)?;
            node.left = Some(left);
            inserted = was_inserted;
            if node.left.as_ref().is_some_and(|left| {
                authenticated_map_priority_order(left.key, node.document_key).is_lt()
            }) {
                return Ok((rotate_frontier_right(transaction, node)?, inserted));
            }
        }
        Ordering::Greater => {
            let (right, was_inserted) =
                upsert_frontier_map(transaction, node.right.take(), document, depth + 1)?;
            node.right = Some(right);
            inserted = was_inserted;
            if node.right.as_ref().is_some_and(|right| {
                authenticated_map_priority_order(right.key, node.document_key).is_lt()
            }) {
                return Ok((rotate_frontier_left(transaction, node)?, inserted));
            }
        }
    }
    node.node_digest = node.recompute_digest();
    store_frontier_map_node(transaction, &node)?;
    Ok((node.as_link(), inserted))
}

fn rotate_frontier_right(
    transaction: &Connection,
    mut node: FrontierMapNode,
) -> Result<MapLink, FrontierError> {
    let left = node.left.take().ok_or_else(|| {
        FrontierError::Corrupt("frontier map right rotation has no left child".into())
    })?;
    let mut left_node = load_frontier_map_node(transaction, left.key, Some(left.digest))?
        .ok_or_else(|| FrontierError::Corrupt("frontier map rotation child is missing".into()))?;
    node.left = left_node.right.take();
    node.node_digest = node.recompute_digest();
    store_frontier_map_node(transaction, &node)?;
    left_node.right = Some(node.as_link());
    left_node.node_digest = left_node.recompute_digest();
    store_frontier_map_node(transaction, &left_node)?;
    Ok(left_node.as_link())
}

fn rotate_frontier_left(
    transaction: &Connection,
    mut node: FrontierMapNode,
) -> Result<MapLink, FrontierError> {
    let right = node.right.take().ok_or_else(|| {
        FrontierError::Corrupt("frontier map left rotation has no right child".into())
    })?;
    let mut right_node = load_frontier_map_node(transaction, right.key, Some(right.digest))?
        .ok_or_else(|| FrontierError::Corrupt("frontier map rotation child is missing".into()))?;
    node.right = right_node.left.take();
    node.node_digest = node.recompute_digest();
    store_frontier_map_node(transaction, &node)?;
    right_node.left = Some(node.as_link());
    right_node.node_digest = right_node.recompute_digest();
    store_frontier_map_node(transaction, &right_node)?;
    Ok(right_node.as_link())
}

/// Persist an exact terminal document frontier once inside a fresh candidate.
///
/// The authenticated map is a deterministic treap: keys provide the binary
/// search order and their domain-separated digests provide heap priority. A
/// Cartesian-tree build therefore produces exactly the same root as repeated
/// live upserts, but writes every terminal node once instead of rewriting an
/// authentication path for every inserted document.
pub fn seed_terminal_frontier_documents_candidate(
    connection: &Connection,
    expected_root: &PhysicalFrontierRoot,
    documents: &[PhysicalFrontierDocument],
) -> Result<(), FrontierError> {
    seed_terminal_frontier_documents_candidate_with_policy(
        connection,
        expected_root,
        documents,
        false,
    )
}

/// Persist the exact sparse terminal overlay of a frontier whose immutable
/// baseline documents live outside SQLite.
///
/// The authenticated map itself must still match exactly; only the equality
/// between its row count and the logical frontier's total document count is
/// relaxed. This is the rebuild counterpart of a lazy-genesis frontier, where
/// `document_count` includes immutable baseline documents while the map stores
/// only documents changed since that baseline.
pub fn seed_sparse_terminal_frontier_documents_candidate(
    connection: &Connection,
    expected_root: &PhysicalFrontierRoot,
    documents: &[PhysicalFrontierDocument],
) -> Result<(), FrontierError> {
    seed_terminal_frontier_documents_candidate_with_policy(
        connection,
        expected_root,
        documents,
        true,
    )
}

fn seed_terminal_frontier_documents_candidate_with_policy(
    connection: &Connection,
    expected_root: &PhysicalFrontierRoot,
    documents: &[PhysicalFrontierDocument],
    sparse: bool,
) -> Result<(), FrontierError> {
    if connection.is_autocommit() {
        return Err(FrontierError::InvalidInput(
            "terminal document frontier requires an active candidate-build transaction".into(),
        ));
    }
    let stored_frontier = read_frontier(connection)?;
    if stored_frontier.canonical_bytes != expected_root.canonical_bytes
        || stored_frontier.digest != expected_root.digest()
        || stored_frontier.applied_batch_count != expected_root.acceptance_sequence
    {
        return Err(FrontierError::FrontierRegression);
    }
    let stored_documents: i64 =
        connection.query_row("SELECT COUNT(*) FROM frontier_documents", [], |row| {
            row.get(0)
        })?;
    if stored_documents != 0 {
        return Err(FrontierError::InvalidInput(
            "terminal document frontier requires an empty frontier_documents table".into(),
        ));
    }
    if documents
        .windows(2)
        .any(|pair| pair[0].document_key >= pair[1].document_key)
    {
        return Err(FrontierError::InvalidInput(
            "terminal frontier documents are not sorted unique".into(),
        ));
    }
    let stored_count = documents.len() as u64;
    if (!sparse && expected_root.document_count != stored_count)
        || (sparse && stored_count > expected_root.document_count)
    {
        return Err(FrontierError::FrontierRegression);
    }
    if documents.is_empty() {
        let empty = authenticated_map_empty_digest();
        if expected_root.document_map_root_key.is_some()
            || expected_root.document_map_root_digest != empty
        {
            return Err(FrontierError::FrontierRegression);
        }
        return Ok(());
    }

    #[derive(Clone, Copy, Debug, Default)]
    struct ShapeNode {
        left: Option<usize>,
        right: Option<usize>,
    }

    let mut shape = vec![ShapeNode::default(); documents.len()];
    let mut stack = Vec::<usize>::new();
    for index in 0..documents.len() {
        let mut last = None;
        while stack.last().is_some_and(|prior| {
            authenticated_map_priority_order(
                documents[index].document_key,
                documents[*prior].document_key,
            )
            .is_lt()
        }) {
            last = stack.pop();
        }
        if let Some(parent) = stack.last().copied() {
            shape[parent].right = Some(index);
        }
        shape[index].left = last;
        stack.push(index);
    }
    let root_index = stack[0];

    let mut digests = vec![None; documents.len()];
    let mut constructed = vec![None; documents.len()];
    let mut pending = vec![(root_index, 0_usize, false)];
    while let Some((index, depth, visited)) = pending.pop() {
        ensure_depth(depth, "terminal frontier map construction")?;
        if !visited {
            pending.push((index, depth, true));
            if let Some(right) = shape[index].right {
                pending.push((right, depth + 1, false));
            }
            if let Some(left) = shape[index].left {
                pending.push((left, depth + 1, false));
            }
            continue;
        }
        let link = |child: usize| -> Result<MapLink, FrontierError> {
            Ok(MapLink {
                key: documents[child].document_key,
                digest: digests[child].ok_or_else(|| {
                    FrontierError::Corrupt(
                        "terminal frontier child digest was not constructed".into(),
                    )
                })?,
            })
        };
        let left = shape[index].left.map(link).transpose()?;
        let right = shape[index].right.map(link).transpose()?;
        let mut node = FrontierMapNode {
            document_key: documents[index].document_key,
            encoded: documents[index].canonical_bytes.clone(),
            value_digest: ContentDigest::of(&documents[index].canonical_bytes),
            left,
            right,
            node_digest: authenticated_map_empty_digest(),
        };
        node.node_digest = node.recompute_digest();
        digests[index] = Some(node.node_digest);
        constructed[index] = Some(node);
    }
    let root_digest = digests[root_index].ok_or_else(|| {
        FrontierError::Corrupt("terminal frontier root digest was not constructed".into())
    })?;
    if expected_root.document_map_root_key != Some(documents[root_index].document_key)
        || expected_root.document_map_root_digest != root_digest
    {
        return Err(FrontierError::FrontierRegression);
    }
    for node in constructed.into_iter().flatten() {
        store_frontier_map_node(connection, &node)?;
    }
    Ok(())
}

/// Install one exact sequence-zero genesis frontier in an otherwise fresh
/// candidate, then construct its authenticated document map once.
///
/// The baseline is not an accepted operation. Requiring an empty physical
/// history and the canonical empty accepted-batch root prevents this primitive
/// from being used to rewrite an established frontier while still allowing a
/// higher layer to introduce a separately authenticated genesis binding.
pub fn seed_genesis_frontier_candidate(
    connection: &Connection,
    genesis_root: &PhysicalFrontierRoot,
    documents: &[PhysicalFrontierDocument],
) -> Result<(), FrontierError> {
    if connection.is_autocommit() {
        return Err(FrontierError::InvalidInput(
            "genesis frontier requires an active candidate-build transaction".into(),
        ));
    }
    let stored = read_frontier(connection)?;
    let stored_documents: i64 =
        connection.query_row("SELECT COUNT(*) FROM frontier_documents", [], |row| {
            row.get(0)
        })?;
    let stored_batches: i64 =
        connection.query_row("SELECT COUNT(*) FROM applied_batches", [], |row| row.get(0))?;
    let accepted_nodes: i64 =
        connection.query_row("SELECT COUNT(*) FROM accepted_batch_nodes", [], |row| {
            row.get(0)
        })?;
    let empty_map = authenticated_map_empty_digest();
    if stored.applied_batch_count != 0
        || stored_documents != 0
        || stored_batches != 0
        || accepted_nodes != 0
        || genesis_root.acceptance_sequence != 0
        || genesis_root.batch_map_root_key.is_some()
        || genesis_root.batch_map_root_digest != empty_map
    {
        return Err(FrontierError::InvalidInput(
            "genesis frontier requires a fresh sequence-zero physical history".into(),
        ));
    }
    connection.execute(
        "UPDATE frontier
         SET frontier_root = ?1, frontier_root_digest = ?2, applied_batch_count = 0
         WHERE singleton = 1",
        params![
            &genesis_root.canonical_bytes,
            genesis_root.digest().as_bytes().as_slice(),
        ],
    )?;
    if genesis_root.document_map_root_key.is_none()
        && genesis_root.document_map_root_digest == empty_map
    {
        if !documents.is_empty() {
            return Err(FrontierError::InvalidInput(
                "lazy genesis stores no baseline documents in the SQLite overlay".into(),
            ));
        }
        return Ok(());
    }
    seed_terminal_frontier_documents_candidate(connection, genesis_root, documents)
}

fn write_clock_node(connection: &Connection, node: &ClockNode) -> Result<MapLink, FrontierError> {
    let value_digest = causal_clock_counter_digest(node.peer, node.counter);
    let peer_key = AuthenticatedMapKey::from(node.peer);
    let digest = authenticated_map_node_digest(
        peer_key,
        value_digest,
        node.left.as_ref().map(|child| (child.key, child.digest)),
        node.right.as_ref().map(|child| (child.key, child.digest)),
    );
    let link = MapLink {
        key: peer_key,
        digest,
    };
    connection.execute(
        "INSERT OR IGNORE INTO causal_clock_nodes (
             node_digest, peer_id, counter, value_digest, left_peer_id, left_digest,
             right_peer_id, right_digest
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            digest.as_bytes().as_slice(),
            node.peer.as_slice(),
            i64::try_from(node.counter)
                .map_err(|_| FrontierError::Corrupt("causal counter exceeds SQLite".into()))?,
            value_digest.as_bytes().as_slice(),
            node.left.as_ref().map(|child| child.key.as_slice()),
            node.left
                .as_ref()
                .map(|child| child.digest.as_bytes().as_slice()),
            node.right.as_ref().map(|child| child.key.as_slice()),
            node.right
                .as_ref()
                .map(|child| child.digest.as_bytes().as_slice()),
        ],
    )?;
    let _ = load_clock_node(connection, &link)?;
    Ok(link)
}

fn upsert_causal_clock(
    connection: &Connection,
    root: Option<MapLink>,
    peer: [u8; 16],
    counter: u64,
    depth: usize,
) -> Result<MapLink, FrontierError> {
    ensure_depth(depth, "causal clock update")?;
    let Some(root) = root else {
        return write_clock_node(
            connection,
            &ClockNode {
                peer,
                counter,
                left: None,
                right: None,
            },
        );
    };
    let mut node = load_clock_node(connection, &root)?;
    match peer.cmp(&node.peer) {
        Ordering::Equal => {
            node.counter = node.counter.max(counter);
            write_clock_node(connection, &node)
        }
        Ordering::Less => {
            node.left = Some(upsert_causal_clock(
                connection,
                node.left.take(),
                peer,
                counter,
                depth + 1,
            )?);
            if node.left.as_ref().is_some_and(|left| {
                authenticated_map_priority_order(left.key, node.peer.into()).is_lt()
            }) {
                rotate_clock_right(connection, node)
            } else {
                write_clock_node(connection, &node)
            }
        }
        Ordering::Greater => {
            node.right = Some(upsert_causal_clock(
                connection,
                node.right.take(),
                peer,
                counter,
                depth + 1,
            )?);
            if node.right.as_ref().is_some_and(|right| {
                authenticated_map_priority_order(right.key, node.peer.into()).is_lt()
            }) {
                rotate_clock_left(connection, node)
            } else {
                write_clock_node(connection, &node)
            }
        }
    }
}

fn rotate_clock_right(
    connection: &Connection,
    mut node: ClockNode,
) -> Result<MapLink, FrontierError> {
    let left = node
        .left
        .take()
        .ok_or_else(|| FrontierError::Corrupt("causal clock rotation has no left child".into()))?;
    let mut left_node = load_clock_node(connection, &left)?;
    node.left = left_node.right.take();
    left_node.right = Some(write_clock_node(connection, &node)?);
    write_clock_node(connection, &left_node)
}

fn rotate_clock_left(
    connection: &Connection,
    mut node: ClockNode,
) -> Result<MapLink, FrontierError> {
    let right = node
        .right
        .take()
        .ok_or_else(|| FrontierError::Corrupt("causal clock rotation has no right child".into()))?;
    let mut right_node = load_clock_node(connection, &right)?;
    node.right = right_node.left.take();
    right_node.left = Some(write_clock_node(connection, &node)?);
    write_clock_node(connection, &right_node)
}

type ClockSplit = (Option<MapLink>, Option<u64>, Option<MapLink>);

fn union_clocks(
    connection: &Connection,
    left: Option<MapLink>,
    right: Option<MapLink>,
    depth: usize,
) -> Result<Option<MapLink>, FrontierError> {
    ensure_depth(depth, "causal clock union")?;
    let (left_link, right_link) = match (left, right) {
        (None, right) => return Ok(right),
        (left, None) => return Ok(left),
        (Some(left), Some(right)) => (left, right),
    };
    if left_link == right_link {
        return Ok(Some(left_link));
    }
    if left_link.key == right_link.key {
        let left_node = load_clock_node(connection, &left_link)?;
        let right_node = load_clock_node(connection, &right_link)?;
        return Ok(Some(write_clock_node(
            connection,
            &ClockNode {
                peer: left_node.peer,
                counter: left_node.counter.max(right_node.counter),
                left: union_clocks(connection, left_node.left, right_node.left, depth + 1)?,
                right: union_clocks(connection, left_node.right, right_node.right, depth + 1)?,
            },
        )?));
    }
    if authenticated_map_priority_order(left_link.key, right_link.key).is_lt() {
        let left_node = load_clock_node(connection, &left_link)?;
        let (less, counter, greater) = split_clock(
            connection,
            Some(right_link),
            key_as_id(left_link.key, "causal peer ID")?,
            depth + 1,
        )?;
        Ok(Some(write_clock_node(
            connection,
            &ClockNode {
                peer: left_node.peer,
                counter: left_node.counter.max(counter.unwrap_or(0)),
                left: union_clocks(connection, left_node.left, less, depth + 1)?,
                right: union_clocks(connection, left_node.right, greater, depth + 1)?,
            },
        )?))
    } else {
        let right_node = load_clock_node(connection, &right_link)?;
        let (less, counter, greater) = split_clock(
            connection,
            Some(left_link),
            key_as_id(right_link.key, "causal peer ID")?,
            depth + 1,
        )?;
        Ok(Some(write_clock_node(
            connection,
            &ClockNode {
                peer: right_node.peer,
                counter: right_node.counter.max(counter.unwrap_or(0)),
                left: union_clocks(connection, less, right_node.left, depth + 1)?,
                right: union_clocks(connection, greater, right_node.right, depth + 1)?,
            },
        )?))
    }
}

fn split_clock(
    connection: &Connection,
    root: Option<MapLink>,
    key: [u8; 16],
    depth: usize,
) -> Result<ClockSplit, FrontierError> {
    ensure_depth(depth, "causal clock split")?;
    let Some(link) = root else {
        return Ok((None, None, None));
    };
    let node = load_clock_node(connection, &link)?;
    match key.cmp(&node.peer) {
        Ordering::Equal => Ok((node.left, Some(node.counter), node.right)),
        Ordering::Less => {
            let (less, counter, greater_left) = split_clock(connection, node.left, key, depth + 1)?;
            let greater = write_clock_node(
                connection,
                &ClockNode {
                    left: greater_left,
                    ..node
                },
            )?;
            Ok((less, counter, Some(greater)))
        }
        Ordering::Greater => {
            let (less_right, counter, greater) =
                split_clock(connection, node.right, key, depth + 1)?;
            let less = write_clock_node(
                connection,
                &ClockNode {
                    right: less_right,
                    ..node
                },
            )?;
            Ok((Some(less), counter, greater))
        }
    }
}

fn derive_causal_clock_root(
    transaction: &Connection,
    sealed: Option<&dyn SealedAcceptedIndexRead>,
    root: &PhysicalFrontierRoot,
    batch: &PhysicalAcceptedBatch,
) -> Result<MapLink, FrontierError> {
    let mut clock = None;
    for parent in &batch.causal_dependency_heads {
        let record = authenticated_batch_record(transaction, sealed, root, *parent, None)?
            .ok_or(FrontierError::MissingDependency(*parent))?;
        clock = union_clocks(transaction, clock, Some(record.clock_link(transaction)?), 0)?;
    }
    let expected = causal_clock_lookup(transaction, clock.clone(), batch.causal_peer_id)?
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| FrontierError::Corrupt("causal counter overflowed".into()))?;
    if batch.causal_counter != expected {
        return Err(FrontierError::InvalidInput(format!(
            "accepted batch {} causal counter {} does not follow {}",
            HexId(&batch.batch_id),
            batch.causal_counter,
            expected.saturating_sub(1)
        )));
    }
    upsert_causal_clock(
        transaction,
        clock,
        batch.causal_peer_id,
        batch.causal_counter,
        0,
    )
}

fn write_batch_map_node(
    connection: &Connection,
    node: &BatchMapNode,
) -> Result<MapLink, FrontierError> {
    let batch_key = AuthenticatedMapKey::from(node.batch_id);
    let digest = authenticated_map_node_digest(
        batch_key,
        node.value_digest,
        node.left.as_ref().map(|child| (child.key, child.digest)),
        node.right.as_ref().map(|child| (child.key, child.digest)),
    );
    let link = MapLink {
        key: batch_key,
        digest,
    };
    connection.execute(
        "INSERT OR IGNORE INTO accepted_batch_nodes (
             node_digest, batch_id, value_digest, left_batch_id, left_digest,
             right_batch_id, right_digest
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            digest.as_bytes().as_slice(),
            node.batch_id.as_slice(),
            node.value_digest.as_bytes().as_slice(),
            node.left.as_ref().map(|child| child.key.as_slice()),
            node.left
                .as_ref()
                .map(|child| child.digest.as_bytes().as_slice()),
            node.right.as_ref().map(|child| child.key.as_slice()),
            node.right
                .as_ref()
                .map(|child| child.digest.as_bytes().as_slice()),
        ],
    )?;
    let _ = load_batch_map_node(connection, &link)?;
    Ok(link)
}

fn upsert_batch_map(
    connection: &Connection,
    sealed: Option<&dyn SealedAcceptedIndexRead>,
    root: Option<MapLink>,
    batch_id: [u8; 16],
    value_digest: ContentDigest,
    depth: usize,
) -> Result<MapLink, FrontierError> {
    ensure_depth(depth, "accepted batch map update")?;
    let Some(root) = root else {
        return write_batch_map_node(
            connection,
            &BatchMapNode {
                batch_id,
                value_digest,
                left: None,
                right: None,
            },
        );
    };
    let mut node = load_batch_map_node_for_apply(connection, sealed, &root)?;
    match batch_id.cmp(&node.batch_id) {
        Ordering::Equal => {
            node.value_digest = value_digest;
            write_batch_map_node(connection, &node)
        }
        Ordering::Less => {
            node.left = Some(upsert_batch_map(
                connection,
                sealed,
                node.left.take(),
                batch_id,
                value_digest,
                depth + 1,
            )?);
            if node.left.as_ref().is_some_and(|left| {
                authenticated_map_priority_order(left.key, node.batch_id.into()).is_lt()
            }) {
                rotate_batch_right(connection, sealed, node)
            } else {
                write_batch_map_node(connection, &node)
            }
        }
        Ordering::Greater => {
            node.right = Some(upsert_batch_map(
                connection,
                sealed,
                node.right.take(),
                batch_id,
                value_digest,
                depth + 1,
            )?);
            if node.right.as_ref().is_some_and(|right| {
                authenticated_map_priority_order(right.key, node.batch_id.into()).is_lt()
            }) {
                rotate_batch_left(connection, sealed, node)
            } else {
                write_batch_map_node(connection, &node)
            }
        }
    }
}

fn rotate_batch_right(
    connection: &Connection,
    sealed: Option<&dyn SealedAcceptedIndexRead>,
    mut node: BatchMapNode,
) -> Result<MapLink, FrontierError> {
    let left = node.left.take().ok_or_else(|| {
        FrontierError::Corrupt("accepted batch-map rotation has no left child".into())
    })?;
    let mut left_node = load_batch_map_node_for_apply(connection, sealed, &left)?;
    node.left = left_node.right.take();
    left_node.right = Some(write_batch_map_node(connection, &node)?);
    write_batch_map_node(connection, &left_node)
}

fn rotate_batch_left(
    connection: &Connection,
    sealed: Option<&dyn SealedAcceptedIndexRead>,
    mut node: BatchMapNode,
) -> Result<MapLink, FrontierError> {
    let right = node.right.take().ok_or_else(|| {
        FrontierError::Corrupt("accepted batch-map rotation has no right child".into())
    })?;
    let mut right_node = load_batch_map_node_for_apply(connection, sealed, &right)?;
    node.right = right_node.left.take();
    right_node.left = Some(write_batch_map_node(connection, &node)?);
    write_batch_map_node(connection, &right_node)
}

fn stored_matches_request(
    record: &StoredBatch,
    batch: &PhysicalAcceptedBatch,
) -> Result<bool, FrontierError> {
    validate_stored_batch_physical(record)?;
    Ok(record.batch_id == batch.batch_id
        && record.manifest_digest.as_slice() == batch.manifest_digest.as_bytes()
        && record.semantic_effect == batch.semantic_effect
        && record.semantic_effect_digest.as_slice() == batch.semantic_effect_digest.as_bytes()
        && record.dependency_frontier == batch.dependency_frontier
        && record.prior_frontier_root == batch.prior_frontier_root.canonical_bytes
        && record.post_frontier_root == batch.post_frontier_root.canonical_bytes
        && record.affected_documents == batch.affected_documents_bytes
        && record.causal_dependency_heads == batch.causal_dependency_heads_bytes
        && record.causal_peer_id.as_slice() == batch.causal_peer_id
        && u64::try_from(record.causal_counter).ok() == Some(batch.causal_counter)
        && u64::try_from(record.acceptance_sequence).ok() == Some(batch.acceptance_sequence)
        && u64::try_from(record.retained_bytes).ok() == Some(batch.retained_bytes))
}

fn insert_event(
    transaction: &Connection,
    batch: &PhysicalAcceptedBatch,
    clock_root: &MapLink,
) -> Result<(), FrontierError> {
    let sequence = i64::try_from(batch.acceptance_sequence)
        .map_err(|_| FrontierError::InvalidInput("acceptance sequence exceeds SQLite".into()))?;
    let causal_counter = i64::try_from(batch.causal_counter)
        .map_err(|_| FrontierError::InvalidInput("causal counter exceeds SQLite".into()))?;
    let retained_bytes = i64::try_from(batch.retained_bytes)
        .map_err(|_| FrontierError::InvalidInput("retained-byte count exceeds SQLite".into()))?;
    transaction.execute(
        "INSERT INTO applied_batches (
             sequence, batch_id, manifest_digest, semantic_effect, semantic_effect_digest,
             dependency_frontier, dependency_frontier_digest, prior_frontier_root,
             prior_frontier_root_digest, post_frontier_root, post_frontier_root_digest,
             affected_documents, affected_documents_digest, causal_dependency_heads,
             causal_peer_id, causal_counter, causal_clock_root_key, causal_clock_root_digest,
             acceptance_sequence, retained_bytes
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                   ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)",
        params![
            sequence,
            batch.batch_id.as_slice(),
            batch.manifest_digest.as_bytes().as_slice(),
            &batch.semantic_effect,
            batch.semantic_effect_digest.as_bytes().as_slice(),
            &batch.dependency_frontier,
            ContentDigest::of(&batch.dependency_frontier)
                .as_bytes()
                .as_slice(),
            &batch.prior_frontier_root.canonical_bytes,
            batch.prior_frontier_root.digest().as_bytes().as_slice(),
            &batch.post_frontier_root.canonical_bytes,
            batch.post_frontier_root.digest().as_bytes().as_slice(),
            &batch.affected_documents_bytes,
            ContentDigest::of(&batch.affected_documents_bytes)
                .as_bytes()
                .as_slice(),
            &batch.causal_dependency_heads_bytes,
            batch.causal_peer_id.as_slice(),
            causal_counter,
            clock_root.key.as_slice(),
            clock_root.digest.as_bytes().as_slice(),
            sequence,
            retained_bytes,
        ],
    )?;
    Ok(())
}

fn validate_root_shape(root: &PhysicalFrontierRoot) -> Result<(), FrontierError> {
    let empty_digest = authenticated_map_empty_digest();
    if (root.document_map_root_key.is_none() && root.document_map_root_digest != empty_digest)
        || (root.acceptance_sequence == 0) != root.batch_map_root_key.is_none()
        || (root.batch_map_root_key.is_none() && root.batch_map_root_digest != empty_digest)
    {
        return Err(FrontierError::InvalidInput(
            "physical frontier authenticated-map shape is inconsistent".into(),
        ));
    }
    Ok(())
}

fn validate_request_shape(request: &PhysicalApplyRequest) -> Result<(), FrontierError> {
    let batch = &request.batch;
    validate_root_shape(&batch.prior_frontier_root)?;
    validate_root_shape(&batch.post_frontier_root)?;
    if batch.acceptance_sequence == 0 || batch.causal_counter == 0 {
        return Err(FrontierError::InvalidInput(
            "physical accepted sequence and causal counter must be positive".into(),
        ));
    }
    if batch
        .affected_documents
        .windows(2)
        .any(|pair| pair[0].document_key >= pair[1].document_key)
        || batch
            .causal_dependency_heads
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err(FrontierError::InvalidInput(
            "physical affected documents or dependencies are not sorted unique".into(),
        ));
    }
    if request.materialization.is_some() != request.materialization_input_digest.is_some() {
        return Err(FrontierError::InvalidInput(
            "physical materialization and its digest must be supplied together".into(),
        ));
    }
    if request
        .materialization
        .as_ref()
        .is_some_and(|change| change.batch_id != batch.batch_id)
    {
        return Err(FrontierError::InvalidInput(
            "physical materialization is bound to another batch".into(),
        ));
    }
    Ok(())
}

/// The parts of an installed `checkpoint_generation_anchor` that apply and
/// reopen validation need. Reading it is one row; an ordinary live database has
/// no such row and gets `None`.
struct CheckpointAnchorFacts {
    covered_count: u64,
    covered_batch_root: Option<MapLink>,
    checkpoint_frontier_root: Vec<u8>,
    checkpoint_frontier_root_digest: ContentDigest,
    materialization_frontier_root_digest: ContentDigest,
}

fn load_checkpoint_anchor_facts(
    connection: &Connection,
) -> Result<Option<CheckpointAnchorFacts>, FrontierError> {
    let row = connection
        .query_row(
            "SELECT covered_count, covered_batch_root_key, covered_batch_root_digest,
                    checkpoint_frontier_root, checkpoint_frontier_root_digest,
                    materialization_frontier_root_digest
             FROM checkpoint_generation_anchor WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((count, root_key, root_digest, frontier, frontier_digest, materialization)) = row
    else {
        return Ok(None);
    };
    let covered_batch_root_digest = decode_digest(&root_digest)?;
    let covered_batch_root = match root_key {
        Some(key) => Some(MapLink {
            key: decode_id(&key, "covered batch root key")?.into(),
            digest: covered_batch_root_digest,
        }),
        None => None,
    };
    Ok(Some(CheckpointAnchorFacts {
        covered_count: u64::try_from(count)
            .map_err(|_| FrontierError::Corrupt("covered count is invalid".into()))?,
        covered_batch_root,
        checkpoint_frontier_root: frontier,
        checkpoint_frontier_root_digest: decode_digest(&frontier_digest)?,
        materialization_frontier_root_digest: decode_digest(&materialization)?,
    }))
}

/// Is `batch_id` in the SEALED covered map -- as opposed to merely in the active
/// map, which also holds the hot tail?
///
/// The distinction is the whole point. An id that is in the active map but has
/// no `applied_batches` row is EITHER a covered batch whose row left SQLite at
/// the cutover, OR a hot batch whose row was lost. The first is an ordinary
/// re-delivery; the second is corruption, and reporting it as a re-delivery
/// would hide it. Only a descent of the anchor's covered root can tell them
/// apart.
fn is_covered_batch(
    connection: &Connection,
    sealed: &dyn SealedAcceptedIndexRead,
    anchor: &CheckpointAnchorFacts,
    batch_id: [u8; 16],
) -> Result<bool, FrontierError> {
    let Some(root) = anchor.covered_batch_root.clone() else {
        return Ok(false);
    };
    let mut current = Some(root);
    let mut depth = 0;
    while let Some(link) = current {
        ensure_depth(depth, "covered accepted batch lookup")?;
        let node = load_composite_batch_map_node(connection, sealed, &link)?;
        match batch_id.cmp(&node.batch_id) {
            Ordering::Equal => return Ok(true),
            Ordering::Less => current = node.left,
            Ordering::Greater => current = node.right,
        }
        depth += 1;
    }
    Ok(false)
}

/// Refuse a batch whose id the frontier ALREADY CONTAINS but whose exact record
/// is not in SQLite.
///
/// This runs unconditionally on the checkpoint path, before the sequence check
/// and before insertion -- not, as it first did, only on the branch where the
/// sequence was already wrong. That earlier placement was free on the healthy
/// path and wrong: a covered id offered at exactly `current + 1` skipped the
/// probe entirely, `upsert_batch_map` took its `Ordering::Equal` arm, and the
/// covered key's value was REPLACED. The batch that had been accepted was erased
/// from the authenticated map while the sequence advanced, which is precisely
/// the state the sealed three-root count check would later reject -- after
/// SQLite had committed. Verified by probe before this guard existed.
fn guard_batch_already_accepted(
    connection: &Connection,
    sealed: Option<&dyn SealedAcceptedIndexRead>,
    current_root: &PhysicalFrontierRoot,
    batch_id: [u8; 16],
) -> Result<(), FrontierError> {
    let Some(sealed) = sealed else {
        return Ok(());
    };
    if batch_map_value(connection, Some(sealed), current_root, batch_id)?.is_none() {
        return Ok(());
    }
    let Some(anchor) = load_checkpoint_anchor_facts(connection)? else {
        return Ok(());
    };
    if is_covered_batch(connection, sealed, &anchor, batch_id)? {
        return Err(FrontierError::CoveredBatchRedelivery(batch_id));
    }
    Err(FrontierError::Corrupt(format!(
        "authenticated accepted batch {} is missing its exact record",
        HexId(&batch_id)
    )))
}

pub fn preflight(
    connection: &Connection,
    current_root: &PhysicalFrontierRoot,
    request: &PhysicalApplyRequest,
) -> Result<PreflightDisposition, FrontierError> {
    preflight_inner(connection, None, current_root, request)
}

/// [`preflight`] for an anchored candidate, resolving covered ancestors through
/// the sealed accepted index.
///
/// Preflight shares the apply path's covered-`load_batch` shape, so it needs the
/// same treatment: on an anchored candidate a re-delivered covered batch has no
/// row, and answering "is this new?" from the row alone reports New for a batch
/// the frontier already contains.
pub fn preflight_checkpoint(
    connection: &Connection,
    sealed: &dyn SealedAcceptedIndexRead,
    current_root: &PhysicalFrontierRoot,
    request: &PhysicalApplyRequest,
) -> Result<PreflightDisposition, FrontierError> {
    preflight_inner(connection, Some(sealed), current_root, request)
}

fn preflight_inner(
    connection: &Connection,
    sealed: Option<&dyn SealedAcceptedIndexRead>,
    current_root: &PhysicalFrontierRoot,
    request: &PhysicalApplyRequest,
) -> Result<PreflightDisposition, FrontierError> {
    validate_request_shape(request)?;
    let batch = &request.batch;
    let stored_frontier = read_frontier(connection)?;
    if stored_frontier.canonical_bytes != current_root.canonical_bytes
        || stored_frontier.digest != current_root.digest()
        || stored_frontier.applied_batch_count != current_root.acceptance_sequence
    {
        return Err(FrontierError::Corrupt(
            "physical frontier row differs from the validated current root".into(),
        ));
    }
    if let Some(existing) = load_batch(connection, batch.batch_id)? {
        let clock_root = MapLink {
            key: decode_id(&existing.causal_clock_root_key, "causal clock root key")?.into(),
            digest: decode_digest(&existing.causal_clock_root_digest)?,
        };
        if authenticated_batch_record(connection, sealed, current_root, batch.batch_id, None)?
            .is_none()
        {
            return Err(FrontierError::Corrupt(format!(
                "stored batch {} is absent from the authenticated accepted map",
                HexId(&batch.batch_id)
            )));
        }
        if stored_matches_request(&existing, batch)? {
            if batch_map_value(connection, sealed, current_root, batch.batch_id)?
                != Some(accepted_batch_causal_record_digest(batch, &clock_root))
            {
                return Err(FrontierError::Corrupt(format!(
                    "accepted batch {} differs from its authenticated causal record",
                    HexId(&batch.batch_id)
                )));
            }
            if current_root.acceptance_sequence >= batch.acceptance_sequence
                && current_root.state_digest != batch.prior_frontier_root.state_digest
            {
                return Ok(PreflightDisposition::Duplicate);
            }
            return Err(FrontierError::FrontierRegression);
        }
        return Err(FrontierError::BatchCollision(batch.batch_id));
    }
    guard_batch_already_accepted(connection, sealed, current_root, batch.batch_id)?;
    for dependency in &batch.causal_dependency_heads {
        if authenticated_batch_record(connection, sealed, current_root, *dependency, None)?
            .is_none()
        {
            return Err(FrontierError::MissingDependency(*dependency));
        }
    }
    let expected = current_root
        .acceptance_sequence
        .checked_add(1)
        .ok_or_else(|| FrontierError::Corrupt("applied batch sequence overflowed".into()))?;
    if batch.acceptance_sequence != expected {
        return Err(FrontierError::AcceptanceOrder {
            expected,
            found: batch.acceptance_sequence,
        });
    }
    if current_root != &batch.prior_frontier_root
        || batch.post_frontier_root.acceptance_sequence != batch.acceptance_sequence
    {
        return Err(FrontierError::FrontierRegression);
    }
    if request.materialization.is_some() {
        sqlite_materialization::ensure_stamp(
            connection,
            current_root.acceptance_sequence,
            current_root.digest(),
        )?;
    }
    Ok(PreflightDisposition::New)
}

pub fn apply(
    connection: &mut Connection,
    current_root: &PhysicalFrontierRoot,
    request: &PhysicalApplyRequest,
) -> Result<ApplyResult, FrontierError> {
    apply_with_transaction_policy(connection, None, current_root, request, false, true)
}

/// Apply one transition inside a candidate-build transaction already owned by
/// the caller. This is deliberately separate from [`apply`]: ordinary live
/// applies continue to own and durably commit exactly one transaction each.
pub fn apply_candidate(
    connection: &mut Connection,
    current_root: &PhysicalFrontierRoot,
    request: &PhysicalApplyRequest,
) -> Result<ApplyResult, FrontierError> {
    apply_with_transaction_policy(connection, None, current_root, request, true, true)
}

/// Apply one authenticated accepted-prefix transition while constructing a
/// fresh terminal candidate, without incrementally persisting its document
/// treap. The caller must seed and authenticate the exact terminal document
/// frontier with [`seed_terminal_frontier_documents_candidate`] before the
/// candidate can pass its closing proof and commit.
pub fn apply_terminal_prefix_candidate(
    connection: &mut Connection,
    current_root: &PhysicalFrontierRoot,
    request: &PhysicalApplyRequest,
) -> Result<ApplyResult, FrontierError> {
    guard_terminal_prefix_candidate(connection, request)?;
    apply_with_transaction_policy(connection, None, current_root, request, true, false)
}

/// The two preconditions a terminal-prefix candidate apply owns, shared by the
/// live and anchored entry points so they cannot drift apart.
fn guard_terminal_prefix_candidate(
    connection: &Connection,
    request: &PhysicalApplyRequest,
) -> Result<(), FrontierError> {
    if request.materialization.is_some() || request.materialization_input_digest.is_some() {
        return Err(FrontierError::InvalidInput(
            "terminal prefix candidate accepts history without materialization".into(),
        ));
    }
    let stored_documents: i64 =
        connection.query_row("SELECT COUNT(*) FROM frontier_documents", [], |row| {
            row.get(0)
        })?;
    if stored_documents != 0 {
        return Err(FrontierError::InvalidInput(
            "terminal prefix candidate requires an unseeded document frontier".into(),
        ));
    }
    Ok(())
}

/// Apply one transition into an ANCHORED candidate, resolving covered history
/// through the injected sealed accepted index.
///
/// This is [`apply_candidate`] for a database whose covered history left SQLite
/// at a generation cutover. Without the reader the first tail apply fails at
/// `load_batch_map_node` — `validate_checkpoint_anchor_input` requires the
/// installed root to name the covered batch-map root, so the walk starts covered
/// by construction and there is no empty-tree option.
///
/// The reader is consulted only where a node is genuinely absent from SQLite,
/// and the nodes on the descent path are then written back as ORDINARY HOT
/// ROWS. Those rows are NOT copies of the sealed nodes: a path node has one
/// child link changed, so it is a structurally new node at a new digest, and a
/// rotation rewrites its participants the same way. Nodes OFF the path are not
/// touched at all and stay reachable only as sealed links.
///
/// What is preserved is the tree, not the bytes. Node priority folds the KEY
/// alone (`authenticated_map_priority_order`), and the sealed writer folds it
/// the same way, so for a given key/value set the treap is canonical: the
/// resulting root digest equals a from-empty replay of the same history
/// regardless of insertion order.
///
/// Cost is one descent: O(height), expected O(log n) under digest-derived
/// priorities, and hard-refused above the 256-node depth ceiling
/// (`MAX_MAP_DEPTH`) — plus one composite lookup per causal dependency head.
pub fn apply_checkpoint_candidate(
    connection: &mut Connection,
    sealed: &dyn SealedAcceptedIndexRead,
    current_root: &PhysicalFrontierRoot,
    request: &PhysicalApplyRequest,
) -> Result<ApplyResult, FrontierError> {
    apply_with_transaction_policy(connection, Some(sealed), current_root, request, true, true)
}

/// [`apply`] for a LIVE anchored database, resolving covered history through
/// the sealed accepted index.
///
/// Candidate variants alone are not enough. Once an anchored candidate is
/// promoted, its hot batch-map root still carries off-path child links into
/// sealed covered nodes, so the next ordinary live apply meets exactly the same
/// absent node — at a causal dependency head, or during the insertion descent.
/// Calling [`apply`] there (`sealed = None`) reproduces the failure this whole
/// seam exists to remove.
pub fn apply_checkpoint(
    connection: &mut Connection,
    sealed: &dyn SealedAcceptedIndexRead,
    current_root: &PhysicalFrontierRoot,
    request: &PhysicalApplyRequest,
) -> Result<ApplyResult, FrontierError> {
    apply_with_transaction_policy(connection, Some(sealed), current_root, request, false, true)
}

/// [`apply_terminal_prefix_candidate`] for an anchored candidate. See
/// [`apply_checkpoint_candidate`] for what the reader is for.
pub fn apply_checkpoint_terminal_prefix_candidate(
    connection: &mut Connection,
    sealed: &dyn SealedAcceptedIndexRead,
    current_root: &PhysicalFrontierRoot,
    request: &PhysicalApplyRequest,
) -> Result<ApplyResult, FrontierError> {
    guard_terminal_prefix_candidate(connection, request)?;
    apply_with_transaction_policy(connection, Some(sealed), current_root, request, true, false)
}

fn apply_with_transaction_policy(
    connection: &mut Connection,
    sealed: Option<&dyn SealedAcceptedIndexRead>,
    current_root: &PhysicalFrontierRoot,
    request: &PhysicalApplyRequest,
    candidate_build: bool,
    apply_frontier_documents: bool,
) -> Result<ApplyResult, FrontierError> {
    validate_request_shape(request)?;
    let batch = &request.batch;
    let stored_frontier = read_frontier(connection)?;
    if stored_frontier.canonical_bytes != current_root.canonical_bytes
        || stored_frontier.digest != current_root.digest()
        || stored_frontier.applied_batch_count != current_root.acceptance_sequence
    {
        return Err(FrontierError::Corrupt(
            "physical frontier row differs from the validated current root".into(),
        ));
    }
    if let Some(existing) = load_batch(connection, batch.batch_id)? {
        let existing_clock_root = MapLink {
            key: decode_id(&existing.causal_clock_root_key, "causal clock root key")?.into(),
            digest: decode_digest(&existing.causal_clock_root_digest)?,
        };
        let authenticated =
            authenticated_batch_record(connection, sealed, current_root, batch.batch_id, None)?;
        if authenticated.is_none() {
            return Err(FrontierError::Corrupt(format!(
                "stored batch {} is absent from the authenticated accepted map",
                HexId(&batch.batch_id)
            )));
        }
        if stored_matches_request(&existing, batch)? {
            if batch_map_value(connection, sealed, current_root, batch.batch_id)?
                != Some(accepted_batch_causal_record_digest(
                    batch,
                    &existing_clock_root,
                ))
            {
                return Err(FrontierError::Corrupt(format!(
                    "accepted batch {} differs from its authenticated causal record",
                    HexId(&batch.batch_id)
                )));
            }
            if current_root.acceptance_sequence >= batch.acceptance_sequence
                && current_root.state_digest != batch.prior_frontier_root.state_digest
            {
                if let Some(input_digest) = request.materialization_input_digest {
                    sqlite_materialization::ensure_stamp(
                        connection,
                        batch.acceptance_sequence,
                        batch.post_frontier_root.digest(),
                    )?;
                    if sqlite_materialization::recorded_digest(
                        connection,
                        batch.acceptance_sequence,
                    )? != Some(input_digest)
                    {
                        return Err(FrontierError::MaterializationCollision(batch.batch_id));
                    }
                }
                return Ok(ApplyResult {
                    disposition: ApplyDisposition::Duplicate,
                    materialization: ApplyChangeInstrumentation::default(),
                });
            }
            return Err(FrontierError::FrontierRegression);
        }
        return Err(FrontierError::BatchCollision(batch.batch_id));
    }
    guard_batch_already_accepted(connection, sealed, current_root, batch.batch_id)?;

    for dependency in &batch.causal_dependency_heads {
        if authenticated_batch_record(connection, sealed, current_root, *dependency, None)?
            .is_none()
        {
            return Err(FrontierError::MissingDependency(*dependency));
        }
    }
    let expected_sequence = current_root
        .acceptance_sequence
        .checked_add(1)
        .ok_or_else(|| FrontierError::Corrupt("applied batch sequence overflowed".into()))?;
    if batch.acceptance_sequence != expected_sequence {
        return Err(FrontierError::AcceptanceOrder {
            expected: expected_sequence,
            found: batch.acceptance_sequence,
        });
    }
    if current_root != &batch.prior_frontier_root
        || batch.post_frontier_root.acceptance_sequence != batch.acceptance_sequence
    {
        return Err(FrontierError::FrontierRegression);
    }
    if request.materialization.is_some() {
        sqlite_materialization::ensure_stamp(
            connection,
            current_root.acceptance_sequence,
            current_root.digest(),
        )?;
    }

    if candidate_build {
        if connection.is_autocommit() {
            return Err(FrontierError::InvalidInput(
                "candidate apply requires an active candidate-build transaction".into(),
            ));
        }
        return apply_in_open_transaction(
            OpenApplyTransaction::Candidate(connection),
            sealed,
            current_root,
            request,
            &stored_frontier,
            apply_frontier_documents,
        );
    }
    if !connection.is_autocommit() {
        return Err(FrontierError::InvalidInput(
            "ordinary apply cannot join a caller-owned transaction".into(),
        ));
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let result = apply_in_open_transaction(
        OpenApplyTransaction::Ordinary(&transaction),
        sealed,
        current_root,
        request,
        &stored_frontier,
        true,
    )?;
    transaction.commit()?;
    Ok(result)
}

enum OpenApplyTransaction<'transaction, 'connection> {
    Ordinary(&'transaction Transaction<'connection>),
    Candidate(&'transaction Connection),
}

impl OpenApplyTransaction<'_, '_> {
    fn connection(&self) -> &Connection {
        match self {
            Self::Ordinary(transaction) => transaction,
            Self::Candidate(connection) => connection,
        }
    }
}

fn apply_in_open_transaction(
    transaction: OpenApplyTransaction<'_, '_>,
    sealed: Option<&dyn SealedAcceptedIndexRead>,
    current_root: &PhysicalFrontierRoot,
    request: &PhysicalApplyRequest,
    stored_frontier: &StoredFrontier,
    apply_frontier_documents: bool,
) -> Result<ApplyResult, FrontierError> {
    let connection = transaction.connection();
    let batch = &request.batch;
    // Re-read after BEGIN IMMEDIATE so every physical premise is protected by the write
    // transaction even if this API is used without Tine's higher-level single-writer lease.
    let transaction_frontier = read_frontier(connection)?;
    if &transaction_frontier != stored_frontier {
        return Err(FrontierError::Corrupt(
            "physical frontier changed while beginning apply".into(),
        ));
    }
    let clock_root = derive_causal_clock_root(connection, sealed, current_root, batch)?;
    let causal_record_digest = accepted_batch_causal_record_digest(batch, &clock_root);
    let post_batch_root = upsert_batch_map(
        connection,
        sealed,
        current_root.batch_map_root_key.map(|key| MapLink {
            key: key.into(),
            digest: current_root.batch_map_root_digest,
        }),
        batch.batch_id,
        causal_record_digest,
        0,
    )?;
    if batch.post_frontier_root.batch_map_root_key
        != Some(key_as_id(post_batch_root.key, "accepted batch ID")?)
        || batch.post_frontier_root.batch_map_root_digest != post_batch_root.digest
    {
        return Err(FrontierError::FrontierRegression);
    }
    insert_event(connection, batch, &clock_root)?;
    match request.fault {
        ApplyFault::ReturnAfterInsert => return Err(FrontierError::InjectedFailure),
        ApplyFault::AbortAfterInsert => std::process::abort(),
        _ => {}
    }

    if apply_frontier_documents {
        let mut document_root = current_root.document_map_root_key.map(|key| MapLink {
            key,
            digest: current_root.document_map_root_digest,
        });
        let mut new_documents = 0_u64;
        for document in &batch.affected_documents {
            let (root, inserted) = upsert_frontier_map(connection, document_root, document, 0)?;
            document_root = Some(root);
            new_documents = new_documents.saturating_add(u64::from(inserted));
        }
        let (document_key, document_digest) = document_root
            .map(|root| (Some(root.key), root.digest))
            .unwrap_or((None, current_root.document_map_root_digest));
        if batch.post_frontier_root.document_count < current_root.document_count
            || batch.post_frontier_root.document_count
                > current_root.document_count.saturating_add(new_documents)
            || batch.post_frontier_root.document_map_root_key != document_key
            || batch.post_frontier_root.document_map_root_digest != document_digest
        {
            return Err(FrontierError::FrontierRegression);
        }
    }

    let mut materialization_stats = ApplyChangeInstrumentation::default();
    if let (Some(change), Some(input_digest)) = (
        request.materialization.as_ref(),
        request.materialization_input_digest,
    ) {
        materialization_stats = match &transaction {
            OpenApplyTransaction::Ordinary(transaction) => sqlite_materialization::apply_change(
                transaction,
                change,
                batch.acceptance_sequence,
                input_digest,
                batch.post_frontier_root.digest(),
            )?,
            OpenApplyTransaction::Candidate(transaction) => {
                sqlite_materialization::apply_change_in_open_candidate(
                    transaction,
                    change,
                    batch.acceptance_sequence,
                    input_digest,
                    batch.post_frontier_root.digest(),
                )?
            }
        };
    }
    if matches!(request.fault, ApplyFault::ReturnAfterMaterialization) {
        return Err(FrontierError::InjectedFailure);
    }
    connection.execute(
        "UPDATE frontier SET frontier_root = ?1, frontier_root_digest = ?2,
                             applied_batch_count = ?3 WHERE singleton = 1",
        params![
            &batch.post_frontier_root.canonical_bytes,
            batch.post_frontier_root.digest().as_bytes().as_slice(),
            i64::try_from(batch.acceptance_sequence).map_err(|_| FrontierError::Corrupt(
                "applied batch sequence exceeds SQLite".into()
            ))?,
        ],
    )?;
    Ok(ApplyResult {
        disposition: ApplyDisposition::Applied,
        materialization: materialization_stats,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    use crate::sealed_accepted_index::{
        authenticated_map_root, AuthenticatedMapRootV1, SealedAcceptedCausalClockEntryV2,
        SealedAcceptedIndexObjectStore, SealedAcceptedIndexReader, SealedAcceptedIndexWriter,
        SealedAcceptedObjectKind,
    };
    use crate::sqlite::{
        PhysicalEntityId, PhysicalReferencePosting, PhysicalReferenceTarget, PhysicalSqliteDatabase,
    };
    use crate::sqlite_materialization::test_parse_config_hash;

    use super::*;

    static NEXT_DATABASE: AtomicU64 = AtomicU64::new(1);

    #[derive(Default)]
    struct TestSealedStore {
        objects: Vec<(SealedAcceptedObjectKind, ContentDigest, Vec<u8>)>,
    }

    impl SealedAcceptedIndexObjectStore for TestSealedStore {
        fn read_sealed_accepted_object(
            &self,
            kind: SealedAcceptedObjectKind,
            address: ContentDigest,
        ) -> Result<Option<Vec<u8>>, SealedAcceptedIndexError> {
            Ok(self
                .objects
                .iter()
                .find(|(found_kind, found_address, _)| {
                    *found_kind == kind && *found_address == address
                })
                .map(|(_, _, bytes)| bytes.clone()))
        }

        fn publish_sealed_accepted_object(
            &mut self,
            kind: SealedAcceptedObjectKind,
            address: ContentDigest,
            bytes: &[u8],
        ) -> Result<(), SealedAcceptedIndexError> {
            if let Some((_, _, existing)) =
                self.objects.iter().find(|(found_kind, found_address, _)| {
                    *found_kind == kind && *found_address == address
                })
            {
                if existing.as_slice() != bytes {
                    return Err(SealedAcceptedIndexError::Store(
                        "test object collision".into(),
                    ));
                }
                return Ok(());
            }
            self.objects.push((kind, address, bytes.to_vec()));
            Ok(())
        }
    }

    struct TestDatabase {
        path: PathBuf,
    }

    impl TestDatabase {
        fn new() -> Self {
            let serial = NEXT_DATABASE.fetch_add(1, AtomicOrdering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "tine-storage-frontier-{}-{serial}.sqlite3",
                std::process::id()
            ));
            Self { path }
        }

        fn create() -> (Self, Connection) {
            let database = Self::new();
            let connection = Connection::open(&database.path).unwrap();
            connection
                .execute_batch(
                    "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = FULL;
                 PRAGMA foreign_keys = ON;
                 PRAGMA trusted_schema = OFF;",
                )
                .unwrap();
            (database, connection)
        }

        fn reopen(&self) -> Connection {
            let connection = Connection::open(&self.path).unwrap();
            connection
                .execute_batch(
                    "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = FULL;
                 PRAGMA foreign_keys = ON;
                 PRAGMA trusted_schema = OFF;",
                )
                .unwrap();
            connection
        }
    }

    impl Drop for TestDatabase {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
            let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
        }
    }

    fn id(value: u128) -> [u8; 16] {
        value.to_be_bytes()
    }

    fn key(value: u128) -> AuthenticatedMapKey {
        AuthenticatedMapKey::from(id(value))
    }

    /// The reference Cartesian root, over general keys.
    fn map_root(
        entries: &[(AuthenticatedMapKey, ContentDigest)],
    ) -> (Option<AuthenticatedMapKey>, ContentDigest) {
        if entries.is_empty() {
            return (None, authenticated_map_empty_digest());
        }
        let root_index = (0..entries.len())
            .min_by(|left, right| {
                authenticated_map_priority_order(entries[*left].0, entries[*right].0)
            })
            .unwrap();
        let (key, value) = entries[root_index];
        let (left_key, left_digest) = map_root(&entries[..root_index]);
        let (right_key, right_digest) = map_root(&entries[root_index + 1..]);
        let digest = authenticated_map_node_digest(
            key,
            value,
            left_key.map(|child| (child, left_digest)),
            right_key.map(|child| (child, right_digest)),
        );
        (Some(key), digest)
    }

    /// Same root, projected back to the 16-byte identity the batch map keeps.
    fn batch_map_root(entries: &[([u8; 16], ContentDigest)]) -> (Option<[u8; 16]>, ContentDigest) {
        let widened = entries
            .iter()
            .map(|(id, digest)| (AuthenticatedMapKey::from(*id), *digest))
            .collect::<Vec<_>>();
        let (root_key, digest) = map_root(&widened);
        (
            root_key.map(|key| key_as_id(key, "test batch ID").unwrap()),
            digest,
        )
    }

    fn root(
        sequence: u64,
        documents: &[PhysicalFrontierDocument],
        batches: &[([u8; 16], ContentDigest)],
    ) -> PhysicalFrontierRoot {
        let document_entries = documents
            .iter()
            .map(|document| {
                (
                    document.document_key,
                    ContentDigest::of(&document.canonical_bytes),
                )
            })
            .collect::<Vec<_>>();
        let (document_map_root_key, document_map_root_digest) = map_root(&document_entries);
        let (batch_map_root_key, batch_map_root_digest) = batch_map_root(batches);
        let mut canonical_bytes = b"synthetic physical frontier\0".to_vec();
        canonical_bytes.extend_from_slice(&sequence.to_be_bytes());
        canonical_bytes.extend_from_slice(document_map_root_digest.as_bytes());
        canonical_bytes.extend_from_slice(batch_map_root_digest.as_bytes());
        PhysicalFrontierRoot {
            canonical_bytes,
            acceptance_sequence: sequence,
            document_count: documents.len() as u64,
            document_map_root_key,
            document_map_root_digest,
            batch_map_root_key,
            batch_map_root_digest,
            state_digest: ContentDigest::of(&sequence.to_be_bytes()),
        }
    }

    fn empty_checkpoint_root_and_anchor() -> (
        PhysicalCheckpointFrontierRoot,
        PhysicalCheckpointGenerationAnchor,
    ) {
        let empty = authenticated_map_empty_digest();
        let generation = PhysicalCheckpointGenerationBinding {
            generation_id: id(2),
            predecessor_generation_id: Some(id(1)),
            full_anchor_generation_id: id(2),
            covered_count: 0,
            covered_document_count: 0,
            covered_block_count: 0,
            covered_retained_bytes_total: 0,
            covered_semantic_capsules_root_digest: ContentDigest::of(b"capsules"),
            covered_batch_root_key: None,
            covered_batch_root_digest: empty,
            covered_status_root_key: None,
            covered_status_root_digest: empty,
            covered_sequence_root_digest: None,
            covered_sequence_height: 0,
            covered_causal_tip_root_key: None,
            covered_causal_tip_root_digest: empty,
            covered_head_facts_root_digest: ContentDigest::of(b"heads"),
            current_projection_payload_pins_root_digest: ContentDigest::of(b"payloads"),
            nonlinear_state_root_digest: ContentDigest::of(b"nonlinear"),
            retention_pins_root_digest: ContentDigest::of(b"retention"),
        };
        let mut canonical_bytes = b"synthetic physical checkpoint frontier\0".to_vec();
        canonical_bytes.extend_from_slice(generation.generation_id.as_slice());
        let root = PhysicalCheckpointFrontierRoot {
            canonical_bytes: canonical_bytes.clone(),
            acceptance_sequence: 0,
            document_count: 0,
            document_overlay_count: 0,
            retained_bytes_total: 0,
            document_map_root_key: None,
            document_map_root_digest: empty,
            batch_map_root_key: None,
            batch_map_root_digest: empty,
            batch_map_count: 0,
            status_map_root_key: None,
            status_map_root_digest: empty,
            status_map_count: 0,
            sequence_root_digest: None,
            sequence_height: 0,
            sequence_count: 0,
            generation: generation.clone(),
            state_digest: ContentDigest::of(b"checkpoint-state"),
        };
        let anchor = PhysicalCheckpointGenerationAnchor {
            generation,
            checkpoint_frontier_root: canonical_bytes,
            terminal_batch_id: None,
            terminal_evidence_digest: None,
            materialization_frontier_root_digest: root.digest(),
        };
        (root, anchor)
    }

    fn claim() -> PhysicalClaim {
        PhysicalClaim {
            workspace_id: id(1),
            lineage_digest: ContentDigest::of(b"lineage"),
            oplog_protocol_version: 7,
            operation_schema_version: 8,
            object_envelope_schema_version: 9,
            manifest_encoding_version: 10,
            managed_entity_set_version: 11,
        }
    }

    fn materialization(batch_id: [u8; 16]) -> PhysicalMaterializationChange {
        let page_id = batch_id;
        PhysicalMaterializationChange {
            batch_id,
            replacements: vec![sqlite_materialization::PhysicalPage {
                page_id,
                query_page_order: None,
                home_document_id: id(50),
                name: format!("page-{}", u128::from_be_bytes(batch_id)),
                name_key: format!("page-{}", u128::from_be_bytes(batch_id)),
                path: format!("pages/{}.md", u128::from_be_bytes(batch_id)),
                text_kind: 0,
                journal_day: None,
                preamble: None,
                searchable_text: "frontier materialization".into(),
                normalized_searchable_text: "frontier materialization".into(),
                references: Vec::new(),
                properties: Vec::new(),
                tags: Vec::new(),
                property_atoms: Vec::new(),
                blocks: Vec::new(),
            }],
            deletions: Vec::new(),
            pages_with_live_metadata_delta: BTreeSet::from([page_id]),
            derived_reference_postings: Vec::new(),
            derived_aliases: Vec::new(),
            portable_path_claims: Vec::new(),
            block_home_claims: Vec::new(),
            page_name_identity_records: Vec::new(),
            portable_path_identity_records: Vec::new(),
            logseq_uuid_introductions: Vec::new(),
        }
    }

    fn request(
        prior: &PhysicalFrontierRoot,
        sequence: u64,
        dependency: Option<[u8; 16]>,
        documents: Vec<PhysicalFrontierDocument>,
        prior_batch_entries: &[([u8; 16], ContentDigest)],
        materialized: bool,
    ) -> (PhysicalApplyRequest, Vec<([u8; 16], ContentDigest)>) {
        let batch_id = id(100 + sequence as u128);
        let peer = id(900);
        let dependencies = dependency.into_iter().collect::<Vec<_>>();
        let mut batch = PhysicalAcceptedBatch {
            batch_id,
            manifest_digest: ContentDigest::of(&[b'm', sequence as u8]),
            event_binding_digest: ContentDigest::of(&[b'e', sequence as u8]),
            semantic_effect: vec![b's', sequence as u8],
            semantic_effect_digest: ContentDigest::of(&[b's', sequence as u8]),
            dependency_frontier: vec![b'd', sequence as u8],
            prior_frontier_root: prior.clone(),
            post_frontier_root: prior.clone(),
            affected_documents: documents.clone(),
            affected_documents_bytes: documents
                .iter()
                .flat_map(|document| {
                    document
                        .document_key
                        .as_slice()
                        .iter()
                        .copied()
                        .chain(document.canonical_bytes.iter().copied())
                })
                .collect(),
            causal_dependency_heads: dependencies.clone(),
            causal_dependency_heads_bytes: dependencies.iter().flatten().copied().collect(),
            causal_peer_id: peer,
            causal_counter: sequence,
            acceptance_sequence: sequence,
            retained_bytes: sequence * 10,
        };
        let clock_value = causal_clock_counter_digest(peer, sequence);
        let clock_root = MapLink {
            key: peer.into(),
            digest: authenticated_map_node_digest(peer.into(), clock_value, None, None),
        };
        let record_digest = accepted_batch_causal_record_digest(&batch, &clock_root);
        let mut batch_entries = prior_batch_entries.to_vec();
        batch_entries.push((batch_id, record_digest));
        batch_entries.sort_unstable_by_key(|entry| entry.0);
        batch.post_frontier_root = root(sequence, &documents, &batch_entries);
        let materialization = materialized.then(|| materialization(batch_id));
        let materialization_input_digest =
            materialized.then(|| ContentDigest::of(&[b'i', sequence as u8]));
        (
            PhysicalApplyRequest {
                batch,
                materialization,
                materialization_input_digest,
                fault: ApplyFault::None,
            },
            batch_entries,
        )
    }

    fn initialized() -> (TestDatabase, Connection, PhysicalFrontierRoot) {
        let (database, connection) = TestDatabase::create();
        let empty = root(0, &[], &[]);
        initialize_schema(
            &connection,
            claim(),
            &empty.canonical_bytes,
            test_parse_config_hash(),
        )
        .unwrap();
        (database, connection, empty)
    }

    fn initialized_facade() -> (TestDatabase, PhysicalSqliteDatabase, PhysicalFrontierRoot) {
        let database = TestDatabase::new();
        let physical = PhysicalSqliteDatabase::open_writable(&database.path).unwrap();
        let empty = root(0, &[], &[]);
        physical
            .initialize_schema(claim(), &empty.canonical_bytes, test_parse_config_hash())
            .unwrap();
        (database, physical, empty)
    }

    #[test]
    fn live_and_checkpoint_candidates_use_one_current_schema() {
        let (_live_path, live, _) = initialized_facade();
        live.validate_schema_and_claim(claim()).unwrap();

        let checkpoint_path = TestDatabase::new();
        let checkpoint = PhysicalSqliteDatabase::open_writable(&checkpoint_path.path).unwrap();
        let (root, anchor) = empty_checkpoint_root_and_anchor();
        checkpoint
            .initialize_checkpoint_candidate_schema(
                claim(),
                &root,
                &anchor,
                test_parse_config_hash(),
            )
            .unwrap();
        checkpoint.validate_schema_and_claim(claim()).unwrap();
        assert_eq!(
            checkpoint.read_frontier().unwrap().canonical_bytes,
            root.canonical_bytes
        );
        assert!(checkpoint.load_all_batches().unwrap().is_empty());
    }

    #[test]
    fn prior_sqlite_schema_is_refused_instead_of_migrated_or_dually_read() {
        for unsupported in [21u32, 26, 27, 29] {
            let (_path, connection, _) = initialized();
            connection
                .pragma_update(None, "user_version", unsupported)
                .unwrap();
            let error = validate_schema_and_claim(&connection, claim()).unwrap_err();
            assert!(matches!(error, FrontierError::Schema(_)));
            assert!(error
                .to_string()
                .contains(&format!("user_version {unsupported}")));
            let found: u32 = connection
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(
                found, unsupported,
                "validation must not rewrite an unsupported schema"
            );
        }

        // And a forged user_version does not get past the canonical SQL check:
        // the widened document-key CHECK is compared structurally.
        let (_narrow_path, narrow) = TestDatabase::create();
        narrow
            .execute_batch(&format!(
                "PRAGMA user_version = {SQLITE_SCHEMA_VERSION};
                 {}",
                FRONTIER_DOCUMENTS_DDL.replace("BETWEEN 1 AND 48", "= 16")
            ))
            .unwrap();
        assert!(matches!(
            validate_schema_sql(
                &narrow,
                "table",
                "frontier_documents",
                FRONTIER_DOCUMENTS_DDL
            ),
            Err(FrontierError::Schema(_))
        ));
    }

    #[test]
    fn live_builds_a_separate_checkpoint_candidate_without_mutation() {
        let (live_path, mut live, empty_live) = initialized_facade();
        let scenario = apply_two_part_fresh_bootstrap(&mut live, &empty_live);
        live.validate_schema_and_claim(claim()).unwrap();
        let live_frontier = live.read_frontier().unwrap();
        live.checkpoint_truncate_and_disable_wal().unwrap();
        drop(live);
        let live_bytes = std::fs::read(&live_path.path).unwrap();

        let (covered_batch_root_key, covered_batch_root_digest) =
            batch_map_root(&scenario.batch_entries);
        let covered_count = scenario.final_root.acceptance_sequence;
        let sequence_digest = ContentDigest::of(b"checkpoint-sequence");
        let generation = PhysicalCheckpointGenerationBinding {
            generation_id: id(2),
            predecessor_generation_id: Some(id(1)),
            full_anchor_generation_id: id(2),
            covered_count,
            covered_document_count: scenario.final_root.document_count,
            covered_block_count: 0,
            covered_retained_bytes_total: 30,
            covered_semantic_capsules_root_digest: ContentDigest::of(b"capsules"),
            covered_batch_root_key,
            covered_batch_root_digest,
            covered_status_root_key: covered_batch_root_key,
            covered_status_root_digest: covered_batch_root_digest,
            covered_sequence_root_digest: Some(sequence_digest),
            covered_sequence_height: 1,
            covered_causal_tip_root_key: Some(id(900)),
            covered_causal_tip_root_digest: ContentDigest::of(b"tip"),
            covered_head_facts_root_digest: ContentDigest::of(b"heads"),
            current_projection_payload_pins_root_digest: ContentDigest::of(b"payloads"),
            nonlinear_state_root_digest: ContentDigest::of(b"nonlinear"),
            retention_pins_root_digest: ContentDigest::of(b"retention"),
        };
        let canonical_bytes = b"current separate checkpoint candidate".to_vec();
        let checkpoint_root = PhysicalCheckpointFrontierRoot {
            canonical_bytes: canonical_bytes.clone(),
            acceptance_sequence: covered_count,
            document_count: scenario.final_root.document_count,
            document_overlay_count: 0,
            retained_bytes_total: 30,
            document_map_root_key: None,
            document_map_root_digest: authenticated_map_empty_digest(),
            batch_map_root_key: covered_batch_root_key,
            batch_map_root_digest: covered_batch_root_digest,
            batch_map_count: covered_count,
            status_map_root_key: covered_batch_root_key,
            status_map_root_digest: covered_batch_root_digest,
            status_map_count: covered_count,
            sequence_root_digest: Some(sequence_digest),
            sequence_height: 1,
            sequence_count: covered_count,
            generation: generation.clone(),
            state_digest: ContentDigest::of(b"state"),
        };
        let anchor = PhysicalCheckpointGenerationAnchor {
            generation,
            checkpoint_frontier_root: canonical_bytes,
            terminal_batch_id: Some(scenario.second_batch_id),
            terminal_evidence_digest: Some(ContentDigest::of(b"terminal evidence")),
            materialization_frontier_root_digest: checkpoint_root.digest(),
        };
        let checkpoint_path = TestDatabase::new();
        let checkpoint = PhysicalSqliteDatabase::open_writable(&checkpoint_path.path).unwrap();
        checkpoint
            .initialize_checkpoint_candidate_schema(
                claim(),
                &checkpoint_root,
                &anchor,
                test_parse_config_hash(),
            )
            .unwrap();
        checkpoint.validate_schema_and_claim(claim()).unwrap();
        assert!(checkpoint.load_all_batches().unwrap().is_empty());
        assert_eq!(
            checkpoint.read_frontier().unwrap().applied_batch_count,
            covered_count
        );
        assert_eq!(std::fs::read(&live_path.path).unwrap(), live_bytes);
        assert_eq!(live_frontier.applied_batch_count, covered_count);
    }

    #[test]
    fn checkpoint_covered_reads_use_the_injected_sealed_reader_without_sql_history_rows() {
        let batch_id = id(101);
        let peer_id = id(900);
        let causal = SealedAcceptedCausalRecordV2 {
            batch_id,
            manifest_fingerprint: ContentDigest::of(b"manifest"),
            event_binding_digest: ContentDigest::of(b"event"),
            causal_peer_id: peer_id,
            causal_counter: 1,
            canonical_causal_clock: vec![SealedAcceptedCausalClockEntryV2 {
                peer_id,
                counter: 1,
            }],
        };
        let mut store = TestSealedStore::default();
        let mut writer = SealedAcceptedIndexWriter::new(&mut store);
        let causal_address = writer.publish_causal(&causal).unwrap();
        let batch_root = writer
            .upsert_map(AuthenticatedMapRootV1::empty(), batch_id, causal_address)
            .unwrap();
        drop(writer);

        let empty = authenticated_map_empty_digest();
        let generation = PhysicalCheckpointGenerationBinding {
            generation_id: id(2),
            predecessor_generation_id: Some(id(1)),
            full_anchor_generation_id: id(2),
            covered_count: 1,
            covered_document_count: 0,
            covered_block_count: 0,
            covered_retained_bytes_total: 10,
            covered_semantic_capsules_root_digest: ContentDigest::of(b"capsules"),
            covered_batch_root_key: batch_root
                .root
                .map(|link| key_as_id(link.key, "test batch ID").unwrap()),
            covered_batch_root_digest: batch_root.root_digest(),
            covered_status_root_key: batch_root
                .root
                .map(|link| key_as_id(link.key, "test batch ID").unwrap()),
            covered_status_root_digest: batch_root.root_digest(),
            covered_sequence_root_digest: Some(ContentDigest::of(b"sequence")),
            covered_sequence_height: 0,
            covered_causal_tip_root_key: Some(peer_id),
            covered_causal_tip_root_digest: ContentDigest::of(b"tip"),
            covered_head_facts_root_digest: ContentDigest::of(b"heads"),
            current_projection_payload_pins_root_digest: ContentDigest::of(b"payloads"),
            nonlinear_state_root_digest: ContentDigest::of(b"nonlinear"),
            retention_pins_root_digest: ContentDigest::of(b"retention"),
        };
        let canonical_bytes = b"covered checkpoint frontier".to_vec();
        let root = PhysicalCheckpointFrontierRoot {
            canonical_bytes: canonical_bytes.clone(),
            acceptance_sequence: 1,
            document_count: 0,
            document_overlay_count: 0,
            retained_bytes_total: 10,
            document_map_root_key: None,
            document_map_root_digest: empty,
            batch_map_root_key: batch_root
                .root
                .map(|link| key_as_id(link.key, "test batch ID").unwrap()),
            batch_map_root_digest: batch_root.root_digest(),
            batch_map_count: 1,
            status_map_root_key: batch_root
                .root
                .map(|link| key_as_id(link.key, "test batch ID").unwrap()),
            status_map_root_digest: batch_root.root_digest(),
            status_map_count: 1,
            sequence_root_digest: Some(ContentDigest::of(b"sequence")),
            sequence_height: 0,
            sequence_count: 1,
            generation: generation.clone(),
            state_digest: ContentDigest::of(b"state"),
        };
        let anchor = PhysicalCheckpointGenerationAnchor {
            generation,
            checkpoint_frontier_root: canonical_bytes,
            terminal_batch_id: Some(batch_id),
            terminal_evidence_digest: Some(ContentDigest::of(b"evidence")),
            materialization_frontier_root_digest: root.digest(),
        };
        let path = TestDatabase::new();
        let database = PhysicalSqliteDatabase::open_writable(&path.path).unwrap();
        database
            .initialize_checkpoint_candidate_schema(
                claim(),
                &root,
                &anchor,
                test_parse_config_hash(),
            )
            .unwrap();
        assert!(database.load_all_batches().unwrap().is_empty());

        {
            let reader = SealedAcceptedIndexReader::new(&store);
            assert!(database
                .contains_checkpoint_batch(&root, &reader, batch_id)
                .unwrap());
            assert!(database
                .authenticate_checkpoint_batch(&root, &reader, batch_id, causal_address)
                .unwrap());
            assert!(database
                .checkpoint_batch_descends_from(&root, &reader, batch_id, batch_id)
                .unwrap());
        }

        store
            .objects
            .retain(|(kind, _, _)| *kind != SealedAcceptedObjectKind::CausalRecord);
        let reader = SealedAcceptedIndexReader::new(&store);
        assert!(matches!(
            database.contains_checkpoint_batch(&root, &reader, batch_id),
            Err(FrontierError::SealedAcceptedIndex(
                SealedAcceptedIndexError::Missing {
                    kind: SealedAcceptedObjectKind::CausalRecord,
                    ..
                }
            ))
        ));
    }

    /// One anchored candidate whose history is covered: present as sealed
    /// objects, absent as rows. Returns the store, the installed live root, and
    /// one ordinary tail batch that causally descends from the covered one.
    fn anchored_candidate_with_covered_history() -> (
        TestDatabase,
        PhysicalSqliteDatabase,
        TestSealedStore,
        PhysicalFrontierRoot,
        PhysicalApplyRequest,
        Vec<([u8; 16], ContentDigest)>,
    ) {
        let covered_batch_id = id(101);
        let tail_id = id(102);
        let peer_id = id(900);
        let causal = SealedAcceptedCausalRecordV2 {
            batch_id: covered_batch_id,
            manifest_fingerprint: ContentDigest::of(b"manifest"),
            event_binding_digest: ContentDigest::of(b"event"),
            causal_peer_id: peer_id,
            causal_counter: 1,
            canonical_causal_clock: vec![SealedAcceptedCausalClockEntryV2 {
                peer_id,
                counter: 1,
            }],
        };
        let mut store = TestSealedStore::default();
        let mut writer = SealedAcceptedIndexWriter::new(&mut store);
        let causal_address = writer.publish_causal(&causal).unwrap();
        let batch_root = writer
            .upsert_map(
                AuthenticatedMapRootV1::empty(),
                covered_batch_id,
                causal_address,
            )
            .unwrap();
        drop(writer);

        let covered_root_key = batch_root
            .root
            .map(|link| key_as_id(link.key, "test batch ID").unwrap());
        let empty = authenticated_map_empty_digest();
        let generation = PhysicalCheckpointGenerationBinding {
            generation_id: id(2),
            predecessor_generation_id: Some(id(1)),
            full_anchor_generation_id: id(2),
            covered_count: 1,
            covered_document_count: 0,
            covered_block_count: 0,
            covered_retained_bytes_total: 10,
            covered_semantic_capsules_root_digest: ContentDigest::of(b"capsules"),
            covered_batch_root_key: covered_root_key,
            covered_batch_root_digest: batch_root.root_digest(),
            covered_status_root_key: covered_root_key,
            covered_status_root_digest: batch_root.root_digest(),
            covered_sequence_root_digest: Some(ContentDigest::of(b"sequence")),
            covered_sequence_height: 0,
            covered_causal_tip_root_key: Some(peer_id),
            covered_causal_tip_root_digest: ContentDigest::of(b"tip"),
            covered_head_facts_root_digest: ContentDigest::of(b"heads"),
            current_projection_payload_pins_root_digest: ContentDigest::of(b"payloads"),
            nonlinear_state_root_digest: ContentDigest::of(b"nonlinear"),
            retention_pins_root_digest: ContentDigest::of(b"retention"),
        };
        let canonical_bytes = b"covered checkpoint frontier".to_vec();
        let checkpoint_root = PhysicalCheckpointFrontierRoot {
            canonical_bytes: canonical_bytes.clone(),
            acceptance_sequence: 1,
            document_count: 0,
            document_overlay_count: 0,
            retained_bytes_total: 10,
            document_map_root_key: None,
            document_map_root_digest: empty,
            batch_map_root_key: covered_root_key,
            batch_map_root_digest: batch_root.root_digest(),
            batch_map_count: 1,
            status_map_root_key: covered_root_key,
            status_map_root_digest: batch_root.root_digest(),
            status_map_count: 1,
            sequence_root_digest: Some(ContentDigest::of(b"sequence")),
            sequence_height: 0,
            sequence_count: 1,
            generation: generation.clone(),
            state_digest: ContentDigest::of(b"state"),
        };
        let anchor = PhysicalCheckpointGenerationAnchor {
            generation,
            checkpoint_frontier_root: canonical_bytes.clone(),
            terminal_batch_id: Some(covered_batch_id),
            terminal_evidence_digest: Some(ContentDigest::of(b"evidence")),
            materialization_frontier_root_digest: checkpoint_root.digest(),
        };

        let path = TestDatabase::new();
        let database = PhysicalSqliteDatabase::open_writable(&path.path).unwrap();
        database
            .initialize_checkpoint_candidate_schema(
                claim(),
                &checkpoint_root,
                &anchor,
                test_parse_config_hash(),
            )
            .unwrap();
        // The premise. If covered history ever stops being absent as rows, every
        // assertion below stops meaning anything.
        assert!(database.load_all_batches().unwrap().is_empty());

        // The installed live root, as apply sees it. It names the COVERED batch
        // map root, because the anchor validation requires exactly that.
        let installed = PhysicalFrontierRoot {
            canonical_bytes,
            acceptance_sequence: 1,
            document_count: 0,
            document_map_root_key: None,
            document_map_root_digest: empty,
            batch_map_root_key: covered_root_key,
            batch_map_root_digest: batch_root.root_digest(),
            state_digest: ContentDigest::of(b"state"),
        };

        let mut tail = PhysicalAcceptedBatch {
            batch_id: tail_id,
            manifest_digest: ContentDigest::of(b"tail manifest"),
            event_binding_digest: ContentDigest::of(b"tail event"),
            semantic_effect: b"tail effect".to_vec(),
            semantic_effect_digest: ContentDigest::of(b"tail effect"),
            dependency_frontier: b"tail dependency".to_vec(),
            prior_frontier_root: installed.clone(),
            post_frontier_root: installed.clone(),
            affected_documents: Vec::new(),
            affected_documents_bytes: Vec::new(),
            causal_dependency_heads: vec![covered_batch_id],
            causal_dependency_heads_bytes: covered_batch_id.to_vec(),
            causal_peer_id: peer_id,
            causal_counter: 2,
            acceptance_sequence: 2,
            retained_bytes: 20,
        };
        // The tail's own clock is the covered clock advanced by one dot: the
        // covered record holds {peer: 1}, so the union with this batch's dot is
        // {peer: 2}.
        let clock_value = causal_clock_counter_digest(peer_id, 2);
        let clock_root = MapLink {
            key: peer_id.into(),
            digest: authenticated_map_node_digest(peer_id.into(), clock_value, None, None),
        };
        let record_digest = accepted_batch_causal_record_digest(&tail, &clock_root);
        let mut entries = vec![(covered_batch_id, causal_address), (tail_id, record_digest)];
        entries.sort_unstable_by_key(|entry| entry.0);
        let (post_key, post_digest) = batch_map_root(&entries);
        let mut post = installed.clone();
        post.acceptance_sequence = 2;
        post.batch_map_root_key = post_key;
        post.batch_map_root_digest = post_digest;
        post.canonical_bytes = b"covered checkpoint frontier + tail".to_vec();
        post.state_digest = ContentDigest::of(b"state after tail");
        tail.post_frontier_root = post;

        let request = PhysicalApplyRequest {
            batch: tail,
            materialization: None,
            materialization_input_digest: None,
            fault: ApplyFault::None,
        };
        (path, database, store, installed, request, entries)
    }

    /// One more ordinary tail batch on top of `prior`, so a test can prove what
    /// the SECOND anchored apply does. `documents` is both the affected set and
    /// the complete document frontier after this batch.
    fn anchored_tail(
        prior: &PhysicalFrontierRoot,
        prior_entries: &[([u8; 16], ContentDigest)],
        parent: [u8; 16],
        documents: &[PhysicalFrontierDocument],
    ) -> (PhysicalApplyRequest, Vec<([u8; 16], ContentDigest)>) {
        let sequence = prior.acceptance_sequence + 1;
        let peer = id(900);
        let batch_id = id(100 + u128::from(sequence));
        let mut batch = PhysicalAcceptedBatch {
            batch_id,
            manifest_digest: ContentDigest::of(&[b'm', sequence as u8]),
            event_binding_digest: ContentDigest::of(&[b'e', sequence as u8]),
            semantic_effect: vec![b's', sequence as u8],
            semantic_effect_digest: ContentDigest::of(&[b's', sequence as u8]),
            dependency_frontier: vec![b'd', sequence as u8],
            prior_frontier_root: prior.clone(),
            post_frontier_root: prior.clone(),
            affected_documents: documents.to_vec(),
            affected_documents_bytes: documents
                .iter()
                .flat_map(|document| {
                    document
                        .document_key
                        .as_slice()
                        .iter()
                        .copied()
                        .chain(document.canonical_bytes.iter().copied())
                })
                .collect(),
            causal_dependency_heads: vec![parent],
            causal_dependency_heads_bytes: parent.to_vec(),
            causal_peer_id: peer,
            causal_counter: sequence,
            acceptance_sequence: sequence,
            retained_bytes: sequence * 10,
        };
        let clock_value = causal_clock_counter_digest(peer, sequence);
        let clock_root = MapLink {
            key: peer.into(),
            digest: authenticated_map_node_digest(peer.into(), clock_value, None, None),
        };
        let record_digest = accepted_batch_causal_record_digest(&batch, &clock_root);
        let mut entries = prior_entries.to_vec();
        entries.push((batch_id, record_digest));
        entries.sort_unstable_by_key(|entry| entry.0);
        let document_entries = documents
            .iter()
            .map(|document| {
                (
                    document.document_key,
                    ContentDigest::of(&document.canonical_bytes),
                )
            })
            .collect::<Vec<_>>();
        let (document_key, document_digest) = map_root(&document_entries);
        let (post_key, post_digest) = batch_map_root(&entries);
        let mut post = prior.clone();
        post.acceptance_sequence = sequence;
        post.document_count = documents.len() as u64;
        post.document_map_root_key = document_key;
        post.document_map_root_digest = document_digest;
        post.batch_map_root_key = post_key;
        post.batch_map_root_digest = post_digest;
        post.canonical_bytes = format!("covered checkpoint frontier + {sequence}").into_bytes();
        post.state_digest = ContentDigest::of(&sequence.to_be_bytes());
        batch.post_frontier_root = post;
        (
            PhysicalApplyRequest {
                batch,
                materialization: None,
                materialization_input_digest: None,
                fault: ApplyFault::None,
            },
            entries,
        )
    }

    /// The blocker this release exists to remove: without the sealed reader, a
    /// tail batch cannot be applied over covered history at all.
    ///
    /// After a generation cutover the covered history leaves SQLite and survives
    /// only as sealed accepted-index objects.
    /// `initialize_checkpoint_candidate_schema` creates EMPTY
    /// `accepted_batch_nodes` / `causal_clock_nodes` / `applied_batches`, while
    /// `validate_checkpoint_anchor_input` REQUIRES the installed frontier root to
    /// name the covered batch-map root — so the walk starts covered by
    /// construction and there is no empty-tree option.
    ///
    /// This is the NECESSITY GATE for `apply_checkpoint_candidate`. It must keep
    /// failing here, because the ordinary live apply must not silently acquire a
    /// fallback: on a live database a missing accepted-batch node IS corruption,
    /// and answering it from elsewhere would hide real damage.
    #[test]
    fn a_tail_batch_over_covered_history_needs_the_sealed_reader() {
        let (_path, mut database, _store, installed, request, _entries) =
            anchored_candidate_with_covered_history();
        database.begin_candidate_build().unwrap();
        let error = database
            .apply_candidate(&installed, &request)
            .expect_err("the live apply has no way to resolve a covered node");
        let FrontierError::Corrupt(detail) = &error else {
            panic!("expected a Corrupt failure naming the missing covered node, got {error:?}");
        };
        assert!(
            detail.contains("accepted-batch node") && detail.contains("missing"),
            "expected the covered accepted-batch node walk to fail, got {detail:?}"
        );
    }

    /// An anchored database that accepted a tail batch can still be REOPENED.
    ///
    /// The anchor validator used to require the active frontier to equal the
    /// anchored root exactly. That was right while nothing could be applied to an
    /// anchored candidate, and became a one-way door the moment something could:
    /// apply advances the frontier's bytes, digest and count, so the database
    /// that had just successfully accepted work refused to open, reporting
    /// `Corrupt("active frontier is behind or misbound to its generation
    /// anchor")`. The cutover would have produced exactly one usable session.
    ///
    /// Verified by this test before the fix; the anchor is now a floor.
    #[test]
    fn an_anchored_database_reopens_after_accepting_a_tail_batch() {
        let (path, mut database, store, installed, request, _entries) =
            anchored_candidate_with_covered_history();
        database.begin_candidate_build().unwrap();
        let reader = SealedAcceptedIndexReader::new(&store);
        database
            .apply_checkpoint_candidate(&reader, &installed, &request)
            .unwrap();
        database.finish_candidate_build().unwrap();
        drop(database);

        let reopened = PhysicalSqliteDatabase::open_writable(&path.path).unwrap();
        reopened
            .validate_schema_and_claim(claim())
            .expect("an anchored database that accepted a tail batch must still open");
        // It really did advance -- otherwise this would pass vacuously.
        assert_eq!(reopened.read_frontier().unwrap().applied_batch_count, 2);
    }

    /// Counts what an anchored apply actually asks the sealed index for.
    ///
    /// The row delta in `accepted_batch_nodes` cannot answer the boundedness
    /// question on its own: it is blind to a read-only walk of the whole covered
    /// tree, and `INSERT OR IGNORE` hides rewrites of nodes already present.
    /// This counts the reads.
    use crate::sealed_accepted_index::SealedAuthenticatedMapNodeV2;

    struct CountingSealedReader<'a> {
        inner: &'a dyn SealedAcceptedIndexRead,
        map_nodes: std::cell::Cell<usize>,
        causal_records: std::cell::Cell<usize>,
    }

    impl<'a> CountingSealedReader<'a> {
        fn new(inner: &'a dyn SealedAcceptedIndexRead) -> Self {
            Self {
                inner,
                map_nodes: std::cell::Cell::new(0),
                causal_records: std::cell::Cell::new(0),
            }
        }
    }

    impl SealedAcceptedIndexRead for CountingSealedReader<'_> {
        fn sealed_map_node(
            &self,
            link: AuthenticatedMapLinkV1,
        ) -> Result<SealedAuthenticatedMapNodeV2, SealedAcceptedIndexError> {
            self.map_nodes.set(self.map_nodes.get() + 1);
            self.inner.sealed_map_node(link)
        }

        fn sealed_causal_record(
            &self,
            batch_id: [u8; 16],
            address: ContentDigest,
        ) -> Result<SealedAcceptedCausalRecordV2, SealedAcceptedIndexError> {
            self.causal_records.set(self.causal_records.get() + 1);
            self.inner.sealed_causal_record(batch_id, address)
        }
    }

    /// An anchored candidate over `covered` sealed batches, each from its own
    /// peer at counter 1, plus one tail batch depending on the first of them.
    fn anchored_candidate_over(
        covered: u128,
    ) -> (
        TestDatabase,
        PhysicalSqliteDatabase,
        TestSealedStore,
        PhysicalFrontierRoot,
        PhysicalApplyRequest,
    ) {
        assert!(covered >= 1);
        let mut store = TestSealedStore::default();
        let mut writer = SealedAcceptedIndexWriter::new(&mut store);
        let mut batch_root = AuthenticatedMapRootV1::empty();
        let mut entries = Vec::new();
        for index in 0..covered {
            let peer_id = id(1000 + index);
            let batch_id = id(2000 + index);
            let causal = SealedAcceptedCausalRecordV2 {
                batch_id,
                manifest_fingerprint: ContentDigest::of(b"manifest"),
                event_binding_digest: ContentDigest::of(b"event"),
                causal_peer_id: peer_id,
                causal_counter: 1,
                canonical_causal_clock: vec![SealedAcceptedCausalClockEntryV2 {
                    peer_id,
                    counter: 1,
                }],
            };
            let address = writer.publish_causal(&causal).unwrap();
            batch_root = writer.upsert_map(batch_root, batch_id, address).unwrap();
            entries.push((batch_id, address));
        }
        drop(writer);
        entries.sort_unstable_by_key(|entry| entry.0);

        let covered_count = u64::try_from(covered).unwrap();
        let covered_root_key = batch_root
            .root
            .map(|link| key_as_id(link.key, "test batch ID").unwrap());
        let empty = authenticated_map_empty_digest();
        let generation = PhysicalCheckpointGenerationBinding {
            generation_id: id(2),
            predecessor_generation_id: Some(id(1)),
            full_anchor_generation_id: id(2),
            covered_count,
            covered_document_count: 0,
            covered_block_count: 0,
            covered_retained_bytes_total: 10,
            covered_semantic_capsules_root_digest: ContentDigest::of(b"capsules"),
            covered_batch_root_key: covered_root_key,
            covered_batch_root_digest: batch_root.root_digest(),
            covered_status_root_key: covered_root_key,
            covered_status_root_digest: batch_root.root_digest(),
            covered_sequence_root_digest: Some(ContentDigest::of(b"sequence")),
            covered_sequence_height: 0,
            covered_causal_tip_root_key: Some(id(1000)),
            covered_causal_tip_root_digest: ContentDigest::of(b"tip"),
            covered_head_facts_root_digest: ContentDigest::of(b"heads"),
            current_projection_payload_pins_root_digest: ContentDigest::of(b"payloads"),
            nonlinear_state_root_digest: ContentDigest::of(b"nonlinear"),
            retention_pins_root_digest: ContentDigest::of(b"retention"),
        };
        let canonical_bytes = format!("covered checkpoint frontier over {covered}").into_bytes();
        let checkpoint_root = PhysicalCheckpointFrontierRoot {
            canonical_bytes: canonical_bytes.clone(),
            acceptance_sequence: covered_count,
            document_count: 0,
            document_overlay_count: 0,
            retained_bytes_total: 10,
            document_map_root_key: None,
            document_map_root_digest: empty,
            batch_map_root_key: covered_root_key,
            batch_map_root_digest: batch_root.root_digest(),
            batch_map_count: covered_count,
            status_map_root_key: covered_root_key,
            status_map_root_digest: batch_root.root_digest(),
            status_map_count: covered_count,
            sequence_root_digest: Some(ContentDigest::of(b"sequence")),
            sequence_height: 0,
            sequence_count: covered_count,
            generation: generation.clone(),
            state_digest: ContentDigest::of(b"state"),
        };
        let anchor = PhysicalCheckpointGenerationAnchor {
            generation,
            checkpoint_frontier_root: canonical_bytes.clone(),
            terminal_batch_id: Some(id(2000 + covered - 1)),
            terminal_evidence_digest: Some(ContentDigest::of(b"evidence")),
            materialization_frontier_root_digest: checkpoint_root.digest(),
        };
        let path = TestDatabase::new();
        let database = PhysicalSqliteDatabase::open_writable(&path.path).unwrap();
        database
            .initialize_checkpoint_candidate_schema(
                claim(),
                &checkpoint_root,
                &anchor,
                test_parse_config_hash(),
            )
            .unwrap();
        assert!(database.load_all_batches().unwrap().is_empty());

        let installed = PhysicalFrontierRoot {
            canonical_bytes,
            acceptance_sequence: covered_count,
            document_count: 0,
            document_map_root_key: None,
            document_map_root_digest: empty,
            batch_map_root_key: covered_root_key,
            batch_map_root_digest: batch_root.root_digest(),
            state_digest: ContentDigest::of(b"state"),
        };

        let parent = id(2000);
        let tail_peer = id(900);
        let tail_id = id(3000);
        let mut tail = PhysicalAcceptedBatch {
            batch_id: tail_id,
            manifest_digest: ContentDigest::of(b"tail manifest"),
            event_binding_digest: ContentDigest::of(b"tail event"),
            semantic_effect: b"tail effect".to_vec(),
            semantic_effect_digest: ContentDigest::of(b"tail effect"),
            dependency_frontier: b"tail dependency".to_vec(),
            prior_frontier_root: installed.clone(),
            post_frontier_root: installed.clone(),
            affected_documents: Vec::new(),
            affected_documents_bytes: Vec::new(),
            causal_dependency_heads: vec![parent],
            causal_dependency_heads_bytes: parent.to_vec(),
            causal_peer_id: tail_peer,
            causal_counter: 1,
            acceptance_sequence: covered_count + 1,
            retained_bytes: 20,
        };
        // The tail's clock is its parent's clock ({peer 1000: 1}) unioned with
        // its own dot ({peer 900: 1}).
        let mut clock_entries = vec![
            (
                AuthenticatedMapKey::from(tail_peer),
                causal_clock_counter_digest(tail_peer, 1),
            ),
            (
                AuthenticatedMapKey::from(id(1000)),
                causal_clock_counter_digest(id(1000), 1),
            ),
        ];
        clock_entries.sort_unstable_by_key(|entry| entry.0);
        let (clock_key, clock_digest) = map_root(&clock_entries);
        let clock_root = MapLink {
            key: clock_key.unwrap(),
            digest: clock_digest,
        };
        let record_digest = accepted_batch_causal_record_digest(&tail, &clock_root);
        let mut post_entries = entries.clone();
        post_entries.push((tail_id, record_digest));
        post_entries.sort_unstable_by_key(|entry| entry.0);
        let (post_key, post_digest) = batch_map_root(&post_entries);
        let mut post = installed.clone();
        post.acceptance_sequence = covered_count + 1;
        post.batch_map_root_key = post_key;
        post.batch_map_root_digest = post_digest;
        post.canonical_bytes = format!("covered checkpoint frontier over {covered} + tail").into();
        post.state_digest = ContentDigest::of(b"state after tail");
        tail.post_frontier_root = post;

        let request = PhysicalApplyRequest {
            batch: tail,
            materialization: None,
            materialization_input_digest: None,
            fault: ApplyFault::None,
        };
        (path, database, store, installed, request)
    }

    /// I-14: the cost of one anchored apply tracks the TREE, not the history.
    ///
    /// The claim the campaign rests on is not "O(log n)" as a hard fact -- treap
    /// depth is expected-logarithmic under digest-derived priorities, with a
    /// hard 256-node refusal ceiling (`MAX_AUTHENTICATED_MAP_DEPTH`). What must
    /// be true is that a 256x larger covered history does not cost 256x more.
    /// Reads, not row growth, are what is counted: a read-only walk of the whole
    /// covered tree would leave the row delta untouched.
    #[test]
    fn an_anchored_apply_reads_the_path_not_the_covered_history() {
        fn sealed_reads(covered: u128) -> (usize, usize) {
            let (_path, mut database, store, installed, request) = anchored_candidate_over(covered);
            let inner = SealedAcceptedIndexReader::new(&store);
            let counting = CountingSealedReader::new(&inner);
            database
                .apply_checkpoint(&counting, &installed, &request)
                .expect("a tail batch applies over covered history");
            (counting.map_nodes.get(), counting.causal_records.get())
        }

        let (small_nodes, small_records) = sealed_reads(1);
        let (large_nodes, large_records) = sealed_reads(256);
        println!("sealed map-node reads: 1 covered => {small_nodes}, 256 covered => {large_nodes}");

        // Causal-record reads track the dependency-head count, not the history
        // size. Two per head, not one: the validation loop authenticates each
        // head and `derive_causal_clock_root` then loads the same record again
        // to union its clock. That is a constant factor on a one-element list,
        // deliberately left alone rather than cached across the two phases.
        assert_eq!(small_records, 2);
        assert_eq!(large_records, 2);

        // 256x the history. Measured: 3 reads over 1 covered batch, 18 over
        // 256 -- two descents' worth of nodes, times a small constant. A walk
        // would be at least 256. The ceiling is deliberately generous: this
        // must fail on a walk, not on an extra probe.
        assert!(
            large_nodes <= 48,
            "sealed map-node reads grew with history, not with depth: \
             {small_nodes} over 1 batch, {large_nodes} over 256"
        );
        assert!(large_nodes > small_nodes, "the deeper tree must cost more");
    }

    /// A minimal checkpoint frontier root that passes
    /// `validate_checkpoint_root_shape`, so a test can break exactly one field.
    fn checkpoint_root_fixture() -> PhysicalCheckpointFrontierRoot {
        let empty = authenticated_map_empty_digest();
        PhysicalCheckpointFrontierRoot {
            canonical_bytes: b"shape fixture".to_vec(),
            acceptance_sequence: 0,
            document_count: 0,
            document_overlay_count: 0,
            retained_bytes_total: 0,
            document_map_root_key: None,
            document_map_root_digest: empty,
            batch_map_root_key: None,
            batch_map_root_digest: empty,
            batch_map_count: 0,
            status_map_root_key: None,
            status_map_root_digest: empty,
            status_map_count: 0,
            sequence_root_digest: None,
            sequence_height: 0,
            sequence_count: 0,
            generation: PhysicalCheckpointGenerationBinding {
                generation_id: id(2),
                predecessor_generation_id: None,
                full_anchor_generation_id: id(2),
                covered_count: 0,
                covered_document_count: 0,
                covered_block_count: 0,
                covered_retained_bytes_total: 0,
                covered_semantic_capsules_root_digest: ContentDigest::of(b"capsules"),
                covered_batch_root_key: None,
                covered_batch_root_digest: empty,
                covered_status_root_key: None,
                covered_status_root_digest: empty,
                covered_sequence_root_digest: None,
                covered_sequence_height: 0,
                covered_causal_tip_root_key: None,
                covered_causal_tip_root_digest: ContentDigest::of(b"tip"),
                covered_head_facts_root_digest: ContentDigest::of(b"heads"),
                current_projection_payload_pins_root_digest: ContentDigest::of(b"payloads"),
                nonlinear_state_root_digest: ContentDigest::of(b"nonlinear"),
                retention_pins_root_digest: ContentDigest::of(b"retention"),
            },
            state_digest: ContentDigest::of(b"state"),
        }
    }

    /// C5: the document overlay starts empty, so every overlay node a tail
    /// apply writes is hot -- and the NEXT apply can descend into it.
    ///
    /// This is the half that cannot be argued from the anchor check alone.
    /// `upsert_frontier_map` has no sealed seam: its descent loads
    /// `frontier_documents` only, and a node that is not there is a hard
    /// corruption error. The anchor requires `document_overlay_count == 0` at
    /// the cutover, which makes the FIRST apply safe; this proves the second
    /// one, which descends through what the first wrote.
    #[test]
    fn a_second_anchored_apply_descends_through_the_overlay_the_first_wrote() {
        let (_path, mut database, store, installed, first, entries) =
            anchored_candidate_with_covered_history();
        let reader = SealedAcceptedIndexReader::new(&store);
        database
            .apply_checkpoint(&reader, &installed, &first)
            .unwrap();

        let document = PhysicalFrontierDocument {
            document_key: key(41),
            canonical_bytes: b"overlay written by the second apply".to_vec(),
        };
        let after_first = first.batch.post_frontier_root.clone();
        let (second, second_entries) = anchored_tail(
            &after_first,
            &entries,
            first.batch.batch_id,
            std::slice::from_ref(&document),
        );
        let after_second = second.batch.post_frontier_root.clone();
        database
            .apply_checkpoint(&reader, &after_first, &second)
            .expect("a tail batch may introduce the first overlay document");
        assert!(after_second.document_map_root_key.is_some());

        // The third apply REWRITES that document, so its descent must load the
        // node the second apply wrote. A sealed link here would fault.
        let updated = PhysicalFrontierDocument {
            document_key: key(41),
            canonical_bytes: b"overlay rewritten by the third apply".to_vec(),
        };
        let (third, _) = anchored_tail(
            &after_second,
            &second_entries,
            second.batch.batch_id,
            std::slice::from_ref(&updated),
        );
        let result = database
            .apply_checkpoint(&reader, &after_second, &third)
            .expect("the overlay node the previous apply wrote is hot and usable");
        assert_eq!(result.disposition, ApplyDisposition::Applied);
        assert_eq!(database.read_frontier().unwrap().applied_batch_count, 4);
    }

    /// C5, the shape half: an overlay that claims to be empty may not carry a
    /// root, and one that carries a root may not claim to be empty. The anchor
    /// requires `document_overlay_count == 0`; without this, that check could
    /// be satisfied by a root the candidate cannot walk.
    #[test]
    fn a_checkpoint_root_may_not_disagree_with_itself_about_the_overlay() {
        let (_path, _database, _store, _installed, _request, _entries) =
            anchored_candidate_with_covered_history();
        let mut root = checkpoint_root_fixture();
        assert!(validate_checkpoint_root_shape(&root).is_ok());

        root.document_map_root_key = Some(AuthenticatedMapKey::from(id(5)));
        assert!(matches!(
            validate_checkpoint_root_shape(&root),
            Err(FrontierError::Corrupt(_))
        ));

        let mut root = checkpoint_root_fixture();
        root.document_overlay_count = 1;
        root.document_count = 1;
        assert!(
            matches!(
                validate_checkpoint_root_shape(&root),
                Err(FrontierError::Corrupt(_))
            ),
            "a nonempty overlay must name its root"
        );

        // And the check is wired where it matters: INSTALLATION, not only the
        // reads that come later. Without this the precondition would be
        // discovered by a candidate that had already been built.
        let mut root = checkpoint_root_fixture();
        root.document_map_root_key = Some(AuthenticatedMapKey::from(id(5)));
        let anchor = PhysicalCheckpointGenerationAnchor {
            generation: root.generation.clone(),
            checkpoint_frontier_root: root.canonical_bytes.clone(),
            terminal_batch_id: None,
            terminal_evidence_digest: None,
            materialization_frontier_root_digest: root.digest(),
        };
        let path = TestDatabase::new();
        let database = PhysicalSqliteDatabase::open_writable(&path.path).unwrap();
        assert!(
            matches!(
                database.initialize_checkpoint_candidate_schema(
                    claim(),
                    &root,
                    &anchor,
                    test_parse_config_hash(),
                ),
                Err(FrontierError::Corrupt(_))
            ),
            "an anchored candidate must not be installable with an unwalkable overlay"
        );
    }

    /// C8-4: the covered-clock check is one-sided, and demonstrably so.
    ///
    /// `materialize_covered_causal_clock` rebuilds the clock from the sealed
    /// entry vector and compares it with the root the sealed record commits to.
    /// Recomputing both from one vector would move both together under any
    /// perturbation -- a check that cannot fail. Here the expected root is taken
    /// from the UNPERTURBED record and only the entries are perturbed.
    #[test]
    fn a_perturbed_covered_clock_cannot_pass_its_canonical_root() {
        let (_database, connection, _empty) = initialized();
        let record = SealedAcceptedCausalRecordV2 {
            batch_id: id(77),
            manifest_fingerprint: ContentDigest::of(b"manifest"),
            event_binding_digest: ContentDigest::of(b"event"),
            causal_peer_id: id(901),
            causal_counter: 4,
            canonical_causal_clock: vec![
                SealedAcceptedCausalClockEntryV2 {
                    peer_id: id(901),
                    counter: 4,
                },
                SealedAcceptedCausalClockEntryV2 {
                    peer_id: id(902),
                    counter: 9,
                },
                SealedAcceptedCausalClockEntryV2 {
                    peer_id: id(903),
                    counter: 2,
                },
            ],
        };
        let canonical = record.clock_root().unwrap().root;

        // Honest input: the rebuild reproduces the canonical root.
        materialize_covered_causal_clock(
            &connection,
            &record.batch_id,
            &record.canonical_causal_clock,
            canonical,
        )
        .expect("an honest covered clock rebuilds to its canonical root");

        // One counter moved on the WRITE side only.
        let mut perturbed = record.canonical_causal_clock.clone();
        perturbed[1].counter += 1;
        let error =
            materialize_covered_causal_clock(&connection, &record.batch_id, &perturbed, canonical)
                .expect_err("a perturbed counter must not pass the canonical root");
        assert!(
            matches!(&error, FrontierError::Corrupt(message)
                if message.contains("differs from its canonical root")),
            "unexpected refusal: {error:?}"
        );

        // A peer added on the write side only.
        let mut extra = record.canonical_causal_clock.clone();
        extra.push(SealedAcceptedCausalClockEntryV2 {
            peer_id: id(904),
            counter: 1,
        });
        assert!(matches!(
            materialize_covered_causal_clock(&connection, &record.batch_id, &extra, canonical),
            Err(FrontierError::Corrupt(_))
        ));

        // An empty clock is refused rather than silently rebuilding nothing.
        assert!(matches!(
            materialize_covered_causal_clock(&connection, &record.batch_id, &[], canonical),
            Err(FrontierError::Corrupt(_))
        ));
    }

    /// A covered re-delivery is refused BY NAME, not as an ordering fault.
    ///
    /// On a live database, re-offering an accepted batch is a `Duplicate`. On an
    /// anchored one the exact `applied_batches` row `stored_matches_request`
    /// compares is gone with the covered history, so storage cannot answer
    /// "equal to what we accepted?" -- and the sequence check would otherwise
    /// report `AcceptanceOrder`, which says "wrong next batch" about a batch we
    /// already have. See `FrontierError::CoveredBatchRedelivery`.
    #[test]
    fn a_covered_redelivery_is_named_rather_than_reported_as_an_ordering_fault() {
        let (_path, mut database, store, installed, request, _entries) =
            anchored_candidate_with_covered_history();
        let covered_batch_id = id(101);
        let mut redelivery = request.clone();
        redelivery.batch.batch_id = covered_batch_id;
        redelivery.batch.acceptance_sequence = 1;
        let reader = SealedAcceptedIndexReader::new(&store);

        assert!(
            matches!(
                database.preflight_checkpoint(&reader, &installed, &redelivery),
                Err(FrontierError::CoveredBatchRedelivery(id)) if id == covered_batch_id
            ),
            "preflight must name the covered re-delivery"
        );
        database.begin_candidate_build().unwrap();
        assert!(
            matches!(
                database.apply_checkpoint_candidate(&reader, &installed, &redelivery),
                Err(FrontierError::CoveredBatchRedelivery(id)) if id == covered_batch_id
            ),
            "apply must name the covered re-delivery"
        );

        // The neuter: the guard names ONLY a batch the frontier really
        // contains. An ordinary out-of-order batch is still an ordering fault.
        let mut out_of_order = request.clone();
        out_of_order.batch.acceptance_sequence = 7;
        assert!(matches!(
            database.apply_checkpoint_candidate(&reader, &installed, &out_of_order),
            Err(FrontierError::AcceptanceOrder {
                expected: 2,
                found: 7
            })
        ));
    }

    /// The re-delivery that a lazy probe missed, and that silently ERASED an
    /// accepted batch.
    ///
    /// The guard first ran only on the branch where the sequence check had
    /// already failed -- free on the healthy path, and wrong. A covered id
    /// offered at exactly `current + 1`, with a post root constructed to match,
    /// skipped the probe entirely: `upsert_batch_map` took its `Ordering::Equal`
    /// arm, REPLACED the covered key's value, and the frontier advanced with the
    /// map count unchanged. Confirmed by probe before the guard moved; that is
    /// the state the sealed three-root count check rejects, after SQLite has
    /// already committed.
    #[test]
    fn a_covered_id_offered_at_the_expected_next_sequence_cannot_replace_its_map_value() {
        let (_path, mut database, store, installed, request, _entries) =
            anchored_candidate_with_covered_history();
        let covered = id(101);
        let peer = id(900);
        let reader = SealedAcceptedIndexReader::new(&store);

        let mut collide = request.clone();
        collide.batch.batch_id = covered;
        let clock_value = causal_clock_counter_digest(peer, 2);
        let clock_root = MapLink {
            key: peer.into(),
            digest: authenticated_map_node_digest(peer.into(), clock_value, None, None),
        };
        let record_digest = accepted_batch_causal_record_digest(&collide.batch, &clock_root);
        let (post_key, post_digest) = batch_map_root(&[(covered, record_digest)]);
        let mut post = installed.clone();
        post.acceptance_sequence = 2;
        post.batch_map_root_key = post_key;
        post.batch_map_root_digest = post_digest;
        post.canonical_bytes = b"the covered key, replaced".to_vec();
        post.state_digest = ContentDigest::of(b"replaced");
        collide.batch.post_frontier_root = post;
        assert_eq!(
            collide.batch.acceptance_sequence, 2,
            "the EXPECTED next sequence"
        );

        assert!(
            matches!(
                database.apply_checkpoint(&reader, &installed, &collide),
                Err(FrontierError::CoveredBatchRedelivery(id)) if id == covered
            ),
            "a covered id at the expected next sequence must not be insertable"
        );
        assert!(database.load_all_batches().unwrap().is_empty());
        assert_eq!(database.read_frontier().unwrap().applied_batch_count, 1);
    }

    /// F3-1: a LOST HOT ROW is corruption, not a re-delivery.
    ///
    /// Both shapes look the same from `load_batch`: an id that is in the
    /// authenticated map with no `applied_batches` row. Only a descent of the
    /// ANCHOR's covered root tells them apart, and calling the second one a
    /// re-delivery would hide real damage behind an ordinary-sounding refusal.
    #[test]
    fn a_lost_hot_row_is_corruption_not_a_covered_redelivery() {
        let (path, mut database, store, installed, request, _entries) =
            anchored_candidate_with_covered_history();
        let reader = SealedAcceptedIndexReader::new(&store);
        let tail_id = request.batch.batch_id;
        database
            .apply_checkpoint(&reader, &installed, &request)
            .unwrap();
        let after = request.batch.post_frontier_root.clone();
        drop(database);

        // Lose the HOT row while its id stays in the authenticated map.
        {
            let connection = Connection::open(&path.path).unwrap();
            connection
                .execute(
                    "DELETE FROM applied_batches WHERE batch_id = ?1",
                    params![tail_id.as_slice()],
                )
                .unwrap();
        }
        let mut database = PhysicalSqliteDatabase::open_writable(&path.path).unwrap();
        let mut redelivery = request.clone();
        redelivery.batch.prior_frontier_root = after.clone();
        let error = database
            .apply_checkpoint(&reader, &after, &redelivery)
            .expect_err("a lost hot row must not be accepted");
        assert!(
            matches!(&error, FrontierError::Corrupt(message)
                if message.contains("is missing its exact record")),
            "lost hot row misclassified: {error:?}"
        );
    }

    /// C2: candidate variants alone are not enough. A promoted anchored
    /// database still has sealed off-path links, so its ORDINARY live apply
    /// needs the reader too.
    #[test]
    fn a_promoted_anchored_database_applies_a_tail_batch_through_ordinary_apply() {
        let (_path, mut database, store, installed, request, _entries) =
            anchored_candidate_with_covered_history();
        let reader = SealedAcceptedIndexReader::new(&store);
        assert!(
            matches!(
                database.apply(&installed, &request),
                Err(FrontierError::Corrupt(_))
            ),
            "the necessity gate: ordinary apply without the reader must still fail"
        );
        let result = database
            .apply_checkpoint(&reader, &installed, &request)
            .expect("ordinary anchored apply resolves covered history");
        assert_eq!(result.disposition, ApplyDisposition::Applied);
        assert_eq!(database.read_frontier().unwrap().applied_batch_count, 2);
    }

    /// Relaxing the anchor to a floor must not relax it into nothing.
    ///
    /// Each arm below is the neuter of one surviving refusal: damage the row it
    /// guards and the open has to refuse, naming that arm. The first version of
    /// this test covered only three arms and left two intervals completely
    /// unchecked — an advanced frontier unbound to anything, and a LAGGING
    /// materialization stamp whose digest could be any 32 bytes. Both were
    /// confirmed open by probe, which is why the matrix here is the shape it is:
    /// a floor is not a check, it is the absence of one everywhere above it.
    #[test]
    fn an_anchored_database_still_refuses_damage_below_and_beyond_its_floor() {
        fn advanced_anchored_database() -> TestDatabase {
            let (path, mut database, store, installed, request, _entries) =
                anchored_candidate_with_covered_history();
            database.begin_candidate_build().unwrap();
            let reader = SealedAcceptedIndexReader::new(&store);
            database
                .apply_checkpoint_candidate(&reader, &installed, &request)
                .unwrap();
            database.finish_candidate_build().unwrap();
            drop(database);
            path
        }

        fn refusal(damage: &str) -> String {
            let path = advanced_anchored_database();
            {
                let connection = Connection::open(&path.path).unwrap();
                connection.execute_batch(damage).unwrap();
            }
            let reopened = PhysicalSqliteDatabase::open_writable(&path.path).unwrap();
            match reopened.validate_schema_and_claim(claim()) {
                Err(FrontierError::Corrupt(message)) => message,
                other => panic!("damage `{damage}` opened anyway: {other:?}"),
            }
        }

        /// A root that is internally consistent -- bytes plus their own digest --
        /// and has nothing to do with this database. Crude damage (`x'00'`) is
        /// caught by the self-binding check and never reaches the arms below it.
        fn foreign_root(label: &str) -> String {
            let bytes = label.as_bytes();
            let digest = ContentDigest::of(bytes);
            format!("x'{}', x'{}'", hex(bytes), hex(digest.as_bytes()))
        }

        fn hex(bytes: &[u8]) -> String {
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        }

        // --- the frontier row ---

        assert!(
            refusal("UPDATE frontier SET applied_batch_count = 0 WHERE singleton = 1")
                .contains("active frontier is behind its generation anchor"),
        );
        assert!(
            refusal("UPDATE frontier SET frontier_root = x'00' WHERE singleton = 1")
                .contains("active frontier digest does not hash its own bytes"),
        );
        assert!(refusal(&format!(
            "UPDATE frontier SET applied_batch_count = 1,
                 (frontier_root, frontier_root_digest) = ({})
                 WHERE singleton = 1",
            foreign_root("a foreign root at the floor")
        ))
        .contains("unadvanced active frontier is misbound to its generation anchor"),);
        // The interval the floor opened: advanced, self-consistent, unrelated.
        assert!(refusal(&format!(
            "UPDATE frontier SET (frontier_root, frontier_root_digest) = ({})
                 WHERE singleton = 1",
            foreign_root("a foreign root above the floor")
        ))
        .contains("active frontier is not the end of the anchored tail"),);

        // --- the retained tail that binds them ---

        assert!(refusal("DELETE FROM applied_batches")
            .contains("anchored tail does not hold exactly the batches above the covered floor"),);
        assert!(refusal(&format!(
            "UPDATE applied_batches SET (prior_frontier_root, prior_frontier_root_digest) = ({})",
            foreign_root("a tail that starts somewhere else")
        ))
        .contains("anchored tail does not chain back to its generation anchor"),);

        // --- the materialization stamp ---

        assert!(refusal(
            "UPDATE materialization_stamp SET acceptance_sequence = 9 WHERE singleton = 1"
        )
        .contains("materialization stamp is ahead of the active checkpoint frontier"),);
        assert!(refusal(
            "UPDATE materialization_stamp SET acceptance_sequence = 0 WHERE singleton = 1"
        )
        .contains("materialization stamp is behind its generation anchor"),);
        // The second interval the floor opened: the stamp LAGS legitimately
        // here (the tail carries no materialization), and its digest was
        // therefore compared with nothing at all.
        assert!(refusal(
            "UPDATE materialization_stamp SET frontier_root_digest = x'\
                 0000000000000000000000000000000000000000000000000000000000000000' \
                 WHERE singleton = 1"
        )
        .contains("materialization stamp is misbound to the frontier at its own sequence"),);
        assert!(refusal(
            "UPDATE materialization_stamp SET acceptance_sequence = 2, \
                 frontier_root_digest = x'\
                 0000000000000000000000000000000000000000000000000000000000000000' \
                 WHERE singleton = 1"
        )
        .contains("materialization stamp is misbound to the frontier at its own sequence"),);
        // Projection evidence claiming more than the stamp admits -- the shape a
        // torn stamp page leaves after a materialized tail.
        assert!(refusal(
            "INSERT INTO materialization_batches (acceptance_sequence, batch_id, input_digest)
                 VALUES (5, x'00000000000000000000000000000042', x'\
                 0000000000000000000000000000000000000000000000000000000000000000')"
        )
        .contains("materialization receipts are ahead of the materialization stamp"),);
    }

    /// The fix: with the reader injected, the same tail batch applies.
    #[test]
    fn an_anchored_candidate_applies_a_tail_batch_over_covered_history() {
        let (_path, mut database, store, installed, request, _entries) =
            anchored_candidate_with_covered_history();
        database.begin_candidate_build().unwrap();
        let reader = SealedAcceptedIndexReader::new(&store);
        let result = database
            .apply_checkpoint_candidate(&reader, &installed, &request)
            .expect("a tail batch applies over covered history through the sealed reader");
        assert_eq!(result.disposition, ApplyDisposition::Applied);
    }

    fn reference_target_name(source_page_id: [u8; 16]) -> String {
        format!("bootstrap-target-{}", u128::from_be_bytes(source_page_id))
    }

    fn configure_fresh_bootstrap_reference(request: &mut PhysicalApplyRequest) -> [u8; 16] {
        let source_page_id = request.batch.batch_id;
        let target_name = reference_target_name(source_page_id);
        request
            .materialization
            .as_mut()
            .unwrap()
            .derived_reference_postings = vec![PhysicalReferencePosting {
            source_page_id,
            source_entity: PhysicalEntityId::Page(source_page_id),
            source_locator: vec![request.batch.acceptance_sequence as u8],
            ordinal: 0,
            kind: 0,
            target: PhysicalReferenceTarget::PageName {
                raw_name: target_name.clone(),
                normalized_name: target_name,
                resolved_page_id: None,
            },
        }];
        source_page_id
    }

    struct FreshBootstrapScenario {
        first_batch_id: [u8; 16],
        second_batch_id: [u8; 16],
        first_source_page_id: [u8; 16],
        second_source_page_id: [u8; 16],
        final_root: PhysicalFrontierRoot,
        batch_entries: Vec<([u8; 16], ContentDigest)>,
    }

    fn apply_two_part_fresh_bootstrap(
        physical: &mut PhysicalSqliteDatabase,
        empty: &PhysicalFrontierRoot,
    ) -> FreshBootstrapScenario {
        apply_two_part_fresh_bootstrap_with(physical, empty, false)
    }

    fn apply_two_part_fresh_bootstrap_with(
        physical: &mut PhysicalSqliteDatabase,
        empty: &PhysicalFrontierRoot,
        candidate: bool,
    ) -> FreshBootstrapScenario {
        let document = PhysicalFrontierDocument {
            document_key: key(50),
            canonical_bytes: b"fresh-bootstrap-document".to_vec(),
        };
        let (mut first, first_entries) = request(empty, 1, None, vec![document.clone()], &[], true);
        let first_source_page_id = configure_fresh_bootstrap_reference(&mut first);
        let first_batch_id = first.batch.batch_id;
        if candidate {
            physical.apply_candidate(empty, &first)
        } else {
            physical.apply(empty, &first)
        }
        .unwrap();
        let first_root = first.batch.post_frontier_root.clone();

        let (mut second, batch_entries) = request(
            &first_root,
            2,
            Some(first_batch_id),
            vec![document],
            &first_entries,
            true,
        );
        let second_source_page_id = configure_fresh_bootstrap_reference(&mut second);
        let second_batch_id = second.batch.batch_id;
        if candidate {
            physical.apply_candidate(&first_root, &second)
        } else {
            physical.apply(&first_root, &second)
        }
        .unwrap();

        FreshBootstrapScenario {
            first_batch_id,
            second_batch_id,
            first_source_page_id,
            second_source_page_id,
            final_root: second.batch.post_frontier_root,
            batch_entries,
        }
    }

    #[test]
    fn ordered_apply_duplicate_collision_and_missing_dependency_are_physical() {
        let (_database, mut connection, empty) = initialized();
        let document = PhysicalFrontierDocument {
            document_key: key(50),
            canonical_bytes: b"document-v1".to_vec(),
        };
        let (first, entries) = request(&empty, 1, None, vec![document.clone()], &[], true);
        let first_result = apply(&mut connection, &empty, &first).unwrap();
        assert_eq!(first_result.disposition, ApplyDisposition::Applied);
        let first_root = first.batch.post_frontier_root.clone();
        assert_eq!(
            apply(&mut connection, &first_root, &first)
                .unwrap()
                .disposition,
            ApplyDisposition::Duplicate
        );

        let mut collision = first.clone();
        collision.batch.manifest_digest = ContentDigest::of(b"colliding manifest");
        assert!(matches!(
            preflight(&connection, &first_root, &collision),
            Err(FrontierError::BatchCollision(id)) if id == first.batch.batch_id
        ));

        let (missing, _) = request(
            &empty,
            1,
            Some(id(9999)),
            vec![document.clone()],
            &[],
            false,
        );
        let (_other_database, other_connection, other_empty) = initialized();
        assert!(matches!(
            preflight(&other_connection, &other_empty, &missing),
            Err(FrontierError::MissingDependency(found)) if found == id(9999)
        ));

        let document_v2 = PhysicalFrontierDocument {
            document_key: key(50),
            canonical_bytes: b"document-v2".to_vec(),
        };
        let (second, _) = request(
            &first_root,
            2,
            Some(first.batch.batch_id),
            vec![document_v2],
            &entries,
            true,
        );
        assert_eq!(
            apply(&mut connection, &first_root, &second)
                .unwrap()
                .disposition,
            ApplyDisposition::Applied
        );
        assert_eq!(
            read_frontier(&connection).unwrap().canonical_bytes,
            second.batch.post_frontier_root.canonical_bytes
        );
    }

    #[test]
    fn materialization_and_terminal_frontier_roll_back_together() {
        let (_database, mut connection, empty) = initialized();
        let document = PhysicalFrontierDocument {
            document_key: key(60),
            canonical_bytes: b"rollback-doc".to_vec(),
        };
        let (mut change, _) = request(&empty, 1, None, vec![document], &[], true);
        change.fault = ApplyFault::ReturnAfterMaterialization;
        assert_eq!(
            apply(&mut connection, &empty, &change),
            Err(FrontierError::InjectedFailure)
        );
        assert_eq!(
            read_frontier(&connection).unwrap().canonical_bytes,
            empty.canonical_bytes
        );
        assert!(load_batch(&connection, change.batch.batch_id)
            .unwrap()
            .is_none());
        let page_rows: i64 = connection
            .query_row("SELECT COUNT(*) FROM pages", [], |row| row.get(0))
            .unwrap();
        let search_rows: i64 = connection
            .query_row("SELECT COUNT(*) FROM search_fts", [], |row| row.get(0))
            .unwrap();
        assert_eq!((page_rows, search_rows), (0, 0));
        assert_eq!(
            sqlite_materialization::recorded_digest(&connection, 1).unwrap(),
            None
        );
        sqlite_materialization::ensure_stamp(&connection, 0, empty.digest()).unwrap();
    }

    #[test]
    fn reopen_authenticates_roots_and_rejects_tampered_nodes() {
        let (database, mut connection, empty) = initialized();
        let document = PhysicalFrontierDocument {
            document_key: key(70),
            canonical_bytes: b"reopen-doc".to_vec(),
        };
        let (change, _) = request(&empty, 1, None, vec![document], &[], true);
        apply(&mut connection, &empty, &change).unwrap();
        let post = change.batch.post_frontier_root.clone();
        drop(connection);

        let connection = database.reopen();
        validate_schema_and_claim(&connection, claim()).unwrap();
        assert!(contains_batch(&connection, &post, change.batch.batch_id).unwrap());
        connection
            .execute(
                "UPDATE accepted_batch_nodes SET value_digest = ?1 WHERE batch_id = ?2",
                params![
                    ContentDigest::of(b"tampered").as_bytes().as_slice(),
                    change.batch.batch_id.as_slice()
                ],
            )
            .unwrap();
        assert!(matches!(
            contains_batch(&connection, &post, change.batch.batch_id),
            Err(FrontierError::Corrupt(_))
        ));
    }

    fn entity_document(uuid: u8, body: &str) -> PhysicalFrontierDocument {
        let mut bytes = vec![0x01];
        bytes.extend_from_slice(&[uuid; 16]);
        PhysicalFrontierDocument {
            document_key: AuthenticatedMapKey::new(&bytes).unwrap(),
            canonical_bytes: body.as_bytes().to_vec(),
        }
    }

    fn membership_document(block: u8, page: u8, body: &str) -> PhysicalFrontierDocument {
        let mut bytes = vec![0x02];
        bytes.extend_from_slice(&[block; 16]);
        bytes.extend_from_slice(&[page; 16]);
        PhysicalFrontierDocument {
            document_key: AuthenticatedMapKey::new(&bytes).unwrap(),
            canonical_bytes: body.as_bytes().to_vec(),
        }
    }

    /// Mixed 17-byte entity and 33-byte membership keys, sorted unique. The
    /// entity `0x11` and the membership `(0x11, *)` pair shares a UUID, and the
    /// last pair is strictly prefix-related.
    fn mixed_width_documents() -> Vec<PhysicalFrontierDocument> {
        let mut documents = vec![
            entity_document(0x11, "entity-11"),
            entity_document(0x22, "entity-22"),
            entity_document(0x33, "entity-33"),
            membership_document(0x11, 0x44, "membership-11-44"),
            membership_document(0x11, 0x55, "membership-11-55"),
            membership_document(0x22, 0x44, "membership-22-44"),
            PhysicalFrontierDocument {
                document_key: AuthenticatedMapKey::new(&[0x03, 0x09]).unwrap(),
                canonical_bytes: b"short-key".to_vec(),
            },
            PhysicalFrontierDocument {
                document_key: AuthenticatedMapKey::new(&[0x03, 0x09, 0x00]).unwrap(),
                canonical_bytes: b"short-key-extension".to_vec(),
            },
            PhysicalFrontierDocument {
                document_key: AuthenticatedMapKey::new(&[0xfe; MAX_AUTHENTICATED_MAP_KEY_BYTES])
                    .unwrap(),
                canonical_bytes: b"maximum-width-key".to_vec(),
            },
        ];
        documents.sort_by_key(|document| document.document_key);
        documents
    }

    fn document_entries(
        documents: &[PhysicalFrontierDocument],
    ) -> Vec<(AuthenticatedMapKey, ContentDigest)> {
        let mut entries = documents
            .iter()
            .map(|document| {
                (
                    document.document_key,
                    ContentDigest::of(&document.canonical_bytes),
                )
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(key, _)| *key);
        entries
    }

    /// The same `(key, value_digest)` set built by the sealed writer.
    fn sealed_document_root(
        documents: &[PhysicalFrontierDocument],
        order: &[usize],
    ) -> AuthenticatedMapRootV1 {
        let entries = document_entries(documents);
        let mut store = TestSealedStore::default();
        let mut root = AuthenticatedMapRootV1::empty();
        for index in order {
            root = SealedAcceptedIndexWriter::new(&mut store)
                .upsert_map(root, entries[*index].0, entries[*index].1)
                .unwrap();
        }
        root
    }

    /// The gate that would have caught a lossy document-map key: the live
    /// SQLite treap, the sealed writer, and the Cartesian builder must produce
    /// the same root key, digest and count over mixed 17/33-byte keys,
    /// whatever order the entries arrive in.
    #[test]
    fn frontier_document_map_root_matches_sealed_and_cartesian_roots() {
        let documents = mixed_width_documents();
        let entries = document_entries(&documents);
        let cartesian = authenticated_map_root(&entries).unwrap();
        let cartesian_key = cartesian.root.map(|link| link.key);
        assert_eq!(cartesian.count, documents.len() as u64);

        // Three sealed insertion orders, all landing on the same root.
        for order in [
            (0..entries.len()).collect::<Vec<_>>(),
            (0..entries.len()).rev().collect::<Vec<_>>(),
            vec![4, 0, 8, 2, 6, 1, 7, 3, 5],
        ] {
            assert_eq!(sealed_document_root(&documents, &order), cartesian);
        }

        // (a) live SQLite upserts, one batch carrying every document.
        let (_single_path, mut single, single_empty) = initialized_facade();
        let (single_request, _) = request(&single_empty, 1, None, documents.clone(), &[], false);
        single.apply(&single_empty, &single_request).unwrap();
        let single_root = single_request.batch.post_frontier_root.clone();
        assert_eq!(single_root.document_map_root_key, cartesian_key);
        assert_eq!(
            single_root.document_map_root_digest,
            cartesian.root_digest()
        );
        assert_eq!(single_root.document_count, cartesian.count);
        assert_eq!(
            single.read_frontier_documents(&single_root).unwrap(),
            documents
        );

        // (b) the same documents arriving as three separate batches, in an
        //     order unrelated to key order.
        let (_split_path, mut split, split_empty) = initialized_facade();
        let mut current = split_empty.clone();
        let mut batch_entries: Vec<([u8; 16], ContentDigest)> = Vec::new();
        let mut delivered: Vec<PhysicalFrontierDocument> = Vec::new();
        let mut previous_batch: Option<[u8; 16]> = None;
        for (sequence, group) in [vec![4, 0, 8], vec![2, 6, 1], vec![7, 3, 5]]
            .into_iter()
            .enumerate()
        {
            let mut affected = group
                .iter()
                .map(|index| documents[*index].clone())
                .collect::<Vec<_>>();
            affected.sort_by_key(|document| document.document_key);
            delivered.extend(affected.iter().cloned());
            delivered.sort_by_key(|document| document.document_key);
            let (mut change, next_entries) = request(
                &current,
                sequence as u64 + 1,
                previous_batch,
                affected,
                &batch_entries,
                false,
            );
            previous_batch = Some(change.batch.batch_id);
            // The post root covers every document delivered so far, not just
            // this batch's slice.
            change.batch.post_frontier_root = root(sequence as u64 + 1, &delivered, &next_entries);
            batch_entries = next_entries;
            split.apply(&current, &change).unwrap();
            current = change.batch.post_frontier_root.clone();
        }
        assert_eq!(current.document_map_root_key, cartesian_key);
        assert_eq!(current.document_map_root_digest, cartesian.root_digest());
        assert_eq!(current.document_count, cartesian.count);
        assert_eq!(split.read_frontier_documents(&current).unwrap(), documents);

        // (c) the bulk Cartesian seed path over the same terminal root.
        let (_terminal_path, mut terminal, terminal_empty) = initialized_facade();
        terminal.begin_candidate_build().unwrap();
        terminal
            .apply_terminal_prefix_candidate(&terminal_empty, &single_request)
            .unwrap();
        terminal
            .seed_terminal_frontier_documents(&single_root, &documents)
            .unwrap();
        terminal.finish_candidate_build().unwrap();
        assert_eq!(
            terminal.read_frontier_documents(&single_root).unwrap(),
            documents
        );
        assert_eq!(
            terminal.read_frontier().unwrap().canonical_bytes,
            single_root.canonical_bytes
        );
    }

    /// Distinct keys that share a prefix must resolve to distinct rows: the
    /// same-UUID entity/membership pair, and a strictly prefix-related pair.
    #[test]
    fn frontier_lookup_resolves_prefix_related_keys_distinctly() {
        let documents = mixed_width_documents();
        let (_database, mut physical, empty) = initialized_facade();
        let (change, _) = request(&empty, 1, None, documents.clone(), &[], false);
        physical.apply(&empty, &change).unwrap();
        let post = change.batch.post_frontier_root.clone();

        for document in &documents {
            assert_eq!(
                physical
                    .frontier_document(&post, document.document_key)
                    .unwrap()
                    .as_deref(),
                Some(document.canonical_bytes.as_slice()),
                "key {} did not resolve to its own value",
                document.document_key
            );
        }

        let entity = entity_document(0x11, "entity-11");
        let membership = membership_document(0x11, 0x44, "membership-11-44");
        assert_ne!(entity.document_key, membership.document_key);
        assert_ne!(
            physical
                .frontier_document(&post, entity.document_key)
                .unwrap(),
            physical
                .frontier_document(&post, membership.document_key)
                .unwrap()
        );

        let prefix = AuthenticatedMapKey::new(&[0x03, 0x09]).unwrap();
        let extension = AuthenticatedMapKey::new(&[0x03, 0x09, 0x00]).unwrap();
        assert_eq!(
            physical.frontier_document(&post, prefix).unwrap().unwrap(),
            b"short-key".to_vec()
        );
        assert_eq!(
            physical
                .frontier_document(&post, extension)
                .unwrap()
                .unwrap(),
            b"short-key-extension".to_vec()
        );

        // An unstored key of either class is simply absent.
        assert_eq!(
            physical
                .frontier_document(&post, entity_document(0x99, "").document_key)
                .unwrap(),
            None
        );
        assert_eq!(
            physical
                .frontier_document(&post, AuthenticatedMapKey::new(&[0x03]).unwrap())
                .unwrap(),
            None
        );
    }

    #[test]
    fn frontier_documents_reject_empty_and_oversized_keys() {
        assert!(AuthenticatedMapKey::new(&[]).is_err());
        assert!(AuthenticatedMapKey::new(&[0; MAX_AUTHENTICATED_MAP_KEY_BYTES + 1]).is_err());

        // The stored table carries the same bound structurally.
        let (_database, connection, _empty) = initialized();
        for bytes in [Vec::new(), vec![0_u8; MAX_AUTHENTICATED_MAP_KEY_BYTES + 1]] {
            assert!(
                connection
                    .execute(
                        "INSERT INTO frontier_documents (
                             document_id, dependencies, dependencies_digest, node_digest
                         ) VALUES (?1, ?2, ?3, ?4)",
                        params![
                            bytes.as_slice(),
                            b"payload".as_slice(),
                            ContentDigest::of(b"payload").as_bytes().as_slice(),
                            ContentDigest::of(b"node").as_bytes().as_slice(),
                        ],
                    )
                    .is_err(),
                "a {}-byte document key must violate the table CHECK",
                bytes.len()
            );
        }
        assert!(connection
            .execute(
                "INSERT INTO frontier_documents (
                     document_id, dependencies, dependencies_digest, node_digest
                 ) VALUES (?1, ?2, ?3, ?4)",
                params![
                    vec![0x07_u8; MAX_AUTHENTICATED_MAP_KEY_BYTES].as_slice(),
                    b"payload".as_slice(),
                    ContentDigest::of(b"payload").as_bytes().as_slice(),
                    ContentDigest::of(b"node").as_bytes().as_slice(),
                ],
            )
            .is_ok());
    }

    #[test]
    fn empty_and_genesis_document_maps_round_trip_through_the_shared_empty_root() {
        // The empty pairing the shared map produces is exactly the pairing the
        // frontier shape check accepts.
        let empty_root = AuthenticatedMapRootV1::empty();
        assert!(empty_root.root.is_none());
        assert_eq!(empty_root.root_digest(), authenticated_map_empty_digest());

        let empty = root(0, &[], &[]);
        assert_eq!(empty.document_map_root_key, None);
        assert_eq!(
            empty.document_map_root_digest,
            authenticated_map_empty_digest()
        );
        validate_root_shape(&empty).unwrap();

        let (_database, mut physical, _empty) = initialized_facade();
        physical.begin_candidate_build().unwrap();
        physical.seed_genesis_frontier(&empty, &[]).unwrap();
        physical.finish_candidate_build().unwrap();
        assert_eq!(
            physical.read_frontier().unwrap(),
            StoredFrontier {
                canonical_bytes: empty.canonical_bytes.clone(),
                digest: empty.digest(),
                applied_batch_count: 0,
            }
        );
        assert!(physical.read_frontier_documents(&empty).unwrap().is_empty());
        assert_eq!(
            physical
                .frontier_document(&empty, entity_document(0x11, "").document_key)
                .unwrap(),
            None
        );

        // A first full-key batch still advances from that empty root, and
        // removing the concept of a document again is not this packet's job:
        // the shared writer's removal path is proved in the index module.
        let documents = mixed_width_documents();
        let (change, _) = request(&empty, 1, None, documents.clone(), &[], false);
        physical.apply(&empty, &change).unwrap();
        assert_eq!(
            physical
                .read_frontier_documents(&change.batch.post_frontier_root)
                .unwrap(),
            documents
        );
    }

    #[test]
    fn terminal_frontier_and_materialization_stamp_are_exactly_equal() {
        let (_database, mut connection, empty) = initialized();
        let document = PhysicalFrontierDocument {
            document_key: key(80),
            canonical_bytes: b"terminal-doc".to_vec(),
        };
        let (change, _) = request(&empty, 1, None, vec![document], &[], true);
        let input_digest = change.materialization_input_digest.unwrap();
        apply(&mut connection, &empty, &change).unwrap();
        let stored = read_frontier(&connection).unwrap();
        assert_eq!(
            stored.canonical_bytes,
            change.batch.post_frontier_root.canonical_bytes
        );
        assert_eq!(stored.digest, change.batch.post_frontier_root.digest());
        assert_eq!(stored.applied_batch_count, change.batch.acceptance_sequence);
        sqlite_materialization::ensure_stamp(
            &connection,
            change.batch.acceptance_sequence,
            change.batch.post_frontier_root.digest(),
        )
        .unwrap();
        assert_eq!(
            sqlite_materialization::recorded_digest(&connection, change.batch.acceptance_sequence)
                .unwrap(),
            Some(input_digest)
        );
    }

    #[test]
    fn fresh_bootstrap_materializes_parser_derived_references() {
        let (_database, mut physical, empty) = initialized_facade();
        let scenario = apply_two_part_fresh_bootstrap(&mut physical, &empty);
        assert_eq!(
            physical
                .reference_page_candidates_for_name(
                    &reference_target_name(scenario.first_source_page_id),
                    2,
                )
                .unwrap(),
            BTreeSet::from([scenario.first_source_page_id])
        );
        assert_eq!(
            physical
                .reference_page_candidates_for_name(
                    &reference_target_name(scenario.second_source_page_id),
                    2,
                )
                .unwrap(),
            BTreeSet::from([scenario.second_source_page_id])
        );
    }

    #[test]
    fn candidate_batch_matches_ordinary_apply_without_per_commit_durability() {
        let (_ordinary_path, mut ordinary, empty) = initialized_facade();
        let ordinary_scenario = apply_two_part_fresh_bootstrap(&mut ordinary, &empty);
        let ordinary_semantic = ordinary.semantic_projection_digest().unwrap();
        let ordinary_rows = ordinary.materialized_row_digest().unwrap();
        let ordinary_writes = ordinary.write_instrumentation();
        assert_eq!(ordinary_writes.ordinary_transactions, 2);
        assert_eq!(ordinary_writes.ordinary_durability_barriers, 0);
        assert_eq!(ordinary_writes.candidate_transactions, 0);
        assert_eq!(ordinary_writes.candidate_durability_barriers, 0);

        let (_candidate_path, mut candidate, candidate_empty) = initialized_facade();
        candidate.begin_candidate_build().unwrap();
        let candidate_scenario =
            apply_two_part_fresh_bootstrap_with(&mut candidate, &candidate_empty, true);
        assert_eq!(
            candidate.read_frontier().unwrap().canonical_bytes,
            candidate_scenario.final_root.canonical_bytes
        );
        candidate.finish_candidate_build().unwrap();

        assert_eq!(candidate_scenario.final_root, ordinary_scenario.final_root);
        assert_eq!(
            candidate.semantic_projection_digest().unwrap(),
            ordinary_semantic
        );
        assert_eq!(candidate.materialized_row_digest().unwrap(), ordinary_rows);
        let candidate_writes = candidate.write_instrumentation();
        assert_eq!(candidate_writes.ordinary_transactions, 0);
        assert_eq!(candidate_writes.ordinary_durability_barriers, 0);
        assert_eq!(candidate_writes.candidate_transactions, 1);
        assert_eq!(candidate_writes.candidate_durability_barriers, 0);
    }

    #[test]
    fn terminal_prefix_bulk_frontier_matches_ordinary_document_treap() {
        let documents = [7_u128, 11, 19, 23, 29, 31, 41]
            .into_iter()
            .map(|value| PhysicalFrontierDocument {
                document_key: key(value),
                canonical_bytes: format!("terminal-document-{value}").into_bytes(),
            })
            .collect::<Vec<_>>();

        let (_ordinary_path, mut ordinary, ordinary_empty) = initialized_facade();
        let (request, _) = request(&ordinary_empty, 1, None, documents.clone(), &[], false);
        ordinary.apply(&ordinary_empty, &request).unwrap();
        let expected = ordinary
            .read_frontier_documents(&request.batch.post_frontier_root)
            .unwrap();

        let (_terminal_path, mut terminal, terminal_empty) = initialized_facade();
        terminal.begin_candidate_build().unwrap();
        terminal
            .apply_terminal_prefix_candidate(&terminal_empty, &request)
            .unwrap();
        let mut wrong = documents.clone();
        wrong[3].canonical_bytes.push(b'!');
        assert_eq!(
            terminal.seed_terminal_frontier_documents(&request.batch.post_frontier_root, &wrong),
            Err(FrontierError::FrontierRegression)
        );
        assert_eq!(
            terminal.diagnostic_row_counts().unwrap().1,
            0,
            "a refused bulk root must not leave partial document rows"
        );
        terminal
            .seed_terminal_frontier_documents(&request.batch.post_frontier_root, &documents)
            .unwrap();
        terminal.finish_candidate_build().unwrap();

        assert_eq!(
            terminal.read_frontier().unwrap().canonical_bytes,
            request.batch.post_frontier_root.canonical_bytes
        );
        assert_eq!(
            terminal
                .read_frontier_documents(&request.batch.post_frontier_root)
                .unwrap(),
            expected
        );
        assert_eq!(
            terminal.semantic_projection_digest().unwrap(),
            ordinary.semantic_projection_digest().unwrap()
        );
    }

    #[test]
    fn sequence_zero_genesis_seeds_overlay_without_fabricated_batches() {
        let mut genesis = root(0, &[], &[]);
        genesis.document_count = 3;
        genesis.canonical_bytes = b"synthetic lazy genesis with three pack documents".to_vec();
        genesis.state_digest = ContentDigest::of(b"lazy genesis state");
        let (database, mut physical, _empty) = initialized_facade();
        physical.begin_candidate_build().unwrap();
        physical.begin_terminal_bootstrap_construction().unwrap();
        physical.seed_lazy_genesis_frontier(&genesis).unwrap();
        physical
            .finish_terminal_graph_projection_construction(
                &[],
                sqlite_materialization::PhysicalTerminalProjectionStamp {
                    acceptance_sequence: 0,
                    frontier_root_digest: genesis.digest(),
                },
            )
            .unwrap();
        physical.finish_candidate_build().unwrap();

        assert_eq!(
            physical.read_frontier().unwrap(),
            StoredFrontier {
                canonical_bytes: genesis.canonical_bytes.clone(),
                digest: genesis.digest(),
                applied_batch_count: 0,
            }
        );
        assert!(physical
            .read_frontier_documents(&genesis)
            .unwrap()
            .is_empty());
        assert!(physical.load_all_batches().unwrap().is_empty());
        assert_eq!(
            physical
                .materialized_read(0, genesis.digest())
                .unwrap()
                .acceptance_sequence(),
            0
        );
        let changed_baseline_document = PhysicalFrontierDocument {
            document_key: key(11),
            canonical_bytes: b"changed baseline document".to_vec(),
        };
        let (mut first, _) = request(
            &genesis,
            1,
            None,
            vec![changed_baseline_document.clone()],
            &[],
            false,
        );
        first.batch.post_frontier_root.document_count = 3;
        let terminal_request = first.clone();
        physical.apply(&genesis, &first).unwrap();
        assert_eq!(
            physical
                .read_frontier_documents(&first.batch.post_frontier_root)
                .unwrap(),
            vec![changed_baseline_document.clone()]
        );
        drop(physical);

        let reopened = PhysicalSqliteDatabase::open_read_only(&database.path).unwrap();
        assert_eq!(
            reopened.read_frontier().unwrap().digest,
            first.batch.post_frontier_root.digest()
        );
        assert_eq!(reopened.load_all_batches().unwrap().len(), 1);
        assert_eq!(
            reopened
                .read_frontier_documents(&first.batch.post_frontier_root)
                .unwrap(),
            vec![changed_baseline_document.clone()]
        );

        let (_rebuild_path, mut rebuilt, _empty) = initialized_facade();
        rebuilt.begin_candidate_build().unwrap();
        rebuilt.seed_lazy_genesis_frontier(&genesis).unwrap();
        rebuilt
            .apply_terminal_prefix_candidate(&genesis, &terminal_request)
            .unwrap();
        assert_eq!(
            rebuilt.seed_terminal_frontier_documents(
                &terminal_request.batch.post_frontier_root,
                std::slice::from_ref(&changed_baseline_document),
            ),
            Err(FrontierError::FrontierRegression),
            "the dense terminal API must not silently reinterpret a sparse frontier"
        );
        rebuilt
            .seed_sparse_terminal_frontier_documents(
                &terminal_request.batch.post_frontier_root,
                std::slice::from_ref(&changed_baseline_document),
            )
            .unwrap();
        rebuilt.finish_candidate_build().unwrap();
        assert_eq!(
            rebuilt
                .read_frontier_documents(&terminal_request.batch.post_frontier_root)
                .unwrap(),
            vec![changed_baseline_document]
        );
    }

    #[test]
    fn failed_unfinished_candidate_transaction_is_not_reopenable_as_accepted_state() {
        let (database, mut candidate, empty) = initialized_facade();
        candidate.begin_candidate_build().unwrap();
        let document = PhysicalFrontierDocument {
            document_key: key(50),
            canonical_bytes: b"fresh-bootstrap-document".to_vec(),
        };
        let (mut first, first_entries) =
            request(&empty, 1, None, vec![document.clone()], &[], true);
        configure_fresh_bootstrap_reference(&mut first);
        candidate.apply_candidate(&empty, &first).unwrap();
        let first_root = first.batch.post_frontier_root.clone();

        let (mut second, _) = request(
            &first_root,
            2,
            Some(first.batch.batch_id),
            vec![document],
            &first_entries,
            true,
        );
        configure_fresh_bootstrap_reference(&mut second);
        second.fault = ApplyFault::ReturnAfterMaterialization;
        assert_eq!(
            candidate.apply_candidate(&first_root, &second),
            Err(FrontierError::InjectedFailure)
        );
        drop(candidate);

        let reopened = PhysicalSqliteDatabase::open_writable(&database.path).unwrap();
        assert_eq!(
            reopened.read_frontier().unwrap().canonical_bytes,
            empty.canonical_bytes
        );
        assert!(reopened.load_all_batches().unwrap().is_empty());
        assert!(reopened.stored_semantic_effects().unwrap().is_empty());
    }

    #[test]
    fn fresh_bootstrap_finalization_retains_only_reachable_accepted_physical_state() {
        let (_database, mut physical, empty) = initialized_facade();
        let scenario = apply_two_part_fresh_bootstrap(&mut physical, &empty);
        let document = PhysicalFrontierDocument {
            document_key: key(50),
            canonical_bytes: b"fresh-bootstrap-document".to_vec(),
        };
        let (mut staged, _) = request(
            &scenario.final_root,
            3,
            Some(scenario.second_batch_id),
            vec![document],
            &scenario.batch_entries,
            true,
        );
        let staged_source_page_id = configure_fresh_bootstrap_reference(&mut staged);
        staged.fault = ApplyFault::ReturnAfterMaterialization;
        assert_eq!(
            physical.apply(&scenario.final_root, &staged),
            Err(FrontierError::InjectedFailure)
        );

        physical.finalize_fresh_bootstrap().unwrap();
        assert_eq!(
            physical.read_frontier().unwrap(),
            StoredFrontier {
                canonical_bytes: scenario.final_root.canonical_bytes.clone(),
                digest: scenario.final_root.digest(),
                applied_batch_count: 2,
            }
        );
        assert_eq!(
            physical
                .load_all_batches()
                .unwrap()
                .into_iter()
                .map(|batch| batch.batch_id)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([scenario.first_batch_id, scenario.second_batch_id])
        );
        assert!(physical
            .load_batch(staged.batch.batch_id)
            .unwrap()
            .is_none());
        assert!(physical
            .reference_page_candidates_for_name(&reference_target_name(staged_source_page_id), 2)
            .unwrap()
            .is_empty());

        let read = physical
            .materialized_read(
                scenario.final_root.acceptance_sequence,
                scenario.final_root.digest(),
            )
            .unwrap();
        assert_eq!(
            read.pages(None, 3)
                .unwrap()
                .into_iter()
                .map(|page| page.page_id)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                scenario.first_source_page_id,
                scenario.second_source_page_id
            ])
        );
        assert!(read.page(staged_source_page_id).unwrap().is_none());
    }

    #[test]
    fn finalized_fresh_bootstrap_reopens_through_public_storage_facade() {
        let (database, mut physical, empty) = initialized_facade();
        let scenario = apply_two_part_fresh_bootstrap(&mut physical, &empty);
        physical.finalize_fresh_bootstrap().unwrap();

        let expected_frontier = physical.read_frontier().unwrap();
        let expected_batches = physical.load_all_batches().unwrap();
        let expected_first_candidates = physical
            .reference_page_candidates_for_name(
                &reference_target_name(scenario.first_source_page_id),
                2,
            )
            .unwrap();
        let expected_second_candidates = physical
            .reference_page_candidates_for_name(
                &reference_target_name(scenario.second_source_page_id),
                2,
            )
            .unwrap();
        let expected_pages = physical
            .materialized_read(
                scenario.final_root.acceptance_sequence,
                scenario.final_root.digest(),
            )
            .unwrap()
            .pages(None, 3)
            .unwrap();
        physical.checkpoint_truncate().unwrap();
        drop(physical);

        let reopened = PhysicalSqliteDatabase::open_read_only(&database.path).unwrap();
        reopened.validate_schema_and_claim(claim()).unwrap();
        assert_eq!(reopened.read_frontier().unwrap(), expected_frontier);
        assert_eq!(reopened.load_all_batches().unwrap(), expected_batches);
        assert_eq!(
            reopened
                .reference_page_candidates_for_name(
                    &reference_target_name(scenario.first_source_page_id),
                    2,
                )
                .unwrap(),
            expected_first_candidates
        );
        assert_eq!(
            reopened
                .reference_page_candidates_for_name(
                    &reference_target_name(scenario.second_source_page_id),
                    2,
                )
                .unwrap(),
            expected_second_candidates
        );
        assert_eq!(
            reopened
                .materialized_read(
                    scenario.final_root.acceptance_sequence,
                    scenario.final_root.digest(),
                )
                .unwrap()
                .pages(None, 3)
                .unwrap(),
            expected_pages
        );
    }
}
