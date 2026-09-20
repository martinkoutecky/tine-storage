//! Physical SQLite materialization engine.
//!
//! This module owns disposable SQL shape and bounded physical reads. Inputs are
//! lowered and semantically validated by tine-core before they cross this boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use rusqlite::{params, Connection, OptionalExtension as _};

/// `PRAGMA application_id` of every Tine projection file.
pub const SQLITE_APPLICATION_ID: u32 = 0x5449_4e45;
/// `PRAGMA user_version` of the Direct Files projection schema. A file whose
/// version differs is rebuilt from the graph, never reinterpreted.
pub const SQLITE_SCHEMA_VERSION: u32 = 30;
pub const MAX_MATERIALIZATION_QUERY_ROWS: usize = 10_000;
pub const MAX_MATERIALIZATION_QUERY_BYTES: usize = 64 * 1024;
pub const MAX_MATERIALIZATION_READ_BYTES: usize = 64 * 1024 * 1024;
const MAX_MATERIALIZATION_FIELD_BYTES: usize = 4 * 1024 * 1024;
const MATERIALIZATION_STRING_OVERHEAD_BYTES: usize = 16;

fn checked_budget_add(
    resource: &'static str,
    current: usize,
    additional: usize,
    maximum: usize,
) -> Result<usize, MaterializationError> {
    let found = current.checked_add(additional).unwrap_or(usize::MAX);
    if found > maximum {
        return Err(resource_limit(resource, found, maximum));
    }
    Ok(found)
}

fn resource_limit(resource: &'static str, found: usize, maximum: usize) -> MaterializationError {
    MaterializationError::ResourceLimit {
        resource,
        found,
        maximum,
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PhysicalEntityId {
    Page([u8; 16]),
    Block([u8; 16]),
}

impl PhysicalEntityId {
    fn sql_parts(self) -> (i64, [u8; 16]) {
        match self {
            Self::Page(id) => (0, id),
            Self::Block(id) => (1, id),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalProperty {
    pub name: String,
    pub normalized_name: String,
    pub value: String,
}

/// One atom of one property element (SPEC §3.3), already flattened and
/// renumbered by the single tine-core producer. The physical layer stores what
/// it is handed; it never atomizes.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPropertyAtom {
    pub normalized_name: String,
    pub ordinal: u32,
    pub atom: String,
    pub atom_key: String,
    /// `0` = the atom came from an explicit page reference in the value,
    /// `1` = a plain text segment.
    pub origin: i64,
    pub atom_num: Option<f64>,
    pub atom_day: Option<i64>,
}

/// One inline tag of one owner, carrying both the spelling the source used and
/// the page-name key `tag('x')` compares on (SPEC §3.2 K18). The key is the
/// caller's -- this crate does not know Tine's page-identity normalization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTag {
    pub tag: String,
    pub tag_key: String,
}

/// Existing shallow query estimate, excluding the outer budget's page name and
/// group overhead. Shared by projection production and consumers.
pub fn query_result_estimated_bytes<'a>(
    result_id: &str,
    raw: &str,
    tags: impl IntoIterator<Item = &'a str>,
    properties: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> usize {
    let id_bytes = if result_id.is_empty() {
        36
    } else {
        result_id.len()
    };
    id_bytes
        .saturating_add(raw.len())
        .saturating_add(tags.into_iter().map(str::len).sum::<usize>())
        .saturating_add(
            properties
                .into_iter()
                .map(|(key, value)| key.len().saturating_add(value.len()))
                .sum::<usize>(),
        )
        .saturating_add(128)
}

/// Stable construction estimate for a shallow page result, shared by the
/// producer and admitted-payload validator. Counts UTF-8 string bytes plus a
/// fixed row allowance; this is not an exact allocator/RSS measurement.
pub fn query_page_result_estimated_bytes<'a>(
    name: &str,
    path: &str,
    properties: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> usize {
    properties.into_iter().fold(
        96usize
            .saturating_add(name.len())
            .saturating_add(path.len()),
        |bytes, (key, value)| bytes.saturating_add(key.len()).saturating_add(value.len()),
    )
}

/// Expand parent-local sibling order into whole-page preorder. Returns original
/// input indices and one-based depths, so producers can share ordering without
/// allocating document payload. Equal orders use block ID.
pub(crate) fn query_block_preorder<'a>(
    blocks: impl IntoIterator<Item = ([u8; 16], Option<[u8; 16]>, &'a str)>,
) -> Result<Vec<(usize, usize)>, MaterializationError> {
    let blocks = blocks.into_iter().collect::<Vec<_>>();
    let ids = blocks.iter().map(|row| row.0).collect::<BTreeSet<_>>();
    if ids.len() != blocks.len() {
        return Err(MaterializationError::InvalidInput(
            "duplicate query block identity".into(),
        ));
    }
    let mut children = BTreeMap::<Option<[u8; 16]>, Vec<usize>>::new();
    for (index, (_, parent, _)) in blocks.iter().enumerate() {
        if parent.is_some_and(|id| !ids.contains(&id)) {
            return Err(MaterializationError::InvalidInput(
                "query block has unknown parent".into(),
            ));
        }
        children.entry(*parent).or_default().push(index);
    }
    for siblings in children.values_mut() {
        siblings.sort_unstable_by_key(|index| (blocks[*index].2, blocks[*index].0));
    }
    let mut pending = children
        .remove(&None)
        .unwrap_or_default()
        .into_iter()
        .rev()
        .map(|index| (index, 1))
        .collect::<Vec<_>>();
    let mut output = Vec::with_capacity(blocks.len());
    while let Some((index, depth)) = pending.pop() {
        output.push((index, depth));
        pending.extend(
            children
                .remove(&Some(blocks[index].0))
                .unwrap_or_default()
                .into_iter()
                .rev()
                .map(|child| (child, depth + 1)),
        );
    }
    if output.len() != blocks.len() {
        return Err(MaterializationError::InvalidInput(
            "cyclic query block ancestry".into(),
        ));
    }
    Ok(output)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTask {
    pub marker: String,
    pub priority: Option<String>,
    pub scheduled: Option<String>,
    pub deadline: Option<String>,
}

/// One block's `[#A]` / `SCHEDULED:` / `DEADLINE:` planning facets, held
/// INDEPENDENTLY of the task marker (SPEC §3.2 M2).
///
/// `tasks` carries a row only when a marker exists, so a markerless
/// `SCHEDULED:` block is invisible there while the tree walk evaluates it. The
/// day columns are `None` when the timestamp text does not parse to a calendar
/// day: a malformed date has presence and no day, and presence has to be
/// physically representable (E1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPlanning {
    pub priority: Option<String>,
    pub scheduled: Option<String>,
    pub scheduled_day: Option<i64>,
    pub deadline: Option<String>,
    pub deadline_day: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalBlock {
    pub block_id: [u8; 16],
    /// Public query identity assigned by the application's current session.
    /// The disposable physical UUID is not a substitute.
    pub query_result_id: String,
    /// Own normalized reference names, before ancestor/page closure expansion.
    pub own_refs: Vec<String>,
    pub home_document_id: [u8; 16],
    pub parent: Option<[u8; 16]>,
    pub order: String,
    pub content: String,
    pub searchable_text: String,
    pub normalized_searchable_text: String,
    /// The block's exact visible text -- `BlockProjection.visible` -- and that
    /// text canonically folded, the two columns every content predicate reads
    /// (SPEC §5.8, §5.10).
    ///
    /// Deliberately NOT `searchable_text`, which both producers collapse
    /// whitespace in for the existing search consumers: a query for a phrase
    /// with two spaces has to be able to tell those apart.
    pub query_visible: String,
    pub query_visible_folded: String,
    pub heading_level: Option<u8>,
    pub collapsed: bool,
    pub logseq_uuid: Option<[u8; 16]>,
    pub logseq_identity_origin: Option<i64>,
    pub properties: Vec<PhysicalProperty>,
    pub tags: Vec<PhysicalTag>,
    pub task: Option<PhysicalTask>,
    pub planning: Option<PhysicalPlanning>,
    /// This block's `:block/path-refs` closure -- its own normalized refs, every
    /// ancestor's, and its page's -- as the ONE tine-core closure function
    /// emitted it (SPEC §5.8 K22). Sorted and de-duplicated by that producer.
    pub path_refs: Vec<String>,
    pub property_atoms: Vec<PhysicalPropertyAtom>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPage {
    pub page_id: [u8; 16],
    /// Direct's current session inventory position. This is rebuildable
    /// metadata, never identity authority.
    pub query_page_order: Option<u64>,
    pub home_document_id: [u8; 16],
    pub name: String,
    pub name_key: String,
    pub path: String,
    pub text_kind: i64,
    /// `yyyymmdd` when this page is a journal whose name parses under the
    /// graph's journal formats, else `None` (SPEC §3.2, §5.8).
    pub journal_day: Option<i64>,
    pub preamble: Option<String>,
    pub searchable_text: String,
    pub normalized_searchable_text: String,
    pub properties: Vec<PhysicalProperty>,
    pub tags: Vec<PhysicalTag>,
    pub property_atoms: Vec<PhysicalPropertyAtom>,
    pub blocks: Vec<PhysicalBlock>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalReferenceTarget {
    PageName {
        raw_name: String,
        normalized_name: String,
        resolved_page_id: Option<[u8; 16]>,
    },
    ExternalUuid {
        raw_claim: [u8; 16],
        resolved_block_id: Option<[u8; 16]>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalReferencePosting {
    pub source_page_id: [u8; 16],
    pub source_entity: PhysicalEntityId,
    pub source_locator: Vec<u8>,
    pub ordinal: u32,
    pub kind: i64,
    pub target: PhysicalReferenceTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalAliasDeclaration {
    pub source_page_id: [u8; 16],
    pub source_entity: PhysicalEntityId,
    pub source_locator: Vec<u8>,
    pub ordinal: u32,
    pub raw_alias: String,
    pub normalized_alias: String,
}

/// One regime-neutral update to the disposable graph projection.
///
/// This contains only parser-derived graph facts. Direct Files applies it from
/// an observed file change; no authority stamp or sync state crosses this
/// boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalGraphProjectionChange {
    pub replacements: Vec<PhysicalPage>,
    pub deletions: Vec<[u8; 16]>,
    /// Parser-derived reference spellings owned by replacement pages.
    ///
    /// Both storage regimes obtain these rows directly from the parser
    /// snapshot. They are disposable graph facts, never write authority.
    pub reference_postings: Vec<PhysicalReferencePosting>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ApplyChangeInstrumentation {
    pub cleanup_page_attempts: usize,
    pub cleanup_existing_pages: usize,
    pub cleanup_owned_rows: usize,
    pub cleanup_fts_rowids: usize,
}

/// Test-facing detail for proving that ready-state maintenance is bounded to
/// changed logical entities. Keep this separate from the stable public
/// instrumentation record: adding fields to that exhaustively constructible
/// record would be a breaking API change.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FtsChangeInstrumentation {
    page_rows: usize,
    block_rows: usize,
    standard_rows: usize,
    substring_rows: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FtsEntityRow {
    entity_type: i64,
    entity_id: [u8; 16],
    page_id: [u8; 16],
    text: String,
    normalized_text: String,
}

impl FtsEntityRow {
    const fn key(&self) -> (i64, [u8; 16]) {
        (self.entity_type, self.entity_id)
    }
}

pub const REFERENCE_POSTINGS_DDL: &str = "CREATE TABLE reference_postings (
    source_page_id BLOB NOT NULL CHECK (length(source_page_id) = 16),
    source_entity_type INTEGER NOT NULL CHECK (source_entity_type IN (0, 1)),
    source_entity_id BLOB NOT NULL CHECK (length(source_entity_id) = 16),
    source_locator BLOB NOT NULL CHECK (length(source_locator) BETWEEN 1 AND 4194304),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    reference_kind INTEGER NOT NULL CHECK (reference_kind BETWEEN 0 AND 7),
    target_type INTEGER NOT NULL CHECK (target_type IN (0, 1)),
    raw_name TEXT CHECK (
        raw_name IS NULL OR length(CAST(raw_name AS BLOB)) BETWEEN 1 AND 4194304
    ),
    normalized_name TEXT CHECK (
        normalized_name IS NULL OR length(CAST(normalized_name AS BLOB)) BETWEEN 1 AND 4194304
    ),
    raw_uuid_claim BLOB CHECK (
        raw_uuid_claim IS NULL OR length(raw_uuid_claim) = 16
    ),
    resolved_page_id BLOB CHECK (
        resolved_page_id IS NULL OR length(resolved_page_id) = 16
    ),
    resolved_block_id BLOB CHECK (
        resolved_block_id IS NULL OR length(resolved_block_id) = 16
    ),
    CHECK (
        (reference_kind BETWEEN 0 AND 5 AND target_type = 0)
        OR
        (reference_kind IN (6, 7) AND target_type = 1)
    ),
    CHECK (
        (target_type = 0 AND raw_name IS NOT NULL AND normalized_name IS NOT NULL
         AND raw_uuid_claim IS NULL AND resolved_block_id IS NULL)
        OR
        (target_type = 1 AND raw_name IS NULL AND normalized_name IS NULL
         AND raw_uuid_claim IS NOT NULL AND resolved_page_id IS NULL)
    ),
    PRIMARY KEY (
        source_page_id, source_entity_type, source_entity_id, source_locator, ordinal
    )
) WITHOUT ROWID, STRICT";
pub const REFERENCE_ALIAS_DECLARATIONS_DDL: &str = "CREATE TABLE reference_alias_declarations (
    source_page_id BLOB NOT NULL CHECK (length(source_page_id) = 16),
    source_entity_type INTEGER NOT NULL CHECK (source_entity_type IN (0, 1)),
    source_entity_id BLOB NOT NULL CHECK (length(source_entity_id) = 16),
    source_locator BLOB NOT NULL CHECK (length(source_locator) BETWEEN 1 AND 4194304),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    raw_alias TEXT NOT NULL CHECK (length(CAST(raw_alias AS BLOB)) BETWEEN 1 AND 4194304),
    normalized_alias TEXT NOT NULL CHECK (
        length(CAST(normalized_alias AS BLOB)) BETWEEN 1 AND 4194304
    ),
    PRIMARY KEY (
        source_page_id, source_entity_type, source_entity_id, source_locator, ordinal
    )
) WITHOUT ROWID, STRICT";
pub const PAGES_DDL: &str = "CREATE TABLE pages (
    page_id BLOB PRIMARY KEY CHECK (length(page_id) = 16),
    home_document_id BLOB NOT NULL CHECK (length(home_document_id) = 16),
    name TEXT NOT NULL CHECK (length(CAST(name AS BLOB)) BETWEEN 1 AND 4194304),
    name_key TEXT NOT NULL CHECK (length(CAST(name_key AS BLOB)) BETWEEN 1 AND 4194304),
    path TEXT NOT NULL CHECK (length(CAST(path AS BLOB)) BETWEEN 1 AND 4194304),
    text_kind INTEGER NOT NULL CHECK (text_kind IN (0, 1)),
    journal_day INTEGER
) STRICT";
const PAGE_TEXT_DDL: &str = "CREATE TABLE page_text (
    page_id BLOB PRIMARY KEY CHECK (length(page_id) = 16)
        REFERENCES pages(page_id) ON DELETE CASCADE,
    preamble TEXT CHECK (preamble IS NULL OR length(CAST(preamble AS BLOB)) <= 16777216),
    searchable_text TEXT NOT NULL CHECK (length(CAST(searchable_text AS BLOB)) <= 4194304),
    normalized_searchable_text TEXT NOT NULL CHECK (
        length(CAST(normalized_searchable_text AS BLOB)) <= 4194304
    )
) STRICT";
pub const BLOCKS_DDL: &str = "CREATE TABLE blocks (
    block_id BLOB PRIMARY KEY CHECK (length(block_id) = 16),
    page_id BLOB NOT NULL CHECK (length(page_id) = 16)
        REFERENCES pages(page_id) ON DELETE CASCADE,
    home_document_id BLOB NOT NULL CHECK (length(home_document_id) = 16),
    parent_block_id BLOB CHECK (
        parent_block_id IS NULL OR length(parent_block_id) = 16
    ),
    order_key TEXT NOT NULL CHECK (length(CAST(order_key AS BLOB)) BETWEEN 1 AND 4194304),
    query_visible_folded TEXT NOT NULL CHECK (
        length(CAST(query_visible_folded AS BLOB)) <= 4194304
    ),
    heading_level INTEGER CHECK (
        heading_level IS NULL OR heading_level BETWEEN 1 AND 6
    ),
    collapsed INTEGER NOT NULL CHECK (collapsed IN (0, 1)),
    logseq_uuid BLOB CHECK (logseq_uuid IS NULL OR length(logseq_uuid) = 16),
    logseq_identity_origin INTEGER CHECK (
        logseq_identity_origin IS NULL
        OR logseq_identity_origin BETWEEN 0 AND 4
    ),
    CHECK (
        (logseq_uuid IS NULL AND logseq_identity_origin IS NULL)
        OR (logseq_uuid IS NOT NULL AND logseq_identity_origin IS NOT NULL)
    )
) STRICT";
const BLOCK_TEXT_DDL: &str = "CREATE TABLE block_text (
    block_id BLOB PRIMARY KEY CHECK (length(block_id) = 16)
        REFERENCES blocks(block_id) ON DELETE CASCADE,
    content TEXT NOT NULL CHECK (length(CAST(content AS BLOB)) <= 4194304),
    searchable_text TEXT NOT NULL CHECK (length(CAST(searchable_text AS BLOB)) <= 4194304),
    normalized_searchable_text TEXT NOT NULL CHECK (
        length(CAST(normalized_searchable_text AS BLOB)) <= 4194304
    ),
    query_visible TEXT NOT NULL CHECK (length(CAST(query_visible AS BLOB)) <= 4194304)
) STRICT";
pub const PROPERTIES_DDL: &str = "CREATE TABLE properties (
    owner_type INTEGER NOT NULL CHECK (owner_type IN (0, 1)),
    owner_id BLOB NOT NULL CHECK (length(owner_id) = 16),
    page_id BLOB NOT NULL CHECK (length(page_id) = 16),
    name TEXT NOT NULL CHECK (length(CAST(name AS BLOB)) BETWEEN 1 AND 4194304),
    normalized_name TEXT NOT NULL CHECK (
        length(CAST(normalized_name AS BLOB)) BETWEEN 1 AND 4194304
    ),
    value TEXT NOT NULL CHECK (length(CAST(value AS BLOB)) <= 4194304),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (owner_type, owner_id, name, ordinal)
) WITHOUT ROWID, STRICT";
pub const TAGS_DDL: &str = "CREATE TABLE tags (
    owner_type INTEGER NOT NULL CHECK (owner_type IN (0, 1)),
    owner_id BLOB NOT NULL CHECK (length(owner_id) = 16),
    page_id BLOB NOT NULL CHECK (length(page_id) = 16),
    tag TEXT NOT NULL CHECK (length(CAST(tag AS BLOB)) BETWEEN 1 AND 4194304),
    tag_key TEXT NOT NULL CHECK (length(CAST(tag_key AS BLOB)) BETWEEN 1 AND 4194304),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (owner_type, owner_id, ordinal)
) WITHOUT ROWID, STRICT";
pub const TASKS_DDL: &str = "CREATE TABLE tasks (
    block_id BLOB PRIMARY KEY CHECK (length(block_id) = 16),
    page_id BLOB NOT NULL CHECK (length(page_id) = 16),
    marker TEXT NOT NULL CHECK (length(CAST(marker AS BLOB)) BETWEEN 1 AND 4194304),
    priority TEXT CHECK (priority IS NULL OR length(CAST(priority AS BLOB)) <= 4194304),
    scheduled TEXT CHECK (scheduled IS NULL OR length(CAST(scheduled AS BLOB)) <= 4194304),
    deadline TEXT CHECK (deadline IS NULL OR length(CAST(deadline AS BLOB)) <= 4194304)
) STRICT";
/// The `[#A]` / `SCHEDULED:` / `DEADLINE:` facets of every block that has any
/// of them, written independently of the task marker (SPEC §3.2 M2).
///
/// `tasks` cannot answer these: both producers write a `tasks` row only under a
/// marker, so a markerless `SCHEDULED:` block is absent from it while the tree
/// walk still evaluates the date. The text columns carry the projected
/// bracketless timestamp exactly as the projection stores it and the `*_day`
/// columns carry its `yyyymmdd` ordinal, NULL when the text is not a calendar
/// day -- so presence (`scheduled IS NOT NULL`) survives a malformed date that
/// has no day at all (E1).
pub const BLOCK_PLANNING_DDL: &str = "CREATE TABLE block_planning (
    block_id BLOB PRIMARY KEY CHECK (length(block_id) = 16),
    page_id BLOB NOT NULL CHECK (length(page_id) = 16),
    priority TEXT CHECK (priority IS NULL OR length(CAST(priority AS BLOB)) <= 4194304),
    scheduled TEXT CHECK (scheduled IS NULL OR length(CAST(scheduled AS BLOB)) <= 4194304),
    scheduled_day INTEGER,
    deadline TEXT CHECK (deadline IS NULL OR length(CAST(deadline AS BLOB)) <= 4194304),
    deadline_day INTEGER,
    CHECK (priority IS NOT NULL OR scheduled IS NOT NULL OR deadline IS NOT NULL),
    CHECK (scheduled IS NOT NULL OR scheduled_day IS NULL),
    CHECK (deadline IS NOT NULL OR deadline_day IS NULL)
) STRICT";
/// OG's materialized `:block/path-refs`: one row per (block, normalized name)
/// in the block's ancestor closure. The rows come from the ONE tine-core
/// closure function; nothing here recomputes them (SPEC §5.8).
pub const BLOCK_PATH_REFS_DDL: &str = "CREATE TABLE block_path_refs (
    block_id BLOB NOT NULL CHECK (length(block_id) = 16),
    page_id BLOB NOT NULL CHECK (length(page_id) = 16),
    normalized_name TEXT NOT NULL CHECK (
        length(CAST(normalized_name AS BLOB)) BETWEEN 1 AND 4194304
    ),
    PRIMARY KEY (block_id, normalized_name)
) WITHOUT ROWID, STRICT";
/// The atoms of every property element (SPEC §3.3, §5.8). `properties` keeps
/// the unsplit source value for presence, autocomplete and display; this table
/// carries the flattened, renumbered atom list a value comparison searches.
pub const QUERY_BLOCK_RESULTS_DDL: &str = "CREATE TABLE query_block_results (
    block_id BLOB PRIMARY KEY CHECK (length(block_id) = 16)
        REFERENCES blocks(block_id) ON DELETE CASCADE,
    page_id BLOB NOT NULL CHECK (length(page_id) = 16)
        REFERENCES pages(page_id) ON DELETE CASCADE,
    preorder INTEGER NOT NULL CHECK (preorder >= 0),
    result_id TEXT NOT NULL CHECK (length(CAST(result_id AS BLOB)) > 0),
    estimated_bytes INTEGER NOT NULL CHECK (estimated_bytes >= 0),
    tag_count INTEGER NOT NULL CHECK (tag_count >= 0),
    property_count INTEGER NOT NULL CHECK (property_count >= 0),
    UNIQUE (page_id, preorder)
) STRICT";
pub const QUERY_PAGE_ORDER_DDL: &str = "CREATE TABLE query_page_order (
    page_id BLOB PRIMARY KEY CHECK (length(page_id) = 16)
        REFERENCES pages(page_id) ON DELETE CASCADE,
    position INTEGER NOT NULL UNIQUE CHECK (position >= 0)
) STRICT";
const QUERY_PROJECTION_STATE_DDL: &str = "CREATE TABLE query_projection_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    revision INTEGER NOT NULL CHECK (revision >= 0)
) STRICT";

pub const QUERY_PAGE_RESULTS_DDL: &str = "CREATE TABLE query_page_results (
    page_id BLOB PRIMARY KEY CHECK (length(page_id) = 16)
        REFERENCES pages(page_id) ON DELETE CASCADE,
    estimated_bytes INTEGER NOT NULL CHECK (estimated_bytes >= 0),
    property_count INTEGER NOT NULL CHECK (property_count >= 0)
) STRICT";
pub const BLOCK_OWN_REFS_DDL: &str = "CREATE TABLE block_own_refs (
    block_id BLOB NOT NULL CHECK (length(block_id) = 16)
        REFERENCES blocks(block_id) ON DELETE CASCADE,
    page_id BLOB NOT NULL CHECK (length(page_id) = 16)
        REFERENCES pages(page_id) ON DELETE CASCADE,
    normalized_name TEXT NOT NULL CHECK (length(CAST(normalized_name AS BLOB)) BETWEEN 1 AND 4194304),
    PRIMARY KEY (block_id, normalized_name)
) WITHOUT ROWID, STRICT";
pub const PROPERTY_ATOMS_DDL: &str = "CREATE TABLE property_atoms (
    owner_type INTEGER NOT NULL CHECK (owner_type IN (0, 1)),
    owner_id BLOB NOT NULL CHECK (length(owner_id) = 16),
    page_id BLOB NOT NULL CHECK (length(page_id) = 16),
    normalized_name TEXT NOT NULL CHECK (
        length(CAST(normalized_name AS BLOB)) BETWEEN 1 AND 4194304
    ),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    atom TEXT NOT NULL CHECK (length(CAST(atom AS BLOB)) <= 4194304),
    atom_key TEXT NOT NULL CHECK (length(CAST(atom_key AS BLOB)) <= 4194304),
    origin INTEGER NOT NULL CHECK (origin IN (0, 1)),
    atom_num REAL,
    atom_day INTEGER,
    PRIMARY KEY (owner_type, owner_id, normalized_name, ordinal)
) WITHOUT ROWID, STRICT";
pub const SEARCH_FTS_DDL: &str = "CREATE VIRTUAL TABLE search_fts USING fts5(
    entity_type UNINDEXED,
    entity_id UNINDEXED,
    page_id UNINDEXED,
    text UNINDEXED,
    normalized_text,
    tokenize = 'unicode61 remove_diacritics 0'
)";
pub const SEARCH_SUBSTRING_FTS_DDL: &str = "CREATE VIRTUAL TABLE search_substring_fts USING fts5(
    normalized_text,
    tokenize = 'trigram'
)";
pub const SEARCH_FTS_OWNERS_DDL: &str = "CREATE TABLE search_fts_owners (
    rowid INTEGER PRIMARY KEY,
    entity_type INTEGER NOT NULL CHECK (entity_type IN (0, 1)),
    entity_id BLOB NOT NULL CHECK (length(entity_id) = 16),
    page_id BLOB NOT NULL CHECK (length(page_id) = 16),
    UNIQUE (entity_type, entity_id)
) STRICT";

pub const PAGES_NAME_INDEX_DDL: &str = "CREATE INDEX pages_name_idx ON pages(name, page_id)";
pub const PAGES_NAME_KEY_INDEX_DDL: &str =
    "CREATE INDEX pages_name_key_idx ON pages(name_key, page_id)";
pub const PAGES_JOURNAL_DAY_INDEX_DDL: &str =
    "CREATE INDEX pages_journal_day_idx ON pages(journal_day, page_id)";
pub const PAGES_PATH_INDEX_DDL: &str = "CREATE INDEX pages_path_idx ON pages(path, page_id)";
pub const PAGES_HOME_DOCUMENT_ID_INDEX_DDL: &str =
    "CREATE INDEX pages_home_document_id_idx ON pages(home_document_id, page_id)";
pub const BLOCKS_PAGE_ORDER_INDEX_DDL: &str =
    "CREATE INDEX blocks_page_order_idx ON blocks(page_id, order_key, block_id)";
pub const BLOCKS_PARENT_PAGE_INDEX_DDL: &str = "CREATE INDEX blocks_parent_page_idx
    ON blocks(parent_block_id, page_id, block_id) WHERE parent_block_id IS NOT NULL";
pub const BLOCKS_LOGSEQ_UUID_INDEX_DDL: &str = "CREATE INDEX blocks_logseq_uuid_idx
    ON blocks(logseq_uuid, block_id) WHERE logseq_uuid IS NOT NULL";
pub const SEARCH_FTS_OWNERS_PAGE_INDEX_DDL: &str =
    "CREATE INDEX search_fts_owners_page_idx ON search_fts_owners(page_id, rowid)";
pub const REFERENCE_POSTINGS_SOURCE_INDEX_DDL: &str = "CREATE INDEX reference_postings_source_idx
    ON reference_postings(source_page_id, source_entity_type, source_entity_id, ordinal)";
pub const REFERENCE_POSTINGS_NORMALIZED_NAME_INDEX_DDL: &str =
    "CREATE INDEX reference_postings_normalized_name_idx
    ON reference_postings(normalized_name, source_page_id, source_entity_type, source_entity_id, ordinal)
    WHERE target_type = 0";
pub const REFERENCE_POSTINGS_RAW_UUID_INDEX_DDL: &str = "CREATE INDEX reference_postings_raw_uuid_idx
    ON reference_postings(raw_uuid_claim, source_page_id, source_entity_type, source_entity_id, ordinal)
    WHERE target_type = 1";
/// Covers the whole navigation name read: the key is exactly the statement's
/// `DISTINCT`/`ORDER BY` tuple, and `reference_kind` rides along so the
/// `<= 4` filter is answered from the index too. Nothing in
/// `NAVIGATION_REFERENCE_NAMES_*_SQL` needs the table.
///
/// `reference_postings_normalized_name_idx` cannot serve this read: it lacks
/// `raw_name`, so each row fell back to the table, and its key order forces a
/// temp B-tree for the `DISTINCT`. Measured on a 10,000-page graph (1.2M
/// postings), draining every distinct spelling went 1.276 s -> 0.022 s and
/// 110,000 rows -> 10,010. Column order is load-bearing: putting
/// `reference_kind` FIRST (a range predicate ahead of the sort keys) makes
/// SQLite sort instead, measuring 1.765 s — slower than no index at all.
pub const REFERENCE_POSTINGS_NAVIGATION_NAMES_INDEX_DDL: &str =
    "CREATE INDEX reference_postings_navigation_names_idx
    ON reference_postings(normalized_name, raw_name, reference_kind)
    WHERE target_type = 0";
pub const REFERENCE_ALIAS_DECLARATIONS_SOURCE_INDEX_DDL: &str =
    "CREATE INDEX reference_alias_declarations_source_idx
    ON reference_alias_declarations(source_page_id, source_entity_type, source_entity_id, ordinal)";
pub const PROPERTIES_LOOKUP_INDEX_DDL: &str = "CREATE INDEX properties_lookup_idx
    ON properties(normalized_name, value, page_id, owner_type, owner_id)";
pub const PROPERTIES_PAGE_INDEX_DDL: &str = "CREATE INDEX properties_page_idx
    ON properties(page_id, owner_type, owner_id, name, ordinal)";
pub const TAGS_LOOKUP_INDEX_DDL: &str =
    "CREATE INDEX tags_lookup_idx ON tags(tag_key, page_id, owner_type, owner_id)";
pub const TAGS_PAGE_INDEX_DDL: &str =
    "CREATE INDEX tags_page_idx ON tags(page_id, owner_type, owner_id, ordinal)";
pub const TASKS_MARKER_INDEX_DDL: &str =
    "CREATE INDEX tasks_marker_idx ON tasks(marker, page_id, block_id)";
pub const TASKS_DEADLINE_INDEX_DDL: &str =
    "CREATE INDEX tasks_deadline_idx ON tasks(deadline, scheduled, page_id, block_id)";
pub const TASKS_PAGE_INDEX_DDL: &str = "CREATE INDEX tasks_page_idx ON tasks(page_id, block_id)";
pub const BLOCK_PLANNING_PRIORITY_INDEX_DDL: &str =
    "CREATE INDEX block_planning_priority_idx ON block_planning(priority, page_id, block_id)";
pub const BLOCK_PLANNING_SCHEDULED_DAY_INDEX_DDL: &str =
    "CREATE INDEX block_planning_scheduled_day_idx
     ON block_planning(scheduled_day, page_id, block_id)";
pub const BLOCK_PLANNING_DEADLINE_DAY_INDEX_DDL: &str =
    "CREATE INDEX block_planning_deadline_day_idx
     ON block_planning(deadline_day, page_id, block_id)";
// Presence, not day: `scheduled IS NOT NULL` cannot search a `*_day` index --
// the malformed-date rows have a NULL day and are exactly the rows presence
// must still find (SPEC §5.7 C2).
pub const BLOCK_PLANNING_SCHEDULED_INDEX_DDL: &str =
    "CREATE INDEX block_planning_scheduled_idx ON block_planning(scheduled, page_id, block_id)";
pub const BLOCK_PLANNING_DEADLINE_INDEX_DDL: &str =
    "CREATE INDEX block_planning_deadline_idx ON block_planning(deadline, page_id, block_id)";
pub const BLOCK_PATH_REFS_LOOKUP_INDEX_DDL: &str = "CREATE INDEX block_path_refs_lookup_idx
    ON block_path_refs(normalized_name, page_id, block_id)";
pub const BLOCK_PATH_REFS_PAGE_INDEX_DDL: &str = "CREATE INDEX block_path_refs_page_idx
    ON block_path_refs(page_id, block_id, normalized_name)";
pub const PROPERTY_ATOMS_KEY_INDEX_DDL: &str = "CREATE INDEX property_atoms_key_idx
    ON property_atoms(normalized_name, atom_key, page_id, owner_type, owner_id)";
pub const PROPERTY_ATOMS_NUM_INDEX_DDL: &str = "CREATE INDEX property_atoms_num_idx
    ON property_atoms(normalized_name, atom_num, page_id, owner_type, owner_id)";
pub const PROPERTY_ATOMS_DAY_INDEX_DDL: &str = "CREATE INDEX property_atoms_day_idx
    ON property_atoms(normalized_name, atom_day, page_id, owner_type, owner_id)";
pub const PROPERTY_ATOMS_PAGE_INDEX_DDL: &str = "CREATE INDEX property_atoms_page_idx
    ON property_atoms(page_id, owner_type, owner_id, normalized_name, ordinal)";

// A terminal bootstrap candidate is a brand-new, unpublished database. Its
// ordinary secondary indexes can be built once after the complete row set is
// present instead of being maintained for every inserted row. The primary-key
// indexes and both FTS virtual tables remain live throughout construction.
// This list must reproduce the exact normal schema before the transaction can
// commit.
const TERMINAL_DEFERRED_INDEXES: [(&str, &str); 32] = [
    ("pages_name_idx", PAGES_NAME_INDEX_DDL),
    ("pages_name_key_idx", PAGES_NAME_KEY_INDEX_DDL),
    ("pages_journal_day_idx", PAGES_JOURNAL_DAY_INDEX_DDL),
    ("pages_path_idx", PAGES_PATH_INDEX_DDL),
    (
        "pages_home_document_id_idx",
        PAGES_HOME_DOCUMENT_ID_INDEX_DDL,
    ),
    ("blocks_page_order_idx", BLOCKS_PAGE_ORDER_INDEX_DDL),
    ("blocks_parent_page_idx", BLOCKS_PARENT_PAGE_INDEX_DDL),
    ("blocks_logseq_uuid_idx", BLOCKS_LOGSEQ_UUID_INDEX_DDL),
    (
        "search_fts_owners_page_idx",
        SEARCH_FTS_OWNERS_PAGE_INDEX_DDL,
    ),
    (
        "reference_postings_source_idx",
        REFERENCE_POSTINGS_SOURCE_INDEX_DDL,
    ),
    (
        "reference_postings_normalized_name_idx",
        REFERENCE_POSTINGS_NORMALIZED_NAME_INDEX_DDL,
    ),
    (
        "reference_postings_raw_uuid_idx",
        REFERENCE_POSTINGS_RAW_UUID_INDEX_DDL,
    ),
    (
        "reference_postings_navigation_names_idx",
        REFERENCE_POSTINGS_NAVIGATION_NAMES_INDEX_DDL,
    ),
    (
        "reference_alias_declarations_source_idx",
        REFERENCE_ALIAS_DECLARATIONS_SOURCE_INDEX_DDL,
    ),
    ("properties_lookup_idx", PROPERTIES_LOOKUP_INDEX_DDL),
    ("properties_page_idx", PROPERTIES_PAGE_INDEX_DDL),
    ("tags_lookup_idx", TAGS_LOOKUP_INDEX_DDL),
    ("tags_page_idx", TAGS_PAGE_INDEX_DDL),
    ("tasks_marker_idx", TASKS_MARKER_INDEX_DDL),
    ("tasks_deadline_idx", TASKS_DEADLINE_INDEX_DDL),
    ("tasks_page_idx", TASKS_PAGE_INDEX_DDL),
    (
        "block_planning_priority_idx",
        BLOCK_PLANNING_PRIORITY_INDEX_DDL,
    ),
    (
        "block_planning_scheduled_day_idx",
        BLOCK_PLANNING_SCHEDULED_DAY_INDEX_DDL,
    ),
    (
        "block_planning_deadline_day_idx",
        BLOCK_PLANNING_DEADLINE_DAY_INDEX_DDL,
    ),
    (
        "block_planning_scheduled_idx",
        BLOCK_PLANNING_SCHEDULED_INDEX_DDL,
    ),
    (
        "block_planning_deadline_idx",
        BLOCK_PLANNING_DEADLINE_INDEX_DDL,
    ),
    (
        "block_path_refs_lookup_idx",
        BLOCK_PATH_REFS_LOOKUP_INDEX_DDL,
    ),
    ("block_path_refs_page_idx", BLOCK_PATH_REFS_PAGE_INDEX_DDL),
    ("property_atoms_key_idx", PROPERTY_ATOMS_KEY_INDEX_DDL),
    ("property_atoms_num_idx", PROPERTY_ATOMS_NUM_INDEX_DDL),
    ("property_atoms_day_idx", PROPERTY_ATOMS_DAY_INDEX_DDL),
    ("property_atoms_page_idx", PROPERTY_ATOMS_PAGE_INDEX_DDL),
];

const MATERIALIZATION_TABLE_COLUMNS: [(&str, &[&str]); 18] = [
    (
        "reference_postings",
        &[
            "source_page_id",
            "source_entity_type",
            "source_entity_id",
            "source_locator",
            "ordinal",
            "reference_kind",
            "target_type",
            "raw_name",
            "normalized_name",
            "raw_uuid_claim",
            "resolved_page_id",
            "resolved_block_id",
        ],
    ),
    (
        "reference_alias_declarations",
        &[
            "source_page_id",
            "source_entity_type",
            "source_entity_id",
            "source_locator",
            "ordinal",
            "raw_alias",
            "normalized_alias",
        ],
    ),
    (
        "pages",
        &[
            "page_id",
            "home_document_id",
            "name",
            "name_key",
            "path",
            "text_kind",
            "journal_day",
        ],
    ),
    (
        "page_text",
        &[
            "page_id",
            "preamble",
            "searchable_text",
            "normalized_searchable_text",
        ],
    ),
    (
        "block_text",
        &[
            "block_id",
            "content",
            "searchable_text",
            "normalized_searchable_text",
            "query_visible",
        ],
    ),
    (
        "blocks",
        &[
            "block_id",
            "page_id",
            "home_document_id",
            "parent_block_id",
            "order_key",
            "query_visible_folded",
            "heading_level",
            "collapsed",
            "logseq_uuid",
            "logseq_identity_origin",
        ],
    ),
    (
        "properties",
        &[
            "owner_type",
            "owner_id",
            "page_id",
            "name",
            "normalized_name",
            "value",
            "ordinal",
        ],
    ),
    (
        "tags",
        &[
            "owner_type",
            "owner_id",
            "page_id",
            "tag",
            "tag_key",
            "ordinal",
        ],
    ),
    (
        "tasks",
        &[
            "block_id",
            "page_id",
            "marker",
            "priority",
            "scheduled",
            "deadline",
        ],
    ),
    (
        "block_planning",
        &[
            "block_id",
            "page_id",
            "priority",
            "scheduled",
            "scheduled_day",
            "deadline",
            "deadline_day",
        ],
    ),
    (
        "block_path_refs",
        &["block_id", "page_id", "normalized_name"],
    ),
    (
        "block_own_refs",
        &["block_id", "page_id", "normalized_name"],
    ),
    (
        "query_block_results",
        &[
            "block_id",
            "page_id",
            "preorder",
            "result_id",
            "estimated_bytes",
            "tag_count",
            "property_count",
        ],
    ),
    ("query_page_order", &["page_id", "position"]),
    ("query_projection_state", &["singleton", "revision"]),
    (
        "query_page_results",
        &["page_id", "estimated_bytes", "property_count"],
    ),
    (
        "property_atoms",
        &[
            "owner_type",
            "owner_id",
            "page_id",
            "normalized_name",
            "ordinal",
            "atom",
            "atom_key",
            "origin",
            "atom_num",
            "atom_day",
        ],
    ),
    (
        "search_fts_owners",
        &["rowid", "entity_type", "entity_id", "page_id"],
    ),
];

const MATERIALIZATION_SCHEMA_OBJECTS: [(&str, &str, &str); 51] = [
    ("table", "reference_postings", REFERENCE_POSTINGS_DDL),
    (
        "table",
        "reference_alias_declarations",
        REFERENCE_ALIAS_DECLARATIONS_DDL,
    ),
    ("table", "pages", PAGES_DDL),
    ("table", "page_text", PAGE_TEXT_DDL),
    ("table", "blocks", BLOCKS_DDL),
    ("table", "block_text", BLOCK_TEXT_DDL),
    ("table", "properties", PROPERTIES_DDL),
    ("table", "tags", TAGS_DDL),
    ("table", "tasks", TASKS_DDL),
    ("table", "block_planning", BLOCK_PLANNING_DDL),
    ("table", "block_path_refs", BLOCK_PATH_REFS_DDL),
    ("table", "block_own_refs", BLOCK_OWN_REFS_DDL),
    ("table", "query_block_results", QUERY_BLOCK_RESULTS_DDL),
    ("table", "query_page_order", QUERY_PAGE_ORDER_DDL),
    ("table", "query_page_results", QUERY_PAGE_RESULTS_DDL),
    (
        "table",
        "query_projection_state",
        QUERY_PROJECTION_STATE_DDL,
    ),
    ("table", "property_atoms", PROPERTY_ATOMS_DDL),
    ("table", "search_fts_owners", SEARCH_FTS_OWNERS_DDL),
    ("table", "search_fts", SEARCH_FTS_DDL),
    ("table", "search_substring_fts", SEARCH_SUBSTRING_FTS_DDL),
    ("index", "pages_name_idx", PAGES_NAME_INDEX_DDL),
    ("index", "pages_name_key_idx", PAGES_NAME_KEY_INDEX_DDL),
    (
        "index",
        "pages_journal_day_idx",
        PAGES_JOURNAL_DAY_INDEX_DDL,
    ),
    ("index", "pages_path_idx", PAGES_PATH_INDEX_DDL),
    (
        "index",
        "pages_home_document_id_idx",
        PAGES_HOME_DOCUMENT_ID_INDEX_DDL,
    ),
    (
        "index",
        "blocks_page_order_idx",
        BLOCKS_PAGE_ORDER_INDEX_DDL,
    ),
    (
        "index",
        "blocks_parent_page_idx",
        BLOCKS_PARENT_PAGE_INDEX_DDL,
    ),
    (
        "index",
        "blocks_logseq_uuid_idx",
        BLOCKS_LOGSEQ_UUID_INDEX_DDL,
    ),
    (
        "index",
        "search_fts_owners_page_idx",
        SEARCH_FTS_OWNERS_PAGE_INDEX_DDL,
    ),
    (
        "index",
        "reference_postings_source_idx",
        REFERENCE_POSTINGS_SOURCE_INDEX_DDL,
    ),
    (
        "index",
        "reference_postings_normalized_name_idx",
        REFERENCE_POSTINGS_NORMALIZED_NAME_INDEX_DDL,
    ),
    (
        "index",
        "reference_postings_raw_uuid_idx",
        REFERENCE_POSTINGS_RAW_UUID_INDEX_DDL,
    ),
    (
        "index",
        "reference_postings_navigation_names_idx",
        REFERENCE_POSTINGS_NAVIGATION_NAMES_INDEX_DDL,
    ),
    (
        "index",
        "reference_alias_declarations_source_idx",
        REFERENCE_ALIAS_DECLARATIONS_SOURCE_INDEX_DDL,
    ),
    (
        "index",
        "properties_lookup_idx",
        PROPERTIES_LOOKUP_INDEX_DDL,
    ),
    ("index", "properties_page_idx", PROPERTIES_PAGE_INDEX_DDL),
    ("index", "tags_lookup_idx", TAGS_LOOKUP_INDEX_DDL),
    ("index", "tags_page_idx", TAGS_PAGE_INDEX_DDL),
    ("index", "tasks_marker_idx", TASKS_MARKER_INDEX_DDL),
    ("index", "tasks_page_idx", TASKS_PAGE_INDEX_DDL),
    (
        "index",
        "block_planning_priority_idx",
        BLOCK_PLANNING_PRIORITY_INDEX_DDL,
    ),
    (
        "index",
        "block_planning_scheduled_day_idx",
        BLOCK_PLANNING_SCHEDULED_DAY_INDEX_DDL,
    ),
    (
        "index",
        "block_planning_deadline_day_idx",
        BLOCK_PLANNING_DEADLINE_DAY_INDEX_DDL,
    ),
    (
        "index",
        "block_planning_scheduled_idx",
        BLOCK_PLANNING_SCHEDULED_INDEX_DDL,
    ),
    (
        "index",
        "block_planning_deadline_idx",
        BLOCK_PLANNING_DEADLINE_INDEX_DDL,
    ),
    (
        "index",
        "block_path_refs_lookup_idx",
        BLOCK_PATH_REFS_LOOKUP_INDEX_DDL,
    ),
    (
        "index",
        "block_path_refs_page_idx",
        BLOCK_PATH_REFS_PAGE_INDEX_DDL,
    ),
    (
        "index",
        "property_atoms_key_idx",
        PROPERTY_ATOMS_KEY_INDEX_DDL,
    ),
    (
        "index",
        "property_atoms_num_idx",
        PROPERTY_ATOMS_NUM_INDEX_DDL,
    ),
    (
        "index",
        "property_atoms_day_idx",
        PROPERTY_ATOMS_DAY_INDEX_DDL,
    ),
    (
        "index",
        "property_atoms_page_idx",
        PROPERTY_ATOMS_PAGE_INDEX_DDL,
    ),
];

pub(crate) fn initialize_graph_projection_schema(
    connection: &Connection,
) -> Result<(), MaterializationError> {
    connection.execute_batch(&format!(
        "{REFERENCE_POSTINGS_DDL};
         {REFERENCE_ALIAS_DECLARATIONS_DDL};
         {PAGES_DDL};
         {PAGE_TEXT_DDL};
         {BLOCKS_DDL};
         {BLOCK_TEXT_DDL};
         {PROPERTIES_DDL};
         {TAGS_DDL};
         {TASKS_DDL};
         {BLOCK_PLANNING_DDL};
         {BLOCK_PATH_REFS_DDL};
         {BLOCK_OWN_REFS_DDL};
         {QUERY_BLOCK_RESULTS_DDL};
         {QUERY_PAGE_ORDER_DDL};
         {QUERY_PAGE_RESULTS_DDL};
         {QUERY_PROJECTION_STATE_DDL};
         {PROPERTY_ATOMS_DDL};
         {SEARCH_FTS_OWNERS_DDL};
         {SEARCH_FTS_DDL};
         {SEARCH_SUBSTRING_FTS_DDL};
         {PAGES_NAME_INDEX_DDL};
         {PAGES_NAME_KEY_INDEX_DDL};
         {PAGES_JOURNAL_DAY_INDEX_DDL};
         {PAGES_PATH_INDEX_DDL};
         {PAGES_HOME_DOCUMENT_ID_INDEX_DDL};
         {BLOCKS_PAGE_ORDER_INDEX_DDL};
         {BLOCKS_PARENT_PAGE_INDEX_DDL};
         {BLOCKS_LOGSEQ_UUID_INDEX_DDL};
         {SEARCH_FTS_OWNERS_PAGE_INDEX_DDL};
         {REFERENCE_POSTINGS_SOURCE_INDEX_DDL};
         {REFERENCE_POSTINGS_NORMALIZED_NAME_INDEX_DDL};
         {REFERENCE_POSTINGS_RAW_UUID_INDEX_DDL};
         {REFERENCE_POSTINGS_NAVIGATION_NAMES_INDEX_DDL};
         {REFERENCE_ALIAS_DECLARATIONS_SOURCE_INDEX_DDL};
         {PROPERTIES_LOOKUP_INDEX_DDL};
         {PROPERTIES_PAGE_INDEX_DDL};
         {TAGS_LOOKUP_INDEX_DDL};
         {TAGS_PAGE_INDEX_DDL};
         {TASKS_MARKER_INDEX_DDL};
         {TASKS_DEADLINE_INDEX_DDL};
         {TASKS_PAGE_INDEX_DDL};
         {BLOCK_PLANNING_PRIORITY_INDEX_DDL};
         {BLOCK_PLANNING_SCHEDULED_DAY_INDEX_DDL};
         {BLOCK_PLANNING_DEADLINE_DAY_INDEX_DDL};
         {BLOCK_PLANNING_SCHEDULED_INDEX_DDL};
         {BLOCK_PLANNING_DEADLINE_INDEX_DDL};
         {BLOCK_PATH_REFS_LOOKUP_INDEX_DDL};
         {BLOCK_PATH_REFS_PAGE_INDEX_DDL};
         {PROPERTY_ATOMS_KEY_INDEX_DDL};
         {PROPERTY_ATOMS_NUM_INDEX_DDL};
         {PROPERTY_ATOMS_DAY_INDEX_DDL};
         {PROPERTY_ATOMS_PAGE_INDEX_DDL};"
    ))?;
    connection.execute("INSERT INTO query_projection_state VALUES (1, 0)", [])?;
    Ok(())
}

pub(crate) fn validate_graph_projection_schema(
    connection: &Connection,
) -> Result<(), MaterializationError> {
    validate_schema_columns(connection, &MATERIALIZATION_TABLE_COLUMNS)?;
    for (object_type, name, expected) in &MATERIALIZATION_SCHEMA_OBJECTS {
        validate_schema_sql(connection, object_type, name, expected)?;
    }
    validate_schema_sql(
        connection,
        "index",
        "tasks_deadline_idx",
        TASKS_DEADLINE_INDEX_DDL,
    )?;
    query_projection_revision(connection)?;
    Ok(())
}

fn validate_schema_columns(
    connection: &Connection,
    tables: &[(&str, &[&str])],
) -> Result<(), MaterializationError> {
    for &(table, expected) in tables {
        let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
        let columns: Vec<String> = statement
            .query_map([], |row| row.get(1))?
            .collect::<Result<_, _>>()?;
        if columns != expected {
            return Err(MaterializationError::Schema(format!(
                "{table} columns {columns:?} != {expected:?}"
            )));
        }
    }
    Ok(())
}

fn validate_schema_sql(
    connection: &Connection,
    object_type: &str,
    name: &str,
    expected: &str,
) -> Result<(), MaterializationError> {
    let found: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type = ?1 AND name = ?2",
        params![object_type, name],
        |row| row.get(0),
    )?;
    if canonical_sql(&found) != canonical_sql(expected) {
        return Err(MaterializationError::Schema(format!(
            "{object_type} {name} does not match canonical DDL"
        )));
    }
    Ok(())
}

fn canonical_sql(sql: &str) -> String {
    sql.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

/// Every table one of [`TERMINAL_DEFERRED_INDEXES`] covers. A build may run
/// with those indexes dropped only while all of these are empty: the per-page
/// cleanup and FTS lookups that precede insertion are full scans without their
/// index — free on an empty table, quadratic on a populated one.
const DEFERRED_INDEX_TABLES: [&str; 11] = [
    "pages",
    "blocks",
    "search_fts_owners",
    "reference_postings",
    "reference_alias_declarations",
    "properties",
    "tags",
    "tasks",
    "block_planning",
    "block_path_refs",
    "property_atoms",
];

/// True when every table a deferred index covers holds no rows, i.e. the
/// projection is fresh or was just reset, so a bulk insert may build its
/// secondary indexes once at the end instead of maintaining them per row.
pub(crate) fn deferred_index_tables_are_empty(
    connection: &Connection,
) -> Result<bool, MaterializationError> {
    for table in DEFERRED_INDEX_TABLES {
        let occupied: bool = connection.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM {table})"),
            [],
            |row| row.get(0),
        )?;
        if occupied {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Drop the ordinary secondary indexes for a bulk build. Primary keys and the
/// FTS virtual tables stay live; [`create_deferred_indexes`] must run in the
/// same transaction before it commits.
pub(crate) fn drop_deferred_indexes(connection: &Connection) -> Result<(), MaterializationError> {
    for (name, _) in TERMINAL_DEFERRED_INDEXES {
        connection.execute(&format!("DROP INDEX {name}"), [])?;
    }
    Ok(())
}

/// Recreate the indexes [`drop_deferred_indexes`] removed, from the same DDL
/// the fresh schema uses, so the stored schema text is byte-identical to a
/// projection that never took the bulk route.
pub(crate) fn create_deferred_indexes(connection: &Connection) -> Result<(), MaterializationError> {
    for (_, ddl) in TERMINAL_DEFERRED_INDEXES {
        connection.execute(ddl, [])?;
    }
    Ok(())
}

fn insert_reference_posting(
    transaction: &Connection,
    posting: &PhysicalReferencePosting,
) -> Result<(), MaterializationError> {
    let (source_entity_type, source_entity_id) = posting.source_entity.sql_parts();
    let locator = &posting.source_locator;
    let (
        target_type,
        raw_name,
        normalized_name,
        raw_uuid_claim,
        resolved_page_id,
        resolved_block_id,
    ) = match &posting.target {
        PhysicalReferenceTarget::PageName {
            raw_name,
            normalized_name,
            resolved_page_id,
        } => (
            0_i64,
            Some(raw_name.as_str()),
            Some(normalized_name.as_str()),
            None,
            resolved_page_id.map(|id| id.to_vec()),
            None,
        ),
        PhysicalReferenceTarget::ExternalUuid {
            raw_claim,
            resolved_block_id,
        } => (
            1_i64,
            None,
            None,
            Some(raw_claim.to_vec()),
            None,
            resolved_block_id.map(|id| id.to_vec()),
        ),
    };
    execute_cached(
        transaction,
        "INSERT INTO reference_postings (
             source_page_id, source_entity_type, source_entity_id, source_locator,
             ordinal, reference_kind, target_type, raw_name, normalized_name,
             raw_uuid_claim, resolved_page_id, resolved_block_id
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            posting.source_page_id.as_slice(),
            source_entity_type,
            source_entity_id.as_slice(),
            locator,
            i64::from(posting.ordinal),
            posting.kind,
            target_type,
            raw_name,
            normalized_name,
            raw_uuid_claim,
            resolved_page_id,
            resolved_block_id,
        ],
    )?;
    Ok(())
}

fn insert_alias_declaration(
    transaction: &Connection,
    alias: &PhysicalAliasDeclaration,
) -> Result<(), MaterializationError> {
    let (source_entity_type, source_entity_id) = alias.source_entity.sql_parts();
    let locator = &alias.source_locator;
    execute_cached(
        transaction,
        "INSERT INTO reference_alias_declarations (
             source_page_id, source_entity_type, source_entity_id, source_locator,
             ordinal, raw_alias, normalized_alias
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            alias.source_page_id.as_slice(),
            source_entity_type,
            source_entity_id.as_slice(),
            locator,
            i64::from(alias.ordinal),
            &alias.raw_alias,
            &alias.normalized_alias,
        ],
    )?;
    Ok(())
}

pub(crate) fn query_projection_revision(
    connection: &Connection,
) -> Result<u64, MaterializationError> {
    let revision: Option<i64> = connection
        .query_row(
            "SELECT revision FROM query_projection_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    revision
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| {
            MaterializationError::Corrupt("query projection revision is missing or invalid".into())
        })
}

fn advance_query_projection_revision(connection: &Connection) -> Result<(), MaterializationError> {
    let changed = connection.execute(
        "UPDATE query_projection_state SET revision = revision + 1
         WHERE singleton = 1 AND revision < 9223372036854775807",
        [],
    )?;
    if changed != 1 {
        return Err(MaterializationError::Corrupt(
            "query projection revision is missing or exhausted".into(),
        ));
    }
    Ok(())
}

pub(crate) fn apply_graph_projection_rows(
    transaction: &Connection,
    replacements: &[PhysicalPage],
    deletions: &[[u8; 16]],
    fts_instrumentation: Option<&mut FtsChangeInstrumentation>,
) -> Result<ApplyChangeInstrumentation, MaterializationError> {
    advance_query_projection_revision(transaction)?;
    let affected_pages = replacements
        .iter()
        .map(|page| page.page_id)
        .chain(deletions.iter().copied())
        .collect::<BTreeSet<_>>();
    let old_fts = load_fts_source_rows(transaction, &affected_pages)?;
    let mut instrumentation = ApplyChangeInstrumentation::default();
    for page_id in deletions {
        let cleanup = delete_page(transaction, *page_id)?;
        instrumentation.cleanup_page_attempts += 1;
        instrumentation.cleanup_existing_pages += cleanup.existing_pages;
        instrumentation.cleanup_owned_rows += cleanup.owned_rows;
        instrumentation.cleanup_fts_rowids += cleanup.fts_rowids;
    }
    for page in replacements {
        let cleanup = delete_page(transaction, page.page_id)?;
        instrumentation.cleanup_page_attempts += 1;
        instrumentation.cleanup_existing_pages += cleanup.existing_pages;
        instrumentation.cleanup_owned_rows += cleanup.owned_rows;
        instrumentation.cleanup_fts_rowids += cleanup.fts_rowids;
    }
    for page in replacements {
        insert_page(transaction, page)?;
    }
    let new_fts = replacement_fts_rows(replacements)?;
    reconcile_fts_rows(
        transaction,
        old_fts,
        new_fts,
        &mut instrumentation,
        fts_instrumentation,
    )?;
    Ok(instrumentation)
}

pub(crate) fn replace_graph_projection_reference_facts(
    transaction: &Connection,
    change: &PhysicalGraphProjectionChange,
    aliases: &[PhysicalAliasDeclaration],
) -> Result<(), MaterializationError> {
    let replacement_ids = change
        .replacements
        .iter()
        .map(|page| page.page_id)
        .collect::<BTreeSet<_>>();
    if change
        .reference_postings
        .iter()
        .any(|posting| !replacement_ids.contains(&posting.source_page_id))
    {
        return Err(MaterializationError::InvalidInput(
            "graph-projection reference postings must belong to replacement pages".into(),
        ));
    }
    if aliases
        .iter()
        .any(|alias| !replacement_ids.contains(&alias.source_page_id))
    {
        return Err(MaterializationError::InvalidInput(
            "graph-projection aliases must belong to replacement pages".into(),
        ));
    }
    let affected_pages = replacement_ids
        .iter()
        .chain(change.deletions.iter())
        .copied()
        .collect::<BTreeSet<_>>();
    for page_id in &affected_pages {
        transaction.execute(
            "DELETE FROM reference_postings WHERE source_page_id = ?1",
            params![page_id.as_slice()],
        )?;
        transaction.execute(
            "DELETE FROM reference_alias_declarations WHERE source_page_id = ?1",
            params![page_id.as_slice()],
        )?;
    }
    for posting in &change.reference_postings {
        insert_reference_posting(transaction, posting)?;
    }
    for alias in aliases {
        insert_alias_declaration(transaction, alias)?;
    }
    Ok(())
}

pub(crate) fn reset_graph_projection_rows(
    transaction: &Connection,
) -> Result<(), MaterializationError> {
    advance_query_projection_revision(transaction)?;
    transaction.execute_batch(
        "DELETE FROM search_substring_fts;
         DELETE FROM search_fts;
         DELETE FROM search_fts_owners;
         DELETE FROM property_atoms;
         DELETE FROM block_path_refs;
         DELETE FROM block_own_refs;
         DELETE FROM query_block_results;
         DELETE FROM query_page_order;
         DELETE FROM query_page_results;
         DELETE FROM block_planning;
         DELETE FROM tasks;
         DELETE FROM tags;
         DELETE FROM properties;
         DELETE FROM reference_alias_declarations;
         DELETE FROM reference_postings;
         DELETE FROM block_text;
         DELETE FROM blocks;
         DELETE FROM page_text;
         DELETE FROM pages;",
    )?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct PageCleanupInstrumentation {
    existing_pages: usize,
    owned_rows: usize,
    fts_rowids: usize,
}

fn delete_page(
    transaction: &Connection,
    page_id: [u8; 16],
) -> Result<PageCleanupInstrumentation, MaterializationError> {
    let page = &page_id;
    let existing: i64 = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM pages WHERE page_id = ?1)",
        params![page.as_slice()],
        |row| row.get(0),
    )?;
    let mut instrumentation = PageCleanupInstrumentation {
        existing_pages: usize::from(existing != 0),
        ..PageCleanupInstrumentation::default()
    };
    instrumentation.owned_rows = instrumentation.owned_rows.saturating_add(
        transaction.execute(
            "DELETE FROM block_own_refs WHERE block_id IN (SELECT block_id FROM blocks WHERE page_id = ?1)",
            params![page.as_slice()],
        )?,
    );
    for table in [
        "query_block_results",
        "query_page_order",
        "query_page_results",
    ] {
        instrumentation.owned_rows =
            instrumentation
                .owned_rows
                .saturating_add(transaction.execute(
                    &format!("DELETE FROM {table} WHERE page_id = ?1"),
                    params![page.as_slice()],
                )?);
    }
    instrumentation.owned_rows = instrumentation
        .owned_rows
        .saturating_add(transaction.execute(
            "DELETE FROM properties WHERE page_id = ?1",
            params![page.as_slice()],
        )?);
    instrumentation.owned_rows = instrumentation
        .owned_rows
        .saturating_add(transaction.execute(
            "DELETE FROM property_atoms WHERE page_id = ?1",
            params![page.as_slice()],
        )?);
    instrumentation.owned_rows = instrumentation
        .owned_rows
        .saturating_add(transaction.execute(
            "DELETE FROM block_path_refs WHERE page_id = ?1",
            params![page.as_slice()],
        )?);
    instrumentation.owned_rows = instrumentation
        .owned_rows
        .saturating_add(transaction.execute(
            "DELETE FROM block_planning WHERE page_id = ?1",
            params![page.as_slice()],
        )?);
    instrumentation.owned_rows = instrumentation
        .owned_rows
        .saturating_add(transaction.execute(
            "DELETE FROM tags WHERE page_id = ?1",
            params![page.as_slice()],
        )?);
    instrumentation.owned_rows = instrumentation
        .owned_rows
        .saturating_add(transaction.execute(
            "DELETE FROM tasks WHERE page_id = ?1",
            params![page.as_slice()],
        )?);
    instrumentation.owned_rows += transaction.execute(
        "DELETE FROM block_text WHERE block_id IN (SELECT block_id FROM blocks WHERE page_id = ?1)",
        params![page.as_slice()],
    )?;
    instrumentation.owned_rows += transaction.execute(
        "DELETE FROM page_text WHERE page_id = ?1",
        params![page.as_slice()],
    )?;
    instrumentation.owned_rows = instrumentation
        .owned_rows
        .saturating_add(transaction.execute(
            "DELETE FROM blocks WHERE page_id = ?1",
            params![page.as_slice()],
        )?);
    instrumentation.owned_rows = instrumentation
        .owned_rows
        .saturating_add(transaction.execute(
            "DELETE FROM pages WHERE page_id = ?1",
            params![page.as_slice()],
        )?);
    Ok(instrumentation)
}

/// Execute one materialized row insert through the connection's
/// prepared-statement cache.
///
/// A graph-sized build runs the same handful of insert statements once per
/// page, block, and facet, so re-preparing each one per row dominates it. The
/// SQL text, parameters, and owning transaction are unchanged.
fn execute_cached(
    transaction: &Connection,
    sql: &str,
    parameters: &[&dyn rusqlite::ToSql],
) -> Result<usize, MaterializationError> {
    Ok(transaction.prepare_cached(sql)?.execute(parameters)?)
}

fn insert_page(transaction: &Connection, page: &PhysicalPage) -> Result<(), MaterializationError> {
    let page_id = &page.page_id;
    execute_cached(
        transaction,
        "INSERT INTO pages (
             page_id, home_document_id, name, name_key, path, text_kind,
             journal_day
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            page_id.as_slice(),
            page.home_document_id.as_slice(),
            &page.name,
            &page.name_key,
            page.path.as_str(),
            page.text_kind,
            page.journal_day,
        ],
    )?;
    execute_cached(
        transaction,
        "INSERT INTO page_text (page_id, preamble, searchable_text, normalized_searchable_text)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            page_id.as_slice(),
            &page.preamble,
            &page.searchable_text,
            &page.normalized_searchable_text,
        ],
    )?;
    insert_properties(
        transaction,
        PhysicalEntityId::Page(page.page_id),
        page.page_id,
        &page.properties,
    )?;
    insert_tags(
        transaction,
        PhysicalEntityId::Page(page.page_id),
        page.page_id,
        &page.tags,
    )?;
    insert_property_atoms(
        transaction,
        PhysicalEntityId::Page(page.page_id),
        page.page_id,
        &page.property_atoms,
    )?;
    let page_estimated = query_page_result_estimated_bytes(
        &page.name,
        &page.path,
        page.properties
            .iter()
            .map(|property| (property.name.as_str(), property.value.as_str())),
    );
    execute_cached(transaction,
        "INSERT INTO query_page_results (page_id, estimated_bytes, property_count) VALUES (?1, ?2, ?3)",
        params![page.page_id.as_slice(),
            i64::try_from(page_estimated).map_err(|_| MaterializationError::InvalidInput("query page estimate exceeds SQLite".into()))?,
            i64::try_from(page.properties.len()).map_err(|_| MaterializationError::InvalidInput("query page property count exceeds SQLite".into()))?])?;
    if let Some(position) = page.query_page_order {
        execute_cached(
            transaction,
            "INSERT INTO query_page_order (page_id, position) VALUES (?1, ?2)",
            params![
                page.page_id.as_slice(),
                i64::try_from(position).map_err(|_| MaterializationError::InvalidInput(
                    "query page position exceeds SQLite".into()
                ))?
            ],
        )?;
    }
    for block in &page.blocks {
        insert_block(transaction, page.page_id, block)?;
    }
    let traversal = query_block_preorder(
        page.blocks
            .iter()
            .map(|block| (block.block_id, block.parent, block.order.as_str())),
    )?;
    for (preorder, (index, _depth)) in traversal.into_iter().enumerate() {
        let block = &page.blocks[index];
        let estimated = query_result_estimated_bytes(
            &block.query_result_id,
            &block.content,
            block.tags.iter().map(|tag| tag.tag.as_str()),
            block
                .properties
                .iter()
                .map(|property| (property.name.as_str(), property.value.as_str())),
        );
        execute_cached(transaction,
            "INSERT INTO query_block_results (block_id, page_id, preorder, result_id, estimated_bytes, tag_count, property_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![block.block_id.as_slice(), page.page_id.as_slice(), preorder as i64,
                &block.query_result_id, i64::try_from(estimated).map_err(|_| MaterializationError::InvalidInput("query estimate exceeds SQLite".into()))?,
                block.tags.len() as i64, block.properties.len() as i64])?;
    }
    Ok(())
}

fn insert_block(
    transaction: &Connection,
    page_id: [u8; 16],
    block: &PhysicalBlock,
) -> Result<(), MaterializationError> {
    let (logseq_uuid, origin) = match (block.logseq_uuid, block.logseq_identity_origin) {
        (Some(uuid), Some(origin)) => (Some(uuid.to_vec()), Some(origin)),
        (None, None) => (None, None),
        _ => {
            return Err(MaterializationError::InvalidInput(format!(
                "block {} has incomplete Logseq identity metadata",
                uuid::Uuid::from_bytes(block.block_id)
            )));
        }
    };
    execute_cached(
        transaction,
        "INSERT INTO blocks (
             block_id, page_id, home_document_id, parent_block_id, order_key,
             query_visible_folded, heading_level,
             collapsed, logseq_uuid, logseq_identity_origin
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            block.block_id.as_slice(),
            page_id.as_slice(),
            block.home_document_id.as_slice(),
            block.parent.map(|parent| parent.to_vec()),
            &block.order,
            &block.query_visible_folded,
            block.heading_level.map(i64::from),
            i64::from(block.collapsed),
            logseq_uuid,
            origin,
        ],
    )?;
    execute_cached(
        transaction,
        "INSERT INTO block_text (block_id, content, searchable_text, normalized_searchable_text, query_visible)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![block.block_id.as_slice(), &block.content, &block.searchable_text,
            &block.normalized_searchable_text, &block.query_visible],
    )?;
    let owner = PhysicalEntityId::Block(block.block_id);
    insert_properties(transaction, owner, page_id, &block.properties)?;
    insert_tags(transaction, owner, page_id, &block.tags)?;
    insert_property_atoms(transaction, owner, page_id, &block.property_atoms)?;
    insert_path_refs(transaction, block.block_id, page_id, &block.path_refs)?;
    for name in block.own_refs.iter().collect::<BTreeSet<_>>() {
        execute_cached(
            transaction,
            "INSERT INTO block_own_refs (block_id, page_id, normalized_name) VALUES (?1, ?2, ?3)",
            params![block.block_id.as_slice(), page_id.as_slice(), name],
        )?;
    }
    if let Some(task) = &block.task {
        execute_cached(
            transaction,
            "INSERT INTO tasks (
                 block_id, page_id, marker, priority, scheduled, deadline
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                block.block_id.as_slice(),
                page_id.as_slice(),
                &task.marker,
                &task.priority,
                &task.scheduled,
                &task.deadline,
            ],
        )?;
    }
    if let Some(planning) = &block.planning {
        if planning.priority.is_none()
            && planning.scheduled.is_none()
            && planning.deadline.is_none()
        {
            return Err(MaterializationError::InvalidInput(format!(
                "block {} carries an empty planning facet",
                uuid::Uuid::from_bytes(block.block_id)
            )));
        }
        execute_cached(
            transaction,
            "INSERT INTO block_planning (
                 block_id, page_id, priority, scheduled, scheduled_day,
                 deadline, deadline_day
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                block.block_id.as_slice(),
                page_id.as_slice(),
                &planning.priority,
                &planning.scheduled,
                planning.scheduled_day,
                &planning.deadline,
                planning.deadline_day,
            ],
        )?;
    }
    Ok(())
}

fn load_fts_source_rows(
    transaction: &Connection,
    page_ids: &BTreeSet<[u8; 16]>,
) -> Result<BTreeMap<(i64, [u8; 16]), FtsEntityRow>, MaterializationError> {
    let mut rows = BTreeMap::new();
    for page_id in page_ids {
        let page = transaction
            .query_row(
                "SELECT searchable_text, normalized_searchable_text
                 FROM pages LEFT JOIN page_text USING (page_id) WHERE page_id = ?1",
                params![page_id.as_slice()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        if let Some((text, normalized_text)) = page {
            let row = FtsEntityRow {
                entity_type: 0,
                entity_id: *page_id,
                page_id: *page_id,
                text,
                normalized_text,
            };
            rows.insert(row.key(), row);
        }
        let mut statement = transaction.prepare(
            "SELECT block_id, searchable_text, normalized_searchable_text
             FROM blocks LEFT JOIN block_text USING (block_id) WHERE page_id = ?1 ORDER BY block_id",
        )?;
        let blocks = statement.query_map(params![page_id.as_slice()], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        for block in blocks {
            let (block_id, text, normalized_text) = block?;
            let row = FtsEntityRow {
                entity_type: 1,
                entity_id: decode_id(&block_id)?,
                page_id: *page_id,
                text,
                normalized_text,
            };
            if rows.insert(row.key(), row).is_some() {
                return Err(MaterializationError::Corrupt(
                    "duplicate FTS source entity in graph projection".into(),
                ));
            }
        }
    }
    Ok(rows)
}

fn replacement_fts_rows(
    replacements: &[PhysicalPage],
) -> Result<BTreeMap<(i64, [u8; 16]), FtsEntityRow>, MaterializationError> {
    let mut rows = BTreeMap::new();
    for page in replacements {
        let page_row = FtsEntityRow {
            entity_type: 0,
            entity_id: page.page_id,
            page_id: page.page_id,
            text: page.searchable_text.clone(),
            normalized_text: page.normalized_searchable_text.clone(),
        };
        if rows.insert(page_row.key(), page_row).is_some() {
            return Err(MaterializationError::InvalidInput(
                "replacement pages contain a duplicate page ID".into(),
            ));
        }
        for block in &page.blocks {
            let block_row = FtsEntityRow {
                entity_type: 1,
                entity_id: block.block_id,
                page_id: page.page_id,
                text: block.searchable_text.clone(),
                normalized_text: block.normalized_searchable_text.clone(),
            };
            if rows.insert(block_row.key(), block_row).is_some() {
                return Err(MaterializationError::InvalidInput(
                    "replacement pages contain a duplicate block ID".into(),
                ));
            }
        }
    }
    Ok(rows)
}

fn delete_fts_entity(
    transaction: &Connection,
    entity_type: i64,
    entity_id: [u8; 16],
) -> Result<bool, MaterializationError> {
    let rowid: Option<i64> = transaction
        .query_row(
            "SELECT rowid FROM search_fts_owners
             WHERE entity_type = ?1 AND entity_id = ?2",
            params![entity_type, entity_id.as_slice()],
            |row| row.get(0),
        )
        .optional()?;
    let Some(rowid) = rowid else {
        return Ok(false);
    };
    transaction.execute(
        "DELETE FROM search_substring_fts WHERE rowid = ?1",
        params![rowid],
    )?;
    transaction.execute("DELETE FROM search_fts WHERE rowid = ?1", params![rowid])?;
    transaction.execute(
        "DELETE FROM search_fts_owners WHERE rowid = ?1",
        params![rowid],
    )?;
    Ok(true)
}

fn reconcile_fts_rows(
    transaction: &Connection,
    old: BTreeMap<(i64, [u8; 16]), FtsEntityRow>,
    new: BTreeMap<(i64, [u8; 16]), FtsEntityRow>,
    instrumentation: &mut ApplyChangeInstrumentation,
    mut fts_instrumentation: Option<&mut FtsChangeInstrumentation>,
) -> Result<(), MaterializationError> {
    let keys = old
        .keys()
        .chain(new.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    for key in keys {
        let before = old.get(&key);
        let after = new.get(&key);
        if before == after {
            continue;
        }
        if let Some(stats) = fts_instrumentation.as_deref_mut() {
            if key.0 == 0 {
                stats.page_rows = stats.page_rows.saturating_add(1);
            } else {
                stats.block_rows = stats.block_rows.saturating_add(1);
            }
            stats.standard_rows = stats.standard_rows.saturating_add(1);
            stats.substring_rows = stats.substring_rows.saturating_add(1);
        }
        if delete_fts_entity(transaction, key.0, key.1)? {
            instrumentation.cleanup_fts_rowids =
                instrumentation.cleanup_fts_rowids.saturating_add(1);
        }
        if let Some(after) = after {
            insert_fts_row(transaction, after)?;
        }
    }
    Ok(())
}

fn insert_fts_row(
    transaction: &Connection,
    row: &FtsEntityRow,
) -> Result<(), MaterializationError> {
    if row.normalized_text.len() > MAX_MATERIALIZATION_FIELD_BYTES {
        return Err(resource_limit(
            "normalized searchable text bytes",
            row.normalized_text.len(),
            MAX_MATERIALIZATION_FIELD_BYTES,
        ));
    }
    let entity_type = match row.entity_type {
        0 => "page",
        1 => "block",
        _ => {
            return Err(MaterializationError::InvalidInput(
                "unknown FTS entity type".into(),
            ));
        }
    };
    execute_cached(
        transaction,
        "INSERT INTO search_fts_owners (entity_type, entity_id, page_id)
         VALUES (?1, ?2, ?3)",
        params![
            row.entity_type,
            row.entity_id.as_slice(),
            row.page_id.as_slice(),
        ],
    )?;
    let rowid = transaction.last_insert_rowid();
    execute_cached(
        transaction,
        "INSERT INTO search_fts (
             rowid, entity_type, entity_id, page_id, text, normalized_text
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            rowid,
            entity_type,
            uuid::Uuid::from_bytes(row.entity_id).simple().to_string(),
            uuid::Uuid::from_bytes(row.page_id).simple().to_string(),
            &row.text,
            &row.normalized_text,
        ],
    )?;
    execute_cached(
        transaction,
        "INSERT INTO search_substring_fts (rowid, normalized_text) VALUES (?1, ?2)",
        params![rowid, &row.normalized_text],
    )?;
    Ok(())
}

fn insert_properties(
    transaction: &Connection,
    owner: PhysicalEntityId,
    page_id: [u8; 16],
    properties: &[PhysicalProperty],
) -> Result<(), MaterializationError> {
    let (owner_type, owner_id) = owner.sql_parts();
    for (ordinal, property) in properties.iter().enumerate() {
        execute_cached(
            transaction,
            "INSERT INTO properties (
                 owner_type, owner_id, page_id, name, normalized_name, value, ordinal
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                owner_type,
                owner_id.as_slice(),
                page_id.as_slice(),
                &property.name,
                &property.normalized_name,
                &property.value,
                i64::try_from(ordinal).map_err(|_| {
                    MaterializationError::InvalidInput("property ordinal overflowed".into())
                })?,
            ],
        )?;
    }
    Ok(())
}

fn insert_tags(
    transaction: &Connection,
    owner: PhysicalEntityId,
    page_id: [u8; 16],
    tags: &[PhysicalTag],
) -> Result<(), MaterializationError> {
    let (owner_type, owner_id) = owner.sql_parts();
    for (ordinal, tag) in tags.iter().enumerate() {
        execute_cached(
            transaction,
            "INSERT INTO tags (owner_type, owner_id, page_id, tag, tag_key, ordinal)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                owner_type,
                owner_id.as_slice(),
                page_id.as_slice(),
                &tag.tag,
                &tag.tag_key,
                i64::try_from(ordinal).map_err(|_| {
                    MaterializationError::InvalidInput("tag ordinal overflowed".into())
                })?,
            ],
        )?;
    }
    Ok(())
}

fn insert_property_atoms(
    transaction: &Connection,
    owner: PhysicalEntityId,
    page_id: [u8; 16],
    atoms: &[PhysicalPropertyAtom],
) -> Result<(), MaterializationError> {
    let (owner_type, owner_id) = owner.sql_parts();
    for atom in atoms {
        execute_cached(
            transaction,
            "INSERT INTO property_atoms (
                 owner_type, owner_id, page_id, normalized_name, ordinal,
                 atom, atom_key, origin, atom_num, atom_day
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                owner_type,
                owner_id.as_slice(),
                page_id.as_slice(),
                &atom.normalized_name,
                i64::from(atom.ordinal),
                &atom.atom,
                &atom.atom_key,
                atom.origin,
                atom.atom_num,
                atom.atom_day,
            ],
        )?;
    }
    Ok(())
}

fn insert_path_refs(
    transaction: &Connection,
    block_id: [u8; 16],
    page_id: [u8; 16],
    names: &[String],
) -> Result<(), MaterializationError> {
    for name in names {
        execute_cached(
            transaction,
            "INSERT INTO block_path_refs (block_id, page_id, normalized_name)
             VALUES (?1, ?2, ?3)",
            params![block_id.as_slice(), page_id.as_slice(), name],
        )?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPageRow {
    pub page_id: [u8; 16],
    pub home_document_id: [u8; 16],
    pub name: String,
    pub name_key: String,
    pub path: String,
    pub text_kind: i64,
    pub preamble: Option<String>,
    pub searchable_text: String,
}

/// Lightweight page row for navigation/autocomplete.  It deliberately omits
/// searchable body text so a title lookup never retains graph-sized content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalNavigationPageRow {
    pub page_id: [u8; 16],
    pub name: String,
    pub name_key: String,
    pub path: String,
    pub text_kind: i64,
    pub preamble: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalNavigationAliasRow {
    pub source_page_id: [u8; 16],
    pub owner_name: String,
    pub owner_path: String,
    pub normalized_alias: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// One distinct page-reference spelling in the graph.
///
/// Deliberately NOT per source page: the consumer folds these by name, so
/// carrying `source_page_id` and the owner's `path` meant transporting one row
/// per (page, spelling) pair — 110,000 rows for 10,010 names on a 10,000-page
/// graph — and forced a `pages` join the covering index cannot serve.
pub struct PhysicalNavigationReferenceNameRow {
    pub normalized_name: String,
    pub raw_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalBlockRow {
    pub block_id: [u8; 16],
    pub page_id: [u8; 16],
    pub home_document_id: [u8; 16],
    pub parent: Option<[u8; 16]>,
    pub order: String,
    pub content: String,
    pub searchable_text: String,
    pub heading_level: Option<u8>,
    pub collapsed: bool,
    pub logseq_uuid: Option<[u8; 16]>,
    pub logseq_identity_origin: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalBlockReferenceCountRow {
    pub raw_uuid_claim: [u8; 16],
    pub distinct_source_blocks: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalBlockReferrerCandidateRow {
    pub source_page_id: [u8; 16],
    pub source_block_id: [u8; 16],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPageReferrerCandidateRow {
    pub source_page_id: [u8; 16],
    pub source: PhysicalEntityId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPlainTextCandidatePageRow {
    pub page_id: [u8; 16],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalFuzzyCandidatePageRow {
    pub page_id: [u8; 16],
    pub path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalBlockPropertyCandidateRow {
    pub page_id: [u8; 16],
    pub block_id: [u8; 16],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPropertyFacetRow {
    pub owner: PhysicalEntityId,
    pub page_id: [u8; 16],
    pub source_name: String,
    pub normalized_name: String,
    pub value: String,
    pub ordinal: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTaskCandidatePageRow {
    pub page_id: [u8; 16],
}

/// One physical task-index candidate with the raw block and page transport
/// fields needed for parser-owned task re-evaluation.
///
/// Priority, planning, heading, and other semantic facets are intentionally
/// absent: the application parser remains the authority for those meanings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTaskCandidateBlockRow {
    pub block_id: [u8; 16],
    pub page_id: [u8; 16],
    pub parent: Option<[u8; 16]>,
    pub order: String,
    pub content: String,
    pub logseq_uuid: Option<[u8; 16]>,
    pub page_name: String,
    pub page_path: String,
    pub page_text_kind: i64,
}

/// Structural coordinates for a task candidate whose parser-owned document is
/// already resident in the application.
///
/// Unlike [`PhysicalTaskCandidateBlockRow`], this deliberately does not copy
/// raw content or a public UUID across SQLite. Direct Files uses the page path
/// and full structural order to recover the exact current `DocBlock`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTaskCandidateLocatorRow {
    pub block_id: [u8; 16],
    pub page_id: [u8; 16],
    pub parent: Option<[u8; 16]>,
    pub order: String,
    pub page_name: String,
    pub page_path: String,
    pub page_text_kind: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPropertyRow {
    pub owner: PhysicalEntityId,
    pub page_id: [u8; 16],
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTagRow {
    pub owner: PhysicalEntityId,
    pub page_id: [u8; 16],
    pub tag: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTaskRow {
    pub block_id: [u8; 16],
    pub page_id: [u8; 16],
    pub marker: String,
    pub priority: Option<String>,
    pub scheduled: Option<String>,
    pub deadline: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalSearchHit {
    pub entity: PhysicalEntityId,
    pub page_id: [u8; 16],
    pub text: String,
    pub rank: f64,
}

#[derive(Default)]
struct MaterializationReadBudget {
    bytes: usize,
}

impl MaterializationReadBudget {
    fn add(&mut self, bytes: usize) -> Result<(), MaterializationError> {
        self.bytes = checked_budget_add(
            "materialization read output bytes",
            self.bytes,
            bytes,
            MAX_MATERIALIZATION_READ_BYTES,
        )?;
        Ok(())
    }
}

fn checked_output_bytes<'a>(
    fixed_bytes: usize,
    fields: impl IntoIterator<Item = Option<&'a str>>,
) -> Result<usize, MaterializationError> {
    fields.into_iter().try_fold(fixed_bytes, |total, field| {
        let Some(field) = field else {
            return Ok(total);
        };
        total
            .checked_add(field.len())
            .and_then(|total| total.checked_add(MATERIALIZATION_STRING_OVERHEAD_BYTES))
            .ok_or_else(|| {
                resource_limit(
                    "materialization read output bytes",
                    usize::MAX,
                    MAX_MATERIALIZATION_READ_BYTES,
                )
            })
    })
}

fn page_row_output_bytes(row: &PhysicalPageRow) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        64,
        [
            Some(row.name.as_str()),
            Some(row.name_key.as_str()),
            Some(row.path.as_str()),
            row.preamble.as_deref(),
            Some(row.searchable_text.as_str()),
        ],
    )
}

fn navigation_page_row_output_bytes(
    row: &PhysicalNavigationPageRow,
) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        32,
        [
            Some(row.name.as_str()),
            Some(row.name_key.as_str()),
            Some(row.path.as_str()),
            row.preamble.as_deref(),
        ],
    )
}

fn navigation_alias_row_output_bytes(
    row: &PhysicalNavigationAliasRow,
) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        16,
        [
            Some(row.owner_name.as_str()),
            Some(row.owner_path.as_str()),
            Some(row.normalized_alias.as_str()),
        ],
    )
}

fn navigation_reference_name_row_output_bytes(
    row: &PhysicalNavigationReferenceNameRow,
) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        0,
        [
            Some(row.raw_name.as_str()),
            Some(row.normalized_name.as_str()),
        ],
    )
}

fn block_row_output_bytes(row: &PhysicalBlockRow) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        96,
        [
            Some(row.order.as_str()),
            Some(row.content.as_str()),
            Some(row.searchable_text.as_str()),
        ],
    )
}

fn task_candidate_block_row_output_bytes(
    row: &PhysicalTaskCandidateBlockRow,
) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        72,
        [
            Some(row.order.as_str()),
            Some(row.content.as_str()),
            Some(row.page_name.as_str()),
            Some(row.page_path.as_str()),
        ],
    )
}

fn task_candidate_locator_row_output_bytes(
    row: &PhysicalTaskCandidateLocatorRow,
) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        64,
        [
            Some(row.order.as_str()),
            Some(row.page_name.as_str()),
            Some(row.page_path.as_str()),
        ],
    )
}

fn property_row_output_bytes(row: &PhysicalPropertyRow) -> Result<usize, MaterializationError> {
    checked_output_bytes(64, [Some(row.name.as_str()), Some(row.value.as_str())])
}

fn tag_row_output_bytes(row: &PhysicalTagRow) -> Result<usize, MaterializationError> {
    checked_output_bytes(64, [Some(row.tag.as_str())])
}

fn task_row_output_bytes(row: &PhysicalTaskRow) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        64,
        [
            Some(row.marker.as_str()),
            row.priority.as_deref(),
            row.scheduled.as_deref(),
            row.deadline.as_deref(),
        ],
    )
}

fn search_hit_output_bytes(row: &PhysicalSearchHit) -> Result<usize, MaterializationError> {
    checked_output_bytes(72, [Some(row.text.as_str())])
}

fn collect_read_rows<T>(
    rows: impl IntoIterator<Item = Result<T, MaterializationError>>,
    row_bytes: impl Fn(&T) -> Result<usize, MaterializationError>,
) -> Result<Vec<T>, MaterializationError> {
    let mut output = Vec::new();
    let mut budget = MaterializationReadBudget::default();
    for row in rows {
        let row = row?;
        budget.add(row_bytes(&row)?)?;
        output.push(row);
    }
    Ok(output)
}

fn checked_query_text(value: &str) -> Result<(), MaterializationError> {
    if value.len() > MAX_MATERIALIZATION_QUERY_BYTES {
        return Err(resource_limit(
            "materialization query bytes",
            value.len(),
            MAX_MATERIALIZATION_QUERY_BYTES,
        ));
    }
    Ok(())
}

/// A bounded, read-only view of regime-neutral graph facts.
pub struct SqliteGraphProjectionRead<'a> {
    connection: &'a Connection,
}

fn allow_any_page_header(_path: &str, _kind: i64) -> Result<(), MaterializationError> {
    Ok(())
}

const TASK_CANDIDATE_BLOCKS_SQL: &str =
    "SELECT t.block_id, t.page_id, b.parent_block_id, b.order_key,
            bt.content, b.logseq_uuid, p.name, p.path, p.text_kind
     FROM tasks AS t
     JOIN blocks AS b
       ON b.block_id = t.block_id AND b.page_id = t.page_id
     JOIN pages AS p ON p.page_id = t.page_id
     LEFT JOIN block_text AS bt ON bt.block_id = b.block_id
     WHERE t.marker = ?1
     ORDER BY t.page_id, t.block_id LIMIT ?2";

const TASK_CANDIDATE_BLOCKS_AFTER_SQL: &str =
    "SELECT t.block_id, t.page_id, b.parent_block_id, b.order_key,
            bt.content, b.logseq_uuid, p.name, p.path, p.text_kind
     FROM tasks AS t
     JOIN blocks AS b
       ON b.block_id = t.block_id AND b.page_id = t.page_id
     JOIN pages AS p ON p.page_id = t.page_id
     LEFT JOIN block_text AS bt ON bt.block_id = b.block_id
     WHERE t.marker = ?1
       AND (t.page_id, t.block_id) > (?2, ?3)
     ORDER BY t.page_id, t.block_id LIMIT ?4";

const TASK_CANDIDATE_LOCATORS_SQL: &str =
    "SELECT t.block_id, t.page_id, b.parent_block_id, b.order_key,
            p.name, p.path, p.text_kind
     FROM tasks AS t
     JOIN blocks AS b
       ON b.block_id = t.block_id AND b.page_id = t.page_id
     JOIN pages AS p ON p.page_id = t.page_id
     WHERE t.marker = ?1
     ORDER BY t.page_id, t.block_id LIMIT ?2";

const TASK_CANDIDATE_LOCATORS_AFTER_SQL: &str =
    "SELECT t.block_id, t.page_id, b.parent_block_id, b.order_key,
            p.name, p.path, p.text_kind
     FROM tasks AS t
     JOIN blocks AS b
       ON b.block_id = t.block_id AND b.page_id = t.page_id
     JOIN pages AS p ON p.page_id = t.page_id
     WHERE t.marker = ?1
       AND (t.page_id, t.block_id) > (?2, ?3)
     ORDER BY t.page_id, t.block_id LIMIT ?4";

/// Paged navigation readers, keyed on index-served columns.
///
/// Every `*_after` reader in this file is drained batch by batch by the
/// consumer, so its per-batch cost must be bounded by the batch, not by the
/// table: the keyset predicate is a row-value comparison SQLite turns into one
/// index range (`SEARCH … USING INDEX … (a,b)>(?,?)`), and the `ORDER BY`
/// prefix is the index order, so `LIMIT` ends the scan. `DISTINCT` and a
/// trailing sort term may still use a temporary b-tree; both are bounded by
/// the batch. `paged_navigation_readers_use_an_index_range` pins this.
pub(crate) const NAVIGATION_REFERENCE_NAMES_FIRST_SQL: &str =
    "SELECT DISTINCT r.normalized_name, r.raw_name
     FROM reference_postings r
     WHERE r.target_type = 0 AND r.reference_kind <= 4
     ORDER BY r.normalized_name, r.raw_name LIMIT ?1";
pub(crate) const NAVIGATION_REFERENCE_NAMES_AFTER_SQL: &str =
    "SELECT DISTINCT r.normalized_name, r.raw_name
     FROM reference_postings r
     WHERE r.target_type = 0 AND r.reference_kind <= 4
       AND (r.normalized_name, r.raw_name) > (?1, ?2)
     ORDER BY r.normalized_name, r.raw_name LIMIT ?3";
pub(crate) const NAVIGATION_ALIASES_FIRST_SQL: &str =
    "SELECT DISTINCT d.source_page_id, p.name, p.path, d.normalized_alias
     FROM reference_alias_declarations d
     JOIN pages p ON p.page_id = d.source_page_id
     ORDER BY d.source_page_id, d.normalized_alias LIMIT ?1";
pub(crate) const NAVIGATION_ALIASES_AFTER_SQL: &str =
    "SELECT DISTINCT d.source_page_id, p.name, p.path, d.normalized_alias
     FROM reference_alias_declarations d
     JOIN pages p ON p.page_id = d.source_page_id
     WHERE (d.source_page_id, d.normalized_alias) > (?1, ?2)
     ORDER BY d.source_page_id, d.normalized_alias LIMIT ?3";

impl<'a> SqliteGraphProjectionRead<'a> {
    pub(crate) const fn new(connection: &'a Connection) -> Self {
        Self { connection }
    }

    /// The `EXPLAIN QUERY PLAN` detail lines for `sql`, for plan-shape guards.
    #[cfg(test)]
    pub(crate) fn query_plan(
        &self,
        sql: &str,
        args: &[rusqlite::types::Value],
    ) -> Result<Vec<String>, MaterializationError> {
        let mut statement = self
            .connection
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
        let rows = statement.query_map(rusqlite::params_from_iter(args.iter()), |row| {
            row.get::<_, String>(3)
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn page(&self, page_id: [u8; 16]) -> Result<Option<PhysicalPageRow>, MaterializationError> {
        self.page_with_header_validation(page_id, allow_any_page_header)
    }

    pub fn page_with_header_validation(
        &self,
        page_id: [u8; 16],
        mut validate_header: impl FnMut(&str, i64) -> Result<(), MaterializationError>,
    ) -> Result<Option<PhysicalPageRow>, MaterializationError> {
        let page = self
            .connection
            .query_row(
                "SELECT page_id, home_document_id, name, name_key, path,
                        text_kind, preamble, searchable_text
                 FROM pages LEFT JOIN page_text USING (page_id) WHERE page_id = ?1",
                params![page_id.as_slice()],
                |row| page_row_with_header_validation(row, &mut validate_header),
            )
            .optional()
            .map_err(MaterializationError::from)?;
        let page = page.transpose()?;
        if let Some(row) = &page {
            let mut budget = MaterializationReadBudget::default();
            budget.add(page_row_output_bytes(row)?)?;
        }
        Ok(page)
    }

    pub fn block(
        &self,
        block_id: [u8; 16],
    ) -> Result<Option<PhysicalBlockRow>, MaterializationError> {
        let block = self
            .connection
            .query_row(
                "SELECT block_id, page_id, home_document_id, parent_block_id,
                        order_key, content, searchable_text, heading_level,
                        collapsed, logseq_uuid, logseq_identity_origin
                 FROM blocks LEFT JOIN block_text USING (block_id) WHERE block_id = ?1",
                params![block_id.as_slice()],
                block_row,
            )
            .optional()
            .map_err(MaterializationError::from)?;
        if let Some(row) = &block {
            let mut budget = MaterializationReadBudget::default();
            budget.add(block_row_output_bytes(row)?)?;
        }
        Ok(block)
    }

    /// Return every bounded block candidate that claims one public Logseq UUID.
    ///
    /// Duplicate claims are valid physical input. The disposable projection
    /// must preserve them so the application can diagnose or deterministically
    /// resolve the semantic ambiguity instead of letting SQLite select an
    /// arbitrary owner.
    pub fn blocks_by_logseq_uuid(
        &self,
        logseq_uuid: [u8; 16],
        limit: usize,
    ) -> Result<Vec<PhysicalBlockRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        let mut statement = self.connection.prepare(
            "SELECT block_id, page_id, home_document_id, parent_block_id,
                    order_key, content, searchable_text, heading_level,
                    collapsed, logseq_uuid, logseq_identity_origin
             FROM blocks LEFT JOIN block_text USING (block_id) WHERE logseq_uuid = ?1
             ORDER BY block_id LIMIT ?2",
        )?;
        let rows = statement.query_map(params![logseq_uuid.as_slice(), limit], block_row)?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            block_row_output_bytes,
        )
    }

    /// Bounded stable page listing for application-facing exact queries. This
    /// only reads the stamped materialization captured on construction; it is
    /// intentionally not a filesystem or graph-tree enumeration.
    pub fn pages(
        &self,
        kind: Option<i64>,
        limit: usize,
    ) -> Result<Vec<PhysicalPageRow>, MaterializationError> {
        self.pages_with_header_validation(kind, limit, allow_any_page_header)
    }

    pub(crate) fn pages_with_header_validation(
        &self,
        kind: Option<i64>,
        limit: usize,
        mut validate_header: impl FnMut(&str, i64) -> Result<(), MaterializationError>,
    ) -> Result<Vec<PhysicalPageRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match kind {
            Some(kind) => (
                "SELECT page_id, home_document_id, name, name_key, path,
                        text_kind, preamble, searchable_text
                 FROM pages LEFT JOIN page_text USING (page_id) WHERE text_kind = ?1 ORDER BY path, page_id LIMIT ?2",
                vec![kind.into(), limit.into()],
            ),
            None => (
                "SELECT page_id, home_document_id, name, name_key, path,
                        text_kind, preamble, searchable_text
                 FROM pages LEFT JOIN page_text USING (page_id) ORDER BY path, page_id LIMIT ?1",
                vec![limit.into()],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            page_row_with_header_validation(row, &mut validate_header)
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from).and_then(|row| row)),
            page_row_output_bytes,
        )
    }

    /// Stable pagination over the small page fields needed by navigation.
    /// Body/search text is deliberately excluded.
    pub fn navigation_pages_after_with_header_validation(
        &self,
        after_path: Option<&str>,
        after_page_id: Option<&[u8; 16]>,
        limit: usize,
        mut validate_header: impl FnMut(&str, i64) -> Result<(), MaterializationError>,
    ) -> Result<Vec<PhysicalNavigationPageRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        if after_path.is_some() != after_page_id.is_some() {
            return Err(MaterializationError::InvalidQuery(
                "navigation page cursor requires both path and page ID".into(),
            ));
        }
        if let Some(path) = after_path {
            checked_query_text(path)?;
        }
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match (after_path, after_page_id) {
            (None, None) => (
                "SELECT page_id, name, name_key, path, text_kind, preamble, page_text.page_id
                     FROM pages LEFT JOIN page_text USING (page_id) ORDER BY path, page_id LIMIT ?1",
                vec![limit.into()],
            ),
            (Some(path), Some(page_id)) => (
                "SELECT page_id, name, name_key, path, text_kind, preamble, page_text.page_id
                     FROM pages LEFT JOIN page_text USING (page_id)
                     WHERE path > ?1 OR (path = ?1 AND page_id > ?2)
                     ORDER BY path, page_id LIMIT ?3",
                vec![
                    path.to_owned().into(),
                    page_id.to_vec().into(),
                    limit.into(),
                ],
            ),
            _ => unreachable!("cursor presence was validated above"),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            navigation_page_row_with_header_validation(row, &mut validate_header)
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from).and_then(|row| row)),
            navigation_page_row_output_bytes,
        )
    }

    /// Stable, deduplicated alias declarations joined to their owning page.
    ///
    /// Pages in `(source_page_id, normalized_alias)` order — the order
    /// `reference_alias_declarations_source_idx` yields — so every batch is a
    /// bounded index range and `LIMIT` stops the scan. The cursor tuple keeps
    /// its `(owner_path, normalized_alias, source_page_id)` shape; `owner_path`
    /// is carried but no longer orders anything (an order over the joined
    /// page path cannot be served by any index, so each batch used to scan and
    /// sort the whole join: GH tine#543).
    pub fn navigation_aliases_after(
        &self,
        after: Option<(&str, &str, &[u8; 16])>,
        limit: usize,
    ) -> Result<Vec<PhysicalNavigationAliasRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        if let Some((path, alias, _)) = after {
            checked_query_text(path)?;
            checked_query_text(alias)?;
        }
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (NAVIGATION_ALIASES_FIRST_SQL, vec![limit.into()]),
            Some((_, alias, page_id)) => (
                NAVIGATION_ALIASES_AFTER_SQL,
                vec![
                    page_id.to_vec().into(),
                    alias.to_owned().into(),
                    limit.into(),
                ],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            let page_id: Vec<u8> = row.get(0)?;
            Ok(PhysicalNavigationAliasRow {
                source_page_id: decode_id_sql(&page_id)?,
                owner_name: row.get(1)?,
                owner_path: row.get(2)?,
                normalized_alias: row.get(3)?,
            })
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            navigation_alias_row_output_bytes,
        )
    }

    /// Every distinct page-reference spelling in the graph, once each.
    /// Property-key pseudo pages are excluded because the legacy navigation
    /// surface never advertised them.
    ///
    /// Pages in `(normalized_name, raw_name)` order — the key order of
    /// `reference_postings_navigation_names_idx` — so each batch is one
    /// bounded range over a COVERING index: no table lookup, no `pages` join,
    /// and no temp B-tree for either the `DISTINCT` or the `ORDER BY`. The
    /// cursor is that same pair.
    ///
    /// The cursor used to be `(owner_path, raw_name, normalized_name,
    /// source_page_id)` and a row was emitted per (page, spelling), so a
    /// 10,000-page graph drained 110,000 rows in 215 batches taking 1.276 s —
    /// after which the only consumer folded them by name and read no other
    /// column. Keyed to the `DISTINCT` instead: 10,010 rows, 20 batches,
    /// 0.022 s (GH tine#543).
    pub fn navigation_reference_names_after(
        &self,
        after: Option<(&str, &str)>,
        limit: usize,
    ) -> Result<Vec<PhysicalNavigationReferenceNameRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        if let Some((normalized, raw)) = after {
            checked_query_text(normalized)?;
            checked_query_text(raw)?;
        }
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (NAVIGATION_REFERENCE_NAMES_FIRST_SQL, vec![limit.into()]),
            Some((normalized, raw)) => (
                NAVIGATION_REFERENCE_NAMES_AFTER_SQL,
                vec![
                    normalized.to_owned().into(),
                    raw.to_owned().into(),
                    limit.into(),
                ],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            Ok(PhysicalNavigationReferenceNameRow {
                normalized_name: row.get(0)?,
                raw_name: row.get(1)?,
            })
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            navigation_reference_name_row_output_bytes,
        )
    }

    /// Aggregate raw UUID postings by distinct source block. Raw claims are
    /// used deliberately: a dangling `((uuid))` still drives a badge if a
    /// matching block later appears.
    pub fn block_reference_counts_after(
        &self,
        after: Option<[u8; 16]>,
        limit: usize,
    ) -> Result<Vec<PhysicalBlockReferenceCountRow>, MaterializationError> {
        self.block_reference_counts_query(None, after, limit)
    }

    fn block_reference_counts_query(
        &self,
        source_page_id: Option<[u8; 16]>,
        after: Option<[u8; 16]>,
        limit: usize,
    ) -> Result<Vec<PhysicalBlockReferenceCountRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match (source_page_id, after) {
            (None, None) => (
                "SELECT raw_uuid_claim, COUNT(DISTINCT source_entity_id)
                 FROM reference_postings
                 WHERE target_type = 1 AND source_entity_type = 1
                 GROUP BY raw_uuid_claim ORDER BY raw_uuid_claim LIMIT ?1",
                vec![limit.into()],
            ),
            (None, Some(after)) => (
                "SELECT raw_uuid_claim, COUNT(DISTINCT source_entity_id)
                 FROM reference_postings
                 WHERE target_type = 1 AND source_entity_type = 1
                   AND raw_uuid_claim > ?1
                 GROUP BY raw_uuid_claim ORDER BY raw_uuid_claim LIMIT ?2",
                vec![after.to_vec().into(), limit.into()],
            ),
            (Some(page_id), None) => (
                "SELECT raw_uuid_claim, COUNT(DISTINCT source_entity_id)
                 FROM reference_postings
                 WHERE target_type = 1 AND source_entity_type = 1
                   AND source_page_id = ?1
                 GROUP BY raw_uuid_claim ORDER BY raw_uuid_claim LIMIT ?2",
                vec![page_id.to_vec().into(), limit.into()],
            ),
            (Some(page_id), Some(after)) => (
                "SELECT raw_uuid_claim, COUNT(DISTINCT source_entity_id)
                 FROM reference_postings
                 WHERE target_type = 1 AND source_entity_type = 1
                   AND source_page_id = ?1 AND raw_uuid_claim > ?2
                 GROUP BY raw_uuid_claim ORDER BY raw_uuid_claim LIMIT ?3",
                vec![page_id.to_vec().into(), after.to_vec().into(), limit.into()],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            let uuid: Vec<u8> = row.get(0)?;
            let count: i64 = row.get(1)?;
            Ok(PhysicalBlockReferenceCountRow {
                raw_uuid_claim: decode_id_sql(&uuid)?,
                distinct_source_blocks: u64::try_from(count).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Integer,
                        "negative block-reference count".into(),
                    )
                })?,
            })
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            |_| Ok(32),
        )
    }

    pub fn block_referrer_candidates_after(
        &self,
        raw_uuid_claim: [u8; 16],
        after: Option<([u8; 16], [u8; 16])>,
        limit: usize,
    ) -> Result<Vec<PhysicalBlockReferrerCandidateRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (
                "SELECT DISTINCT source_page_id, source_entity_id
                 FROM reference_postings
                 WHERE target_type = 1 AND source_entity_type = 1
                   AND raw_uuid_claim = ?1
                 ORDER BY source_page_id, source_entity_id LIMIT ?2",
                vec![raw_uuid_claim.to_vec().into(), limit.into()],
            ),
            Some((page_id, block_id)) => (
                "SELECT DISTINCT source_page_id, source_entity_id
                 FROM reference_postings
                 WHERE target_type = 1 AND source_entity_type = 1
                   AND raw_uuid_claim = ?1
                   AND (source_page_id > ?2
                     OR (source_page_id = ?2 AND source_entity_id > ?3))
                 ORDER BY source_page_id, source_entity_id LIMIT ?4",
                vec![
                    raw_uuid_claim.to_vec().into(),
                    page_id.to_vec().into(),
                    block_id.to_vec().into(),
                    limit.into(),
                ],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            let page_id: Vec<u8> = row.get(0)?;
            let block_id: Vec<u8> = row.get(1)?;
            Ok(PhysicalBlockReferrerCandidateRow {
                source_page_id: decode_id_sql(&page_id)?,
                source_block_id: decode_id_sql(&block_id)?,
            })
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            |_| Ok(32),
        )
    }

    /// Stable source candidates for one normalized explicit page-reference
    /// target. Property-key pseudo pages are not backlinks. Duplicate syntax
    /// occurrences collapse to one source entity; the parser-owned application
    /// page verifies exact membership before exposure.
    pub fn page_referrer_candidates_after(
        &self,
        normalized_name: &str,
        after: Option<([u8; 16], PhysicalEntityId)>,
        limit: usize,
    ) -> Result<Vec<PhysicalPageReferrerCandidateRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        checked_query_text(normalized_name)?;
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (
                "SELECT DISTINCT source_page_id, source_entity_type, source_entity_id
                 FROM reference_postings
                 WHERE target_type = 0 AND reference_kind <= 4
                   AND normalized_name = ?1
                 ORDER BY source_page_id, source_entity_type, source_entity_id LIMIT ?2",
                vec![normalized_name.to_owned().into(), limit.into()],
            ),
            Some((page_id, source)) => {
                let (source_type, source_id) = source.sql_parts();
                (
                    "SELECT DISTINCT source_page_id, source_entity_type, source_entity_id
                     FROM reference_postings
                     WHERE target_type = 0 AND reference_kind <= 4
                       AND normalized_name = ?1
                       AND (source_page_id > ?2
                         OR (source_page_id = ?2 AND source_entity_type > ?3)
                         OR (source_page_id = ?2 AND source_entity_type = ?3
                             AND source_entity_id > ?4))
                     ORDER BY source_page_id, source_entity_type, source_entity_id LIMIT ?5",
                    vec![
                        normalized_name.to_owned().into(),
                        page_id.to_vec().into(),
                        source_type.into(),
                        source_id.to_vec().into(),
                        limit.into(),
                    ],
                )
            }
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        let rows = rows.map(
            |row| -> Result<PhysicalPageReferrerCandidateRow, MaterializationError> {
                let (page_id, source_type, source_id) = row.map_err(MaterializationError::from)?;
                Ok(PhysicalPageReferrerCandidateRow {
                    source_page_id: decode_id(&page_id)?,
                    source: decode_entity(source_type, &source_id)?,
                })
            },
        );
        collect_read_rows(rows, |_| Ok(32))
    }

    /// Page-level candidates for one normalized literal phrase under the
    /// indexed `unicode61` token contract. Punctuation may make this
    /// overinclusive; the application parser decides exact membership.
    pub fn plain_text_candidate_pages_after(
        &self,
        normalized_phrase: &str,
        after: Option<[u8; 16]>,
        limit: usize,
    ) -> Result<Vec<PhysicalPlainTextCandidatePageRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        checked_query_text(normalized_phrase)?;
        if normalized_phrase.trim().is_empty()
            || !normalized_phrase.chars().any(char::is_alphanumeric)
        {
            return Err(MaterializationError::InvalidQuery(
                "normalized literal phrase has no unicode61 word token".into(),
            ));
        }
        let phrase = format!("\"{}\"", normalized_phrase.replace('"', "\"\""));
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (
                "SELECT DISTINCT owner.page_id
                 FROM search_fts
                 JOIN search_fts_owners owner ON owner.rowid = search_fts.rowid
                 WHERE normalized_text MATCH ?1
                 ORDER BY owner.page_id LIMIT ?2",
                vec![phrase.into(), limit.into()],
            ),
            Some(page_id) => (
                "SELECT DISTINCT owner.page_id
                 FROM search_fts
                 JOIN search_fts_owners owner ON owner.rowid = search_fts.rowid
                 WHERE normalized_text MATCH ?1 AND owner.page_id > ?2
                 ORDER BY owner.page_id LIMIT ?3",
                vec![phrase.into(), page_id.to_vec().into(), limit.into()],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            let page_id: Vec<u8> = row.get(0)?;
            Ok(PhysicalPlainTextCandidatePageRow {
                page_id: decode_id_sql(&page_id)?,
            })
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            |_| Ok(16),
        )
    }

    /// Page-level candidates for the legacy ordered-subsequence matcher.
    /// Stored text is already application-normalized, so SQLite only selects
    /// pages; the parser-owned matcher still ranks blocks and produces evidence.
    pub fn fuzzy_subsequence_candidate_pages_after(
        &self,
        normalized_needle: &str,
        after: Option<[u8; 16]>,
        limit: usize,
    ) -> Result<Vec<PhysicalFuzzyCandidatePageRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        checked_query_text(normalized_needle)?;
        if normalized_needle.is_empty() {
            return Err(MaterializationError::InvalidQuery(
                "normalized fuzzy needle must be non-empty".into(),
            ));
        }
        let mut pattern = String::with_capacity(normalized_needle.len().saturating_mul(2) + 1);
        pattern.push('%');
        for character in normalized_needle.chars() {
            if matches!(character, '%' | '_' | '\\') {
                pattern.push('\\');
            }
            pattern.push(character);
            pattern.push('%');
        }
        checked_query_text(&pattern)?;
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (
                "SELECT DISTINCT owner.page_id, pages.path
                 FROM search_substring_fts AS substring
                 JOIN search_fts_owners AS owner ON owner.rowid = substring.rowid
                 JOIN pages ON pages.page_id = owner.page_id
                 WHERE substring.normalized_text LIKE ?1 ESCAPE '\\'
                 ORDER BY owner.page_id LIMIT ?2",
                vec![pattern.into(), limit.into()],
            ),
            Some(page_id) => (
                "SELECT DISTINCT owner.page_id, pages.path
                 FROM search_substring_fts AS substring
                 JOIN search_fts_owners AS owner ON owner.rowid = substring.rowid
                 JOIN pages ON pages.page_id = owner.page_id
                 WHERE substring.normalized_text LIKE ?1 ESCAPE '\\'
                   AND owner.page_id > ?2
                 ORDER BY owner.page_id LIMIT ?3",
                vec![pattern.into(), page_id.to_vec().into(), limit.into()],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            let page_id: Vec<u8> = row.get(0)?;
            Ok(PhysicalFuzzyCandidatePageRow {
                page_id: decode_id_sql(&page_id)?,
                path: row.get(1)?,
            })
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            |row| checked_output_bytes(16, [Some(row.path.as_str())]),
        )
    }

    /// Stable block owners for one canonical property key. Rows are candidates:
    /// callers retain semantic ownership of property parsing and subtree shape.
    pub fn block_property_candidates_after(
        &self,
        normalized_name: &str,
        after: Option<([u8; 16], [u8; 16])>,
        limit: usize,
    ) -> Result<Vec<PhysicalBlockPropertyCandidateRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        checked_query_text(normalized_name)?;
        if normalized_name.trim().is_empty() {
            return Err(MaterializationError::InvalidQuery(
                "normalized property name must be non-empty".into(),
            ));
        }
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (
                "SELECT DISTINCT page_id, owner_id
                 FROM properties
                 WHERE owner_type = 1 AND normalized_name = ?1
                 ORDER BY page_id, owner_id LIMIT ?2",
                vec![normalized_name.to_owned().into(), limit.into()],
            ),
            Some((page_id, block_id)) => (
                "SELECT DISTINCT page_id, owner_id
                 FROM properties
                 WHERE owner_type = 1 AND normalized_name = ?1
                   AND (page_id > ?2 OR (page_id = ?2 AND owner_id > ?3))
                 ORDER BY page_id, owner_id LIMIT ?4",
                vec![
                    normalized_name.to_owned().into(),
                    page_id.to_vec().into(),
                    block_id.to_vec().into(),
                    limit.into(),
                ],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            let page_id: Vec<u8> = row.get(0)?;
            let block_id: Vec<u8> = row.get(1)?;
            Ok(PhysicalBlockPropertyCandidateRow {
                page_id: decode_id_sql(&page_id)?,
                block_id: decode_id_sql(&block_id)?,
            })
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            |_| Ok(32),
        )
    }

    /// Traverse property facts in their stable primary-key order. The caller
    /// can request block owners only (query-builder policy) or both page and
    /// block owners (editor autocomplete policy). Values remain parser-derived
    /// facts; policy such as hidden/internal keys belongs to the caller.
    pub fn property_facet_rows_after(
        &self,
        block_owners_only: bool,
        after: Option<(PhysicalEntityId, String, u32)>,
        limit: usize,
    ) -> Result<Vec<PhysicalPropertyFacetRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        if let Some((owner, name, _)) = &after {
            checked_query_text(name)?;
            if block_owners_only && !matches!(owner, PhysicalEntityId::Block(_)) {
                return Err(MaterializationError::InvalidQuery(
                    "block-only property cursor must identify a block".into(),
                ));
            }
        }
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (
                "SELECT owner_type, owner_id, page_id, name, normalized_name, value, ordinal
                 FROM properties
                 WHERE (?1 = 0 OR owner_type = 1)
                 ORDER BY owner_type, owner_id, name, ordinal LIMIT ?2",
                vec![i64::from(block_owners_only).into(), limit.into()],
            ),
            Some((owner, name, ordinal)) => {
                let (owner_type, owner_id) = owner.sql_parts();
                (
                    "SELECT owner_type, owner_id, page_id, name, normalized_name, value, ordinal
                     FROM properties
                     WHERE (?1 = 0 OR owner_type = 1)
                       AND (owner_type > ?2
                         OR (owner_type = ?2 AND owner_id > ?3)
                         OR (owner_type = ?2 AND owner_id = ?3 AND name > ?4)
                         OR (owner_type = ?2 AND owner_id = ?3 AND name = ?4 AND ordinal > ?5))
                     ORDER BY owner_type, owner_id, name, ordinal LIMIT ?6",
                    vec![
                        i64::from(block_owners_only).into(),
                        owner_type.into(),
                        owner_id.to_vec().into(),
                        name.into(),
                        i64::from(ordinal).into(),
                        limit.into(),
                    ],
                )
            }
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })?;
        collect_read_rows(
            rows.map(|row| {
                let (owner_type, owner_id, page_id, source_name, normalized_name, value, ordinal) =
                    row?;
                Ok(PhysicalPropertyFacetRow {
                    owner: decode_entity(owner_type, &owner_id)?,
                    page_id: decode_id(&page_id)?,
                    source_name,
                    normalized_name,
                    value,
                    ordinal: u32::try_from(ordinal).map_err(|_| {
                        MaterializationError::Corrupt(
                            "property ordinal is negative or exceeds u32".into(),
                        )
                    })?,
                })
            }),
            |row| {
                Ok(row
                    .source_name
                    .len()
                    .saturating_add(row.normalized_name.len())
                    .saturating_add(row.value.len())
                    .saturating_add(96))
            },
        )
    }

    pub fn properties(
        &self,
        owner: PhysicalEntityId,
        limit: usize,
    ) -> Result<Vec<PhysicalPropertyRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        let (owner_type, owner_id) = owner.sql_parts();
        let mut statement = self.connection.prepare(
            "SELECT owner_type, owner_id, page_id, name, value
             FROM properties WHERE owner_type = ?1 AND owner_id = ?2
             ORDER BY name, ordinal, value LIMIT ?3",
        )?;
        let rows = property_rows(statement.query_map(
            params![owner_type, owner_id.as_slice(), limit],
            property_tuple,
        )?);
        rows
    }

    pub fn tags(
        &self,
        tag: &str,
        limit: usize,
    ) -> Result<Vec<PhysicalTagRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        checked_query_text(tag)?;
        let mut statement = self.connection.prepare(
            "SELECT owner_type, owner_id, page_id, tag
             FROM tags WHERE tag = ?1
             ORDER BY page_id, owner_type, owner_id, ordinal LIMIT ?2",
        )?;
        let rows = statement.query_map(params![tag, limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        let rows = rows.map(|row| {
            let (owner_type, owner_id, page_id, tag) = row?;
            Ok(PhysicalTagRow {
                owner: decode_entity(owner_type, &owner_id)?,
                page_id: decode_id(&page_id)?,
                tag,
            })
        });
        collect_read_rows(rows, tag_row_output_bytes)
    }

    pub fn tasks(
        &self,
        marker: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PhysicalTaskRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        if let Some(marker) = marker {
            checked_query_text(marker)?;
        }
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match marker {
            Some(marker) => (
                "SELECT block_id, page_id, marker, priority, scheduled, deadline
                 FROM tasks WHERE marker = ?1
                 ORDER BY deadline IS NULL, deadline, scheduled IS NULL, scheduled,
                          page_id, block_id LIMIT ?2",
                vec![
                    rusqlite::types::Value::Text(marker.to_owned()),
                    limit.into(),
                ],
            ),
            None => (
                "SELECT block_id, page_id, marker, priority, scheduled, deadline
                 FROM tasks
                 ORDER BY deadline IS NULL, deadline, scheduled IS NULL, scheduled,
                          page_id, block_id LIMIT ?1",
                vec![limit.into()],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;
        let rows = rows.map(|row| {
            let (block_id, page_id, marker, priority, scheduled, deadline) = row?;
            Ok(PhysicalTaskRow {
                block_id: decode_id(&block_id)?,
                page_id: decode_id(&page_id)?,
                marker,
                priority,
                scheduled,
                deadline,
            })
        });
        collect_read_rows(rows, task_row_output_bytes)
    }

    /// Distinct pages containing one exact task marker, in stable page-ID order.
    /// The task index supplies candidates only; application policy re-evaluates
    /// parser-owned current pages before exposing results.
    pub fn task_candidate_pages_after(
        &self,
        marker: &str,
        after: Option<[u8; 16]>,
        limit: usize,
    ) -> Result<Vec<PhysicalTaskCandidatePageRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        checked_query_text(marker)?;
        if marker.trim().is_empty() {
            return Err(MaterializationError::InvalidQuery(
                "task marker must be non-empty".into(),
            ));
        }
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (
                "SELECT DISTINCT page_id FROM tasks
                 WHERE marker = ?1 ORDER BY page_id LIMIT ?2",
                vec![marker.to_owned().into(), limit.into()],
            ),
            Some(page_id) => (
                "SELECT DISTINCT page_id FROM tasks
                 WHERE marker = ?1 AND page_id > ?2
                 ORDER BY page_id LIMIT ?3",
                vec![
                    marker.to_owned().into(),
                    page_id.to_vec().into(),
                    limit.into(),
                ],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            let page_id: Vec<u8> = row.get(0)?;
            Ok(PhysicalTaskCandidatePageRow {
                page_id: decode_id_sql(&page_id)?,
            })
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            |_| Ok(16),
        )
    }

    /// Raw block candidates for one canonical task marker in strict
    /// `(page_id, block_id)` order.
    ///
    /// The task index is only a physical prefilter. The marker comparison is
    /// deliberately exact: callers canonicalize marker case before crossing
    /// this storage boundary, then re-evaluate parser-owned task semantics.
    pub fn task_candidate_blocks_after(
        &self,
        marker: &str,
        after: Option<([u8; 16], [u8; 16])>,
        limit: usize,
    ) -> Result<Vec<PhysicalTaskCandidateBlockRow>, MaterializationError> {
        self.task_candidate_blocks_after_with_header_validation(
            marker,
            after,
            limit,
            allow_any_page_header,
        )
    }

    /// [`Self::task_candidate_blocks_after`] with application-owned page
    /// header validation for every joined candidate page.
    pub(crate) fn task_candidate_blocks_after_with_header_validation(
        &self,
        marker: &str,
        after: Option<([u8; 16], [u8; 16])>,
        limit: usize,
        mut validate_header: impl FnMut(&str, i64) -> Result<(), MaterializationError>,
    ) -> Result<Vec<PhysicalTaskCandidateBlockRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        checked_query_text(marker)?;
        if marker.trim().is_empty() {
            return Err(MaterializationError::InvalidQuery(
                "task marker must be non-empty".into(),
            ));
        }
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (
                TASK_CANDIDATE_BLOCKS_SQL,
                vec![marker.to_owned().into(), limit.into()],
            ),
            Some((page_id, block_id)) => (
                TASK_CANDIDATE_BLOCKS_AFTER_SQL,
                vec![
                    marker.to_owned().into(),
                    page_id.to_vec().into(),
                    block_id.to_vec().into(),
                    limit.into(),
                ],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            task_candidate_block_row_with_header_validation(row, &mut validate_header)
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from).and_then(|row| row)),
            task_candidate_block_row_output_bytes,
        )
    }

    /// Structural task candidates for a caller that already owns the exact
    /// parser document at this projection generation.
    ///
    /// The cursor and marker rules match [`Self::task_candidate_blocks_after`],
    /// but the row omits raw content and external UUID transport. The caller
    /// must locate and identity-check the parser block before exposing a result;
    /// this physical API does not make a semantic claim by itself.
    pub fn task_candidate_locators_after(
        &self,
        marker: &str,
        after: Option<([u8; 16], [u8; 16])>,
        limit: usize,
    ) -> Result<Vec<PhysicalTaskCandidateLocatorRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        checked_query_text(marker)?;
        if marker.trim().is_empty() {
            return Err(MaterializationError::InvalidQuery(
                "task marker must be non-empty".into(),
            ));
        }
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (
                TASK_CANDIDATE_LOCATORS_SQL,
                vec![marker.to_owned().into(), limit.into()],
            ),
            Some((page_id, block_id)) => (
                TASK_CANDIDATE_LOCATORS_AFTER_SQL,
                vec![
                    marker.to_owned().into(),
                    page_id.to_vec().into(),
                    block_id.to_vec().into(),
                    limit.into(),
                ],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            let block_id: Vec<u8> = row.get(0)?;
            let page_id: Vec<u8> = row.get(1)?;
            let parent: Option<Vec<u8>> = row.get(2)?;
            Ok(PhysicalTaskCandidateLocatorRow {
                block_id: decode_id_sql(&block_id)?,
                page_id: decode_id_sql(&page_id)?,
                parent: parent.as_deref().map(decode_id_sql).transpose()?,
                order: row.get(3)?,
                page_name: row.get(4)?,
                page_path: row.get(5)?,
                page_text_kind: row.get(6)?,
            })
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            task_candidate_locator_row_output_bytes,
        )
    }

    pub fn search(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<PhysicalSearchHit>, MaterializationError> {
        let limit = checked_limit(limit)?;
        checked_query_text(query)?;
        if query.trim().is_empty() {
            return Err(MaterializationError::InvalidQuery(
                "FTS query must be non-empty".into(),
            ));
        }
        let mut statement = self.connection.prepare(
            "SELECT entity_type, entity_id, page_id, text, bm25(search_fts)
             FROM search_fts WHERE search_fts MATCH ?1
             ORDER BY bm25(search_fts), entity_type, entity_id LIMIT ?2",
        )?;
        let rows = statement.query_map(params![query, limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, f64>(4)?,
            ))
        })?;
        let rows = rows.map(|row| {
            let (entity_type, entity_id, page_id, text, rank) = row?;
            let uuid = uuid::Uuid::parse_str(&entity_id)
                .map_err(|error| MaterializationError::Corrupt(error.to_string()))?
                .into_bytes();
            let entity = match entity_type.as_str() {
                "page" => PhysicalEntityId::Page(uuid),
                "block" => PhysicalEntityId::Block(uuid),
                _ => {
                    return Err(MaterializationError::Corrupt(format!(
                        "unknown FTS entity type {entity_type:?}"
                    )));
                }
            };
            Ok(PhysicalSearchHit {
                entity,
                page_id: uuid::Uuid::parse_str(&page_id)
                    .map_err(|error| MaterializationError::Corrupt(error.to_string()))?
                    .into_bytes(),
                text,
                rank,
            })
        });
        collect_read_rows(rows, search_hit_output_bytes)
    }
}

fn page_row_with_header_validation(
    row: &rusqlite::Row<'_>,
    validate_header: &mut impl FnMut(&str, i64) -> Result<(), MaterializationError>,
) -> rusqlite::Result<Result<PhysicalPageRow, MaterializationError>> {
    let page_id: Vec<u8> = row.get(0)?;
    let home_document_id: Vec<u8> = row.get(1)?;
    let path: String = row.get(4)?;
    let kind: i64 = row.get(5)?;
    if let Err(error) = validate_header(path.as_str(), kind) {
        return Ok(Err(error));
    }
    Ok(Ok(PhysicalPageRow {
        page_id: decode_id_sql(&page_id)?,
        home_document_id: decode_id_sql(&home_document_id)?,
        name: row.get(2)?,
        name_key: row.get(3)?,
        path,
        text_kind: kind,
        preamble: row.get(6)?,
        searchable_text: row.get(7)?,
    }))
}

fn navigation_page_row_with_header_validation(
    row: &rusqlite::Row<'_>,
    validate_header: &mut impl FnMut(&str, i64) -> Result<(), MaterializationError>,
) -> rusqlite::Result<Result<PhysicalNavigationPageRow, MaterializationError>> {
    // Preamble may legitimately be NULL. The keyed payload row may not be
    // missing: distinguish cache damage from an empty preamble without reading
    // the large search text solely to check presence.
    let _payload_id: Vec<u8> = row.get(6)?;
    let page_id: Vec<u8> = row.get(0)?;
    let path: String = row.get(3)?;
    let kind: i64 = row.get(4)?;
    if let Err(error) = validate_header(path.as_str(), kind) {
        return Ok(Err(error));
    }
    Ok(Ok(PhysicalNavigationPageRow {
        page_id: decode_id_sql(&page_id)?,
        name: row.get(1)?,
        name_key: row.get(2)?,
        path,
        text_kind: kind,
        preamble: row.get(5)?,
    }))
}

fn block_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<PhysicalBlockRow> {
    let block_id: Vec<u8> = row.get(0)?;
    let page_id: Vec<u8> = row.get(1)?;
    let home_document_id: Vec<u8> = row.get(2)?;
    let parent: Option<Vec<u8>> = row.get(3)?;
    let heading_level: Option<i64> = row.get(7)?;
    let logseq_uuid: Option<Vec<u8>> = row.get(9)?;
    let origin: Option<i64> = row.get(10)?;
    Ok(PhysicalBlockRow {
        block_id: decode_id_sql(&block_id)?,
        page_id: decode_id_sql(&page_id)?,
        home_document_id: decode_id_sql(&home_document_id)?,
        parent: parent.as_deref().map(decode_id_sql).transpose()?,
        order: row.get(4)?,
        content: row.get(5)?,
        searchable_text: row.get(6)?,
        heading_level: heading_level
            .map(|value| u8::try_from(value).map_err(sql_decode_error))
            .transpose()?,
        collapsed: row.get::<_, i64>(8)? != 0,
        logseq_uuid: logseq_uuid.as_deref().map(decode_id_sql).transpose()?,
        logseq_identity_origin: origin,
    })
}

fn task_candidate_block_row_with_header_validation(
    row: &rusqlite::Row<'_>,
    validate_header: &mut impl FnMut(&str, i64) -> Result<(), MaterializationError>,
) -> rusqlite::Result<Result<PhysicalTaskCandidateBlockRow, MaterializationError>> {
    let block_id: Vec<u8> = row.get(0)?;
    let page_id: Vec<u8> = row.get(1)?;
    let parent: Option<Vec<u8>> = row.get(2)?;
    let logseq_uuid: Option<Vec<u8>> = row.get(5)?;
    let page_path: String = row.get(7)?;
    let page_text_kind: i64 = row.get(8)?;
    if let Err(error) = validate_header(page_path.as_str(), page_text_kind) {
        return Ok(Err(error));
    }
    Ok(Ok(PhysicalTaskCandidateBlockRow {
        block_id: decode_id_sql(&block_id)?,
        page_id: decode_id_sql(&page_id)?,
        parent: parent.as_deref().map(decode_id_sql).transpose()?,
        order: row.get(3)?,
        content: row.get(4)?,
        logseq_uuid: logseq_uuid.as_deref().map(decode_id_sql).transpose()?,
        page_name: row.get(6)?,
        page_path,
        page_text_kind,
    }))
}

fn property_tuple(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<(i64, Vec<u8>, Vec<u8>, String, String)> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
    ))
}

fn property_rows(
    rows: rusqlite::MappedRows<
        '_,
        impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<(i64, Vec<u8>, Vec<u8>, String, String)>,
    >,
) -> Result<Vec<PhysicalPropertyRow>, MaterializationError> {
    let rows = rows.map(|row| {
        let (owner_type, owner_id, page_id, name, value) = row?;
        Ok(PhysicalPropertyRow {
            owner: decode_entity(owner_type, &owner_id)?,
            page_id: decode_id(&page_id)?,
            name,
            value,
        })
    });
    collect_read_rows(rows, property_row_output_bytes)
}

fn checked_limit(limit: usize) -> Result<i64, MaterializationError> {
    if limit == 0 || limit > MAX_MATERIALIZATION_QUERY_ROWS {
        return Err(MaterializationError::InvalidQuery(format!(
            "query limit {limit} is outside 1..={MAX_MATERIALIZATION_QUERY_ROWS}"
        )));
    }
    i64::try_from(limit)
        .map_err(|_| MaterializationError::InvalidQuery("query limit overflowed".into()))
}

fn decode_entity(entity_type: i64, bytes: &[u8]) -> Result<PhysicalEntityId, MaterializationError> {
    match entity_type {
        0 => Ok(PhysicalEntityId::Page(decode_id(bytes)?)),
        1 => Ok(PhysicalEntityId::Block(decode_id(bytes)?)),
        _ => Err(MaterializationError::Corrupt(format!(
            "unknown entity type {entity_type}"
        ))),
    }
}

fn decode_id_sql(bytes: &[u8]) -> rusqlite::Result<[u8; 16]> {
    bytes.try_into().map_err(sql_decode_error)
}

fn sql_decode_error(error: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(error))
}

fn decode_id(bytes: &[u8]) -> Result<[u8; 16], MaterializationError> {
    bytes
        .try_into()
        .map_err(|_| MaterializationError::Corrupt("invalid UUID length".into()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaterializationError {
    Sqlite(String),
    Schema(String),
    Corrupt(String),
    ResourceLimit {
        resource: &'static str,
        found: usize,
        maximum: usize,
    },
    InvalidInput(String),
    Incomplete(String),
    InvalidQuery(String),
}

impl fmt::Display for MaterializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(f, "SQLite materialization error: {error}"),
            Self::Schema(error) => write!(f, "materialization schema mismatch: {error}"),
            Self::Corrupt(error) => write!(f, "corrupt materialization: {error}"),
            Self::ResourceLimit {
                resource,
                found,
                maximum,
            } => write!(
                f,
                "materialization {resource} {found} exceeds limit {maximum}"
            ),
            Self::InvalidInput(error) => write!(f, "invalid materialization input: {error}"),
            Self::Incomplete(error) => write!(f, "incomplete materialization input: {error}"),
            Self::InvalidQuery(error) => write!(f, "invalid materialization query: {error}"),
        }
    }
}

impl std::error::Error for MaterializationError {}

impl From<rusqlite::Error> for MaterializationError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: u128) -> [u8; 16] {
        value.to_be_bytes()
    }

    #[test]
    fn query_preorder_rejects_incomplete_or_cyclic_trees() {
        assert!(query_block_preorder([(id(1), Some(id(2)), "a")]).is_err());
        assert!(
            query_block_preorder([(id(1), Some(id(2)), "a"), (id(2), Some(id(1)), "b")]).is_err()
        );
        assert!(query_block_preorder([(id(1), None, "a"), (id(1), None, "b")]).is_err());
        assert_eq!(
            query_block_preorder([(id(2), None, "z"), (id(1), None, "z")]).unwrap(),
            [(1, 1), (0, 1)]
        );
        assert_eq!(
            query_result_estimated_bytes("", "é", ["tag"], [("key", "value")]),
            36 + 2 + 3 + 3 + 5 + 128
        );
    }
}
