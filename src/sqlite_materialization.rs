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
pub enum PhysicalEntityCoordinate {
    Page(i64),
    Block(i64),
}

impl PhysicalEntityCoordinate {
    const fn sql_parts(self) -> (i64, i64) {
        match self {
            Self::Page(id) => (0, id),
            Self::Block(id) => (1, id),
        }
    }
}

/// Public identity at the storage boundary. Page identity is its relative
/// path; block identity is the live result ID supplied by the application.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PhysicalEntityId {
    Page(String),
    Block(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalProperty {
    pub name: String,
    pub normalized_name: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalName {
    pub raw: String,
    pub key: String,
}

/// One atom of one property element (SPEC §3.3), already flattened and
/// renumbered by the single tine-core producer. The physical layer stores what
/// it is handed; it never atomizes.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPropertyAtom {
    pub name: String,
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
    blocks: impl IntoIterator<Item = (&'a str, Option<&'a str>, &'a str)>,
) -> Result<Vec<(usize, usize)>, MaterializationError> {
    let blocks = blocks.into_iter().collect::<Vec<_>>();
    let ids = blocks.iter().map(|row| row.0).collect::<BTreeSet<_>>();
    if ids.len() != blocks.len() {
        return Err(MaterializationError::InvalidInput(
            "duplicate query block identity".into(),
        ));
    }
    let mut children = BTreeMap::<Option<&str>, Vec<usize>>::new();
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
    /// Public query identity assigned by the application's current session.
    pub result_id: String,
    /// Own normalized reference set, before ancestor/page closure expansion.
    ///
    /// `raw` is retained for the synthetic-membership case, but membership is
    /// keyed by `key`: normalize-equivalent spellings are one parser-owned set
    /// member. Raw reference occurrences remain separate posting rows.
    pub own_refs: Vec<PhysicalName>,
    pub parent: Option<String>,
    pub order: String,
    pub content: String,
    /// Application-owned folded visible tokens. Storage feeds this ephemeral
    /// value to the contentless search index and never persists it as text.
    pub search_tokens: String,
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
    pub path_refs: Vec<PhysicalName>,
    pub property_atoms: Vec<PhysicalPropertyAtom>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPage {
    /// Direct's current session inventory position. This is rebuildable
    /// metadata, never identity authority.
    pub position: Option<u64>,
    pub name: String,
    pub name_key: String,
    pub path: String,
    pub text_kind: i64,
    /// `yyyymmdd` when this page is a journal whose name parses under the
    /// graph's journal formats, else `None` (SPEC §3.2, §5.8).
    pub journal_day: Option<i64>,
    pub preamble: Option<String>,
    /// Application-owned folded visible tokens. Storage feeds this ephemeral
    /// value to the contentless search index and never persists it as text.
    pub search_tokens: String,
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
    },
    ExternalUuid {
        raw_claim: [u8; 16],
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalReferencePosting {
    pub source_page_path: String,
    pub source_entity: PhysicalEntityId,
    pub source_locator: Vec<u8>,
    pub ordinal: u32,
    pub kind: i64,
    pub target: PhysicalReferenceTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalAliasDeclaration {
    pub source_page_path: String,
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
    pub deletions: Vec<String>,
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
}

pub const NAMES_DDL: &str = "CREATE TABLE names (
    name_id INTEGER PRIMARY KEY,
    key TEXT NOT NULL CHECK (length(CAST(key AS BLOB)) BETWEEN 1 AND 4194304),
    raw TEXT NOT NULL CHECK (length(CAST(raw AS BLOB)) BETWEEN 1 AND 4194304),
    UNIQUE (key, raw)
) STRICT";
pub const REFERENCE_POSTINGS_DDL: &str = "CREATE TABLE reference_postings (
    posting_id INTEGER PRIMARY KEY,
    source_page_id INTEGER NOT NULL REFERENCES pages(page_id) ON DELETE CASCADE,
    source_entity_type INTEGER NOT NULL CHECK (source_entity_type IN (0, 1)),
    source_entity_id INTEGER NOT NULL,
    source_locator BLOB CHECK (
        source_locator IS NULL OR length(source_locator) BETWEEN 1 AND 4194304
    ),
    ordinal INTEGER CHECK (ordinal IS NULL OR ordinal >= 0),
    reference_kind INTEGER NOT NULL CHECK (reference_kind BETWEEN 0 AND 8),
    target_type INTEGER NOT NULL CHECK (target_type IN (0, 1)),
    target_name_id INTEGER REFERENCES names(name_id),
    raw_uuid_claim BLOB CHECK (
        raw_uuid_claim IS NULL OR length(raw_uuid_claim) = 16
    ),
    own INTEGER NOT NULL CHECK (own IN (0, 1)),
    CHECK (
        (reference_kind BETWEEN 0 AND 5 AND target_type = 0)
        OR
        (reference_kind IN (6, 7) AND target_type = 1)
        OR
        (reference_kind = 8 AND target_type = 0)
    ),
    CHECK (
        (target_type = 0 AND target_name_id IS NOT NULL
         AND raw_uuid_claim IS NULL)
        OR
        (target_type = 1 AND target_name_id IS NULL
         AND raw_uuid_claim IS NOT NULL)
    ),
    CHECK (
        (reference_kind = 8 AND source_entity_type = 1 AND source_locator IS NULL
         AND ordinal IS NULL AND own = 1)
        OR
        (reference_kind < 8 AND source_locator IS NOT NULL AND ordinal IS NOT NULL)
    )
) STRICT";
pub const REFERENCE_ALIAS_DECLARATIONS_DDL: &str = "CREATE TABLE reference_alias_declarations (
    source_page_id INTEGER NOT NULL REFERENCES pages(page_id) ON DELETE CASCADE,
    source_entity_type INTEGER NOT NULL CHECK (source_entity_type IN (0, 1)),
    source_entity_id INTEGER NOT NULL,
    source_locator BLOB NOT NULL CHECK (length(source_locator) BETWEEN 1 AND 4194304),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    alias_name_id INTEGER NOT NULL REFERENCES names(name_id),
    PRIMARY KEY (
        source_page_id, source_entity_type, source_entity_id, source_locator, ordinal
    )
) WITHOUT ROWID, STRICT";
pub const PAGES_DDL: &str = "CREATE TABLE pages (
    page_id INTEGER PRIMARY KEY,
    name_id INTEGER NOT NULL REFERENCES names(name_id),
    path TEXT NOT NULL UNIQUE CHECK (length(CAST(path AS BLOB)) BETWEEN 1 AND 4194304),
    text_kind INTEGER NOT NULL CHECK (text_kind IN (0, 1)),
    journal_day INTEGER,
    position INTEGER UNIQUE CHECK (position IS NULL OR position >= 0),
    estimated_bytes INTEGER NOT NULL CHECK (estimated_bytes >= 0),
    property_count INTEGER NOT NULL CHECK (property_count >= 0)
) STRICT";
const PAGE_TEXT_DDL: &str = "CREATE TABLE page_text (
    page_id INTEGER PRIMARY KEY
        REFERENCES pages(page_id) ON DELETE CASCADE,
    preamble TEXT CHECK (preamble IS NULL OR length(CAST(preamble AS BLOB)) <= 16777216)
) STRICT";
pub const BLOCKS_DDL: &str = "CREATE TABLE blocks (
    block_id INTEGER PRIMARY KEY,
    page_id INTEGER NOT NULL
        REFERENCES pages(page_id) ON DELETE CASCADE,
    result_id TEXT NOT NULL UNIQUE CHECK (length(CAST(result_id AS BLOB)) > 0),
    parent_block_id INTEGER REFERENCES blocks(block_id),
    order_key TEXT NOT NULL CHECK (length(CAST(order_key AS BLOB)) BETWEEN 1 AND 4194304),
    heading_level INTEGER CHECK (
        heading_level IS NULL OR heading_level BETWEEN 1 AND 6
    ),
    collapsed INTEGER NOT NULL CHECK (collapsed IN (0, 1)),
    logseq_uuid BLOB CHECK (logseq_uuid IS NULL OR length(logseq_uuid) = 16),
    logseq_identity_origin INTEGER CHECK (
        logseq_identity_origin IS NULL
        OR logseq_identity_origin BETWEEN 0 AND 4
    ),
    preorder INTEGER NOT NULL CHECK (preorder >= 0),
    estimated_bytes INTEGER NOT NULL CHECK (estimated_bytes >= 0),
    tag_count INTEGER NOT NULL CHECK (tag_count >= 0),
    property_count INTEGER NOT NULL CHECK (property_count >= 0),
    UNIQUE (page_id, preorder),
    CHECK (
        (logseq_uuid IS NULL AND logseq_identity_origin IS NULL)
        OR (logseq_uuid IS NOT NULL AND logseq_identity_origin IS NOT NULL)
    )
) STRICT";
const BLOCK_TEXT_DDL: &str = "CREATE TABLE block_text (
    block_id INTEGER PRIMARY KEY
        REFERENCES blocks(block_id) ON DELETE CASCADE,
    content TEXT NOT NULL CHECK (length(CAST(content AS BLOB)) <= 4194304)
) STRICT";
pub const PROPERTIES_DDL: &str = "CREATE TABLE properties (
    owner_type INTEGER NOT NULL CHECK (owner_type IN (0, 1)),
    owner_id INTEGER NOT NULL,
    page_id INTEGER NOT NULL REFERENCES pages(page_id) ON DELETE CASCADE,
    name_id INTEGER NOT NULL REFERENCES names(name_id),
    value TEXT NOT NULL CHECK (length(CAST(value AS BLOB)) <= 4194304),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (owner_type, owner_id, name_id, ordinal)
) WITHOUT ROWID, STRICT";
pub const TAGS_DDL: &str = "CREATE TABLE tags (
    owner_type INTEGER NOT NULL CHECK (owner_type IN (0, 1)),
    owner_id INTEGER NOT NULL,
    page_id INTEGER NOT NULL REFERENCES pages(page_id) ON DELETE CASCADE,
    name_id INTEGER NOT NULL REFERENCES names(name_id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (owner_type, owner_id, ordinal)
) WITHOUT ROWID, STRICT";
pub const TASKS_DDL: &str = "CREATE TABLE tasks (
    block_id INTEGER PRIMARY KEY REFERENCES blocks(block_id) ON DELETE CASCADE,
    page_id INTEGER NOT NULL REFERENCES pages(page_id) ON DELETE CASCADE,
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
    block_id INTEGER PRIMARY KEY REFERENCES blocks(block_id) ON DELETE CASCADE,
    page_id INTEGER NOT NULL REFERENCES pages(page_id) ON DELETE CASCADE,
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
    block_id INTEGER NOT NULL REFERENCES blocks(block_id) ON DELETE CASCADE,
    page_id INTEGER NOT NULL REFERENCES pages(page_id) ON DELETE CASCADE,
    name_id INTEGER NOT NULL REFERENCES names(name_id),
    PRIMARY KEY (block_id, name_id)
) WITHOUT ROWID, STRICT";
/// The atoms of every property element (SPEC §3.3, §5.8). `properties` keeps
/// the unsplit source value for presence, autocomplete and display; this table
/// carries the flattened, renumbered atom list a value comparison searches.
const QUERY_PROJECTION_STATE_DDL: &str = "CREATE TABLE query_projection_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    next_entity_id INTEGER NOT NULL CHECK (next_entity_id > 0)
) STRICT";
pub const PROPERTY_ATOMS_DDL: &str = "CREATE TABLE property_atoms (
    owner_type INTEGER NOT NULL CHECK (owner_type IN (0, 1)),
    owner_id INTEGER NOT NULL,
    page_id INTEGER NOT NULL REFERENCES pages(page_id) ON DELETE CASCADE,
    name_id INTEGER NOT NULL REFERENCES names(name_id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    atom TEXT NOT NULL CHECK (length(CAST(atom AS BLOB)) <= 4194304),
    atom_key TEXT NOT NULL CHECK (length(CAST(atom_key AS BLOB)) <= 4194304),
    origin INTEGER NOT NULL CHECK (origin IN (0, 1)),
    atom_num REAL,
    atom_day INTEGER,
    PRIMARY KEY (owner_type, owner_id, name_id, ordinal)
) WITHOUT ROWID, STRICT";
pub const SEARCH_FTS_DDL: &str = "CREATE VIRTUAL TABLE search_fts USING fts5(
    normalized_text,
    content = '',
    contentless_delete = 1,
    detail = none,
    tokenize = 'trigram case_sensitive 1 remove_diacritics 0'
)";

pub const NAMES_RAW_KEY_INDEX_DDL: &str =
    "CREATE INDEX names_raw_key_idx ON names(raw, key, name_id)";
pub const PAGES_NAME_INDEX_DDL: &str = "CREATE INDEX pages_name_idx ON pages(name_id, page_id)";
pub const PAGES_JOURNAL_DAY_INDEX_DDL: &str =
    "CREATE INDEX pages_journal_day_idx ON pages(journal_day, page_id)";
pub const PAGES_PATH_INDEX_DDL: &str = "CREATE INDEX pages_path_idx ON pages(path, page_id)";
pub const BLOCKS_PAGE_ORDER_INDEX_DDL: &str =
    "CREATE INDEX blocks_page_order_idx ON blocks(page_id, order_key, block_id)";
pub const BLOCKS_PARENT_PAGE_INDEX_DDL: &str = "CREATE INDEX blocks_parent_page_idx
    ON blocks(parent_block_id, page_id, block_id) WHERE parent_block_id IS NOT NULL";
pub const BLOCKS_LOGSEQ_UUID_INDEX_DDL: &str = "CREATE INDEX blocks_logseq_uuid_idx
    ON blocks(logseq_uuid, block_id) WHERE logseq_uuid IS NOT NULL";
pub const REFERENCE_POSTINGS_SOURCE_INDEX_DDL: &str = "CREATE INDEX reference_postings_source_idx
    ON reference_postings(source_page_id, source_entity_type, source_entity_id, reference_kind, ordinal)";
pub const REFERENCE_POSTINGS_TARGET_NAME_INDEX_DDL: &str =
    "CREATE INDEX reference_postings_target_name_idx
    ON reference_postings(target_name_id, source_page_id, source_entity_type, source_entity_id, reference_kind, ordinal)
    WHERE target_type = 0";
pub const REFERENCE_POSTINGS_OCCURRENCE_INDEX_DDL: &str =
    "CREATE UNIQUE INDEX reference_postings_occurrence_idx
    ON reference_postings(source_page_id, source_entity_type, source_entity_id, source_locator, ordinal)
    WHERE reference_kind < 8";
pub const REFERENCE_POSTINGS_OWN_INDEX_DDL: &str = "CREATE UNIQUE INDEX reference_postings_own_idx
    ON reference_postings(source_entity_id, target_name_id)
    WHERE source_entity_type = 1 AND target_type = 0 AND own = 1";
pub const REFERENCE_POSTINGS_RAW_UUID_INDEX_DDL: &str = "CREATE INDEX reference_postings_raw_uuid_idx
    ON reference_postings(raw_uuid_claim, source_page_id, source_entity_type, source_entity_id, ordinal)
    WHERE target_type = 1";
/// Covers the whole navigation name read: the key is exactly the statement's
/// `DISTINCT`/`ORDER BY` tuple, and `reference_kind` rides along so the
/// `<= 4` filter is answered from the index too. Nothing in
/// `NAVIGATION_REFERENCE_NAMES_*_SQL` needs the table.
///
/// The dictionary supplies spelling/key pairs once; this postings index keeps
/// the navigation read bounded to referenced name IDs without copying either
/// string into every occurrence row.
pub const REFERENCE_POSTINGS_NAVIGATION_NAMES_INDEX_DDL: &str =
    "CREATE INDEX reference_postings_navigation_names_idx
    ON reference_postings(target_name_id, reference_kind)
    WHERE target_type = 0";
pub const REFERENCE_ALIAS_DECLARATIONS_SOURCE_INDEX_DDL: &str =
    "CREATE INDEX reference_alias_declarations_source_idx
    ON reference_alias_declarations(source_page_id, alias_name_id, source_entity_type, source_entity_id, ordinal)";
pub const REFERENCE_ALIAS_DECLARATIONS_NAME_INDEX_DDL: &str =
    "CREATE INDEX reference_alias_declarations_name_idx
    ON reference_alias_declarations(alias_name_id, source_page_id, source_entity_type, source_entity_id)";
pub const PROPERTIES_LOOKUP_INDEX_DDL: &str = "CREATE INDEX properties_lookup_idx
    ON properties(name_id, value, page_id, owner_type, owner_id)";
pub const PROPERTIES_PAGE_INDEX_DDL: &str = "CREATE INDEX properties_page_idx
    ON properties(page_id, owner_type, owner_id, name_id, ordinal)";
pub const TAGS_LOOKUP_INDEX_DDL: &str =
    "CREATE INDEX tags_lookup_idx ON tags(name_id, page_id, owner_type, owner_id)";
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
    ON block_path_refs(name_id, page_id, block_id)";
pub const BLOCK_PATH_REFS_PAGE_INDEX_DDL: &str = "CREATE INDEX block_path_refs_page_idx
    ON block_path_refs(page_id, block_id, name_id)";
pub const PROPERTY_ATOMS_KEY_INDEX_DDL: &str = "CREATE INDEX property_atoms_key_idx
    ON property_atoms(name_id, atom_key, page_id, owner_type, owner_id)";
pub const PROPERTY_ATOMS_NUM_INDEX_DDL: &str = "CREATE INDEX property_atoms_num_idx
    ON property_atoms(name_id, atom_num, page_id, owner_type, owner_id)";
pub const PROPERTY_ATOMS_DAY_INDEX_DDL: &str = "CREATE INDEX property_atoms_day_idx
    ON property_atoms(name_id, atom_day, page_id, owner_type, owner_id)";
pub const PROPERTY_ATOMS_PAGE_INDEX_DDL: &str = "CREATE INDEX property_atoms_page_idx
    ON property_atoms(page_id, owner_type, owner_id, name_id, ordinal)";

// A terminal bootstrap candidate is a brand-new, unpublished database. Its
// ordinary secondary indexes can be built once after the complete row set is
// present instead of being maintained for every inserted row. The primary-key
// indexes and contentless FTS virtual table remain live throughout construction.
// This list must reproduce the exact normal schema before the transaction can
// commit.
const TERMINAL_DEFERRED_INDEXES: [(&str, &str); 33] = [
    ("names_raw_key_idx", NAMES_RAW_KEY_INDEX_DDL),
    ("pages_name_idx", PAGES_NAME_INDEX_DDL),
    ("pages_journal_day_idx", PAGES_JOURNAL_DAY_INDEX_DDL),
    ("pages_path_idx", PAGES_PATH_INDEX_DDL),
    ("blocks_page_order_idx", BLOCKS_PAGE_ORDER_INDEX_DDL),
    ("blocks_parent_page_idx", BLOCKS_PARENT_PAGE_INDEX_DDL),
    ("blocks_logseq_uuid_idx", BLOCKS_LOGSEQ_UUID_INDEX_DDL),
    (
        "reference_postings_source_idx",
        REFERENCE_POSTINGS_SOURCE_INDEX_DDL,
    ),
    (
        "reference_postings_target_name_idx",
        REFERENCE_POSTINGS_TARGET_NAME_INDEX_DDL,
    ),
    (
        "reference_postings_occurrence_idx",
        REFERENCE_POSTINGS_OCCURRENCE_INDEX_DDL,
    ),
    (
        "reference_postings_own_idx",
        REFERENCE_POSTINGS_OWN_INDEX_DDL,
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
    (
        "reference_alias_declarations_name_idx",
        REFERENCE_ALIAS_DECLARATIONS_NAME_INDEX_DDL,
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

const MATERIALIZATION_TABLE_COLUMNS: [(&str, &[&str]); 14] = [
    ("names", &["name_id", "key", "raw"]),
    (
        "reference_postings",
        &[
            "posting_id",
            "source_page_id",
            "source_entity_type",
            "source_entity_id",
            "source_locator",
            "ordinal",
            "reference_kind",
            "target_type",
            "target_name_id",
            "raw_uuid_claim",
            "own",
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
            "alias_name_id",
        ],
    ),
    (
        "pages",
        &[
            "page_id",
            "name_id",
            "path",
            "text_kind",
            "journal_day",
            "position",
            "estimated_bytes",
            "property_count",
        ],
    ),
    ("page_text", &["page_id", "preamble"]),
    ("block_text", &["block_id", "content"]),
    (
        "blocks",
        &[
            "block_id",
            "page_id",
            "result_id",
            "parent_block_id",
            "order_key",
            "heading_level",
            "collapsed",
            "logseq_uuid",
            "logseq_identity_origin",
            "preorder",
            "estimated_bytes",
            "tag_count",
            "property_count",
        ],
    ),
    (
        "properties",
        &[
            "owner_type",
            "owner_id",
            "page_id",
            "name_id",
            "value",
            "ordinal",
        ],
    ),
    (
        "tags",
        &["owner_type", "owner_id", "page_id", "name_id", "ordinal"],
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
    ("block_path_refs", &["block_id", "page_id", "name_id"]),
    (
        "query_projection_state",
        &["singleton", "revision", "next_entity_id"],
    ),
    (
        "property_atoms",
        &[
            "owner_type",
            "owner_id",
            "page_id",
            "name_id",
            "ordinal",
            "atom",
            "atom_key",
            "origin",
            "atom_num",
            "atom_day",
        ],
    ),
];

const MATERIALIZATION_SCHEMA_OBJECTS: [(&str, &str, &str); 47] = [
    ("table", "names", NAMES_DDL),
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
    (
        "table",
        "query_projection_state",
        QUERY_PROJECTION_STATE_DDL,
    ),
    ("table", "property_atoms", PROPERTY_ATOMS_DDL),
    ("table", "search_fts", SEARCH_FTS_DDL),
    ("index", "names_raw_key_idx", NAMES_RAW_KEY_INDEX_DDL),
    ("index", "pages_name_idx", PAGES_NAME_INDEX_DDL),
    (
        "index",
        "pages_journal_day_idx",
        PAGES_JOURNAL_DAY_INDEX_DDL,
    ),
    ("index", "pages_path_idx", PAGES_PATH_INDEX_DDL),
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
        "reference_postings_source_idx",
        REFERENCE_POSTINGS_SOURCE_INDEX_DDL,
    ),
    (
        "index",
        "reference_postings_target_name_idx",
        REFERENCE_POSTINGS_TARGET_NAME_INDEX_DDL,
    ),
    (
        "index",
        "reference_postings_occurrence_idx",
        REFERENCE_POSTINGS_OCCURRENCE_INDEX_DDL,
    ),
    (
        "index",
        "reference_postings_own_idx",
        REFERENCE_POSTINGS_OWN_INDEX_DDL,
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
        "reference_alias_declarations_name_idx",
        REFERENCE_ALIAS_DECLARATIONS_NAME_INDEX_DDL,
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
    initialize_graph_projection_schema_without_secondary_indexes(connection)?;
    create_deferred_indexes(connection)?;
    Ok(())
}

/// Initialize the tables, primary-key indexes, and contentless FTS table used
/// by a brand-new unpublished build. Ordinary secondary indexes are omitted so
/// the fresh-build transaction can create them once after all streamed rows.
pub(crate) fn initialize_graph_projection_schema_without_secondary_indexes(
    connection: &Connection,
) -> Result<(), MaterializationError> {
    connection.execute_batch(&format!(
        "{NAMES_DDL};
         {PAGES_DDL};
         {BLOCKS_DDL};
         {REFERENCE_POSTINGS_DDL};
         {REFERENCE_ALIAS_DECLARATIONS_DDL};
         {PAGE_TEXT_DDL};
         {BLOCK_TEXT_DDL};
         {PROPERTIES_DDL};
         {TAGS_DDL};
         {TASKS_DDL};
         {BLOCK_PLANNING_DDL};
         {BLOCK_PATH_REFS_DDL};
         {QUERY_PROJECTION_STATE_DDL};
         {PROPERTY_ATOMS_DDL};
         {SEARCH_FTS_DDL};"
    ))?;
    connection.execute("INSERT INTO query_projection_state VALUES (1, 0, 1)", [])?;
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
    "names",
    "pages",
    "blocks",
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
) -> Result<i64, MaterializationError> {
    let source_page_id = page_coordinate(transaction, &posting.source_page_path)?;
    let (source_entity_type, source_entity_id) =
        entity_coordinate(transaction, &posting.source_entity)?;
    validate_entity_page(
        transaction,
        source_page_id,
        source_entity_type,
        source_entity_id,
    )?;
    insert_reference_posting_at(
        transaction,
        posting,
        source_page_id,
        source_entity_type,
        source_entity_id,
    )
}

/// A fresh append allocated every coordinate its postings can name, so it
/// resolves and checks ownership from those maps instead of three lookups
/// per posting. The refusals are the ones `insert_reference_posting` makes.
fn insert_fresh_reference_posting(
    transaction: &Connection,
    posting: &PhysicalReferencePosting,
    page_ids: &BTreeMap<String, i64>,
    block_ids: &BTreeMap<String, i64>,
    block_pages: &BTreeMap<&str, &str>,
) -> Result<i64, MaterializationError> {
    let source_path = posting.source_page_path.as_str();
    let source_page_id = *page_ids.get(source_path).ok_or_else(|| {
        MaterializationError::InvalidInput(format!("unknown page path {source_path:?}"))
    })?;
    let (source_entity_type, source_entity_id, owner_path) = match &posting.source_entity {
        PhysicalEntityId::Page(path) => {
            let page_id = *page_ids.get(path).ok_or_else(|| {
                MaterializationError::InvalidInput(format!("unknown page path {path:?}"))
            })?;
            (0, page_id, path.as_str())
        }
        PhysicalEntityId::Block(result_id) => {
            let unknown = || {
                MaterializationError::InvalidInput(format!("unknown block result ID {result_id:?}"))
            };
            let block_id = *block_ids.get(result_id).ok_or_else(unknown)?;
            let owner = *block_pages.get(result_id.as_str()).ok_or_else(unknown)?;
            (1, block_id, owner)
        }
    };
    if owner_path != source_path {
        return Err(MaterializationError::InvalidInput(
            "reference source entity does not belong to its source page".into(),
        ));
    }
    insert_reference_posting_at(
        transaction,
        posting,
        source_page_id,
        source_entity_type,
        source_entity_id,
    )
}

fn insert_reference_posting_at(
    transaction: &Connection,
    posting: &PhysicalReferencePosting,
    source_page_id: i64,
    source_entity_type: i64,
    source_entity_id: i64,
) -> Result<i64, MaterializationError> {
    let locator = &posting.source_locator;
    let (target_type, target_name_id, raw_uuid_claim) = match &posting.target {
        PhysicalReferenceTarget::PageName {
            raw_name,
            normalized_name,
        } => (
            0_i64,
            Some(intern_name(transaction, normalized_name, raw_name)?),
            None,
        ),
        PhysicalReferenceTarget::ExternalUuid { raw_claim } => {
            (1_i64, None, Some(raw_claim.to_vec()))
        }
    };
    execute_cached(
        transaction,
        "INSERT INTO reference_postings (
             source_page_id, source_entity_type, source_entity_id, source_locator,
             ordinal, reference_kind, target_type, target_name_id,
             raw_uuid_claim, own
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0)",
        params![
            source_page_id,
            source_entity_type,
            source_entity_id,
            locator,
            i64::from(posting.ordinal),
            posting.kind,
            target_type,
            target_name_id,
            raw_uuid_claim,
        ],
    )?;
    Ok(transaction.last_insert_rowid())
}

fn insert_alias_declaration(
    transaction: &Connection,
    alias: &PhysicalAliasDeclaration,
) -> Result<(), MaterializationError> {
    let source_page_id = page_coordinate(transaction, &alias.source_page_path)?;
    let (source_entity_type, source_entity_id) =
        entity_coordinate(transaction, &alias.source_entity)?;
    validate_entity_page(
        transaction,
        source_page_id,
        source_entity_type,
        source_entity_id,
    )?;
    let alias_name_id = intern_name(transaction, &alias.normalized_alias, &alias.raw_alias)?;
    let locator = &alias.source_locator;
    execute_cached(
        transaction,
        "INSERT INTO reference_alias_declarations (
             source_page_id, source_entity_type, source_entity_id, source_locator,
             ordinal, alias_name_id
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            source_page_id,
            source_entity_type,
            source_entity_id,
            locator,
            i64::from(alias.ordinal),
            alias_name_id,
        ],
    )?;
    Ok(())
}

fn intern_name(connection: &Connection, key: &str, raw: &str) -> Result<i64, MaterializationError> {
    if key.is_empty() || raw.is_empty() {
        return Err(MaterializationError::InvalidInput(
            "name key and spelling must be non-empty".into(),
        ));
    }
    // Nearly every call names an existing row (a build interns each tag,
    // property, and path reference once per use), so look it up before
    // attempting the insert.
    let select_existing = || {
        query_row_cached(
            connection,
            "SELECT name_id FROM names WHERE key = ?1 AND raw = ?2",
            params![key, raw],
            |row| row.get(0),
        )
        .optional()
    };
    if let Some(name_id) = select_existing()? {
        return Ok(name_id);
    }
    if execute_cached(
        connection,
        "INSERT INTO names (key, raw) VALUES (?1, ?2) ON CONFLICT(key, raw) DO NOTHING",
        params![key, raw],
    )? == 1
    {
        return Ok(connection.last_insert_rowid());
    }
    select_existing()?
        .ok_or_else(|| MaterializationError::Corrupt("interned name vanished after insert".into()))
}

fn page_coordinate(connection: &Connection, path: &str) -> Result<i64, MaterializationError> {
    query_row_cached(
        connection,
        "SELECT page_id FROM pages WHERE path = ?1",
        params![path],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| MaterializationError::InvalidInput(format!("unknown page path {path:?}")))
}

fn block_coordinate(connection: &Connection, result_id: &str) -> Result<i64, MaterializationError> {
    query_row_cached(
        connection,
        "SELECT block_id FROM blocks WHERE result_id = ?1",
        params![result_id],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| {
        MaterializationError::InvalidInput(format!("unknown block result ID {result_id:?}"))
    })
}

fn entity_coordinate(
    connection: &Connection,
    entity: &PhysicalEntityId,
) -> Result<(i64, i64), MaterializationError> {
    match entity {
        PhysicalEntityId::Page(path) => Ok((0, page_coordinate(connection, path)?)),
        PhysicalEntityId::Block(result_id) => Ok((1, block_coordinate(connection, result_id)?)),
    }
}

fn validate_entity_page(
    connection: &Connection,
    source_page_id: i64,
    entity_type: i64,
    entity_id: i64,
) -> Result<(), MaterializationError> {
    let owner_page_id = match entity_type {
        0 => entity_id,
        1 => query_row_cached(
            connection,
            "SELECT page_id FROM blocks WHERE block_id = ?1",
            params![entity_id],
            |row| row.get(0),
        )?,
        _ => {
            return Err(MaterializationError::InvalidInput(
                "unknown source entity type".into(),
            ))
        }
    };
    if owner_page_id != source_page_id {
        return Err(MaterializationError::InvalidInput(
            "reference source entity does not belong to its source page".into(),
        ));
    }
    Ok(())
}

pub(crate) fn query_projection_revision(
    connection: &Connection,
) -> Result<u64, MaterializationError> {
    let revision: Option<i64> = query_row_cached(
        connection,
        "SELECT revision FROM query_projection_state WHERE singleton = 1",
        &[],
        |row| row.get(0),
    )
    .optional()?;
    revision
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| {
            MaterializationError::Corrupt("query projection revision is missing or invalid".into())
        })
}

pub(crate) fn advance_query_projection_revision(
    connection: &Connection,
) -> Result<(), MaterializationError> {
    let changed = execute_cached(
        connection,
        "UPDATE query_projection_state SET revision = revision + 1
         WHERE singleton = 1 AND revision < 9223372036854775807",
        &[],
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
    change: &PhysicalGraphProjectionChange,
    aliases: &[PhysicalAliasDeclaration],
    deferred_indexes: bool,
    fts_instrumentation: Option<&mut FtsChangeInstrumentation>,
) -> Result<ApplyChangeInstrumentation, MaterializationError> {
    validate_change_ownership(change, aliases)?;
    let replacement_paths = change
        .replacements
        .iter()
        .map(|page| page.path.as_str())
        .collect::<BTreeSet<_>>();
    if replacement_paths.len() != change.replacements.len() {
        return Err(MaterializationError::InvalidInput(
            "replacement pages contain a duplicate path".into(),
        ));
    }
    let mut page_ids = BTreeMap::new();
    let mut retained_positions = BTreeMap::new();
    for path in replacement_paths
        .iter()
        .copied()
        .chain(change.deletions.iter().map(String::as_str))
    {
        if let Some((id, position)) = query_row_cached(
            transaction,
            "SELECT page_id, position FROM pages WHERE path = ?1",
            params![path],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
        )
        .optional()?
        {
            page_ids.insert(path.to_owned(), id);
            if replacement_paths.contains(path) {
                retained_positions.insert(path.to_owned(), position);
            }
        }
    }
    let new_page_count = change
        .replacements
        .iter()
        .filter(|page| !page_ids.contains_key(&page.path))
        .count();
    let block_count = change
        .replacements
        .iter()
        .map(|page| page.blocks.len())
        .sum::<usize>();
    let allocated =
        allocate_entity_coordinates(transaction, new_page_count.saturating_add(block_count))?;
    let mut next = allocated.into_iter();
    for page in &change.replacements {
        if !page_ids.contains_key(&page.path) {
            page_ids.insert(
                page.path.clone(),
                next.next().expect("allocated page coordinate"),
            );
        }
    }
    let mut block_ids = BTreeMap::new();
    for page in &change.replacements {
        for block in &page.blocks {
            if block.result_id.is_empty()
                || block_ids
                    .insert(
                        block.result_id.clone(),
                        next.next().expect("allocated block coordinate"),
                    )
                    .is_some()
            {
                return Err(MaterializationError::InvalidInput(
                    "replacement blocks contain an empty or duplicate result ID".into(),
                ));
            }
        }
    }
    let affected_pages = page_ids.values().copied().collect::<BTreeSet<_>>();
    let affected_name_ids = affected_name_ids(transaction, &affected_pages)?;
    let mut instrumentation = ApplyChangeInstrumentation::default();
    for path in &change.deletions {
        let cleanup = match page_ids.get(path) {
            Some(page_id) => delete_page(transaction, *page_id)?,
            None => PageCleanupInstrumentation::default(),
        };
        instrumentation.cleanup_page_attempts += 1;
        instrumentation.cleanup_existing_pages += cleanup.existing_pages;
        instrumentation.cleanup_owned_rows += cleanup.owned_rows;
        instrumentation.cleanup_fts_rowids += cleanup.fts_rowids;
    }
    for page in &change.replacements {
        let cleanup = delete_page(transaction, page_ids[&page.path])?;
        instrumentation.cleanup_page_attempts += 1;
        instrumentation.cleanup_existing_pages += cleanup.existing_pages;
        instrumentation.cleanup_owned_rows += cleanup.owned_rows;
        instrumentation.cleanup_fts_rowids += cleanup.fts_rowids;
    }
    for page in &change.replacements {
        insert_page(
            transaction,
            page,
            page_ids[&page.path],
            retained_positions.get(&page.path).copied().flatten(),
            &block_ids,
        )?;
    }
    insert_replacement_fts_rows(
        transaction,
        &change.replacements,
        &page_ids,
        &block_ids,
        fts_instrumentation,
    )?;
    let mut deferred_own_memberships =
        deferred_indexes.then(|| DeferredOwnReferenceMemberships::new(&change.replacements));
    for posting in &change.reference_postings {
        let posting_id = insert_reference_posting(transaction, posting)?;
        if let Some(memberships) = &mut deferred_own_memberships {
            memberships.capture_first_occurrence(posting, posting_id);
        }
    }
    for alias in aliases {
        insert_alias_declaration(transaction, alias)?;
    }
    if let Some(memberships) = deferred_own_memberships {
        insert_deferred_own_reference_memberships(
            transaction,
            &change.replacements,
            &page_ids,
            &block_ids,
            &memberships,
        )?;
    } else {
        insert_own_reference_memberships(transaction, &change.replacements)?;
    }
    reclaim_affected_names(transaction, &affected_name_ids)?;
    Ok(instrumentation)
}

/// Append one bounded chunk to a brand-new unpublished projection whose
/// ordinary secondary indexes have not been created yet.
///
/// This deliberately shares every row-level insertion helper with ordinary
/// apply, but omits replacement cleanup and name reclamation: every page and
/// block must be new for the lifetime of the build transaction. The schema's
/// unique constraints are the final guard, while the explicit checks make a
/// repeated chunk a typed input error rather than an accidental replacement.
pub(crate) fn append_fresh_graph_projection_rows(
    connection: &Connection,
    change: &PhysicalGraphProjectionChange,
    aliases: &[PhysicalAliasDeclaration],
) -> Result<ApplyChangeInstrumentation, MaterializationError> {
    validate_change_ownership(change, aliases)?;
    if !change.deletions.is_empty() {
        return Err(MaterializationError::InvalidInput(
            "fresh projection append cannot delete pages".into(),
        ));
    }

    let replacement_paths = change
        .replacements
        .iter()
        .map(|page| page.path.as_str())
        .collect::<BTreeSet<_>>();
    if replacement_paths.len() != change.replacements.len() {
        return Err(MaterializationError::InvalidInput(
            "fresh projection append contains a duplicate page path".into(),
        ));
    }
    for path in &replacement_paths {
        let exists: bool = query_row_cached(
            connection,
            "SELECT EXISTS(SELECT 1 FROM pages WHERE path = ?1)",
            params![path],
            |row| row.get(0),
        )?;
        if exists {
            return Err(MaterializationError::InvalidInput(format!(
                "fresh projection append repeats page path {path:?}"
            )));
        }
    }

    let block_count = change
        .replacements
        .iter()
        .map(|page| page.blocks.len())
        .sum::<usize>();
    let allocated = allocate_entity_coordinates(
        connection,
        change.replacements.len().saturating_add(block_count),
    )?;
    let mut next = allocated.into_iter();
    let page_ids = change
        .replacements
        .iter()
        .map(|page| {
            (
                page.path.clone(),
                next.next().expect("allocated fresh page coordinate"),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut block_ids = BTreeMap::new();
    for page in &change.replacements {
        for block in &page.blocks {
            if block.result_id.is_empty() || block_ids.contains_key(&block.result_id) {
                return Err(MaterializationError::InvalidInput(
                    "fresh projection blocks contain an empty or duplicate result ID".into(),
                ));
            }
            let exists: bool = query_row_cached(
                connection,
                "SELECT EXISTS(SELECT 1 FROM blocks WHERE result_id = ?1)",
                params![&block.result_id],
                |row| row.get(0),
            )?;
            if exists {
                return Err(MaterializationError::InvalidInput(format!(
                    "fresh projection append repeats block result ID {:?}",
                    block.result_id
                )));
            }
            block_ids.insert(
                block.result_id.clone(),
                next.next().expect("allocated fresh block coordinate"),
            );
        }
    }

    if !change.replacements.is_empty()
        || !change.reference_postings.is_empty()
        || !aliases.is_empty()
    {
        advance_query_projection_revision(connection)?;
    }
    for page in &change.replacements {
        insert_page(connection, page, page_ids[&page.path], None, &block_ids)?;
    }
    insert_replacement_fts_rows(
        connection,
        &change.replacements,
        &page_ids,
        &block_ids,
        None,
    )?;
    let block_pages = change
        .replacements
        .iter()
        .flat_map(|page| {
            page.blocks
                .iter()
                .map(|block| (block.result_id.as_str(), page.path.as_str()))
        })
        .collect::<BTreeMap<_, _>>();
    let mut own_memberships = DeferredOwnReferenceMemberships::new(&change.replacements);
    for posting in &change.reference_postings {
        let posting_id = insert_fresh_reference_posting(
            connection,
            posting,
            &page_ids,
            &block_ids,
            &block_pages,
        )?;
        own_memberships.capture_first_occurrence(posting, posting_id);
    }
    for alias in aliases {
        insert_alias_declaration(connection, alias)?;
    }
    insert_deferred_own_reference_memberships(
        connection,
        &change.replacements,
        &page_ids,
        &block_ids,
        &own_memberships,
    )?;
    Ok(ApplyChangeInstrumentation::default())
}

fn validate_change_ownership(
    change: &PhysicalGraphProjectionChange,
    aliases: &[PhysicalAliasDeclaration],
) -> Result<(), MaterializationError> {
    let replacement_ids = change
        .replacements
        .iter()
        .map(|page| page.path.as_str())
        .collect::<BTreeSet<_>>();
    if change
        .reference_postings
        .iter()
        .any(|posting| !replacement_ids.contains(posting.source_page_path.as_str()))
    {
        return Err(MaterializationError::InvalidInput(
            "graph-projection reference postings must belong to replacement pages".into(),
        ));
    }
    if aliases
        .iter()
        .any(|alias| !replacement_ids.contains(alias.source_page_path.as_str()))
    {
        return Err(MaterializationError::InvalidInput(
            "graph-projection aliases must belong to replacement pages".into(),
        ));
    }
    Ok(())
}

fn allocate_entity_coordinates(
    connection: &Connection,
    count: usize,
) -> Result<Vec<i64>, MaterializationError> {
    let count = i64::try_from(count).map_err(|_| {
        MaterializationError::InvalidInput("entity coordinate count exceeds SQLite".into())
    })?;
    let start: i64 = query_row_cached(
        connection,
        "SELECT next_entity_id FROM query_projection_state WHERE singleton = 1",
        &[],
        |row| row.get(0),
    )?;
    let end = start.checked_add(count).ok_or_else(|| {
        MaterializationError::InvalidInput("entity coordinate allocator exhausted".into())
    })?;
    let changed = execute_cached(connection,
        "UPDATE query_projection_state SET next_entity_id = ?1 WHERE singleton = 1 AND next_entity_id = ?2",
        params![end, start],
    )?;
    if changed != 1 {
        return Err(MaterializationError::Corrupt(
            "entity coordinate allocator is missing".into(),
        ));
    }
    Ok((start..end).collect())
}

fn affected_name_ids(
    connection: &Connection,
    page_ids: &BTreeSet<i64>,
) -> Result<BTreeSet<i64>, MaterializationError> {
    let mut output = BTreeSet::new();
    for page_id in page_ids {
        for sql in [
            "SELECT name_id FROM pages WHERE page_id = ?1",
            "SELECT target_name_id FROM reference_postings WHERE source_page_id = ?1 AND target_name_id IS NOT NULL",
            "SELECT alias_name_id FROM reference_alias_declarations WHERE source_page_id = ?1",
            "SELECT name_id FROM properties WHERE page_id = ?1",
            "SELECT name_id FROM tags WHERE page_id = ?1",
            "SELECT name_id FROM block_path_refs WHERE page_id = ?1",
            "SELECT name_id FROM property_atoms WHERE page_id = ?1",
        ] {
            let mut statement = connection.prepare_cached(sql)?;
            for row in statement.query_map(params![page_id], |row| row.get::<_, i64>(0))? {
                output.insert(row?);
            }
        }
    }
    Ok(output)
}

fn reclaim_affected_names(
    connection: &Connection,
    affected: &BTreeSet<i64>,
) -> Result<(), MaterializationError> {
    for name_id in affected {
        execute_cached(
            connection,
            "DELETE FROM names WHERE name_id = ?1
             AND NOT EXISTS (SELECT 1 FROM pages WHERE name_id = ?1)
             AND NOT EXISTS (
                 SELECT 1 FROM reference_postings
                 WHERE target_name_id = ?1 AND target_type = 0
             )
             AND NOT EXISTS (SELECT 1 FROM reference_alias_declarations WHERE alias_name_id = ?1)
             AND NOT EXISTS (SELECT 1 FROM properties WHERE name_id = ?1)
             AND NOT EXISTS (SELECT 1 FROM tags WHERE name_id = ?1)
             AND NOT EXISTS (SELECT 1 FROM block_path_refs WHERE name_id = ?1)
             AND NOT EXISTS (SELECT 1 FROM property_atoms WHERE name_id = ?1)",
            params![name_id],
        )?;
    }
    Ok(())
}

const OWN_REFERENCE_OCCURRENCE_SQL: &str = "SELECT r.posting_id
     FROM reference_postings AS r
     JOIN names AS n ON n.name_id = r.target_name_id
     WHERE r.source_page_id = ?1
       AND r.source_entity_type = 1 AND r.source_entity_id = ?2
       AND r.target_type = 0 AND r.reference_kind < 8
       AND n.key = ?3 ORDER BY r.posting_id LIMIT 1";
const MARK_OWN_REFERENCE_SQL: &str = "UPDATE reference_postings SET own = 1 WHERE posting_id = ?1";

/// While a fresh build has its secondary indexes deferred, remember the first
/// inserted occurrence for each parser-owned membership. This derives the
/// answer from the current operation's rows instead of repeatedly scanning the
/// ever-growing postings table before its source index exists.
struct DeferredOwnReferenceMemberships<'a> {
    membership_indexes: BTreeMap<&'a str, BTreeMap<&'a str, BTreeMap<&'a str, usize>>>,
    first_occurrences: Vec<Option<i64>>,
}

impl<'a> DeferredOwnReferenceMemberships<'a> {
    fn new(pages: &'a [PhysicalPage]) -> Self {
        let mut membership_indexes =
            BTreeMap::<&'a str, BTreeMap<&'a str, BTreeMap<&'a str, usize>>>::new();
        let mut membership_count = 0;
        for page in pages {
            for block in &page.blocks {
                let names = membership_indexes
                    .entry(page.path.as_str())
                    .or_default()
                    .entry(block.result_id.as_str())
                    .or_default();
                for name in &block.own_refs {
                    if !names.contains_key(name.key.as_str()) {
                        names.insert(name.key.as_str(), membership_count);
                        membership_count += 1;
                    }
                }
            }
        }
        Self {
            membership_indexes,
            first_occurrences: vec![None; membership_count],
        }
    }

    fn capture_first_occurrence(&mut self, posting: &PhysicalReferencePosting, posting_id: i64) {
        let PhysicalEntityId::Block(block_id) = &posting.source_entity else {
            return;
        };
        let PhysicalReferenceTarget::PageName {
            normalized_name, ..
        } = &posting.target
        else {
            return;
        };
        let membership = self
            .membership_indexes
            .get(posting.source_page_path.as_str())
            .and_then(|blocks| blocks.get(block_id.as_str()))
            .and_then(|names| names.get(normalized_name.as_str()));
        if let Some(&membership) = membership {
            self.first_occurrences[membership].get_or_insert(posting_id);
        }
    }

    fn first_occurrence(&self, page: &str, block: &str, name: &str) -> Option<i64> {
        self.membership_indexes
            .get(page)
            .and_then(|blocks| blocks.get(block))
            .and_then(|names| names.get(name))
            .and_then(|&membership| self.first_occurrences[membership])
    }
}

fn insert_deferred_own_reference_memberships(
    connection: &Connection,
    pages: &[PhysicalPage],
    page_ids: &BTreeMap<String, i64>,
    block_ids: &BTreeMap<String, i64>,
    memberships: &DeferredOwnReferenceMemberships<'_>,
) -> Result<(), MaterializationError> {
    for page in pages {
        let page_id = page_ids[&page.path];
        for block in &page.blocks {
            let block_id = block_ids[&block.result_id];
            let mut seen = BTreeSet::new();
            for name in &block.own_refs {
                if !seen.insert(name.key.as_str()) {
                    continue;
                }
                if let Some(posting_id) = memberships.first_occurrence(
                    page.path.as_str(),
                    block.result_id.as_str(),
                    name.key.as_str(),
                ) {
                    execute_cached(connection, MARK_OWN_REFERENCE_SQL, params![posting_id])?;
                } else {
                    insert_synthetic_own_reference(connection, page_id, block_id, name)?;
                }
            }
        }
    }
    Ok(())
}

fn insert_own_reference_memberships(
    connection: &Connection,
    pages: &[PhysicalPage],
) -> Result<(), MaterializationError> {
    for page in pages {
        let page_id = page_coordinate(connection, &page.path)?;
        for block in &page.blocks {
            let block_id = block_coordinate(connection, &block.result_id)?;
            let mut seen = BTreeSet::new();
            for name in &block.own_refs {
                // `own_refs` is the normalized parser set. One occurrence with
                // the same normalized key witnesses that membership; its raw
                // spelling is not the source of the membership claim. Every
                // occurrence keeps its own dictionary spelling regardless.
                if !seen.insert(name.key.as_str()) {
                    continue;
                }
                let occurrence: Option<i64> = query_row_cached(
                    connection,
                    OWN_REFERENCE_OCCURRENCE_SQL,
                    params![page_id, block_id, &name.key],
                    |row| row.get(0),
                )
                .optional()?;
                if let Some(posting_id) = occurrence {
                    execute_cached(connection, MARK_OWN_REFERENCE_SQL, params![posting_id])?;
                } else {
                    insert_synthetic_own_reference(connection, page_id, block_id, name)?;
                }
            }
        }
    }
    Ok(())
}

fn insert_synthetic_own_reference(
    connection: &Connection,
    page_id: i64,
    block_id: i64,
    name: &PhysicalName,
) -> Result<(), MaterializationError> {
    let name_id = intern_name(connection, &name.key, &name.raw)?;
    execute_cached(
        connection,
        "INSERT INTO reference_postings (
            source_page_id, source_entity_type, source_entity_id,
            source_locator, ordinal, reference_kind, target_type,
            target_name_id, raw_uuid_claim, own
         ) VALUES (?1, 1, ?2, NULL, NULL, 8, 0, ?3, NULL, 1)",
        params![page_id, block_id, name_id],
    )?;
    Ok(())
}

pub(crate) fn reset_graph_projection_rows(
    transaction: &Connection,
) -> Result<(), MaterializationError> {
    advance_query_projection_revision(transaction)?;
    transaction.execute_batch(
        "DELETE FROM search_fts;
         DELETE FROM property_atoms;
         DELETE FROM block_path_refs;
         DELETE FROM block_planning;
         DELETE FROM tasks;
         DELETE FROM tags;
         DELETE FROM properties;
         DELETE FROM reference_alias_declarations;
         DELETE FROM reference_postings;
         DELETE FROM block_text;
         DELETE FROM blocks;
         DELETE FROM page_text;
         DELETE FROM pages;
         DELETE FROM names;
         UPDATE query_projection_state SET next_entity_id = 1 WHERE singleton = 1;",
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
    page_id: i64,
) -> Result<PageCleanupInstrumentation, MaterializationError> {
    let page = &page_id;
    let existing: i64 = query_row_cached(
        transaction,
        "SELECT EXISTS(SELECT 1 FROM pages WHERE page_id = ?1)",
        params![page],
        |row| row.get(0),
    )?;
    let mut instrumentation = PageCleanupInstrumentation {
        existing_pages: usize::from(existing != 0),
        ..PageCleanupInstrumentation::default()
    };
    let block_ids = transaction
        .prepare_cached("SELECT block_id FROM blocks WHERE page_id = ?1 ORDER BY block_id")?
        .query_map(params![page], |row| row.get::<_, i64>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    for rowid in std::iter::once(page_id).chain(block_ids) {
        instrumentation.fts_rowids = instrumentation.fts_rowids.saturating_add(execute_cached(
            transaction,
            "DELETE FROM search_fts WHERE rowid = ?1",
            params![rowid],
        )?);
    }
    for table in ["reference_postings", "reference_alias_declarations"] {
        instrumentation.owned_rows = instrumentation.owned_rows.saturating_add(execute_cached(
            transaction,
            &format!("DELETE FROM {table} WHERE source_page_id = ?1"),
            params![page],
        )?);
    }
    instrumentation.owned_rows = instrumentation.owned_rows.saturating_add(execute_cached(
        transaction,
        "DELETE FROM properties WHERE page_id = ?1",
        params![page],
    )?);
    instrumentation.owned_rows = instrumentation.owned_rows.saturating_add(execute_cached(
        transaction,
        "DELETE FROM property_atoms WHERE page_id = ?1",
        params![page],
    )?);
    instrumentation.owned_rows = instrumentation.owned_rows.saturating_add(execute_cached(
        transaction,
        "DELETE FROM block_path_refs WHERE page_id = ?1",
        params![page],
    )?);
    instrumentation.owned_rows = instrumentation.owned_rows.saturating_add(execute_cached(
        transaction,
        "DELETE FROM block_planning WHERE page_id = ?1",
        params![page],
    )?);
    instrumentation.owned_rows = instrumentation.owned_rows.saturating_add(execute_cached(
        transaction,
        "DELETE FROM tags WHERE page_id = ?1",
        params![page],
    )?);
    instrumentation.owned_rows = instrumentation.owned_rows.saturating_add(execute_cached(
        transaction,
        "DELETE FROM tasks WHERE page_id = ?1",
        params![page],
    )?);
    instrumentation.owned_rows += execute_cached(
        transaction,
        "DELETE FROM block_text WHERE block_id IN (SELECT block_id FROM blocks WHERE page_id = ?1)",
        params![page],
    )?;
    instrumentation.owned_rows += execute_cached(
        transaction,
        "DELETE FROM page_text WHERE page_id = ?1",
        params![page],
    )?;
    instrumentation.owned_rows = instrumentation.owned_rows.saturating_add(execute_cached(
        transaction,
        "DELETE FROM blocks WHERE page_id = ?1",
        params![page],
    )?);
    instrumentation.owned_rows = instrumentation.owned_rows.saturating_add(execute_cached(
        transaction,
        "DELETE FROM pages WHERE page_id = ?1",
        params![page],
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

/// Read one row through the prepared-statement cache, for the same reason as
/// [`execute_cached`]: a build resolves page, block, and name coordinates
/// once per reference posting and own-reference membership.
fn query_row_cached<T>(
    connection: &Connection,
    sql: &str,
    parameters: &[&dyn rusqlite::ToSql],
    map: impl FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    connection.prepare_cached(sql)?.query_row(parameters, map)
}

fn insert_page(
    transaction: &Connection,
    page: &PhysicalPage,
    page_id: i64,
    retained_position: Option<i64>,
    block_ids: &BTreeMap<String, i64>,
) -> Result<(), MaterializationError> {
    let name_id = intern_name(transaction, &page.name_key, &page.name)?;
    let page_estimated = query_page_result_estimated_bytes(
        &page.name,
        &page.path,
        page.properties
            .iter()
            .map(|property| (property.name.as_str(), property.value.as_str())),
    );
    execute_cached(
        transaction,
        "INSERT INTO pages (
             page_id, name_id, path, text_kind, journal_day, position,
             estimated_bytes, property_count
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            page_id,
            name_id,
            page.path.as_str(),
            page.text_kind,
            page.journal_day,
            page.position
                .map(i64::try_from)
                .transpose()
                .map_err(|_| {
                    MaterializationError::InvalidInput("query page position exceeds SQLite".into())
                })?
                .or(retained_position),
            i64::try_from(page_estimated).map_err(|_| MaterializationError::InvalidInput(
                "query page estimate exceeds SQLite".into()
            ))?,
            i64::try_from(page.properties.len()).map_err(
                |_| MaterializationError::InvalidInput(
                    "query page property count exceeds SQLite".into()
                )
            )?,
        ],
    )?;
    execute_cached(
        transaction,
        "INSERT INTO page_text (page_id, preamble) VALUES (?1, ?2)",
        params![page_id, &page.preamble],
    )?;
    insert_properties(
        transaction,
        PhysicalEntityCoordinate::Page(page_id),
        page_id,
        &page.properties,
    )?;
    insert_tags(
        transaction,
        PhysicalEntityCoordinate::Page(page_id),
        page_id,
        &page.tags,
    )?;
    insert_property_atoms(
        transaction,
        PhysicalEntityCoordinate::Page(page_id),
        page_id,
        &page.property_atoms,
    )?;
    let traversal = query_block_preorder(page.blocks.iter().map(|block| {
        (
            block.result_id.as_str(),
            block.parent.as_deref(),
            block.order.as_str(),
        )
    }))?;
    for (preorder, (index, _depth)) in traversal.into_iter().enumerate() {
        let block = &page.blocks[index];
        let block_id = block_ids[&block.result_id];
        let parent_id = block
            .parent
            .as_ref()
            .map(|parent| {
                block_ids.get(parent).copied().ok_or_else(|| {
                    MaterializationError::InvalidInput("query block has unknown parent".into())
                })
            })
            .transpose()?;
        let estimated = query_result_estimated_bytes(
            &block.result_id,
            &block.content,
            block.tags.iter().map(|tag| tag.tag.as_str()),
            block
                .properties
                .iter()
                .map(|property| (property.name.as_str(), property.value.as_str())),
        );
        insert_block(
            transaction,
            page_id,
            block,
            block_id,
            parent_id,
            preorder,
            estimated,
        )?;
    }
    Ok(())
}

fn insert_block(
    transaction: &Connection,
    page_id: i64,
    block: &PhysicalBlock,
    block_id: i64,
    parent_id: Option<i64>,
    preorder: usize,
    estimated: usize,
) -> Result<(), MaterializationError> {
    let (logseq_uuid, origin) = match (block.logseq_uuid, block.logseq_identity_origin) {
        (Some(uuid), Some(origin)) => (Some(uuid.to_vec()), Some(origin)),
        (None, None) => (None, None),
        _ => {
            return Err(MaterializationError::InvalidInput(format!(
                "block {:?} has incomplete Logseq identity metadata",
                block.result_id
            )));
        }
    };
    execute_cached(
        transaction,
        "INSERT INTO blocks (
             block_id, page_id, result_id, parent_block_id, order_key,
             heading_level, collapsed, logseq_uuid, logseq_identity_origin,
             preorder, estimated_bytes, tag_count, property_count
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            block_id,
            page_id,
            &block.result_id,
            parent_id,
            &block.order,
            block.heading_level.map(i64::from),
            i64::from(block.collapsed),
            logseq_uuid,
            origin,
            i64::try_from(preorder).map_err(|_| MaterializationError::InvalidInput(
                "query preorder exceeds SQLite".into()
            ))?,
            i64::try_from(estimated).map_err(|_| MaterializationError::InvalidInput(
                "query estimate exceeds SQLite".into()
            ))?,
            i64::try_from(block.tags.len()).map_err(|_| MaterializationError::InvalidInput(
                "query tag count exceeds SQLite".into()
            ))?,
            i64::try_from(block.properties.len()).map_err(|_| {
                MaterializationError::InvalidInput("query property count exceeds SQLite".into())
            })?,
        ],
    )?;
    execute_cached(
        transaction,
        "INSERT INTO block_text (block_id, content) VALUES (?1, ?2)",
        params![block_id, &block.content],
    )?;
    let owner = PhysicalEntityCoordinate::Block(block_id);
    insert_properties(transaction, owner, page_id, &block.properties)?;
    insert_tags(transaction, owner, page_id, &block.tags)?;
    insert_property_atoms(transaction, owner, page_id, &block.property_atoms)?;
    insert_path_refs(transaction, block_id, page_id, &block.path_refs)?;
    if let Some(task) = &block.task {
        execute_cached(
            transaction,
            "INSERT INTO tasks (
                 block_id, page_id, marker, priority, scheduled, deadline
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                block_id,
                page_id,
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
                "block {:?} carries an empty planning facet",
                block.result_id
            )));
        }
        execute_cached(
            transaction,
            "INSERT INTO block_planning (
                 block_id, page_id, priority, scheduled, scheduled_day,
                 deadline, deadline_day
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                block_id,
                page_id,
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

fn insert_replacement_fts_rows(
    transaction: &Connection,
    replacements: &[PhysicalPage],
    page_ids: &BTreeMap<String, i64>,
    block_ids: &BTreeMap<String, i64>,
    mut instrumentation: Option<&mut FtsChangeInstrumentation>,
) -> Result<(), MaterializationError> {
    for page in replacements {
        insert_fts_row(transaction, page_ids[&page.path], &page.search_tokens)?;
        if let Some(stats) = instrumentation.as_deref_mut() {
            stats.page_rows = stats.page_rows.saturating_add(1);
        }
        for block in &page.blocks {
            insert_fts_row(
                transaction,
                block_ids[&block.result_id],
                &block.search_tokens,
            )?;
            if let Some(stats) = instrumentation.as_deref_mut() {
                stats.block_rows = stats.block_rows.saturating_add(1);
            }
        }
    }
    Ok(())
}

fn insert_fts_row(
    transaction: &Connection,
    rowid: i64,
    search_tokens: &str,
) -> Result<(), MaterializationError> {
    if search_tokens.len() > MAX_MATERIALIZATION_FIELD_BYTES {
        return Err(resource_limit(
            "search token bytes",
            search_tokens.len(),
            MAX_MATERIALIZATION_FIELD_BYTES,
        ));
    }
    execute_cached(
        transaction,
        "INSERT INTO search_fts (rowid, normalized_text) VALUES (?1, ?2)",
        params![rowid, search_tokens],
    )?;
    Ok(())
}

fn insert_properties(
    transaction: &Connection,
    owner: PhysicalEntityCoordinate,
    page_id: i64,
    properties: &[PhysicalProperty],
) -> Result<(), MaterializationError> {
    let (owner_type, owner_id) = owner.sql_parts();
    for (ordinal, property) in properties.iter().enumerate() {
        execute_cached(
            transaction,
            "INSERT INTO properties (
                 owner_type, owner_id, page_id, name_id, value, ordinal
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                owner_type,
                owner_id,
                page_id,
                intern_name(transaction, &property.normalized_name, &property.name)?,
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
    owner: PhysicalEntityCoordinate,
    page_id: i64,
    tags: &[PhysicalTag],
) -> Result<(), MaterializationError> {
    let (owner_type, owner_id) = owner.sql_parts();
    for (ordinal, tag) in tags.iter().enumerate() {
        execute_cached(
            transaction,
            "INSERT INTO tags (owner_type, owner_id, page_id, name_id, ordinal)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                owner_type,
                owner_id,
                page_id,
                intern_name(transaction, &tag.tag_key, &tag.tag)?,
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
    owner: PhysicalEntityCoordinate,
    page_id: i64,
    atoms: &[PhysicalPropertyAtom],
) -> Result<(), MaterializationError> {
    let (owner_type, owner_id) = owner.sql_parts();
    for atom in atoms {
        execute_cached(
            transaction,
            "INSERT INTO property_atoms (
                 owner_type, owner_id, page_id, name_id, ordinal,
                 atom, atom_key, origin, atom_num, atom_day
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                owner_type,
                owner_id,
                page_id,
                intern_name(transaction, &atom.normalized_name, &atom.name)?,
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
    block_id: i64,
    page_id: i64,
    names: &[PhysicalName],
) -> Result<(), MaterializationError> {
    for name in names {
        execute_cached(
            transaction,
            "INSERT INTO block_path_refs (block_id, page_id, name_id)
             VALUES (?1, ?2, ?3)",
            params![
                block_id,
                page_id,
                intern_name(transaction, &name.key, &name.raw)?
            ],
        )?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPageRow {
    pub name: String,
    pub name_key: String,
    pub path: String,
    pub text_kind: i64,
    pub preamble: Option<String>,
}

/// Lightweight page row for navigation/autocomplete.  It deliberately omits
/// searchable body text so a title lookup never retains graph-sized content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalNavigationPageRow {
    pub cursor: i64,
    pub name: String,
    pub name_key: String,
    pub path: String,
    pub text_kind: i64,
    pub preamble: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalNavigationAliasRow {
    pub source_page_path: String,
    pub cursor: i64,
    pub name_cursor: i64,
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
    pub result_id: String,
    pub page_path: String,
    pub parent: Option<String>,
    pub order: String,
    pub content: String,
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
    pub source_page_path: String,
    pub source_block_id: String,
    pub page_cursor: i64,
    pub block_cursor: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPageReferrerCandidateRow {
    pub source_page_path: String,
    pub source: PhysicalEntityId,
    pub page_cursor: i64,
    pub entity_cursor: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalBlockPropertyCandidateRow {
    pub page_path: String,
    pub block_id: String,
    pub page_cursor: i64,
    pub block_cursor: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPropertyFacetRow {
    pub owner: PhysicalEntityId,
    pub page_path: String,
    pub owner_cursor: i64,
    pub name_cursor: i64,
    pub source_name: String,
    pub normalized_name: String,
    pub value: String,
    pub ordinal: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTaskCandidatePageRow {
    pub page_path: String,
    pub cursor: i64,
}

/// One physical task-index candidate with the raw block and page transport
/// fields needed for parser-owned task re-evaluation.
///
/// Priority, planning, heading, and other semantic facets are intentionally
/// absent: the application parser remains the authority for those meanings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTaskCandidateBlockRow {
    pub block_id: String,
    pub page_path: String,
    pub parent: Option<String>,
    pub page_cursor: i64,
    pub block_cursor: i64,
    pub order: String,
    pub content: String,
    pub logseq_uuid: Option<[u8; 16]>,
    pub page_name: String,
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
    pub block_id: String,
    pub page_path: String,
    pub parent: Option<String>,
    pub page_cursor: i64,
    pub block_cursor: i64,
    pub order: String,
    pub page_name: String,
    pub page_text_kind: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPropertyRow {
    pub owner: PhysicalEntityId,
    pub page_path: String,
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTagRow {
    pub owner: PhysicalEntityId,
    pub page_path: String,
    pub tag: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTaskRow {
    pub block_id: String,
    pub page_path: String,
    pub marker: String,
    pub priority: Option<String>,
    pub scheduled: Option<String>,
    pub deadline: Option<String>,
}

#[derive(Default)]
pub(crate) struct MaterializationReadBudget {
    bytes: usize,
}

impl MaterializationReadBudget {
    pub(crate) fn add(&mut self, bytes: usize) -> Result<(), MaterializationError> {
        self.bytes = checked_budget_add(
            "materialization read output bytes",
            self.bytes,
            bytes,
            MAX_MATERIALIZATION_READ_BYTES,
        )?;
        Ok(())
    }
}

pub(crate) fn checked_output_bytes<'a>(
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

fn public_entity_id(entity: &PhysicalEntityId) -> &str {
    match entity {
        PhysicalEntityId::Page(path) | PhysicalEntityId::Block(path) => path,
    }
}

fn page_row_output_bytes(row: &PhysicalPageRow) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        64,
        [
            Some(row.name.as_str()),
            Some(row.name_key.as_str()),
            Some(row.path.as_str()),
            row.preamble.as_deref(),
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
            Some(row.source_page_path.as_str()),
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
            Some(row.result_id.as_str()),
            Some(row.page_path.as_str()),
            row.parent.as_deref(),
            Some(row.order.as_str()),
            Some(row.content.as_str()),
        ],
    )
}

fn task_candidate_block_row_output_bytes(
    row: &PhysicalTaskCandidateBlockRow,
) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        72,
        [
            Some(row.block_id.as_str()),
            Some(row.page_path.as_str()),
            row.parent.as_deref(),
            Some(row.order.as_str()),
            Some(row.content.as_str()),
            Some(row.page_name.as_str()),
        ],
    )
}

fn task_candidate_locator_row_output_bytes(
    row: &PhysicalTaskCandidateLocatorRow,
) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        64,
        [
            Some(row.block_id.as_str()),
            Some(row.page_path.as_str()),
            row.parent.as_deref(),
            Some(row.order.as_str()),
            Some(row.page_name.as_str()),
        ],
    )
}

fn property_row_output_bytes(row: &PhysicalPropertyRow) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        64,
        [
            Some(public_entity_id(&row.owner)),
            Some(row.page_path.as_str()),
            Some(row.name.as_str()),
            Some(row.value.as_str()),
        ],
    )
}

fn tag_row_output_bytes(row: &PhysicalTagRow) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        64,
        [
            Some(public_entity_id(&row.owner)),
            Some(row.page_path.as_str()),
            Some(row.tag.as_str()),
        ],
    )
}

fn task_row_output_bytes(row: &PhysicalTaskRow) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        64,
        [
            Some(row.block_id.as_str()),
            Some(row.page_path.as_str()),
            Some(row.marker.as_str()),
            row.priority.as_deref(),
            row.scheduled.as_deref(),
            row.deadline.as_deref(),
        ],
    )
}

fn block_referrer_candidate_row_output_bytes(
    row: &PhysicalBlockReferrerCandidateRow,
) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        32,
        [
            Some(row.source_page_path.as_str()),
            Some(row.source_block_id.as_str()),
        ],
    )
}

fn page_referrer_candidate_row_output_bytes(
    row: &PhysicalPageReferrerCandidateRow,
) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        32,
        [
            Some(row.source_page_path.as_str()),
            Some(public_entity_id(&row.source)),
        ],
    )
}

fn block_property_candidate_row_output_bytes(
    row: &PhysicalBlockPropertyCandidateRow,
) -> Result<usize, MaterializationError> {
    checked_output_bytes(
        32,
        [Some(row.page_path.as_str()), Some(row.block_id.as_str())],
    )
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
    "SELECT b.block_id, p.page_id, b.result_id, p.path, parent.result_id,
            b.order_key, bt.content, b.logseq_uuid, n.raw, p.text_kind
     FROM tasks AS t
     JOIN blocks AS b
       ON b.block_id = t.block_id AND b.page_id = t.page_id
     JOIN pages AS p ON p.page_id = t.page_id
     JOIN names AS n ON n.name_id = p.name_id
     LEFT JOIN blocks AS parent ON parent.block_id = b.parent_block_id
     LEFT JOIN block_text AS bt ON bt.block_id = b.block_id
     WHERE t.marker = ?1
     ORDER BY t.page_id, t.block_id LIMIT ?2";

const TASK_CANDIDATE_BLOCKS_AFTER_SQL: &str =
    "SELECT b.block_id, p.page_id, b.result_id, p.path, parent.result_id,
            b.order_key, bt.content, b.logseq_uuid, n.raw, p.text_kind
     FROM tasks AS t
     JOIN blocks AS b
       ON b.block_id = t.block_id AND b.page_id = t.page_id
     JOIN pages AS p ON p.page_id = t.page_id
     JOIN names AS n ON n.name_id = p.name_id
     LEFT JOIN blocks AS parent ON parent.block_id = b.parent_block_id
     LEFT JOIN block_text AS bt ON bt.block_id = b.block_id
     WHERE t.marker = ?1
       AND (t.page_id, t.block_id) > (?2, ?3)
     ORDER BY t.page_id, t.block_id LIMIT ?4";

const TASK_CANDIDATE_LOCATORS_SQL: &str =
    "SELECT b.block_id, p.page_id, b.result_id, p.path, parent.result_id,
            b.order_key, n.raw, p.text_kind
     FROM tasks AS t
     JOIN blocks AS b
       ON b.block_id = t.block_id AND b.page_id = t.page_id
     JOIN pages AS p ON p.page_id = t.page_id
     JOIN names AS n ON n.name_id = p.name_id
     LEFT JOIN blocks AS parent ON parent.block_id = b.parent_block_id
     WHERE t.marker = ?1
     ORDER BY t.page_id, t.block_id LIMIT ?2";

const TASK_CANDIDATE_LOCATORS_AFTER_SQL: &str =
    "SELECT b.block_id, p.page_id, b.result_id, p.path, parent.result_id,
            b.order_key, n.raw, p.text_kind
     FROM tasks AS t
     JOIN blocks AS b
       ON b.block_id = t.block_id AND b.page_id = t.page_id
     JOIN pages AS p ON p.page_id = t.page_id
     JOIN names AS n ON n.name_id = p.name_id
     LEFT JOIN blocks AS parent ON parent.block_id = b.parent_block_id
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
pub(crate) const NAVIGATION_REFERENCE_NAMES_FIRST_SQL: &str = "SELECT DISTINCT n.key, n.raw
     FROM reference_postings r JOIN names n ON n.name_id = r.target_name_id
     WHERE r.target_type = 0 AND r.reference_kind <= 4
     ORDER BY n.key, n.raw LIMIT ?1";
pub(crate) const NAVIGATION_REFERENCE_NAMES_AFTER_SQL: &str = "SELECT DISTINCT n.key, n.raw
     FROM reference_postings r JOIN names n ON n.name_id = r.target_name_id
     WHERE r.target_type = 0 AND r.reference_kind <= 4
       AND (n.key, n.raw) > (?1, ?2)
     ORDER BY n.key, n.raw LIMIT ?3";
pub(crate) const NAVIGATION_ALIASES_FIRST_SQL: &str =
    "SELECT DISTINCT d.source_page_id, d.alias_name_id, owner.raw, p.path, alias.key
     FROM reference_alias_declarations d
     JOIN pages p ON p.page_id = d.source_page_id
     JOIN names owner ON owner.name_id = p.name_id
     JOIN names alias ON alias.name_id = d.alias_name_id
     ORDER BY d.source_page_id, d.alias_name_id LIMIT ?1";
pub(crate) const NAVIGATION_ALIASES_AFTER_SQL: &str =
    "SELECT DISTINCT d.source_page_id, d.alias_name_id, owner.raw, p.path, alias.key
     FROM reference_alias_declarations d
     JOIN pages p ON p.page_id = d.source_page_id
     JOIN names owner ON owner.name_id = p.name_id
     JOIN names alias ON alias.name_id = d.alias_name_id
     WHERE (d.source_page_id, d.alias_name_id) > (?1, ?2)
     ORDER BY d.source_page_id, d.alias_name_id LIMIT ?3";

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

    pub fn page(&self, path: &str) -> Result<Option<PhysicalPageRow>, MaterializationError> {
        self.page_with_header_validation(path, allow_any_page_header)
    }

    pub fn page_with_header_validation(
        &self,
        path: &str,
        mut validate_header: impl FnMut(&str, i64) -> Result<(), MaterializationError>,
    ) -> Result<Option<PhysicalPageRow>, MaterializationError> {
        let page = self
            .connection
            .query_row(
                "SELECT n.raw, n.key, p.path, p.text_kind, t.preamble
                 FROM pages AS p JOIN names AS n ON n.name_id = p.name_id
                 LEFT JOIN page_text AS t USING (page_id) WHERE p.path = ?1",
                params![path],
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

    pub fn block(&self, result_id: &str) -> Result<Option<PhysicalBlockRow>, MaterializationError> {
        let block = self
            .connection
            .query_row(
                "SELECT b.result_id, p.path, parent.result_id, b.order_key,
                        bt.content, b.heading_level,
                        b.collapsed, b.logseq_uuid, b.logseq_identity_origin
                 FROM blocks AS b JOIN pages AS p ON p.page_id = b.page_id
                 LEFT JOIN blocks AS parent ON parent.block_id = b.parent_block_id
                 LEFT JOIN block_text AS bt USING (block_id) WHERE b.result_id = ?1",
                params![result_id],
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
            "SELECT b.result_id, p.path, parent.result_id, b.order_key,
                    bt.content, b.heading_level,
                    b.collapsed, b.logseq_uuid, b.logseq_identity_origin
             FROM blocks AS b JOIN pages AS p ON p.page_id = b.page_id
             LEFT JOIN blocks AS parent ON parent.block_id = b.parent_block_id
             LEFT JOIN block_text AS bt USING (block_id) WHERE b.logseq_uuid = ?1
             ORDER BY b.block_id LIMIT ?2",
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
                "SELECT n.raw, n.key, p.path, p.text_kind, t.preamble
                 FROM pages AS p JOIN names AS n ON n.name_id = p.name_id
                 LEFT JOIN page_text AS t USING (page_id) WHERE p.text_kind = ?1 ORDER BY p.path, p.page_id LIMIT ?2",
                vec![kind.into(), limit.into()],
            ),
            None => (
                "SELECT n.raw, n.key, p.path, p.text_kind, t.preamble
                 FROM pages AS p JOIN names AS n ON n.name_id = p.name_id
                 LEFT JOIN page_text AS t USING (page_id) ORDER BY p.path, p.page_id LIMIT ?1",
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
        after_page_id: Option<i64>,
        limit: usize,
        mut validate_header: impl FnMut(&str, i64) -> Result<(), MaterializationError>,
    ) -> Result<Vec<PhysicalNavigationPageRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        if after_path.is_some() != after_page_id.is_some() {
            return Err(MaterializationError::InvalidQuery(
                "navigation page cursor requires both path and page ID".into(),
            ));
        }
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match (after_path, after_page_id) {
            (None, None) => (
                "SELECT p.page_id, n.raw, n.key, p.path, p.text_kind, t.preamble, t.page_id
                     FROM pages AS p JOIN names AS n ON n.name_id = p.name_id
                     LEFT JOIN page_text AS t USING (page_id) ORDER BY p.path, p.page_id LIMIT ?1",
                vec![limit.into()],
            ),
            (Some(path), Some(page_id)) => (
                "SELECT p.page_id, n.raw, n.key, p.path, p.text_kind, t.preamble, t.page_id
                     FROM pages AS p JOIN names AS n ON n.name_id = p.name_id
                     LEFT JOIN page_text AS t USING (page_id)
                     WHERE p.path > ?1 OR (p.path = ?1 AND p.page_id > ?2)
                     ORDER BY p.path, p.page_id LIMIT ?3",
                vec![path.to_owned().into(), page_id.into(), limit.into()],
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
        after: Option<(i64, i64)>,
        limit: usize,
    ) -> Result<Vec<PhysicalNavigationAliasRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (NAVIGATION_ALIASES_FIRST_SQL, vec![limit.into()]),
            Some((page_id, name_id)) => (
                NAVIGATION_ALIASES_AFTER_SQL,
                vec![page_id.into(), name_id.into(), limit.into()],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            Ok(PhysicalNavigationAliasRow {
                cursor: row.get(0)?,
                name_cursor: row.get(1)?,
                owner_name: row.get(2)?,
                owner_path: row.get(3)?,
                normalized_alias: row.get(4)?,
                source_page_path: row.get(3)?,
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
        source_page_id: Option<i64>,
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
                vec![page_id.into(), limit.into()],
            ),
            (Some(page_id), Some(after)) => (
                "SELECT raw_uuid_claim, COUNT(DISTINCT source_entity_id)
                 FROM reference_postings
                 WHERE target_type = 1 AND source_entity_type = 1
                   AND source_page_id = ?1 AND raw_uuid_claim > ?2
                 GROUP BY raw_uuid_claim ORDER BY raw_uuid_claim LIMIT ?3",
                vec![page_id.into(), after.to_vec().into(), limit.into()],
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
        after: Option<(i64, i64)>,
        limit: usize,
    ) -> Result<Vec<PhysicalBlockReferrerCandidateRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (
                "SELECT DISTINCT r.source_page_id, r.source_entity_id, p.path, b.result_id
                 FROM reference_postings r JOIN pages p ON p.page_id = r.source_page_id
                 JOIN blocks b ON b.block_id = r.source_entity_id
                 WHERE r.target_type = 1 AND r.source_entity_type = 1
                   AND r.raw_uuid_claim = ?1
                 ORDER BY r.source_page_id, r.source_entity_id LIMIT ?2",
                vec![raw_uuid_claim.to_vec().into(), limit.into()],
            ),
            Some((page_id, block_id)) => (
                "SELECT DISTINCT r.source_page_id, r.source_entity_id, p.path, b.result_id
                 FROM reference_postings r JOIN pages p ON p.page_id = r.source_page_id
                 JOIN blocks b ON b.block_id = r.source_entity_id
                 WHERE r.target_type = 1 AND r.source_entity_type = 1
                   AND r.raw_uuid_claim = ?1
                   AND (r.source_page_id > ?2
                     OR (r.source_page_id = ?2 AND r.source_entity_id > ?3))
                 ORDER BY r.source_page_id, r.source_entity_id LIMIT ?4",
                vec![
                    raw_uuid_claim.to_vec().into(),
                    page_id.into(),
                    block_id.into(),
                    limit.into(),
                ],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            Ok(PhysicalBlockReferrerCandidateRow {
                page_cursor: row.get(0)?,
                block_cursor: row.get(1)?,
                source_page_path: row.get(2)?,
                source_block_id: row.get(3)?,
            })
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            block_referrer_candidate_row_output_bytes,
        )
    }

    /// Stable source candidates for one normalized explicit page-reference
    /// target. Property-key pseudo pages are not backlinks. Duplicate syntax
    /// occurrences collapse to one source entity; the parser-owned application
    /// page verifies exact membership before exposure.
    pub fn page_referrer_candidates_after(
        &self,
        normalized_name: &str,
        after: Option<(i64, PhysicalEntityCoordinate)>,
        limit: usize,
    ) -> Result<Vec<PhysicalPageReferrerCandidateRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        checked_query_text(normalized_name)?;
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (
                "SELECT DISTINCT r.source_page_id, r.source_entity_type, r.source_entity_id,
                    p.path, CASE r.source_entity_type WHEN 0 THEN p.path ELSE b.result_id END
                 FROM reference_postings r JOIN names n ON n.name_id = r.target_name_id
                 JOIN pages p ON p.page_id = r.source_page_id
                 LEFT JOIN blocks b ON r.source_entity_type = 1 AND b.block_id = r.source_entity_id
                 WHERE r.target_type = 0 AND r.reference_kind <= 4 AND n.key = ?1
                 ORDER BY r.source_page_id, r.source_entity_type, r.source_entity_id LIMIT ?2",
                vec![normalized_name.to_owned().into(), limit.into()],
            ),
            Some((page_id, source)) => {
                let (source_type, source_id) = source.sql_parts();
                (
                    "SELECT DISTINCT r.source_page_id, r.source_entity_type, r.source_entity_id,
                        p.path, CASE r.source_entity_type WHEN 0 THEN p.path ELSE b.result_id END
                     FROM reference_postings r JOIN names n ON n.name_id = r.target_name_id
                     JOIN pages p ON p.page_id = r.source_page_id
                     LEFT JOIN blocks b ON r.source_entity_type = 1 AND b.block_id = r.source_entity_id
                     WHERE r.target_type = 0 AND r.reference_kind <= 4 AND n.key = ?1
                       AND (r.source_page_id > ?2
                         OR (r.source_page_id = ?2 AND r.source_entity_type > ?3)
                         OR (r.source_page_id = ?2 AND r.source_entity_type = ?3
                             AND r.source_entity_id > ?4))
                     ORDER BY r.source_page_id, r.source_entity_type, r.source_entity_id LIMIT ?5",
                    vec![
                        normalized_name.to_owned().into(),
                        page_id.into(),
                        source_type.into(),
                        source_id.into(),
                        limit.into(),
                    ],
                )
            }
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        let rows = rows.map(
            |row| -> Result<PhysicalPageReferrerCandidateRow, MaterializationError> {
                let (page_id, source_type, source_id, page_path, identity) =
                    row.map_err(MaterializationError::from)?;
                Ok(PhysicalPageReferrerCandidateRow {
                    source_page_path: page_path,
                    source: public_entity(source_type, identity)?,
                    page_cursor: page_id,
                    entity_cursor: source_id,
                })
            },
        );
        collect_read_rows(rows, page_referrer_candidate_row_output_bytes)
    }

    /// Stable block owners for one canonical property key. Rows are candidates:
    /// callers retain semantic ownership of property parsing and subtree shape.
    pub fn block_property_candidates_after(
        &self,
        normalized_name: &str,
        after: Option<(i64, i64)>,
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
                "SELECT DISTINCT x.page_id, x.owner_id, p.path, b.result_id
                 FROM properties x JOIN names n ON n.name_id = x.name_id
                 JOIN pages p ON p.page_id = x.page_id JOIN blocks b ON b.block_id = x.owner_id
                 WHERE x.owner_type = 1 AND n.key = ?1
                 ORDER BY x.page_id, x.owner_id LIMIT ?2",
                vec![normalized_name.to_owned().into(), limit.into()],
            ),
            Some((page_id, block_id)) => (
                "SELECT DISTINCT x.page_id, x.owner_id, p.path, b.result_id
                 FROM properties x JOIN names n ON n.name_id = x.name_id
                 JOIN pages p ON p.page_id = x.page_id JOIN blocks b ON b.block_id = x.owner_id
                 WHERE x.owner_type = 1 AND n.key = ?1
                   AND (x.page_id > ?2 OR (x.page_id = ?2 AND x.owner_id > ?3))
                 ORDER BY x.page_id, x.owner_id LIMIT ?4",
                vec![
                    normalized_name.to_owned().into(),
                    page_id.into(),
                    block_id.into(),
                    limit.into(),
                ],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            Ok(PhysicalBlockPropertyCandidateRow {
                page_cursor: row.get(0)?,
                block_cursor: row.get(1)?,
                page_path: row.get(2)?,
                block_id: row.get(3)?,
            })
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            block_property_candidate_row_output_bytes,
        )
    }

    /// Traverse property facts in their stable primary-key order. The caller
    /// can request block owners only (query-builder policy) or both page and
    /// block owners (editor autocomplete policy). Values remain parser-derived
    /// facts; policy such as hidden/internal keys belongs to the caller.
    pub fn property_facet_rows_after(
        &self,
        block_owners_only: bool,
        after: Option<(PhysicalEntityCoordinate, i64, u32)>,
        limit: usize,
    ) -> Result<Vec<PhysicalPropertyFacetRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        if let Some((owner, _, _)) = &after {
            if block_owners_only && !matches!(owner, PhysicalEntityCoordinate::Block(_)) {
                return Err(MaterializationError::InvalidQuery(
                    "block-only property cursor must identify a block".into(),
                ));
            }
        }
        let (sql, args): (&str, Vec<rusqlite::types::Value>) = match after {
            None => (
                "SELECT x.owner_type, x.owner_id, x.name_id,
                        CASE x.owner_type WHEN 0 THEN p.path ELSE b.result_id END,
                        p.path, n.raw, n.key, x.value, x.ordinal
                 FROM properties x JOIN names n ON n.name_id = x.name_id
                 JOIN pages p ON p.page_id = x.page_id
                 LEFT JOIN blocks b ON x.owner_type = 1 AND b.block_id = x.owner_id
                 WHERE (?1 = 0 OR x.owner_type = 1)
                 ORDER BY x.owner_type, x.owner_id, x.name_id, x.ordinal LIMIT ?2",
                vec![i64::from(block_owners_only).into(), limit.into()],
            ),
            Some((owner, name_id, ordinal)) => {
                let (owner_type, owner_id) = owner.sql_parts();
                (
                    "SELECT x.owner_type, x.owner_id, x.name_id,
                            CASE x.owner_type WHEN 0 THEN p.path ELSE b.result_id END,
                            p.path, n.raw, n.key, x.value, x.ordinal
                     FROM properties x JOIN names n ON n.name_id = x.name_id
                     JOIN pages p ON p.page_id = x.page_id
                     LEFT JOIN blocks b ON x.owner_type = 1 AND b.block_id = x.owner_id
                     WHERE (?1 = 0 OR x.owner_type = 1)
                       AND (x.owner_type > ?2
                         OR (x.owner_type = ?2 AND x.owner_id > ?3)
                         OR (x.owner_type = ?2 AND x.owner_id = ?3 AND x.name_id > ?4)
                         OR (x.owner_type = ?2 AND x.owner_id = ?3 AND x.name_id = ?4 AND x.ordinal > ?5))
                     ORDER BY x.owner_type, x.owner_id, x.name_id, x.ordinal LIMIT ?6",
                    vec![
                        i64::from(block_owners_only).into(),
                        owner_type.into(),
                        owner_id.into(),
                        name_id.into(),
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
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, i64>(8)?,
            ))
        })?;
        collect_read_rows(
            rows.map(|row| {
                let (
                    owner_type,
                    owner_id,
                    name_id,
                    identity,
                    page_path,
                    source_name,
                    normalized_name,
                    value,
                    ordinal,
                ) = row?;
                Ok(PhysicalPropertyFacetRow {
                    owner: public_entity(owner_type, identity)?,
                    page_path,
                    owner_cursor: owner_id,
                    name_cursor: name_id,
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
                checked_output_bytes(
                    96,
                    [
                        Some(public_entity_id(&row.owner)),
                        Some(row.page_path.as_str()),
                        Some(row.source_name.as_str()),
                        Some(row.normalized_name.as_str()),
                        Some(row.value.as_str()),
                    ],
                )
            },
        )
    }

    pub fn properties(
        &self,
        owner: PhysicalEntityId,
        limit: usize,
    ) -> Result<Vec<PhysicalPropertyRow>, MaterializationError> {
        let limit = checked_limit(limit)?;
        let (owner_type, owner_id) = entity_coordinate(self.connection, &owner)?;
        let mut statement = self.connection.prepare(
            "SELECT x.owner_type,
                    CASE x.owner_type WHEN 0 THEN p.path ELSE b.result_id END,
                    p.path, n.raw, x.value
             FROM properties x JOIN names n ON n.name_id = x.name_id
             JOIN pages p ON p.page_id = x.page_id
             LEFT JOIN blocks b ON x.owner_type = 1 AND b.block_id = x.owner_id
             WHERE x.owner_type = ?1 AND x.owner_id = ?2
             ORDER BY n.raw, x.ordinal, x.value LIMIT ?3",
        )?;
        let rows = property_rows(
            statement.query_map(params![owner_type, owner_id, limit], property_tuple)?,
        );
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
            "SELECT x.owner_type,
                    CASE x.owner_type WHEN 0 THEN p.path ELSE b.result_id END,
                    p.path, n.raw
             FROM tags x JOIN names n ON n.name_id = x.name_id
             JOIN pages p ON p.page_id = x.page_id
             LEFT JOIN blocks b ON x.owner_type = 1 AND b.block_id = x.owner_id
             WHERE n.raw = ?1
             ORDER BY x.page_id, x.owner_type, x.owner_id, x.ordinal LIMIT ?2",
        )?;
        let rows = statement.query_map(params![tag, limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        let rows = rows.map(|row| {
            let (owner_type, owner_id, page_path, tag) = row?;
            Ok(PhysicalTagRow {
                owner: public_entity(owner_type, owner_id)?,
                page_path,
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
                "SELECT b.result_id, p.path, t.marker, t.priority, t.scheduled, t.deadline
                 FROM tasks t JOIN blocks b ON b.block_id = t.block_id
                 JOIN pages p ON p.page_id = t.page_id WHERE t.marker = ?1
                 ORDER BY t.deadline IS NULL, t.deadline, t.scheduled IS NULL, t.scheduled,
                          t.page_id, t.block_id LIMIT ?2",
                vec![
                    rusqlite::types::Value::Text(marker.to_owned()),
                    limit.into(),
                ],
            ),
            None => (
                "SELECT b.result_id, p.path, t.marker, t.priority, t.scheduled, t.deadline
                 FROM tasks t JOIN blocks b ON b.block_id = t.block_id
                 JOIN pages p ON p.page_id = t.page_id
                 ORDER BY t.deadline IS NULL, t.deadline, t.scheduled IS NULL, t.scheduled,
                          t.page_id, t.block_id LIMIT ?1",
                vec![limit.into()],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;
        let rows = rows.map(|row| {
            let (block_id, page_path, marker, priority, scheduled, deadline) = row?;
            Ok(PhysicalTaskRow {
                block_id,
                page_path,
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
        after: Option<i64>,
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
                "SELECT DISTINCT t.page_id, p.path FROM tasks t
                 JOIN pages p ON p.page_id = t.page_id
                 WHERE t.marker = ?1 ORDER BY t.page_id LIMIT ?2",
                vec![marker.to_owned().into(), limit.into()],
            ),
            Some(page_id) => (
                "SELECT DISTINCT t.page_id, p.path FROM tasks t
                 JOIN pages p ON p.page_id = t.page_id
                 WHERE t.marker = ?1 AND t.page_id > ?2
                 ORDER BY t.page_id LIMIT ?3",
                vec![marker.to_owned().into(), page_id.into(), limit.into()],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            Ok(PhysicalTaskCandidatePageRow {
                cursor: row.get(0)?,
                page_path: row.get(1)?,
            })
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            |row| checked_output_bytes(16, [Some(row.page_path.as_str())]),
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
        after: Option<(i64, i64)>,
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
        after: Option<(i64, i64)>,
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
                    page_id.into(),
                    block_id.into(),
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
        after: Option<(i64, i64)>,
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
                    page_id.into(),
                    block_id.into(),
                    limit.into(),
                ],
            ),
        };
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(args), |row| {
            Ok(PhysicalTaskCandidateLocatorRow {
                block_cursor: row.get(0)?,
                page_cursor: row.get(1)?,
                block_id: row.get(2)?,
                page_path: row.get(3)?,
                parent: row.get(4)?,
                order: row.get(5)?,
                page_name: row.get(6)?,
                page_text_kind: row.get(7)?,
            })
        })?;
        collect_read_rows(
            rows.map(|row| row.map_err(MaterializationError::from)),
            task_candidate_locator_row_output_bytes,
        )
    }
}

fn page_row_with_header_validation(
    row: &rusqlite::Row<'_>,
    validate_header: &mut impl FnMut(&str, i64) -> Result<(), MaterializationError>,
) -> rusqlite::Result<Result<PhysicalPageRow, MaterializationError>> {
    let path: String = row.get(2)?;
    let kind: i64 = row.get(3)?;
    if let Err(error) = validate_header(path.as_str(), kind) {
        return Ok(Err(error));
    }
    Ok(Ok(PhysicalPageRow {
        name: row.get(0)?,
        name_key: row.get(1)?,
        path,
        text_kind: kind,
        preamble: row.get(4)?,
    }))
}

fn navigation_page_row_with_header_validation(
    row: &rusqlite::Row<'_>,
    validate_header: &mut impl FnMut(&str, i64) -> Result<(), MaterializationError>,
) -> rusqlite::Result<Result<PhysicalNavigationPageRow, MaterializationError>> {
    // Preamble may legitimately be NULL. The keyed payload row may not be
    // missing: distinguish cache damage from an empty preamble without reading
    // the large search text solely to check presence.
    let _payload_id: i64 = row.get(6)?;
    let path: String = row.get(3)?;
    let kind: i64 = row.get(4)?;
    if let Err(error) = validate_header(path.as_str(), kind) {
        return Ok(Err(error));
    }
    Ok(Ok(PhysicalNavigationPageRow {
        cursor: row.get(0)?,
        name: row.get(1)?,
        name_key: row.get(2)?,
        path,
        text_kind: kind,
        preamble: row.get(5)?,
    }))
}

fn block_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<PhysicalBlockRow> {
    let heading_level: Option<i64> = row.get(5)?;
    let logseq_uuid: Option<Vec<u8>> = row.get(7)?;
    let origin: Option<i64> = row.get(8)?;
    Ok(PhysicalBlockRow {
        result_id: row.get(0)?,
        page_path: row.get(1)?,
        parent: row.get(2)?,
        order: row.get(3)?,
        content: row.get(4)?,
        heading_level: heading_level
            .map(|value| u8::try_from(value).map_err(sql_decode_error))
            .transpose()?,
        collapsed: row.get::<_, i64>(6)? != 0,
        logseq_uuid: logseq_uuid.as_deref().map(decode_id_sql).transpose()?,
        logseq_identity_origin: origin,
    })
}

fn task_candidate_block_row_with_header_validation(
    row: &rusqlite::Row<'_>,
    validate_header: &mut impl FnMut(&str, i64) -> Result<(), MaterializationError>,
) -> rusqlite::Result<Result<PhysicalTaskCandidateBlockRow, MaterializationError>> {
    let page_path: String = row.get(3)?;
    let logseq_uuid: Option<Vec<u8>> = row.get(7)?;
    let page_text_kind: i64 = row.get(9)?;
    if let Err(error) = validate_header(page_path.as_str(), page_text_kind) {
        return Ok(Err(error));
    }
    Ok(Ok(PhysicalTaskCandidateBlockRow {
        block_cursor: row.get(0)?,
        page_cursor: row.get(1)?,
        block_id: row.get(2)?,
        parent: row.get(4)?,
        order: row.get(5)?,
        content: row.get(6)?,
        logseq_uuid: logseq_uuid.as_deref().map(decode_id_sql).transpose()?,
        page_name: row.get(8)?,
        page_path,
        page_text_kind,
    }))
}

fn property_tuple(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<(i64, String, String, String, String)> {
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
        impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<(i64, String, String, String, String)>,
    >,
) -> Result<Vec<PhysicalPropertyRow>, MaterializationError> {
    let rows = rows.map(|row| {
        let (owner_type, owner_id, page_path, name, value) = row?;
        Ok(PhysicalPropertyRow {
            owner: public_entity(owner_type, owner_id)?,
            page_path,
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

fn public_entity(
    entity_type: i64,
    identity: String,
) -> Result<PhysicalEntityId, MaterializationError> {
    match entity_type {
        0 => Ok(PhysicalEntityId::Page(identity)),
        1 => Ok(PhysicalEntityId::Block(identity)),
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

    fn own_membership_lookup_steps(unrelated_postings: i64) -> (i32, i32, i32, Vec<String>) {
        let connection = Connection::open_in_memory().unwrap();
        initialize_graph_projection_schema(&connection).unwrap();
        connection
            .execute(
                "INSERT INTO names (name_id, key, raw) VALUES (1, 'target', 'Target')",
                [],
            )
            .unwrap();
        for id in 1..=unrelated_postings + 1 {
            connection
                .execute(
                    "INSERT INTO pages (
                         page_id, name_id, path, text_kind, journal_day, position,
                         estimated_bytes, property_count
                     ) VALUES (?1, 1, ?2, 0, NULL, NULL, 0, 0)",
                    params![id, format!("pages/{id}.md")],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO blocks (
                         block_id, page_id, result_id, parent_block_id, order_key,
                         heading_level, collapsed, logseq_uuid, logseq_identity_origin,
                         preorder, estimated_bytes, tag_count, property_count
                     ) VALUES (?1, ?1, ?2, NULL, 'a', NULL, 0, NULL, NULL, 0, 0, 0, 0)",
                    params![id, format!("block-{id}")],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO reference_postings (
                         source_page_id, source_entity_type, source_entity_id,
                         source_locator, ordinal, reference_kind, target_type,
                         target_name_id, raw_uuid_claim, own
                     ) VALUES (?1, 1, ?1, X'01', 0, 0, 0, 1, NULL, 0)",
                    params![id],
                )
                .unwrap();
        }
        let owner = unrelated_postings + 1;
        let plan = connection
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {OWN_REFERENCE_OCCURRENCE_SQL}"
            ))
            .unwrap()
            .query_map(params![owner, owner, "target"], |row| row.get(3))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap();
        let mut statement = connection.prepare(OWN_REFERENCE_OCCURRENCE_SQL).unwrap();
        let posting_id: i64 = statement
            .query_row(params![owner, owner, "target"], |row| row.get(0))
            .unwrap();
        let lookup_steps = statement.get_status(rusqlite::StatementStatus::VmStep);
        let fullscan_steps = statement.get_status(rusqlite::StatementStatus::FullscanStep);
        let mut update = connection.prepare(MARK_OWN_REFERENCE_SQL).unwrap();
        update.execute(params![posting_id]).unwrap();
        let update_steps = update.get_status(rusqlite::StatementStatus::VmStep);
        (lookup_steps, fullscan_steps, update_steps, plan)
    }

    fn own_membership_page() -> PhysicalPage {
        PhysicalPage {
            position: None,
            name: "Owner".into(),
            name_key: "owner".into(),
            path: "pages/owner.md".into(),
            text_kind: 0,
            journal_day: None,
            preamble: None,
            search_tokens: String::new(),
            properties: Vec::new(),
            tags: Vec::new(),
            property_atoms: Vec::new(),
            blocks: vec![PhysicalBlock {
                result_id: "owner-block".into(),
                own_refs: vec![
                    PhysicalName {
                        raw: "Target".into(),
                        key: "target".into(),
                    },
                    PhysicalName {
                        raw: "TARGET".into(),
                        key: "target".into(),
                    },
                ],
                parent: None,
                order: "a".into(),
                content: String::new(),
                search_tokens: String::new(),
                heading_level: None,
                collapsed: false,
                logseq_uuid: None,
                logseq_identity_origin: None,
                properties: Vec::new(),
                tags: Vec::new(),
                task: None,
                planning: None,
                path_refs: Vec::new(),
                property_atoms: Vec::new(),
            }],
        }
    }

    fn deferred_own_membership_write_steps(unrelated_postings: i64) -> (i32, i32) {
        let connection = Connection::open_in_memory().unwrap();
        initialize_graph_projection_schema(&connection).unwrap();
        drop_deferred_indexes(&connection).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_schema
                     WHERE name = 'reference_postings_source_idx'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        connection
            .execute(
                "INSERT INTO names (name_id, key, raw) VALUES (1, 'target', 'Target')",
                [],
            )
            .unwrap();
        let owner_page = own_membership_page();
        let mut memberships =
            DeferredOwnReferenceMemberships::new(std::slice::from_ref(&owner_page));
        for id in 1..=unrelated_postings + 1 {
            let owner = id == unrelated_postings + 1;
            let path = if owner {
                owner_page.path.clone()
            } else {
                format!("pages/{id}.md")
            };
            let block = if owner {
                owner_page.blocks[0].result_id.clone()
            } else {
                format!("block-{id}")
            };
            connection
                .execute(
                    "INSERT INTO pages (
                         page_id, name_id, path, text_kind, journal_day, position,
                         estimated_bytes, property_count
                     ) VALUES (?1, 1, ?2, 0, NULL, NULL, 0, 0)",
                    params![id, &path],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO blocks (
                         block_id, page_id, result_id, parent_block_id, order_key,
                         heading_level, collapsed, logseq_uuid, logseq_identity_origin,
                         preorder, estimated_bytes, tag_count, property_count
                     ) VALUES (?1, ?1, ?2, NULL, 'a', NULL, 0, NULL, NULL, 0, 0, 0, 0)",
                    params![id, &block],
                )
                .unwrap();
            let posting = PhysicalReferencePosting {
                source_page_path: path,
                source_entity: PhysicalEntityId::Block(block),
                source_locator: vec![1],
                ordinal: 0,
                kind: 0,
                target: PhysicalReferenceTarget::PageName {
                    raw_name: "Target".into(),
                    normalized_name: "target".into(),
                },
            };
            let posting_id = insert_reference_posting(&connection, &posting).unwrap();
            memberships.capture_first_occurrence(&posting, posting_id);
        }
        assert_eq!(memberships.first_occurrences.len(), 1);
        let posting_id = memberships
            .first_occurrence("pages/owner.md", "owner-block", "target")
            .unwrap();
        let owner_id = unrelated_postings + 1;
        insert_deferred_own_reference_memberships(
            &connection,
            std::slice::from_ref(&owner_page),
            &BTreeMap::from([(owner_page.path.clone(), owner_id)]),
            &BTreeMap::from([(owner_page.blocks[0].result_id.clone(), owner_id)]),
            &memberships,
        )
        .unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM reference_postings WHERE own = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        connection
            .execute(
                "UPDATE reference_postings SET own = 0 WHERE posting_id = ?1",
                params![posting_id],
            )
            .unwrap();
        let mut update = connection.prepare(MARK_OWN_REFERENCE_SQL).unwrap();
        update.execute(params![posting_id]).unwrap();
        (
            update.get_status(rusqlite::StatementStatus::VmStep),
            update.get_status(rusqlite::StatementStatus::FullscanStep),
        )
    }

    #[test]
    fn own_membership_lookup_and_write_ignore_unrelated_same_target_postings() {
        let small = own_membership_lookup_steps(0);
        let large = own_membership_lookup_steps(1_001);
        assert!(large
            .3
            .iter()
            .any(|step| step.contains("reference_postings_source_idx")));
        assert!(large.3.iter().all(|step| !step.starts_with("SCAN r")));
        assert_eq!(small.1, 0);
        assert_eq!(large.1, 0);
        assert_eq!(large.0, small.0, "small={small:?}, large={large:?}");
        assert_eq!(large.2, small.2, "small={small:?}, large={large:?}");
    }

    #[test]
    fn deferred_own_membership_write_ignores_unrelated_same_target_postings() {
        let small = deferred_own_membership_write_steps(0);
        let large = deferred_own_membership_write_steps(1_001);
        assert_eq!(small.1, 0);
        assert_eq!(large.1, 0);
        assert_eq!(large.0, small.0, "small={small:?}, large={large:?}");
    }

    #[test]
    fn query_preorder_rejects_incomplete_or_cyclic_trees() {
        assert!(query_block_preorder([("one", Some("two"), "a")]).is_err());
        assert!(
            query_block_preorder([("one", Some("two"), "a"), ("two", Some("one"), "b")]).is_err()
        );
        assert!(query_block_preorder([("one", None, "a"), ("one", None, "b")]).is_err());
        assert_eq!(
            query_block_preorder([("two", None, "z"), ("one", None, "z")]).unwrap(),
            [(1, 1), (0, 1)]
        );
        assert_eq!(
            query_result_estimated_bytes("", "é", ["tag"], [("key", "value")]),
            36 + 2 + 3 + 3 + 5 + 128
        );
    }

    #[test]
    fn aggregate_read_budget_charges_public_paths_and_ids() {
        let row = PhysicalBlockReferrerCandidateRow {
            source_page_path: "pages/a.md".into(),
            source_block_id: "b".repeat(1024 * 1024),
            page_cursor: 1,
            block_cursor: 2,
        };
        let row_bytes = block_referrer_candidate_row_output_bytes(&row).unwrap();
        let mut budget = MaterializationReadBudget::default();
        for _ in 0..(MAX_MATERIALIZATION_READ_BYTES / row_bytes) {
            budget.add(row_bytes).unwrap();
        }
        assert!(matches!(
            budget.add(row_bytes),
            Err(MaterializationError::ResourceLimit {
                resource: "materialization read output bytes",
                maximum: MAX_MATERIALIZATION_READ_BYTES,
                ..
            })
        ));
    }
}
