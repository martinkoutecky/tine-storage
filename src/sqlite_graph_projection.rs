//! Regime-neutral disposable graph projection.
//!
//! This database owns only parser-derived graph facts and their indexes. It has
//! no oplog frontier, sync role, authority claim, or managed-storage lifecycle.
//! A Direct Files watcher/parser and a managed accepted-event adapter can feed
//! the same page replacement/delete transaction.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, TransactionBehavior};

use crate::sqlite_materialization::{
    self, ApplyChangeInstrumentation, MaterializationError, PhysicalAliasDeclaration,
    PhysicalGraphProjectionChange, PhysicalPagePortablePathClaim, SqliteGraphProjectionRead,
};
const PREPARED_STATEMENT_CACHE_STATEMENTS: usize = 64;
const SOURCE_REVISION_MAX_BYTES: usize = 4096;
const SOURCE_REVISIONS_DDL: &str = "CREATE TABLE direct_source_revisions (
    page_id BLOB PRIMARY KEY CHECK (length(page_id) = 16),
    revision TEXT NOT NULL CHECK (length(CAST(revision AS BLOB)) BETWEEN 1 AND 4096),
    query_metadata_schema INTEGER NOT NULL DEFAULT 29 CHECK (query_metadata_schema = 29),
    FOREIGN KEY (page_id) REFERENCES pages(page_id) ON DELETE CASCADE
) STRICT";

/// Exact application-authority revision for one disposable projection page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalGraphProjectionSourceRevision {
    pub page_id: [u8; 16],
    pub revision: String,
}

/// Page IDs whose physical facts differ from an application's current source.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PhysicalGraphProjectionSourceDelta {
    pub replacements: Vec<[u8; 16]>,
    pub deletions: Vec<[u8; 16]>,
}

/// Connection-owning standalone graph-fact projection.
///
/// The file is a cache. `synchronous=NORMAL` protects SQLite consistency while
/// avoiding authority-grade barriers on every observed file edit; if the cache
/// is missing, stale, or fails validation, the caller rebuilds it from its
/// actual authority.
pub struct PhysicalGraphProjectionDatabase {
    connection: Connection,
    /// Whether the most recent apply built its secondary indexes once at the
    /// end (the fresh-build route) rather than per inserted row.
    last_apply_deferred_indexes: std::cell::Cell<bool>,
}

/// The smallest page-cache ceiling [`PhysicalGraphProjectionDatabase::set_page_cache_budget`]
/// accepts; below SQLite's own ~2 MiB default a budget is a slowdown, never a saving.
pub const MIN_PAGE_CACHE_BUDGET_BYTES: u64 = 2 * 1024 * 1024;

impl PhysicalGraphProjectionDatabase {
    pub fn open_writable(path: &Path) -> Result<Self, MaterializationError> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.set_prepared_statement_cache_capacity(PREPARED_STATEMENT_CACHE_STATEMENTS);
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA trusted_schema = OFF;",
        )?;
        Ok(Self {
            connection,
            last_apply_deferred_indexes: std::cell::Cell::new(false),
        })
    }

    pub fn open_read_only(path: &Path) -> Result<Self, MaterializationError> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_secs(5))?;
        Ok(Self {
            connection,
            last_apply_deferred_indexes: std::cell::Cell::new(false),
        })
    }

    /// Bound this connection's SQLite page cache at `bytes`, rounded down to
    /// whole KiB and never below [`MIN_PAGE_CACHE_BUDGET_BYTES`].
    ///
    /// SQLite allocates cache pages on demand up to the ceiling, so a generous
    /// budget costs a small graph nothing. A ceiling below a bulk build's
    /// working set is what GH #543 measured: at SQLite's ~2 MiB default a
    /// 600,000-block build spilled and re-read every dirty page (8.8M cache
    /// misses, 27 GB read for 49 MB of Markdown); the same build at 512 MiB
    /// took 8,896 misses. The caller owns the formula; this only applies it.
    pub fn set_page_cache_budget(&self, bytes: u64) -> Result<(), MaterializationError> {
        let kib = i64::try_from(bytes.max(MIN_PAGE_CACHE_BUDGET_BYTES) / 1024).map_err(|_| {
            MaterializationError::InvalidInput("page cache budget overflows".into())
        })?;
        self.connection.pragma_update(None, "cache_size", -kib)?;
        Ok(())
    }

    /// Lower the page-cache ceiling to `bytes` and hand the pages above it
    /// back to the allocator now, rather than when they next age out. A bulk
    /// build raises the ceiling for its duration; this is how it returns the
    /// memory afterwards.
    pub fn shrink_page_cache_budget(&self, bytes: u64) -> Result<(), MaterializationError> {
        self.set_page_cache_budget(bytes)?;
        // SAFETY: the handle belongs to this live connection; the call only
        // frees cache pages that hold no pinned content.
        unsafe { rusqlite::ffi::sqlite3_db_release_memory(self.connection.handle()) };
        Ok(())
    }

    /// Relax (`true`) or restore (`false`) this writer's commit durability
    /// for a bulk build: `PRAGMA synchronous = OFF` skips the fsync at each
    /// commit and each WAL checkpoint; `NORMAL` is the ordinary setting
    /// [`open_writable`](Self::open_writable) applies.
    ///
    /// The projection is a disposable cache rebuilt from the graph files, so
    /// the only thing a missing fsync can cost is the build's own progress.
    /// An application crash leaves WAL mode consistent either way; a power
    /// loss can tear the file, which `quick_check` catches at the next open
    /// and the caller rebuilds. A streaming build commits per batch, and at
    /// GH tine#543's 10,000-page graph that is hundreds of commits and
    /// checkpoints whose fsyncs are pure waiting on Windows. The caller
    /// restores durability before the build's last commit is relied on.
    pub fn set_build_durability(&self, relaxed: bool) -> Result<(), MaterializationError> {
        self.connection.pragma_update(
            None,
            "synchronous",
            if relaxed { "OFF" } else { "NORMAL" },
        )?;
        Ok(())
    }

    /// The page-cache ceiling currently in force, in bytes.
    pub fn page_cache_budget(&self) -> Result<u64, MaterializationError> {
        let cache_size: i64 = self
            .connection
            .query_row("PRAGMA cache_size", [], |row| row.get(0))?;
        if cache_size < 0 {
            return Ok(cache_size.unsigned_abs() * 1024);
        }
        let page_size: u64 = self
            .connection
            .query_row("PRAGMA page_size", [], |row| row.get(0))?;
        Ok(cache_size.unsigned_abs() * page_size)
    }

    /// Whether the most recent `apply*` call took the fresh-build route
    /// (secondary indexes built once after the rows) rather than maintaining
    /// them per row.
    pub fn last_apply_deferred_indexes(&self) -> bool {
        self.last_apply_deferred_indexes.get()
    }

    pub fn initialize_schema(&self) -> Result<(), MaterializationError> {
        sqlite_materialization::initialize_graph_projection_schema(&self.connection)?;
        self.connection
            .execute_batch(&format!("{SOURCE_REVISIONS_DDL};"))?;
        Ok(())
    }

    pub fn validate_schema(&self) -> Result<(), MaterializationError> {
        sqlite_materialization::validate_graph_projection_schema(&self.connection)?;
        let mut statement = self
            .connection
            .prepare("PRAGMA table_info(direct_source_revisions)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        if columns != ["page_id", "revision", "query_metadata_schema"] {
            return Err(MaterializationError::Schema(format!(
                "direct_source_revisions columns {columns:?} != [page_id, revision, query_metadata_schema]"
            )));
        }
        let schema_sql: String = self.connection.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name='direct_source_revisions'",
            [],
            |row| row.get(0),
        )?;
        if schema_sql.split_ascii_whitespace().collect::<Vec<_>>()
            != SOURCE_REVISIONS_DDL
                .split_ascii_whitespace()
                .collect::<Vec<_>>()
        {
            return Err(MaterializationError::Schema(
                "direct source revision metadata schema differs".into(),
            ));
        }
        Ok(())
    }

    pub fn quick_check(&self) -> Result<(), MaterializationError> {
        let result: String = self
            .connection
            .query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
        if result != "ok" {
            return Err(MaterializationError::Corrupt(format!(
                "SQLite graph projection quick_check failed: {result}"
            )));
        }
        Ok(())
    }

    pub fn apply(
        &mut self,
        change: &PhysicalGraphProjectionChange,
    ) -> Result<ApplyChangeInstrumentation, MaterializationError> {
        self.apply_with_aliases(change, &[])
    }

    /// Apply physical page/reference facts and parser-derived aliases in one
    /// SQLite transaction. Existing callers that do not project aliases may
    /// continue to use [`Self::apply`].
    pub(crate) fn apply_with_aliases(
        &mut self,
        change: &PhysicalGraphProjectionChange,
        aliases: &[PhysicalAliasDeclaration],
    ) -> Result<ApplyChangeInstrumentation, MaterializationError> {
        self.apply_inner(change, None, aliases, None, None)
    }

    /// Apply page/reference facts, exact source revisions, and aliases in one
    /// transaction. This additive API keeps `PhysicalGraphProjectionChange`
    /// source-compatible with tine-storage 0.6.0.
    pub fn apply_with_source_revisions_and_aliases(
        &mut self,
        change: &PhysicalGraphProjectionChange,
        revisions: &[PhysicalGraphProjectionSourceRevision],
        aliases: &[PhysicalAliasDeclaration],
    ) -> Result<ApplyChangeInstrumentation, MaterializationError> {
        self.apply_inner(change, Some(revisions), aliases, None, None)
    }

    /// Reconcile a complete ordered Direct inventory in the same page/source
    /// transaction. Unchanged order is not rewritten. Unchanged page facts need
    /// not appear in `change.replacements`. This keeps warm reopen independent
    /// of document parsing and page reconstruction.
    pub fn apply_with_source_revisions_aliases_and_page_order(
        &mut self,
        change: &PhysicalGraphProjectionChange,
        revisions: &[PhysicalGraphProjectionSourceRevision],
        aliases: &[PhysicalAliasDeclaration],
        page_order: &[[u8; 16]],
    ) -> Result<ApplyChangeInstrumentation, MaterializationError> {
        self.apply_inner(change, Some(revisions), aliases, None, Some(page_order))
    }

    fn apply_inner(
        &mut self,
        change: &PhysicalGraphProjectionChange,
        revisions: Option<&[PhysicalGraphProjectionSourceRevision]>,
        aliases: &[PhysicalAliasDeclaration],
        portable_paths: Option<&[PhysicalPagePortablePathClaim]>,
        page_order: Option<&[[u8; 16]]>,
    ) -> Result<ApplyChangeInstrumentation, MaterializationError> {
        let replacement_ids = change
            .replacements
            .iter()
            .map(|page| page.page_id)
            .collect::<BTreeSet<_>>();
        if let Some(revisions) = revisions {
            let revision_ids = validated_source_revisions(revisions)?
                .into_keys()
                .collect::<BTreeSet<_>>();
            if replacement_ids != revision_ids {
                return Err(MaterializationError::InvalidInput(
                    "source revisions must exactly cover replacement pages".into(),
                ));
            }
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(order) = page_order {
            let positions = order
                .iter()
                .enumerate()
                .map(|(position, id)| (*id, position as u64))
                .collect::<BTreeMap<_, _>>();
            if positions.len() != order.len() {
                return Err(MaterializationError::InvalidInput(
                    "duplicate page in query inventory".into(),
                ));
            }
            for page in &change.replacements {
                if let Some(position) = page.query_page_order {
                    if positions.get(&page.page_id) != Some(&position) {
                        return Err(MaterializationError::InvalidInput(
                            "page order differs from complete inventory".into(),
                        ));
                    }
                }
            }
            // A changed inventory may permute occupied positions. Clear only
            // its small order table before page writes; the final reconciliation
            // below restores the complete order within this same transaction.
            if !change.replacements.is_empty() || !change.deletions.is_empty() {
                transaction.execute("DELETE FROM query_page_order", [])?;
            }
        }
        // Fresh-build route (GH #543): into an empty projection, every row of
        // every secondary index lands on a random B-tree leaf (the keys are
        // UUIDs), so a graph-sized build touches the whole index set per
        // page and, past the page-cache ceiling, spills and re-reads it.
        // Building the indexes once after the rows is an external sort
        // instead. Readers on this WAL file see the old snapshot until the
        // commit, and a rollback restores the indexes, so the route is
        // invisible outside this transaction. It is taken only while the
        // covered tables are empty: without indexes the per-page cleanup
        // lookups below are full scans, free here and quadratic otherwise.
        let deferred_indexes = !change.replacements.is_empty()
            && sqlite_materialization::deferred_index_tables_are_empty(&transaction)?;
        if deferred_indexes {
            sqlite_materialization::drop_deferred_indexes(&transaction)?;
        }
        let instrumentation = sqlite_materialization::apply_graph_projection_rows(
            &transaction,
            &change.replacements,
            &change.deletions,
            None,
            None,
        )?;
        sqlite_materialization::replace_graph_projection_reference_facts(
            &transaction,
            change,
            aliases,
        )?;
        if let Some(portable_paths) = portable_paths {
            sqlite_materialization::replace_graph_projection_portable_path_claims(
                &transaction,
                &change.replacements,
                portable_paths,
            )?;
        }
        for page_id in &change.deletions {
            transaction.execute(
                "DELETE FROM direct_source_revisions WHERE page_id = ?1",
                rusqlite::params![page_id.as_slice()],
            )?;
        }
        match revisions {
            Some(revisions) => {
                for revision in revisions {
                    transaction.execute(
                        "INSERT INTO direct_source_revisions (page_id, revision)
                         VALUES (?1, ?2)
                         ON CONFLICT(page_id) DO UPDATE SET revision = excluded.revision",
                        rusqlite::params![revision.page_id.as_slice(), &revision.revision],
                    )?;
                }
            }
            None => {
                for page in &change.replacements {
                    transaction.execute(
                        "DELETE FROM direct_source_revisions WHERE page_id = ?1",
                        rusqlite::params![page.page_id.as_slice()],
                    )?;
                }
            }
        }
        if let Some(order) = page_order {
            reconcile_query_page_order(&transaction, order)?;
        }
        if deferred_indexes {
            sqlite_materialization::create_deferred_indexes(&transaction)?;
        }
        transaction.commit()?;
        self.last_apply_deferred_indexes.set(deferred_indexes);
        Ok(instrumentation)
    }

    /// Compare caller authority revisions to the persisted disposable facts.
    /// Missing metadata is stale, never authoritative.
    pub fn source_delta(
        &self,
        current: &[PhysicalGraphProjectionSourceRevision],
    ) -> Result<PhysicalGraphProjectionSourceDelta, MaterializationError> {
        let current = validated_source_revisions(current)?;
        let mut existing = BTreeMap::<[u8; 16], Option<String>>::new();
        let mut statement = self.connection.prepare(
            "SELECT p.page_id, s.revision
             FROM pages AS p
             LEFT JOIN direct_source_revisions AS s ON s.page_id = p.page_id
             ORDER BY p.page_id",
        )?;
        for row in statement.query_map([], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Option<String>>(1)?))
        })? {
            let (page_id, revision) = row?;
            let page_id: [u8; 16] = page_id.try_into().map_err(|_| {
                MaterializationError::Corrupt("stored page ID is not 16 bytes".into())
            })?;
            existing.insert(page_id, revision);
        }
        let replacements = current
            .iter()
            .filter_map(|(page_id, revision)| {
                (existing.get(page_id).and_then(Option::as_ref) != Some(revision))
                    .then_some(*page_id)
            })
            .collect();
        let deletions = existing
            .keys()
            .filter(|page_id| !current.contains_key(*page_id))
            .copied()
            .collect();
        Ok(PhysicalGraphProjectionSourceDelta {
            replacements,
            deletions,
        })
    }

    pub fn reset(&mut self) -> Result<(), MaterializationError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        sqlite_materialization::reset_graph_projection_rows(&transaction)?;
        transaction.execute("DELETE FROM direct_source_revisions", [])?;
        transaction.commit()?;
        Ok(())
    }

    pub fn read(&self) -> SqliteGraphProjectionRead<'_> {
        SqliteGraphProjectionRead::new(&self.connection)
    }

    pub fn checkpoint_truncate(&self) -> Result<(), MaterializationError> {
        self.connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        Ok(())
    }
}

fn reconcile_query_page_order(
    connection: &Connection,
    order: &[[u8; 16]],
) -> Result<(), MaterializationError> {
    let expected = order.iter().copied().collect::<BTreeSet<_>>();
    let decode_id = |row: &rusqlite::Row<'_>| -> rusqlite::Result<[u8; 16]> {
        row.get::<_, Vec<u8>>(0)?
            .try_into()
            .map_err(|_| rusqlite::Error::InvalidQuery)
    };
    let actual = connection
        .prepare("SELECT page_id FROM pages")?
        .query_map([], decode_id)?
        .collect::<Result<BTreeSet<_>, _>>()?;
    if expected.len() != order.len() || expected != actual {
        return Err(MaterializationError::InvalidInput(
            "query inventory must exactly cover projected pages".into(),
        ));
    }
    let existing = connection
        .prepare("SELECT page_id, position FROM query_page_order ORDER BY position")?
        .query_map([], |row| Ok((decode_id(row)?, row.get::<_, u64>(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    if existing.len() == order.len()
        && existing.iter().zip(order).enumerate().all(
            |(position, ((found_id, found_position), expected_id))| {
                found_id == expected_id && *found_position == position as u64
            },
        )
    {
        return Ok(());
    }
    connection.execute("DELETE FROM query_page_order", [])?;
    let mut insert = connection
        .prepare_cached("INSERT INTO query_page_order (page_id, position) VALUES (?1, ?2)")?;
    for (position, id) in order.iter().enumerate() {
        insert.execute(rusqlite::params![id.as_slice(), position as i64])?;
    }
    Ok(())
}

fn validated_source_revisions(
    revisions: &[PhysicalGraphProjectionSourceRevision],
) -> Result<BTreeMap<[u8; 16], String>, MaterializationError> {
    let mut validated = BTreeMap::new();
    for revision in revisions {
        if revision.revision.is_empty() || revision.revision.len() > SOURCE_REVISION_MAX_BYTES {
            return Err(MaterializationError::InvalidInput(
                "source revision must contain 1..=4096 bytes".into(),
            ));
        }
        if validated
            .insert(revision.page_id, revision.revision.clone())
            .is_some()
        {
            return Err(MaterializationError::InvalidInput(
                "source revisions contain a duplicate page ID".into(),
            ));
        }
    }
    Ok(validated)
}

/// One bound parameter, or one returned column.
///
/// The seam takes VALUES, never a formatted fragment, so
/// "parameters are bound, never interpolated" holds by SIGNATURE rather than by
/// the caller's discipline: there is no way to spell an interpolated statement
/// through this API.
#[derive(Clone, Debug, PartialEq)]
pub enum PhysicalQueryValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl rusqlite::ToSql for PhysicalQueryValue {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, Value, ValueRef};
        Ok(match self {
            PhysicalQueryValue::Null => ToSqlOutput::Borrowed(ValueRef::Null),
            PhysicalQueryValue::Integer(value) => ToSqlOutput::Owned(Value::Integer(*value)),
            PhysicalQueryValue::Real(value) => ToSqlOutput::Owned(Value::Real(*value)),
            PhysicalQueryValue::Text(value) => {
                ToSqlOutput::Borrowed(ValueRef::Text(value.as_ref()))
            }
            PhysicalQueryValue::Blob(value) => ToSqlOutput::Borrowed(ValueRef::Blob(value)),
        })
    }
}

/// The read-only statement seam over the graph projection.
///
/// **Raw SQL crosses this boundary; authority does not.** The projection is a
/// disposable cache derived from the oplog, so a malformed statement fails a
/// read and can never corrupt truth — which is exactly why the projection may
/// have a statement seam and the oplog, the frontier and the Markdown/Org tree
/// may not.
///
/// The restriction is the ENGINE's, not a validator's: this type owns a
/// connection opened `SQLITE_OPEN_READ_ONLY`, and there is no constructor that
/// takes an existing writable handle, so a caller cannot reach it from one.
/// Deliberately there is **no** SQL-text parser or "single SELECT only" check:
/// SQLite already refuses every write through a read-only connection, and a
/// redundant text check would be a runtime refusal with no in-scope failure to
/// name — a future availability bug — that could also reject a legitimate
/// statement.
pub struct PhysicalProjectionQueryReader {
    connection: Connection,
}

impl PhysicalProjectionQueryReader {
    /// Open the projection read-only. The file must already exist; this seam
    /// never creates or upgrades one.
    pub fn open(path: &Path) -> Result<Self, MaterializationError> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.set_prepared_statement_cache_capacity(PREPARED_STATEMENT_CACHE_STATEMENTS);
        connection.execute_batch("PRAGMA trusted_schema = OFF;")?;
        Ok(Self { connection })
    }

    /// Install the fixed query regex predicate over a caller-owned immutable
    /// compiled-regex registry. IDs are bound values, not SQL or regex source.
    /// The application retains its existing regex compiler and semantics.
    pub fn set_query_regex_predicate(
        &self,
        predicate: impl Fn(u64, &str) -> Result<bool, MaterializationError> + Send + 'static,
    ) -> Result<(), MaterializationError> {
        self.connection.create_scalar_function(
            "tine_query_regex",
            2,
            rusqlite::functions::FunctionFlags::SQLITE_UTF8
                | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC
                | rusqlite::functions::FunctionFlags::SQLITE_DIRECTONLY,
            move |context| {
                let id = context.get::<u64>(0)?;
                let text = context.get_raw(1).as_str()?;
                predicate(id, text)
                    .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))
            },
        )?;
        Ok(())
    }

    /// Install a fixed rank function over caller-owned immutable query programs.
    /// `None` maps to SQL NULL (no match); bytes are a lexicographically ordered
    /// BLOB key. The application owns matching and rank encoding, so storage
    /// neither compiles a query grammar nor compresses tuples into lossy scores.
    pub fn set_query_rank_function(
        &self,
        rank: impl Fn(u64, &str) -> Result<Option<Vec<u8>>, MaterializationError> + Send + 'static,
    ) -> Result<(), MaterializationError> {
        self.connection.create_scalar_function(
            "tine_query_rank",
            2,
            rusqlite::functions::FunctionFlags::SQLITE_UTF8
                | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC
                | rusqlite::functions::FunctionFlags::SQLITE_DIRECTONLY,
            move |context| {
                let id = context.get::<u64>(0)?;
                let text = context.get_raw(1).as_str()?;
                rank(id, text).map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))
            },
        )?;
        Ok(())
    }

    /// Run one statement with bound parameters and collect its rows.
    pub fn run_projection_query(
        &self,
        sql: &str,
        parameters: &[PhysicalQueryValue],
    ) -> Result<Vec<Vec<PhysicalQueryValue>>, MaterializationError> {
        let mut rows = Vec::new();
        self.visit_projection_query(sql, parameters, |row| {
            rows.push(row.to_vec());
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(rows)
    }

    /// Visit one row at a time without retaining the complete result set.
    /// A clean break finalizes the statement; a visitor error propagates.
    pub fn visit_projection_query(
        &self,
        sql: &str,
        parameters: &[PhysicalQueryValue],
        mut visitor: impl FnMut(&[PhysicalQueryValue]) -> Result<ControlFlow<()>, MaterializationError>,
    ) -> Result<(), MaterializationError> {
        let mut statement = self.connection.prepare_cached(sql)?;
        let columns = statement.column_count();
        let bound: Vec<&dyn rusqlite::ToSql> = parameters
            .iter()
            .map(|value| value as &dyn rusqlite::ToSql)
            .collect();
        let mut rows = statement.query(bound.as_slice())?;
        while let Some(row) = rows.next()? {
            let values = (0..columns)
                .map(|index| {
                    Ok(match row.get_ref(index)? {
                        rusqlite::types::ValueRef::Null => PhysicalQueryValue::Null,
                        rusqlite::types::ValueRef::Integer(value) => {
                            PhysicalQueryValue::Integer(value)
                        }
                        rusqlite::types::ValueRef::Real(value) => PhysicalQueryValue::Real(value),
                        rusqlite::types::ValueRef::Text(value) => PhysicalQueryValue::Text(
                            String::from_utf8(value.to_vec()).map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    index,
                                    rusqlite::types::Type::Text,
                                    Box::new(error),
                                )
                            })?,
                        ),
                        rusqlite::types::ValueRef::Blob(value) => {
                            PhysicalQueryValue::Blob(value.to_vec())
                        }
                    })
                })
                .collect::<Result<Vec<_>, rusqlite::Error>>()?;
            if visitor(&values)?.is_break() {
                break;
            }
        }
        Ok(())
    }

    /// `EXPLAIN QUERY PLAN` for one statement, one `detail` string per step.
    ///
    /// This exists so the campaign's plan gate can be an ordinary repository
    /// test instead of a scratch harness: a bounded query must show `SEARCH`
    /// on its anchor table and never `SCAN`.
    ///
    /// It takes the same parameters as the query it explains, and binds them,
    /// because with `ANALYZE`/`sqlite_stat4` present the planner may choose a
    /// different plan for a bound value than for an unbound one. An explain
    /// that left them unbound would measure a statement the caller never runs.
    pub fn explain_query_plan(
        &self,
        sql: &str,
        parameters: &[PhysicalQueryValue],
    ) -> Result<Vec<String>, MaterializationError> {
        let mut statement = self
            .connection
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
        let bound: Vec<&dyn rusqlite::ToSql> = parameters
            .iter()
            .map(|value| value as &dyn rusqlite::ToSql)
            .collect();
        let details = statement
            .query_map(bound.as_slice(), |row| row.get::<_, String>(3))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(details)
    }
}

/// Cancellation for one owned read snapshot. Cancellation is sticky, including
/// between SQL statements. The owner must finish/drop the snapshot after its
/// worker observes cancellation; callers drain workers before replacing files.
#[derive(Clone)]
pub struct PhysicalProjectionQueryCancellation {
    cancelled: Arc<AtomicBool>,
    interrupt: Arc<rusqlite::InterruptHandle>,
}

impl PhysicalProjectionQueryCancellation {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.interrupt.interrupt();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// An owned, pinned read transaction. It can move to a query worker without
/// borrowing an actor's connection. No writable connection enters this API.
/// Any query/visitor error closes the snapshot; successful consumers call
/// `finish` or drop it when descriptor and payload reads are complete.
pub struct PhysicalProjectionQuerySnapshot {
    reader: Option<PhysicalProjectionQueryReader>,
    cancellation: PhysicalProjectionQueryCancellation,
}

impl PhysicalProjectionQuerySnapshot {
    fn begin(path: &Path) -> Result<Self, MaterializationError> {
        let reader = PhysicalProjectionQueryReader::open(path)?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let progress_cancelled = Arc::clone(&cancelled);
        reader.connection.progress_handler(
            1000,
            Some(move || progress_cancelled.load(Ordering::Acquire)),
        );
        let cancellation = PhysicalProjectionQueryCancellation {
            cancelled,
            interrupt: Arc::new(reader.connection.get_interrupt_handle()),
        };
        reader.connection.execute_batch("BEGIN DEFERRED")?;
        Ok(Self {
            reader: Some(reader),
            cancellation,
        })
    }

    /// The owner checks its captured projection instance and ready generation
    /// before and after SQLite establishes the read snapshot. Later ordinary
    /// edits do not re-run this acquisition validator.
    pub fn open_direct(
        path: &Path,
        mut validate: impl FnMut() -> Result<(), MaterializationError>,
    ) -> Result<Self, MaterializationError> {
        validate()?;
        let snapshot = Self::begin(path)?;
        // BEGIN DEFERRED alone does not establish a read snapshot. A real read
        // (even an empty schema inventory) pins it before the second guard.
        snapshot
            .reader
            .as_ref()
            .expect("new snapshot")
            .run_projection_query("SELECT rootpage FROM sqlite_schema LIMIT 1", &[])?;
        validate()?;
        Ok(snapshot)
    }

    pub fn cancellation(&self) -> PhysicalProjectionQueryCancellation {
        self.cancellation.clone()
    }

    /// Local projection image revision, read in this snapshot's transaction.
    /// Pair with the owner's projection-instance identity; it is not a saved
    /// revision target or an authority frontier, and is reset by a fresh file.
    pub fn query_revision(&mut self) -> Result<u64, MaterializationError> {
        self.read(|reader| sqlite_materialization::query_projection_revision(&reader.connection))
    }

    /// Install compiled-regex ID lookup on this snapshot's read-only connection.
    pub fn set_query_regex_predicate(
        &mut self,
        predicate: impl Fn(u64, &str) -> Result<bool, MaterializationError> + Send + 'static,
    ) -> Result<(), MaterializationError> {
        self.read(|reader| reader.set_query_regex_predicate(predicate))
    }

    /// Install the fixed rank function on this snapshot's read-only connection.
    pub fn set_query_rank_function(
        &mut self,
        rank: impl Fn(u64, &str) -> Result<Option<Vec<u8>>, MaterializationError> + Send + 'static,
    ) -> Result<(), MaterializationError> {
        self.read(|reader| reader.set_query_rank_function(rank))
    }

    fn read<T>(
        &mut self,
        operation: impl FnOnce(&PhysicalProjectionQueryReader) -> Result<T, MaterializationError>,
    ) -> Result<T, MaterializationError> {
        let result = if self.cancellation.is_cancelled() {
            Err(MaterializationError::Incomplete(
                "query snapshot cancelled".into(),
            ))
        } else if let Some(reader) = self.reader.as_ref() {
            operation(reader)
        } else {
            Err(MaterializationError::Incomplete(
                "query snapshot closed".into(),
            ))
        };
        // Also catch cancellation during a short statement or its row visitor,
        // where SQLite may not execute enough VM steps to call the progress hook.
        let result = if self.cancellation.is_cancelled() {
            Err(MaterializationError::Incomplete(
                "query snapshot cancelled".into(),
            ))
        } else {
            result
        };
        if result.is_err() {
            self.reader.take();
        }
        result
    }

    pub fn run_projection_query(
        &mut self,
        sql: &str,
        parameters: &[PhysicalQueryValue],
    ) -> Result<Vec<Vec<PhysicalQueryValue>>, MaterializationError> {
        self.read(|reader| reader.run_projection_query(sql, parameters))
    }

    pub fn visit_projection_query(
        &mut self,
        sql: &str,
        parameters: &[PhysicalQueryValue],
        mut visitor: impl FnMut(&[PhysicalQueryValue]) -> Result<ControlFlow<()>, MaterializationError>,
    ) -> Result<(), MaterializationError> {
        let cancellation = self.cancellation.clone();
        self.read(|reader| {
            reader.visit_projection_query(sql, parameters, |row| {
                if cancellation.is_cancelled() {
                    return Err(MaterializationError::Incomplete(
                        "query snapshot cancelled".into(),
                    ));
                }
                visitor(row)
            })
        })
    }

    pub fn explain_query_plan(
        &mut self,
        sql: &str,
        parameters: &[PhysicalQueryValue],
    ) -> Result<Vec<String>, MaterializationError> {
        self.read(|reader| reader.explain_query_plan(sql, parameters))
    }

    /// Release this transaction now. Dropping the snapshot has the same effect.
    pub fn finish(self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite_materialization::{
        PhysicalNavigationReferenceNameRow, NAVIGATION_ALIASES_AFTER_SQL,
        NAVIGATION_ALIASES_FIRST_SQL, NAVIGATION_REFERENCE_NAMES_AFTER_SQL,
        NAVIGATION_REFERENCE_NAMES_FIRST_SQL,
    };

    use crate::sqlite_materialization::{
        PhysicalAliasDeclaration, PhysicalBlock, PhysicalEntityId, PhysicalPage, PhysicalPlanning,
        PhysicalReferencePosting, PhysicalReferenceTarget, PhysicalTask,
    };

    /// The GH #543 fixture shape: `pages` pages of 60 blocks, each block
    /// carrying one page link and one tag, every identity a random UUID
    /// (as Direct Files derives them), so every index insert is a random
    /// B-tree leaf exactly as in the reporter-scale build.
    fn gh543_snapshot(
        pages: usize,
    ) -> (
        PhysicalGraphProjectionChange,
        Vec<PhysicalGraphProjectionSourceRevision>,
        Vec<[u8; 16]>,
    ) {
        use crate::sqlite_materialization::{PhysicalReference, PhysicalTag};
        let page_ids = (0..pages)
            .map(|_| *uuid::Uuid::new_v4().as_bytes())
            .collect::<Vec<_>>();
        let mut replacements = Vec::with_capacity(pages);
        let mut postings = Vec::with_capacity(pages * 120);
        for (position, page_id) in page_ids.iter().enumerate() {
            let target = (position + 1) % pages;
            let target_name = format!("Topic {target} 你好");
            let normalized_target = target_name.to_lowercase();
            let mut blocks = Vec::with_capacity(60);
            for block in 0..60 {
                let block_id = *uuid::Uuid::new_v4().as_bytes();
                let tag = format!("tag{}", block % 10);
                let content = format!(
                    "outline sentinel543 你好世界 page {position} block {block} [[{target_name}]] #{tag}"
                );
                for (ordinal, (raw, normalized)) in
                    [(&target_name, &normalized_target), (&tag, &tag)]
                        .into_iter()
                        .enumerate()
                {
                    postings.push(PhysicalReferencePosting {
                        source_page_id: *page_id,
                        source_entity: PhysicalEntityId::Block(block_id),
                        source_locator: b"content".to_vec(),
                        ordinal: ordinal as u32,
                        kind: 0,
                        target: PhysicalReferenceTarget::PageName {
                            raw_name: raw.clone(),
                            normalized_name: normalized.clone(),
                            resolved_page_id: (ordinal == 0).then_some(page_ids[target]),
                        },
                    });
                }
                blocks.push(PhysicalBlock {
                    block_id,
                    query_result_id: uuid::Uuid::from_bytes(block_id).to_string(),
                    own_refs: vec![normalized_target.clone(), tag.clone()],
                    home_document_id: *page_id,
                    parent: None,
                    order: format!("{block:04}"),
                    content: content.clone(),
                    searchable_text: content.clone(),
                    normalized_searchable_text: content.to_lowercase(),
                    query_visible: content.clone(),
                    query_visible_folded: content.to_lowercase(),
                    heading_level: None,
                    collapsed: false,
                    logseq_uuid: None,
                    logseq_identity_origin: None,
                    references: vec![PhysicalReference {
                        target: PhysicalEntityId::Page(page_ids[target]),
                        kind: 0,
                    }],
                    properties: Vec::new(),
                    tags: vec![PhysicalTag {
                        tag: tag.clone(),
                        tag_key: tag.clone(),
                    }],
                    task: None,
                    planning: None,
                    path_refs: vec![normalized_target.clone(), tag],
                    property_atoms: Vec::new(),
                });
            }
            let name = format!("Topic {position} 你好");
            replacements.push(PhysicalPage {
                page_id: *page_id,
                query_page_order: Some(position as u64),
                home_document_id: *page_id,
                name_key: name.to_lowercase(),
                path: format!("pages/主题-{position:05}.md"),
                name,
                text_kind: 0,
                journal_day: None,
                preamble: None,
                searchable_text: String::new(),
                normalized_searchable_text: String::new(),
                references: Vec::new(),
                properties: Vec::new(),
                tags: Vec::new(),
                property_atoms: Vec::new(),
                blocks,
            });
        }
        let revisions = page_ids
            .iter()
            .map(|page_id| PhysicalGraphProjectionSourceRevision {
                page_id: *page_id,
                revision: "probe".into(),
            })
            .collect();
        (
            PhysicalGraphProjectionChange {
                replacements,
                deletions: Vec::new(),
                reference_postings: postings,
            },
            revisions,
            page_ids,
        )
    }

    fn db_status(connection: &Connection, op: std::ffi::c_int) -> i64 {
        let mut current: std::ffi::c_int = 0;
        let mut highwater: std::ffi::c_int = 0;
        // SAFETY: the handle outlives this call and both out-pointers are
        // valid for the duration; `reset = 0` leaves the counters untouched.
        let rc = unsafe {
            rusqlite::ffi::sqlite3_db_status(
                connection.handle(),
                op,
                &mut current,
                &mut highwater,
                0,
            )
        };
        assert_eq!(rc, rusqlite::ffi::SQLITE_OK);
        i64::from(current)
    }

    fn secondary_index_count(connection: &Connection) -> i64 {
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema
                 WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%'",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    /// GH #543: an apply into an empty projection builds the 36 secondary
    /// indexes once after its rows; the committed schema is the fresh schema
    /// byte for byte, and the next apply into the populated projection keeps
    /// every index live. A rollback restores the indexes too.
    #[test]
    fn fresh_build_defers_secondary_indexes_and_restores_the_exact_schema() {
        let path = std::env::temp_dir().join(format!(
            "tine-gh543-deferred-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        let fresh_indexes = secondary_index_count(&database.connection);
        assert_eq!(fresh_indexes, 36);
        let (change, revisions, order) = gh543_snapshot(3);

        database
            .apply_with_source_revisions_aliases_and_page_order(&change, &revisions, &[], &order)
            .unwrap();
        assert!(database.last_apply_deferred_indexes());
        assert_eq!(secondary_index_count(&database.connection), fresh_indexes);
        database.validate_schema().unwrap();
        let blocks: i64 = database
            .connection
            .query_row("SELECT COUNT(*) FROM blocks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(blocks, 180);

        // A populated projection keeps its indexes through an apply.
        let edit = PhysicalGraphProjectionChange {
            replacements: vec![change.replacements[0].clone()],
            deletions: Vec::new(),
            reference_postings: change
                .reference_postings
                .iter()
                .filter(|posting| posting.source_page_id == change.replacements[0].page_id)
                .cloned()
                .collect(),
        };
        database
            .apply_with_source_revisions_and_aliases(&edit, &revisions[..1], &[])
            .unwrap();
        assert!(!database.last_apply_deferred_indexes());
        database.validate_schema().unwrap();

        // A fresh build that fails mid-transaction leaves the indexes in place.
        database.reset().unwrap();
        let mut broken = change.clone();
        broken.reference_postings.push(PhysicalReferencePosting {
            source_page_id: [0xEE; 16],
            source_entity: PhysicalEntityId::Page([0xEE; 16]),
            source_locator: b"nowhere".to_vec(),
            ordinal: 0,
            kind: 0,
            target: PhysicalReferenceTarget::PageName {
                raw_name: "x".into(),
                normalized_name: "x".into(),
                resolved_page_id: None,
            },
        });
        database
            .apply_with_source_revisions_aliases_and_page_order(&broken, &revisions, &[], &order)
            .unwrap_err();
        assert_eq!(secondary_index_count(&database.connection), fresh_indexes);
        database.validate_schema().unwrap();
        drop(database);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    #[test]
    fn page_cache_budget_is_applied_floored_and_shrunk() {
        let path =
            std::env::temp_dir().join(format!("tine-gh543-budget-{}.sqlite", uuid::Uuid::new_v4()));
        let database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        let default_budget = database.page_cache_budget().unwrap();
        assert!(default_budget <= MIN_PAGE_CACHE_BUDGET_BYTES);
        database.set_page_cache_budget(300 * 1024 * 1024).unwrap();
        assert_eq!(database.page_cache_budget().unwrap(), 300 * 1024 * 1024);
        database.set_page_cache_budget(1).unwrap();
        assert_eq!(
            database.page_cache_budget().unwrap(),
            MIN_PAGE_CACHE_BUDGET_BYTES
        );
        database.shrink_page_cache_budget(8 * 1024 * 1024).unwrap();
        assert_eq!(database.page_cache_budget().unwrap(), 8 * 1024 * 1024);
        drop(database);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    /// GH #543 build-cost probe. Not a gate: it returns immediately unless
    /// `TINE_STORAGE_BUILD_PROBE` names the matrix, e.g.
    /// `TINE_STORAGE_BUILD_PROBE="pages=1000;cache_mib=2,64,512;defer=0,1"`,
    /// run as `cargo test --release -p tine-storage gh543_build_probe -- --nocapture`.
    /// `defer=0` pre-seeds one page so the apply takes the ordinary
    /// indexes-live route; `defer=1` applies into the empty projection.
    #[test]
    fn gh543_build_probe() {
        let Ok(spec) = std::env::var("TINE_STORAGE_BUILD_PROBE") else {
            return;
        };
        let axis = |key: &str, default: &str| -> Vec<u64> {
            spec.split(';')
                .find_map(|part| part.strip_prefix(&format!("{key}=")))
                .unwrap_or(default)
                .split(',')
                .map(|value| value.trim().parse::<u64>().unwrap())
                .collect()
        };
        for pages in axis("pages", "1000") {
            let (change, revisions, order) = gh543_snapshot(pages as usize);
            let (seed_change, seed_revisions, seed_ids) = gh543_snapshot(1);
            for cache_mib in axis("cache_mib", "2") {
                for defer in axis("defer", "1") {
                    let path = std::env::temp_dir()
                        .join(format!("tine-gh543-probe-{}.sqlite", uuid::Uuid::new_v4()));
                    let mut database =
                        PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
                    database.initialize_schema().unwrap();
                    database
                        .set_page_cache_budget(cache_mib * 1024 * 1024)
                        .unwrap();
                    // The seed page stays projected, so the ordinary route's
                    // inventory must list it too (after the probe pages, whose
                    // own positions are 0..pages).
                    let order = if defer == 0 {
                        database
                            .apply_with_source_revisions_and_aliases(
                                &seed_change,
                                &seed_revisions,
                                &[],
                            )
                            .unwrap();
                        assert!(database.last_apply_deferred_indexes());
                        let mut with_seed = order.clone();
                        with_seed.push(seed_ids[0]);
                        with_seed
                    } else {
                        order.clone()
                    };
                    let started = std::time::Instant::now();
                    database
                        .apply_with_source_revisions_aliases_and_page_order(
                            &change,
                            &revisions,
                            &[],
                            &order,
                        )
                        .unwrap();
                    let elapsed = started.elapsed();
                    assert_eq!(database.last_apply_deferred_indexes(), defer == 1);
                    let misses = db_status(
                        &database.connection,
                        rusqlite::ffi::SQLITE_DBSTATUS_CACHE_MISS,
                    );
                    let writes = db_status(
                        &database.connection,
                        rusqlite::ffi::SQLITE_DBSTATUS_CACHE_WRITE,
                    );
                    let spills = db_status(
                        &database.connection,
                        rusqlite::ffi::SQLITE_DBSTATUS_CACHE_SPILL,
                    );
                    let used = db_status(
                        &database.connection,
                        rusqlite::ffi::SQLITE_DBSTATUS_CACHE_USED,
                    );
                    let size = |suffix: &str| {
                        std::fs::metadata(format!("{}{suffix}", path.display()))
                            .map(|m| m.len())
                            .unwrap_or(0)
                    };
                    let wal_bytes = size("-wal");
                    let checkpoint_started = std::time::Instant::now();
                    database.checkpoint_truncate().unwrap();
                    let checkpoint = checkpoint_started.elapsed();
                    database.validate_schema().unwrap();
                    let block_count: i64 = database
                        .connection
                        .query_row("SELECT COUNT(*) FROM blocks", [], |row| row.get(0))
                        .unwrap();
                    assert_eq!(block_count, pages as i64 * 60 + i64::from(defer == 0) * 60);
                    println!(
                        "GH543PROBE pages={pages} cache_mib={cache_mib} defer={defer} \
                         apply_ms={} checkpoint_ms={} cache_miss={misses} cache_write={writes} \
                         cache_spill={spills} cache_used_bytes={used} db_bytes={} wal_bytes_before_checkpoint={wal_bytes}",
                        elapsed.as_millis(),
                        checkpoint.as_millis(),
                        size(""),
                    );
                    drop(database);
                    for suffix in ["", "-wal", "-shm"] {
                        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
                    }
                }
            }
        }
    }

    struct SnapshotFixture {
        writer: Connection,
        path: std::path::PathBuf,
    }

    impl SnapshotFixture {
        fn new() -> Self {
            let path = std::env::temp_dir()
                .join(format!("tine-owned-read-{}.sqlite", uuid::Uuid::new_v4()));
            let writer = Connection::open(&path).unwrap();
            writer.busy_timeout(Duration::ZERO).unwrap();
            writer
                .execute_batch(
                    "PRAGMA journal_mode=WAL;
                CREATE TABLE payload (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
                INSERT INTO payload VALUES (1, 'before');
                CREATE TABLE materialization_stamp (singleton INTEGER PRIMARY KEY,
                    acceptance_sequence INTEGER, frontier_root_digest BLOB);
                INSERT INTO materialization_stamp VALUES (1, 7, zeroblob(32));",
                )
                .unwrap();
            Self { writer, path }
        }

        fn snapshot(&self) -> PhysicalProjectionQuerySnapshot {
            PhysicalProjectionQuerySnapshot::open_direct(&self.path, || Ok(())).unwrap()
        }

        fn checkpoint(&self) -> (i64, i64, i64) {
            self.writer
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .unwrap()
        }
    }

    impl Drop for SnapshotFixture {
        fn drop(&mut self) {
            // The owning connection is closed after this body; SQLite owns WAL
            // sidecar cleanup. Best-effort fixture cleanup mirrors existing tests.
            let _ = std::fs::remove_file(&self.path);
        }
    }

    #[test]
    fn owned_snapshot_keeps_selection_and_payload_coherent_while_writer_commits() {
        let fixture = SnapshotFixture::new();
        let mut snapshot = fixture.snapshot();
        let ordinary = PhysicalProjectionQueryReader::open(&fixture.path).unwrap();
        let sql = "SELECT value FROM payload WHERE id = ?1";
        let params = [PhysicalQueryValue::Integer(1)];
        let before = vec![vec![PhysicalQueryValue::Text("before".into())]];
        assert_eq!(snapshot.run_projection_query(sql, &params).unwrap(), before);
        fixture
            .writer
            .execute("UPDATE payload SET value='after'", [])
            .unwrap();
        // The old unpinned seam is the fail-before witness: it observes the
        // later payload, even though selection happened at the earlier state.
        assert_eq!(
            ordinary.run_projection_query(sql, &params).unwrap(),
            vec![vec![PhysicalQueryValue::Text("after".into())]]
        );
        assert_eq!(snapshot.run_projection_query(sql, &params).unwrap(), before);
        assert!(!snapshot
            .explain_query_plan(sql, &params)
            .unwrap()
            .is_empty());
        assert_eq!(fixture.checkpoint().0, 1, "active reader retains WAL");
        snapshot.finish();
        assert_eq!(
            fixture.checkpoint(),
            (0, 0, 0),
            "finished reader releases WAL"
        );
    }

    #[test]
    fn owned_snapshot_streams_stops_and_releases_on_visitor_or_sql_error() {
        let fixture = SnapshotFixture::new();
        let mut snapshot = fixture.snapshot();
        let mut visits = 0;
        snapshot
            .visit_projection_query("SELECT 1 UNION ALL SELECT 2", &[], |_| {
                visits += 1;
                Ok(ControlFlow::Break(()))
            })
            .unwrap();
        assert_eq!(visits, 1);
        assert!(snapshot.run_projection_query("SELECT 1", &[]).is_ok());
        assert!(snapshot
            .visit_projection_query("SELECT 1", &[], |_| {
                Err(MaterializationError::Corrupt("missing output row".into()))
            })
            .is_err());
        assert!(snapshot.reader.is_none());
        assert_eq!(fixture.checkpoint(), (0, 0, 0));
        let mut snapshot = fixture.snapshot();
        assert!(snapshot
            .run_projection_query("DELETE FROM payload", &[])
            .is_err());
        assert!(snapshot.reader.is_none());
        assert!(snapshot.run_projection_query("SELECT 1", &[]).is_err());
    }

    #[test]
    fn owned_snapshot_rejects_malformed_text_without_substitution() {
        let fixture = SnapshotFixture::new();
        let mut snapshot = fixture.snapshot();
        assert!(snapshot
            .run_projection_query("SELECT CAST(x'80ff' AS TEXT)", &[])
            .is_err());
        assert!(snapshot.reader.is_none());
        assert_eq!(fixture.checkpoint(), (0, 0, 0));
    }

    #[test]
    fn owned_snapshot_idle_cancellation_is_sticky_and_drop_releases_wal() {
        let fixture = SnapshotFixture::new();
        let mut snapshot = fixture.snapshot();
        let cancellation = snapshot.cancellation();
        cancellation.cancel();
        assert!(cancellation.is_cancelled());
        assert!(snapshot.run_projection_query("SELECT 1", &[]).is_err());
        assert!(snapshot.reader.is_none());
        assert_eq!(fixture.checkpoint(), (0, 0, 0));
        let snapshot = fixture.snapshot();
        fixture
            .writer
            .execute("UPDATE payload SET value='later'", [])
            .unwrap();
        assert_eq!(fixture.checkpoint().0, 1);
        drop(snapshot);
        assert_eq!(fixture.checkpoint(), (0, 0, 0));
        cancellation.cancel(); // A surviving interrupt handle is safe after close.
    }

    #[test]
    fn owned_snapshot_cancels_active_sql_on_an_independent_worker() {
        let fixture = SnapshotFixture::new();
        let mut snapshot = fixture.snapshot();
        let cancellation = snapshot.cancellation();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let signalled = AtomicBool::new(false);
        snapshot
            .reader
            .as_ref()
            .unwrap()
            .connection
            .create_scalar_function(
                "signal_start",
                0,
                rusqlite::functions::FunctionFlags::SQLITE_UTF8,
                move |_| {
                    if !signalled.swap(true, Ordering::Relaxed) {
                        started_tx.send(()).unwrap();
                    }
                    Ok(0_i64)
                },
            )
            .unwrap();
        let worker = std::thread::spawn(move || {
            let result = snapshot.run_projection_query(
                "WITH RECURSIVE numbers(x) AS (VALUES(1) UNION ALL
                 SELECT x+1 FROM numbers WHERE x<1000000000)
                 SELECT sum(x+signal_start()) FROM numbers",
                &[],
            );
            assert!(result.is_err());
            assert!(snapshot.reader.is_none());
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        cancellation.cancel();
        worker.join().unwrap();
        assert_eq!(fixture.checkpoint(), (0, 0, 0));
    }

    #[test]
    fn owned_snapshot_regex_predicate_uses_bound_ids_and_exact_text() {
        let fixture = SnapshotFixture::new();
        let mut snapshot = fixture.snapshot();
        snapshot
            .set_query_regex_predicate(|id, text| {
                if id != 7 {
                    return Err(MaterializationError::InvalidQuery(
                        "unknown compiled regex ID".into(),
                    ));
                }
                Ok(text == "É  exact\ntext")
            })
            .unwrap();
        assert_eq!(
            snapshot
                .run_projection_query(
                    "SELECT tine_query_regex(?1, ?2)",
                    &[
                        PhysicalQueryValue::Integer(7),
                        PhysicalQueryValue::Text("É  exact\ntext".into())
                    ]
                )
                .unwrap(),
            vec![vec![PhysicalQueryValue::Integer(1)]]
        );
        assert!(snapshot
            .run_projection_query(
                "SELECT tine_query_regex(?1, ?2)",
                &[
                    PhysicalQueryValue::Integer(8),
                    PhysicalQueryValue::Text("text".into())
                ]
            )
            .is_err());
        assert!(snapshot.reader.is_none());
    }

    #[test]
    fn owned_snapshot_rank_preserves_exact_input_nulls_and_blob_order() {
        let fixture = SnapshotFixture::new();
        let mut snapshot = fixture.snapshot();
        snapshot
            .set_query_rank_function(|id, text| {
                assert_eq!(id, 7);
                Ok(match text {
                    "É  exact\ntext" => Some(u64::MAX.to_be_bytes().to_vec()),
                    "lower" => Some((u64::MAX - 1).to_be_bytes().to_vec()),
                    _ => None,
                })
            })
            .unwrap();
        let rows = snapshot
            .run_projection_query(
                "WITH inputs(text) AS (VALUES (?2), ('lower'), ('absent'))
             SELECT text, tine_query_rank(?1, text) AS rank FROM inputs
             WHERE rank IS NOT NULL ORDER BY rank DESC",
                &[
                    PhysicalQueryValue::Integer(7),
                    PhysicalQueryValue::Text("É  exact\ntext".into()),
                ],
            )
            .unwrap();
        assert_eq!(
            rows,
            vec![
                vec![
                    PhysicalQueryValue::Text("É  exact\ntext".into()),
                    PhysicalQueryValue::Blob(u64::MAX.to_be_bytes().to_vec())
                ],
                vec![
                    PhysicalQueryValue::Text("lower".into()),
                    PhysicalQueryValue::Blob((u64::MAX - 1).to_be_bytes().to_vec())
                ],
            ]
        );
    }

    #[test]
    fn owned_snapshot_rank_is_scoped_and_reads_pinned_text() {
        let fixture = SnapshotFixture::new();
        let mut first = fixture.snapshot();
        first
            .set_query_rank_function(|_, text| Ok(Some(text.as_bytes().to_vec())))
            .unwrap();
        fixture
            .writer
            .execute("UPDATE payload SET value='after'", [])
            .unwrap();
        let mut second = fixture.snapshot();
        second
            .set_query_rank_function(|_, text| Ok(Some(format!("second:{text}").into_bytes())))
            .unwrap();
        let sql = "SELECT tine_query_rank(1, value) FROM payload";
        assert_eq!(
            first.run_projection_query(sql, &[]).unwrap(),
            vec![vec![PhysicalQueryValue::Blob(b"before".to_vec())]]
        );
        assert_eq!(
            second.run_projection_query(sql, &[]).unwrap(),
            vec![vec![PhysicalQueryValue::Blob(b"second:after".to_vec())]]
        );
        first.finish();
        second.finish();
        assert_eq!(fixture.checkpoint(), (0, 0, 0));
    }

    #[test]
    fn owned_snapshot_rank_error_and_cancellation_release_transactions() {
        let fixture = SnapshotFixture::new();
        let mut snapshot = fixture.snapshot();
        snapshot
            .set_query_rank_function(|_, _| {
                Err(MaterializationError::InvalidQuery(
                    "unknown rank program".into(),
                ))
            })
            .unwrap();
        fixture
            .writer
            .execute("UPDATE payload SET value='after'", [])
            .unwrap();
        assert!(snapshot
            .run_projection_query("SELECT tine_query_rank(99, value) FROM payload", &[])
            .is_err());
        assert!(snapshot.reader.is_none());
        assert_eq!(fixture.checkpoint(), (0, 0, 0));
        let mut cancelled = fixture.snapshot();
        cancelled.cancellation().cancel();
        assert!(cancelled.set_query_rank_function(|_, _| Ok(None)).is_err());
        assert!(cancelled.reader.is_none());
        assert_eq!(fixture.checkpoint(), (0, 0, 0));
    }

    #[test]
    fn owned_snapshot_rank_cancellation_interrupts_active_scoring() {
        let fixture = SnapshotFixture::new();
        let mut snapshot = fixture.snapshot();
        let cancellation = snapshot.cancellation();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first = AtomicBool::new(true);
        snapshot
            .set_query_rank_function(move |_, text| {
                if first.swap(false, Ordering::Relaxed) {
                    started_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                Ok(Some(text.as_bytes().to_vec()))
            })
            .unwrap();
        fixture
            .writer
            .execute("UPDATE payload SET value='after'", [])
            .unwrap();
        let worker = std::thread::spawn(move || {
            let result = snapshot.run_projection_query(
                "WITH RECURSIVE numbers(x) AS (VALUES(1) UNION ALL
                 SELECT x+1 FROM numbers WHERE x<1000000000)
                 SELECT sum(length(tine_query_rank(1, CAST(x AS TEXT)))) FROM numbers",
                &[],
            );
            assert!(result.is_err());
            assert!(snapshot.reader.is_none());
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(fixture.checkpoint().0, 1);
        cancellation.cancel();
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(fixture.checkpoint(), (0, 0, 0));
    }

    fn page(page_id: u8, task: &str, content: &str) -> PhysicalPage {
        PhysicalPage {
            page_id: [page_id; 16],
            query_page_order: Some(u64::from(page_id)),
            home_document_id: [page_id; 16],
            name: format!("Page {page_id}"),
            name_key: format!("page {page_id}"),
            path: format!("pages/page-{page_id}.md"),
            text_kind: 0,
            journal_day: None,
            preamble: None,
            searchable_text: content.into(),
            normalized_searchable_text: content.to_lowercase(),
            references: Vec::new(),
            properties: Vec::new(),
            tags: Vec::new(),
            property_atoms: Vec::new(),
            blocks: vec![PhysicalBlock {
                block_id: [page_id.saturating_add(100); 16],
                query_result_id: uuid::Uuid::from_bytes([page_id.saturating_add(100); 16])
                    .to_string(),
                own_refs: Vec::new(),
                home_document_id: [page_id; 16],
                parent: None,
                order: "0001".into(),
                content: content.into(),
                searchable_text: content.into(),
                normalized_searchable_text: content.to_lowercase(),
                query_visible: content.into(),
                query_visible_folded: content.to_lowercase(),
                heading_level: None,
                collapsed: false,
                logseq_uuid: None,
                logseq_identity_origin: None,
                references: Vec::new(),
                properties: Vec::new(),
                tags: Vec::new(),
                task: Some(PhysicalTask {
                    marker: task.into(),
                    priority: Some("A".into()),
                    scheduled: None,
                    deadline: None,
                }),
                planning: Some(PhysicalPlanning {
                    priority: Some("A".into()),
                    scheduled: None,
                    scheduled_day: None,
                    deadline: None,
                    deadline_day: None,
                }),
                path_refs: Vec::new(),
                property_atoms: Vec::new(),
            }],
        }
    }

    #[test]
    fn query_revision_tracks_committed_images_and_preserves_pinned_payload() {
        let path = std::env::temp_dir().join(format!(
            "tine-query-revision-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        let open = || PhysicalProjectionQuerySnapshot::open_direct(&path, || Ok(())).unwrap();
        assert_eq!(open().query_revision().unwrap(), 0);
        let change = |content: &str| PhysicalGraphProjectionChange {
            replacements: vec![page(1, content, content)],
            deletions: vec![],
            reference_postings: vec![],
        };
        database.apply(&change("TODO")).unwrap();
        let mut old = open();
        let old_revision = old.query_revision().unwrap();
        database.apply(&change("DONE")).unwrap();
        let mut new = open();
        assert!(new.query_revision().unwrap() > old_revision);
        assert_eq!(old.query_revision().unwrap(), old_revision);
        assert_eq!(
            old.run_projection_query("SELECT content FROM block_text", &[])
                .unwrap(),
            vec![vec![PhysicalQueryValue::Text("TODO".into())]]
        );
        assert_eq!(
            new.run_projection_query("SELECT content FROM block_text", &[])
                .unwrap(),
            vec![vec![PhysicalQueryValue::Text("DONE".into())]]
        );
        let revision = new.query_revision().unwrap();
        drop(old);
        drop(new);
        // Inventory validation fails after page rows were written: all writes,
        // including the image revision, must roll back together.
        let mut invalid = page(1, "TODO", "must roll back");
        invalid.query_page_order = None;
        assert!(database
            .apply_with_source_revisions_aliases_and_page_order(
                &PhysicalGraphProjectionChange {
                    replacements: vec![invalid],
                    deletions: vec![],
                    reference_postings: vec![]
                },
                &[PhysicalGraphProjectionSourceRevision {
                    page_id: [1; 16],
                    revision: "bad".into()
                }],
                &[],
                &[[2; 16]],
            )
            .is_err());
        assert_eq!(open().query_revision().unwrap(), revision);
        assert_eq!(
            open()
                .run_projection_query("SELECT content FROM block_text", &[])
                .unwrap(),
            vec![vec![PhysicalQueryValue::Text("DONE".into())]]
        );
        // An order-only transaction must invalidate image-based query memos.
        database
            .apply_with_source_revisions_aliases_and_page_order(
                &PhysicalGraphProjectionChange {
                    replacements: vec![],
                    deletions: vec![],
                    reference_postings: vec![],
                },
                &[],
                &[],
                &[[1; 16]],
            )
            .unwrap();
        assert!(open().query_revision().unwrap() > revision);
        let before_reset = open().query_revision().unwrap();
        database
            .connection
            .execute_batch("PRAGMA foreign_keys=OFF")
            .unwrap();
        database.reset().unwrap();
        let after_reset = open().query_revision().unwrap();
        assert!(after_reset > before_reset);
        drop(database);
        let database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.validate_schema().unwrap();
        assert_eq!(open().query_revision().unwrap(), after_reset);
        drop(database);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn query_revision_damage_and_cancellation_release_snapshot() {
        let path = std::env::temp_dir().join(format!(
            "tine-query-revision-damage-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        database
            .connection
            .execute("UPDATE query_projection_state SET revision=?1", [i64::MAX])
            .unwrap();
        assert!(database
            .apply(&PhysicalGraphProjectionChange {
                replacements: vec![page(1, "TODO", "not committed")],
                deletions: vec![],
                reference_postings: vec![],
            })
            .is_err());
        let mut snapshot = PhysicalProjectionQuerySnapshot::open_direct(&path, || Ok(())).unwrap();
        assert_eq!(snapshot.query_revision().unwrap(), i64::MAX as u64);
        assert_eq!(
            snapshot
                .run_projection_query("SELECT COUNT(*) FROM pages", &[])
                .unwrap(),
            vec![vec![PhysicalQueryValue::Integer(0)]]
        );
        snapshot.cancellation().cancel();
        assert!(snapshot.query_revision().is_err());
        assert!(snapshot.reader.is_none());
        database
            .connection
            .execute("DELETE FROM query_projection_state", [])
            .unwrap();
        assert!(database.validate_schema().is_err());
        let mut snapshot = PhysicalProjectionQuerySnapshot::open_direct(&path, || Ok(())).unwrap();
        assert!(snapshot.query_revision().is_err());
        assert!(snapshot.reader.is_none());
        drop(database);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn ordered_inventory_reconciles_without_replacing_unchanged_pages() {
        let path = std::env::temp_dir().join(format!(
            "tine-order-reconcile-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        let mut first = page(1, "TODO", "unchanged one");
        let mut second = page(2, "DONE", "unchanged two");
        first.query_page_order = None;
        second.query_page_order = None;
        database
            .apply_with_source_revisions_aliases_and_page_order(
                &PhysicalGraphProjectionChange {
                    replacements: vec![first.clone(), second],
                    deletions: vec![],
                    reference_postings: vec![],
                },
                &[
                    PhysicalGraphProjectionSourceRevision {
                        page_id: [1; 16],
                        revision: "one".into(),
                    },
                    PhysicalGraphProjectionSourceRevision {
                        page_id: [2; 16],
                        revision: "two".into(),
                    },
                ],
                &[],
                &[[1; 16], [2; 16]],
            )
            .unwrap();
        let empty = PhysicalGraphProjectionChange {
            replacements: vec![],
            deletions: vec![],
            reference_postings: vec![],
        };
        for table in [
            "pages",
            "blocks",
            "page_text",
            "block_text",
            "query_block_results",
            "query_page_results",
        ] {
            for operation in ["INSERT", "UPDATE", "DELETE"] {
                database
                    .connection
                    .execute_batch(&format!(
                        "CREATE TEMP TRIGGER no_{table}_{operation} BEFORE {operation} ON {table}
                     BEGIN SELECT RAISE(ABORT, 'unchanged page facts were rewritten'); END;"
                    ))
                    .unwrap();
            }
        }
        database
            .apply_with_source_revisions_aliases_and_page_order(
                &empty,
                &[],
                &[],
                &[[2; 16], [1; 16]],
            )
            .unwrap();
        let order = database
            .connection
            .prepare("SELECT page_id FROM query_page_order ORDER BY position")
            .unwrap()
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(order, [vec![2; 16], vec![1; 16]]);
        for operation in ["INSERT", "UPDATE", "DELETE"] {
            database
                .connection
                .execute_batch(&format!(
                "CREATE TEMP TRIGGER no_order_{operation} BEFORE {operation} ON query_page_order
                 BEGIN SELECT RAISE(ABORT, 'unchanged order was rewritten'); END;"
            ))
                .unwrap();
        }
        database
            .apply_with_source_revisions_aliases_and_page_order(
                &empty,
                &[],
                &[],
                &[[2; 16], [1; 16]],
            )
            .unwrap();
        assert!(database
            .apply_with_source_revisions_aliases_and_page_order(&empty, &[], &[], &[[1; 16]])
            .is_err());
        assert!(database
            .apply_with_source_revisions_aliases_and_page_order(
                &empty,
                &[],
                &[],
                &[[1; 16], [1; 16]]
            )
            .is_err());
        drop(database);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn incomplete_inventory_rolls_back_page_and_source_changes() {
        let path = std::env::temp_dir().join(format!(
            "tine-order-rollback-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        let mut first = page(1, "TODO", "before");
        first.query_page_order = None;
        let revisions = [PhysicalGraphProjectionSourceRevision {
            page_id: [1; 16],
            revision: "before".into(),
        }];
        database
            .apply_with_source_revisions_aliases_and_page_order(
                &PhysicalGraphProjectionChange {
                    replacements: vec![first.clone()],
                    deletions: vec![],
                    reference_postings: vec![],
                },
                &revisions,
                &[],
                &[[1; 16]],
            )
            .unwrap();
        first.blocks[0].content = "after".into();
        assert!(database
            .apply_with_source_revisions_aliases_and_page_order(
                &PhysicalGraphProjectionChange {
                    replacements: vec![first],
                    deletions: vec![],
                    reference_postings: vec![]
                },
                &[PhysicalGraphProjectionSourceRevision {
                    page_id: [1; 16],
                    revision: "after".into()
                }],
                &[],
                &[],
            )
            .is_err());
        assert!(database
            .source_delta(&revisions)
            .unwrap()
            .replacements
            .is_empty());
        assert_eq!(
            database
                .connection
                .query_row("SELECT content FROM block_text", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "before"
        );
        assert_eq!(
            database
                .connection
                .query_row("SELECT count(*) FROM query_page_order", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        drop(database);
        let _ = std::fs::remove_file(path);
    }

    /// The statement seam's restriction is the ENGINE's, not a validator's.
    /// If this ever passes a write, the read-only open has been lost and the
    /// whole justification for the seam (raw SQL crosses, authority does not)
    /// is gone — so the write must be attempted for real, not assumed to fail.
    #[test]
    fn the_query_seam_can_read_the_projection_and_cannot_write_it() {
        let path = std::env::temp_dir().join(format!(
            "tine-storage-query-seam-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        drop(database);

        let reader = PhysicalProjectionQueryReader::open(&path).unwrap();

        // Reads work, and parameters come back as values.
        let rows = reader
            .run_projection_query(
                "SELECT COUNT(*) FROM pages WHERE name_key = ?1",
                &[PhysicalQueryValue::Text("absent".into())],
            )
            .unwrap();
        assert_eq!(rows, vec![vec![PhysicalQueryValue::Integer(0)]]);

        // Every write shape is refused by SQLite itself.
        for write in [
            "DELETE FROM pages",
            "INSERT INTO pages (page_id, name, name_key, text_kind)
             VALUES (zeroblob(16), 'x', 'x', 0)",
            "UPDATE pages SET name = 'x'",
            "DROP TABLE pages",
            "CREATE TABLE smuggled (x INTEGER)",
        ] {
            let error = reader.run_projection_query(write, &[]).unwrap_err();
            assert!(
                matches!(&error, MaterializationError::Sqlite(message)
                    if message.contains("readonly") || message.contains("read-only")),
                "{write} must be refused by the read-only connection, got {error:?}"
            );
        }

        // A value that looks like SQL stays a value: it is bound, not spliced.
        let hostile = "'; DROP TABLE pages; --";
        let rows = reader
            .run_projection_query(
                "SELECT COUNT(*) FROM pages WHERE name_key = ?1",
                &[PhysicalQueryValue::Text(hostile.into())],
            )
            .unwrap();
        assert_eq!(rows, vec![vec![PhysicalQueryValue::Integer(0)]]);
        assert_eq!(
            reader
                .run_projection_query(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name = 'pages'",
                    &[]
                )
                .unwrap(),
            vec![vec![PhysicalQueryValue::Integer(1)]],
            "the bound value must not have been executed as SQL"
        );

        // The plan accessor answers, which is what lets the campaign's plan
        // gate live in the repository instead of a scratch harness.
        let plan = reader
            .explain_query_plan(
                "SELECT page_id FROM pages WHERE name_key = ?1",
                &[PhysicalQueryValue::Text("x".into())],
            )
            .unwrap();
        assert!(
            plan.iter().any(|step| step.contains("SEARCH")),
            "a keyed lookup must plan as a SEARCH, got {plan:?}"
        );

        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    #[test]
    fn standalone_projection_applies_replaces_deletes_and_reads_graph_facts() {
        let path = std::env::temp_dir().join(format!(
            "tine-storage-graph-projection-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        database.validate_schema().unwrap();

        let managed_tables: i64 = database
            .connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table'
                   AND name IN ('materialization_stamp', 'materialization_batches')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            managed_tables, 0,
            "the standalone graph projection must not grow managed-frontier tables"
        );

        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: vec![page(1, "TODO", "Needle first")],
                deletions: Vec::new(),
                reference_postings: Vec::new(),
            })
            .unwrap();
        assert_eq!(database.read().tasks(Some("TODO"), 10).unwrap().len(), 1);
        assert_eq!(database.read().search("needle", 10).unwrap().len(), 2);

        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: vec![page(1, "DONE", "Needle changed")],
                deletions: Vec::new(),
                reference_postings: Vec::new(),
            })
            .unwrap();
        assert!(database.read().tasks(Some("TODO"), 10).unwrap().is_empty());
        assert_eq!(database.read().tasks(Some("DONE"), 10).unwrap().len(), 1);

        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: Vec::new(),
                deletions: vec![[1; 16]],
                reference_postings: Vec::new(),
            })
            .unwrap();
        assert!(database.read().tasks(None, 10).unwrap().is_empty());
        assert!(database.read().search("needle", 10).unwrap().is_empty());
        database.quick_check().unwrap();
        drop(database);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    #[test]
    fn standalone_projection_replaces_reference_names_in_the_page_transaction() {
        let path = std::env::temp_dir().join(format!(
            "tine-storage-graph-reference-projection-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();

        let posting = |raw_name: &str, normalized_name: &str| PhysicalReferencePosting {
            source_page_id: [1; 16],
            source_entity: PhysicalEntityId::Page([1; 16]),
            source_locator: b"preamble".to_vec(),
            ordinal: 0,
            kind: 0,
            target: PhysicalReferenceTarget::PageName {
                raw_name: raw_name.into(),
                normalized_name: normalized_name.into(),
                resolved_page_id: None,
            },
        };
        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: vec![page(1, "TODO", "[[First Target]]")],
                deletions: Vec::new(),
                reference_postings: vec![posting("First Target", "first target")],
            })
            .unwrap();
        assert_eq!(
            database
                .read()
                .navigation_reference_names_after(None, 10)
                .unwrap()
                .into_iter()
                .map(|row| row.raw_name)
                .collect::<Vec<_>>(),
            vec!["First Target"]
        );

        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: vec![page(1, "TODO", "[[Second Target]]")],
                deletions: Vec::new(),
                reference_postings: vec![posting("Second Target", "second target")],
            })
            .unwrap();
        assert_eq!(
            database
                .read()
                .navigation_reference_names_after(None, 10)
                .unwrap()
                .into_iter()
                .map(|row| row.raw_name)
                .collect::<Vec<_>>(),
            vec!["Second Target"]
        );

        let mut orphan = posting("Orphan", "orphan");
        orphan.source_page_id = [2; 16];
        assert!(matches!(
            database.apply(&PhysicalGraphProjectionChange {
                replacements: vec![page(1, "TODO", "unchanged")],
                deletions: Vec::new(),
                reference_postings: vec![orphan],
            }),
            Err(MaterializationError::InvalidInput(_))
        ));
        assert_eq!(
            database
                .read()
                .navigation_reference_names_after(None, 10)
                .unwrap()
                .into_iter()
                .map(|row| row.raw_name)
                .collect::<Vec<_>>(),
            vec!["Second Target"]
        );

        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: Vec::new(),
                deletions: vec![[1; 16]],
                reference_postings: Vec::new(),
            })
            .unwrap();
        assert!(database
            .read()
            .navigation_reference_names_after(None, 10)
            .unwrap()
            .is_empty());
        database.quick_check().unwrap();
        drop(database);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    /// The four navigation readers page on index-served keysets. GH tine#543:
    /// the previous order over the joined page path could not use any index,
    /// so every 512-row batch scanned and sorted the whole join — O(N²) over
    /// the graph, ~11 GB of reads per launch at 10,000 pages.
    fn plan_uses_index_range(plan: &[String], expected: &str) -> Result<(), String> {
        if !plan.iter().any(|line| line.starts_with(expected)) {
            return Err(format!("expected `{expected}…` in plan {plan:?}"));
        }
        // An index-ordered scan streams and `LIMIT` ends it; a table scan or
        // a sort of the whole result does not. SQLite spells an index-driven
        // scan two ways — `USING INDEX` and `USING COVERING INDEX` — and the
        // covering form is the stronger one (it never touches the table), so
        // accepting only the first would reject the better plan.
        if let Some(line) = plan.iter().find(|line| {
            (line.starts_with("SCAN ")
                && !line.contains(" USING INDEX ")
                && !line.contains(" USING COVERING INDEX "))
                || line.as_str() == "USE TEMP B-TREE FOR ORDER BY"
        }) {
            return Err(format!(
                "paged reader would sort or scan the whole table per batch (`{line}`): \
                 key the batch on the columns of an existing index, as \
                 NAVIGATION_REFERENCE_NAMES_AFTER_SQL does; plan {plan:?}"
            ));
        }
        Ok(())
    }

    #[test]
    fn paged_navigation_readers_use_an_index_range() {
        let path = std::env::temp_dir().join(format!(
            "tine-storage-graph-plan-guard-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        let read = database.read();
        let text = |value: &str| rusqlite::types::Value::from(value.to_owned());
        let blob = rusqlite::types::Value::from(vec![0u8; 16]);
        let limit = rusqlite::types::Value::from(512i64);

        let shapes: [(&str, &[rusqlite::types::Value], &str); 4] = [
            (
                NAVIGATION_REFERENCE_NAMES_FIRST_SQL,
                &[limit.clone()],
                "SCAN r USING COVERING INDEX reference_postings_navigation_names_idx",
            ),
            (
                NAVIGATION_REFERENCE_NAMES_AFTER_SQL,
                &[text("topic"), text("Topic"), limit.clone()],
                "SEARCH r USING COVERING INDEX reference_postings_navigation_names_idx",
            ),
            (
                NAVIGATION_ALIASES_FIRST_SQL,
                &[limit.clone()],
                "SCAN d USING INDEX ",
            ),
            (
                NAVIGATION_ALIASES_AFTER_SQL,
                &[blob.clone(), text("alias"), limit.clone()],
                "SEARCH d USING ",
            ),
        ];
        for (sql, args, expected) in shapes {
            let plan = read.query_plan(sql, args).unwrap();
            plan_uses_index_range(&plan, expected).unwrap_or_else(|why| panic!("{why}\n{sql}"));
        }

        // The pre-fix shape (v0.20.1) is the counterexample the guard exists
        // for: an order over the joined path is a full scan plus a sort.
        let pre_fix = read
            .query_plan(
                "SELECT DISTINCT r.source_page_id, p.path, r.raw_name, r.normalized_name
                 FROM reference_postings r JOIN pages p ON p.page_id = r.source_page_id
                 WHERE r.target_type = 0 AND r.reference_kind <= 4
                   AND (p.path > ?1 OR (p.path = ?1 AND r.raw_name > ?2))
                 ORDER BY p.path, r.raw_name, r.normalized_name, r.source_page_id LIMIT ?3",
                &[text("pages/a.md"), text("Topic"), limit],
            )
            .unwrap();
        assert!(
            plan_uses_index_range(&pre_fix, "SEARCH r USING INDEX").is_err(),
            "guard must reject the v0.20.1 plan: {pre_fix:?}"
        );

        drop(read);
        drop(database);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    /// Draining one row at a time visits exactly the distinct rows an
    /// unbounded read returns — no row skipped or repeated at a batch edge,
    /// including two spellings of one name on one page and one spelling on
    /// two pages.
    #[test]
    fn navigation_reference_names_page_without_gaps_or_repeats() {
        let path = std::env::temp_dir().join(format!(
            "tine-storage-graph-name-paging-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        let posting = |page: u8, ordinal: u32, raw_name: &str, normalized_name: &str| {
            PhysicalReferencePosting {
                source_page_id: [page; 16],
                source_entity: PhysicalEntityId::Page([page; 16]),
                source_locator: b"preamble".to_vec(),
                ordinal,
                kind: 0,
                target: PhysicalReferenceTarget::PageName {
                    raw_name: raw_name.into(),
                    normalized_name: normalized_name.into(),
                    resolved_page_id: None,
                },
            }
        };
        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: vec![
                    page(1, "TODO", "a"),
                    page(2, "TODO", "b"),
                    page(3, "TODO", "c"),
                ],
                deletions: Vec::new(),
                reference_postings: vec![
                    posting(1, 0, "Zeta", "zeta"),
                    posting(1, 1, "zeta", "zeta"),
                    posting(1, 2, "Zeta", "zeta"),
                    posting(2, 0, "Zeta", "zeta"),
                    posting(2, 1, "Alpha", "alpha"),
                    posting(3, 0, "alpha", "alpha"),
                    posting(3, 1, "Mid", "mid"),
                ],
            })
            .unwrap();
        let read = database.read();
        let all = read.navigation_reference_names_after(None, 100).unwrap();
        // One row per distinct spelling GRAPH-WIDE, not per source page: the
        // seven postings above carry five distinct (normalized_name, raw_name)
        // pairs. "Zeta" is posted from pages 1 and 2 and appears once.
        assert_eq!(all.len(), 5, "{all:?}");
        assert_eq!(
            all.iter()
                .filter(|row| row.raw_name == "Zeta" && row.normalized_name == "zeta")
                .count(),
            1,
            "a spelling shared by two pages must be offered once: {all:?}"
        );

        for batch in 1..=3 {
            let mut paged = Vec::new();
            let mut after: Option<PhysicalNavigationReferenceNameRow> = None;
            loop {
                let rows = read
                    .navigation_reference_names_after(
                        after
                            .as_ref()
                            .map(|row| (row.normalized_name.as_str(), row.raw_name.as_str())),
                        batch,
                    )
                    .unwrap();
                let done = rows.len() < batch;
                after = rows.last().cloned();
                paged.extend(rows);
                if done {
                    break;
                }
            }
            assert_eq!(paged, all, "batch size {batch}");
        }
        drop(read);
        drop(database);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    #[test]
    fn standalone_projection_replaces_deletes_and_reopens_aliases_atomically() {
        let path = std::env::temp_dir().join(format!(
            "tine-storage-graph-alias-projection-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let alias = |raw_alias: &str, normalized_alias: &str| PhysicalAliasDeclaration {
            source_page_id: [1; 16],
            source_entity: PhysicalEntityId::Page([1; 16]),
            source_locator: b"page-alias".to_vec(),
            ordinal: 0,
            raw_alias: raw_alias.into(),
            normalized_alias: normalized_alias.into(),
        };
        let alias_names = |database: &PhysicalGraphProjectionDatabase| {
            database
                .read()
                .navigation_aliases_after(None, 10)
                .unwrap()
                .into_iter()
                .map(|row| row.normalized_alias)
                .collect::<Vec<_>>()
        };

        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        database
            .apply_with_aliases(
                &PhysicalGraphProjectionChange {
                    replacements: vec![page(1, "TODO", "first")],
                    deletions: Vec::new(),
                    reference_postings: Vec::new(),
                },
                &[alias("First Alias", "first alias")],
            )
            .unwrap();
        assert_eq!(alias_names(&database), vec!["first alias"]);
        drop(database);

        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.validate_schema().unwrap();
        assert_eq!(alias_names(&database), vec!["first alias"]);
        database
            .apply_with_aliases(
                &PhysicalGraphProjectionChange {
                    replacements: vec![page(1, "TODO", "second")],
                    deletions: Vec::new(),
                    reference_postings: Vec::new(),
                },
                &[alias("Second Alias", "second alias")],
            )
            .unwrap();
        assert_eq!(alias_names(&database), vec!["second alias"]);

        let mut orphan = alias("Orphan Alias", "orphan alias");
        orphan.source_page_id = [2; 16];
        assert!(matches!(
            database.apply_with_aliases(
                &PhysicalGraphProjectionChange {
                    replacements: vec![page(1, "TODO", "must roll back")],
                    deletions: Vec::new(),
                    reference_postings: Vec::new(),
                },
                &[orphan],
            ),
            Err(MaterializationError::InvalidInput(_))
        ));
        assert_eq!(alias_names(&database), vec!["second alias"]);
        database.quick_check().unwrap();
        drop(database);

        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.validate_schema().unwrap();
        assert_eq!(alias_names(&database), vec!["second alias"]);
        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: Vec::new(),
                deletions: vec![[1; 16]],
                reference_postings: Vec::new(),
            })
            .unwrap();
        assert!(alias_names(&database).is_empty());
        database.quick_check().unwrap();
        drop(database);

        let database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.validate_schema().unwrap();
        assert!(alias_names(&database).is_empty());
        database.quick_check().unwrap();
        drop(database);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    #[test]
    fn external_uuid_claimants_survive_reopen_replace_and_delete_without_an_owner() {
        let path = std::env::temp_dir().join(format!(
            "tine-storage-external-uuid-claims-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let claim = [0x5a; 16];
        let with_claim = |page_id: u8, content: &str| {
            let mut page = page(page_id, "TODO", content);
            page.blocks[0].logseq_uuid = Some(claim);
            page.blocks[0].logseq_identity_origin = Some(0);
            page
        };
        let claimant_ids = |database: &PhysicalGraphProjectionDatabase| {
            database
                .read()
                .blocks_by_logseq_uuid(claim, 3)
                .unwrap()
                .into_iter()
                .map(|row| row.block_id)
                .collect::<Vec<_>>()
        };

        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: vec![with_claim(1, "first"), with_claim(2, "second")],
                deletions: Vec::new(),
                reference_postings: Vec::new(),
            })
            .unwrap();
        assert_eq!(claimant_ids(&database), vec![[101; 16], [102; 16]]);
        drop(database);

        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.validate_schema().unwrap();
        assert_eq!(claimant_ids(&database), vec![[101; 16], [102; 16]]);
        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: vec![page(1, "DONE", "claim removed")],
                deletions: vec![[2; 16]],
                reference_postings: Vec::new(),
            })
            .unwrap();
        assert!(claimant_ids(&database).is_empty());
        database.quick_check().unwrap();
        drop(database);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }
}
