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
    query_metadata_schema INTEGER NOT NULL DEFAULT 26 CHECK (query_metadata_schema = 26),
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
}

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
        Ok(Self { connection })
    }

    pub fn open_read_only(path: &Path) -> Result<Self, MaterializationError> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_secs(5))?;
        Ok(Self { connection })
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
    pub fn apply_with_aliases(
        &mut self,
        change: &PhysicalGraphProjectionChange,
        aliases: &[PhysicalAliasDeclaration],
    ) -> Result<ApplyChangeInstrumentation, MaterializationError> {
        self.apply_inner(change, None, aliases, None, None)
    }

    /// Apply physical page facts and publish the exact caller-owned source
    /// revisions in the same SQLite transaction.
    pub fn apply_with_source_revisions(
        &mut self,
        change: &PhysicalGraphProjectionChange,
        revisions: &[PhysicalGraphProjectionSourceRevision],
    ) -> Result<ApplyChangeInstrumentation, MaterializationError> {
        self.apply_with_source_revisions_and_aliases(change, revisions, &[])
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

    /// Apply the complete current-state projection needed by both storage
    /// regimes: parser facts, exact source revisions, aliases, and the
    /// caller-derived platform-neutral path identity for every replacement.
    ///
    /// Portable-path keys are intentionally a non-unique candidate index. The
    /// semantic caller decides whether multiple owners are a graph conflict.
    pub fn apply_with_source_revisions_aliases_and_portable_paths(
        &mut self,
        change: &PhysicalGraphProjectionChange,
        revisions: &[PhysicalGraphProjectionSourceRevision],
        aliases: &[PhysicalAliasDeclaration],
        portable_paths: &[PhysicalPagePortablePathClaim],
    ) -> Result<ApplyChangeInstrumentation, MaterializationError> {
        self.apply_inner(change, Some(revisions), aliases, Some(portable_paths), None)
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
        transaction.commit()?;
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

    /// Validate the accepted frontier from inside the same read transaction
    /// that will serve every selection and payload statement.
    pub fn open_managed(
        path: &Path,
        sequence: u64,
        frontier_digest: crate::ContentDigest,
    ) -> Result<Self, MaterializationError> {
        let snapshot = Self::begin(path)?;
        sqlite_materialization::ensure_stamp(
            &snapshot.reader.as_ref().expect("new snapshot").connection,
            sequence,
            frontier_digest,
        )?;
        Ok(snapshot)
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

    /// Install compiled-regex ID lookup on this snapshot's read-only connection.
    pub fn set_query_regex_predicate(
        &mut self,
        predicate: impl Fn(u64, &str) -> Result<bool, MaterializationError> + Send + 'static,
    ) -> Result<(), MaterializationError> {
        self.read(|reader| reader.set_query_regex_predicate(predicate))
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
    use std::collections::BTreeSet;

    use crate::sqlite_materialization::test_parse_config_hash;
    use crate::sqlite_materialization::{
        PhysicalAliasDeclaration, PhysicalBlock, PhysicalEntityId, PhysicalMaterializationChange,
        PhysicalPage, PhysicalPagePortablePathClaim, PhysicalPlanning, PhysicalReferencePosting,
        PhysicalReferenceTarget, PhysicalTask,
    };
    use crate::ContentDigest;

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
    fn owned_snapshot_validates_direct_acquisition_and_managed_stamp() {
        let fixture = SnapshotFixture::new();
        let mut calls = 0;
        let result = PhysicalProjectionQuerySnapshot::open_direct(&fixture.path, || {
            calls += 1;
            if calls == 2 {
                Err(MaterializationError::Incomplete(
                    "projection replaced".into(),
                ))
            } else {
                Ok(())
            }
        });
        assert!(result.is_err());
        assert_eq!(calls, 2);
        assert_eq!(fixture.checkpoint(), (0, 0, 0));
        let digest = ContentDigest::from_bytes([0; 32]);
        assert!(PhysicalProjectionQuerySnapshot::open_managed(&fixture.path, 8, digest).is_err());
        assert!(PhysicalProjectionQuerySnapshot::open_managed(
            &fixture.path,
            7,
            ContentDigest::from_bytes([1; 32])
        )
        .is_err());
        let mut snapshot =
            PhysicalProjectionQuerySnapshot::open_managed(&fixture.path, 7, digest).unwrap();
        fixture
            .writer
            .execute("UPDATE materialization_stamp SET acceptance_sequence=8", [])
            .unwrap();
        assert_eq!(
            snapshot
                .run_projection_query("SELECT acceptance_sequence FROM materialization_stamp", &[])
                .unwrap(),
            vec![vec![PhysicalQueryValue::Integer(7)]]
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
    fn standalone_source_revisions_reuse_exact_pages_and_localize_changes() {
        let path = std::env::temp_dir().join(format!(
            "tine-storage-source-revisions-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        let initial_revisions = vec![
            PhysicalGraphProjectionSourceRevision {
                page_id: [1; 16],
                revision: "rev-1".into(),
            },
            PhysicalGraphProjectionSourceRevision {
                page_id: [2; 16],
                revision: "rev-2".into(),
            },
        ];
        database
            .apply_with_source_revisions(
                &PhysicalGraphProjectionChange {
                    replacements: vec![page(1, "TODO", "first"), page(2, "DONE", "second")],
                    deletions: Vec::new(),
                    reference_postings: Vec::new(),
                },
                &initial_revisions,
            )
            .unwrap();
        assert_eq!(
            database.source_delta(&initial_revisions).unwrap(),
            PhysicalGraphProjectionSourceDelta::default()
        );

        drop(database);
        let mut reopened = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        reopened.validate_schema().unwrap();
        let changed = vec![
            PhysicalGraphProjectionSourceRevision {
                page_id: [1; 16],
                revision: "rev-1-new".into(),
            },
            PhysicalGraphProjectionSourceRevision {
                page_id: [3; 16],
                revision: "rev-3".into(),
            },
        ];
        assert_eq!(
            reopened.source_delta(&changed).unwrap(),
            PhysicalGraphProjectionSourceDelta {
                replacements: vec![[1; 16], [3; 16]],
                deletions: vec![[2; 16]],
            }
        );
        reopened
            .apply_with_source_revisions(
                &PhysicalGraphProjectionChange {
                    replacements: vec![page(1, "DONE", "first changed"), page(3, "TODO", "third")],
                    deletions: vec![[2; 16]],
                    reference_postings: Vec::new(),
                },
                &changed,
            )
            .unwrap();
        assert_eq!(
            reopened.source_delta(&changed).unwrap(),
            PhysicalGraphProjectionSourceDelta::default()
        );
        reopened.quick_check().unwrap();
        drop(reopened);
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

    #[test]
    fn portable_path_candidates_replace_delete_reopen_and_preserve_conflicts() {
        let path = std::env::temp_dir().join(format!(
            "tine-storage-portable-paths-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let shared_key = ContentDigest::of(b"portable/shared");
        let moved_key = ContentDigest::of(b"portable/moved");
        let revision = |page_id: u8, value: &str| PhysicalGraphProjectionSourceRevision {
            page_id: [page_id; 16],
            revision: value.into(),
        };
        let claim = |page_id: u8, key| PhysicalPagePortablePathClaim {
            page_id: [page_id; 16],
            portable_path_key: key,
        };
        let ids = |database: &PhysicalGraphProjectionDatabase, key| {
            database
                .read()
                .pages_by_portable_path_key(key, 10)
                .unwrap()
                .into_iter()
                .map(|row| row.page_id)
                .collect::<Vec<_>>()
        };
        let mut second_page = page(2, "DONE", "second");
        second_page.home_document_id = [1; 16];
        second_page.blocks[0].home_document_id = [1; 16];

        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        database
            .apply_with_source_revisions_aliases_and_portable_paths(
                &PhysicalGraphProjectionChange {
                    replacements: vec![page(1, "TODO", "first"), second_page],
                    deletions: Vec::new(),
                    reference_postings: Vec::new(),
                },
                &[revision(1, "rev-1"), revision(2, "rev-2")],
                &[],
                &[claim(1, shared_key), claim(2, shared_key)],
            )
            .unwrap();
        assert_eq!(ids(&database, shared_key), vec![[1; 16], [2; 16]]);
        assert_eq!(
            database
                .read()
                .pages_by_home_document_id([1; 16], 10)
                .unwrap()
                .into_iter()
                .map(|row| row.page_id)
                .collect::<Vec<_>>(),
            vec![[1; 16], [2; 16]]
        );
        drop(database);

        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.validate_schema().unwrap();
        assert_eq!(ids(&database, shared_key), vec![[1; 16], [2; 16]]);
        database
            .apply_with_source_revisions_aliases_and_portable_paths(
                &PhysicalGraphProjectionChange {
                    replacements: vec![page(1, "DONE", "moved")],
                    deletions: Vec::new(),
                    reference_postings: Vec::new(),
                },
                &[revision(1, "rev-1-moved")],
                &[],
                &[claim(1, moved_key)],
            )
            .unwrap();
        assert_eq!(ids(&database, shared_key), vec![[2; 16]]);
        assert_eq!(ids(&database, moved_key), vec![[1; 16]]);

        assert!(matches!(
            database.apply_with_source_revisions_aliases_and_portable_paths(
                &PhysicalGraphProjectionChange {
                    replacements: vec![page(1, "TODO", "must roll back")],
                    deletions: Vec::new(),
                    reference_postings: Vec::new(),
                },
                &[revision(1, "bad-revision")],
                &[],
                &[],
            ),
            Err(MaterializationError::InvalidInput(_))
        ));
        assert_eq!(ids(&database, moved_key), vec![[1; 16]]);
        assert_eq!(
            database
                .read()
                .page([1; 16])
                .unwrap()
                .unwrap()
                .searchable_text,
            "moved"
        );

        database
            .apply_with_source_revisions_aliases_and_portable_paths(
                &PhysicalGraphProjectionChange {
                    replacements: Vec::new(),
                    deletions: vec![[2; 16]],
                    reference_postings: Vec::new(),
                },
                &[],
                &[],
                &[],
            )
            .unwrap();
        assert!(ids(&database, shared_key).is_empty());
        assert_eq!(
            database
                .read()
                .pages_by_home_document_id([1; 16], 10)
                .unwrap()
                .into_iter()
                .map(|row| row.page_id)
                .collect::<Vec<_>>(),
            vec![[1; 16]]
        );
        database.quick_check().unwrap();
        drop(database);

        let database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.validate_schema().unwrap();
        assert_eq!(ids(&database, moved_key), vec![[1; 16]]);
        drop(database);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    #[test]
    fn ordinary_apply_invalidates_source_reuse_for_replaced_pages() {
        let path = std::env::temp_dir().join(format!(
            "tine-storage-source-invalidation-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        let revision = PhysicalGraphProjectionSourceRevision {
            page_id: [1; 16],
            revision: "exact-source".into(),
        };
        database
            .apply_with_source_revisions(
                &PhysicalGraphProjectionChange {
                    replacements: vec![page(1, "TODO", "first")],
                    deletions: Vec::new(),
                    reference_postings: Vec::new(),
                },
                std::slice::from_ref(&revision),
            )
            .unwrap();
        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: vec![page(1, "DONE", "untracked replacement")],
                deletions: Vec::new(),
                reference_postings: Vec::new(),
            })
            .unwrap();
        assert_eq!(
            database
                .source_delta(std::slice::from_ref(&revision))
                .unwrap(),
            PhysicalGraphProjectionSourceDelta {
                replacements: vec![[1; 16]],
                deletions: Vec::new(),
            }
        );
        drop(database);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    #[test]
    fn standalone_and_managed_adapters_materialize_identical_graph_facts() {
        let path = std::env::temp_dir().join(format!(
            "tine-storage-graph-projection-parity-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let source = page(7, "TODO", "Shared projection needle");
        let posting = PhysicalReferencePosting {
            source_page_id: [7; 16],
            source_entity: PhysicalEntityId::Page([7; 16]),
            source_locator: b"preamble".to_vec(),
            ordinal: 0,
            kind: 0,
            target: PhysicalReferenceTarget::PageName {
                raw_name: "Shared Target".into(),
                normalized_name: "shared target".into(),
                resolved_page_id: None,
            },
        };
        let alias = PhysicalAliasDeclaration {
            source_page_id: [7; 16],
            source_entity: PhysicalEntityId::Page([7; 16]),
            source_locator: b"page-alias".to_vec(),
            ordinal: 0,
            raw_alias: "Shared Alias".into(),
            normalized_alias: "shared alias".into(),
        };
        let path_claim = PhysicalPagePortablePathClaim {
            page_id: [7; 16],
            portable_path_key: ContentDigest::of(b"shared portable path"),
        };

        let mut standalone = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        standalone.initialize_schema().unwrap();
        standalone
            .apply_with_source_revisions_aliases_and_portable_paths(
                &PhysicalGraphProjectionChange {
                    replacements: vec![source.clone()],
                    deletions: Vec::new(),
                    reference_postings: vec![posting.clone()],
                },
                &[PhysicalGraphProjectionSourceRevision {
                    page_id: [7; 16],
                    revision: "shared-source".into(),
                }],
                std::slice::from_ref(&alias),
                std::slice::from_ref(&path_claim),
            )
            .unwrap();

        let managed = Connection::open_in_memory().unwrap();
        let empty = ContentDigest::of(b"empty");
        let frontier = ContentDigest::of(b"frontier-1");
        sqlite_materialization::initialize_schema(&managed, empty, test_parse_config_hash())
            .unwrap();
        let transaction = managed.unchecked_transaction().unwrap();
        sqlite_materialization::apply_change(
            &transaction,
            &PhysicalMaterializationChange {
                batch_id: [9; 16],
                replacements: vec![source],
                deletions: Vec::new(),
                pages_with_live_metadata_delta: BTreeSet::from([[7; 16]]),
                derived_reference_postings: vec![posting],
                derived_aliases: vec![alias],
                portable_path_claims: vec![path_claim],
                block_home_claims: Vec::new(),
                page_name_identity_records: Vec::new(),
                portable_path_identity_records: Vec::new(),
                logseq_uuid_introductions: Vec::new(),
            },
            1,
            ContentDigest::of(b"input"),
            frontier,
        )
        .unwrap();
        transaction.commit().unwrap();
        let managed_read =
            sqlite_materialization::SqliteMaterializedRead::new(&managed, 1, frontier).unwrap();

        assert_eq!(
            standalone.read().tasks(None, 10).unwrap(),
            managed_read.tasks(None, 10).unwrap()
        );
        assert_eq!(
            standalone.read().search("needle", 10).unwrap(),
            managed_read.search("needle", 10).unwrap()
        );
        assert_eq!(
            standalone.read().pages(None, 10).unwrap(),
            managed_read.pages(None, 10).unwrap()
        );
        let derived_counts = |connection: &Connection| {
            [
                "reference_postings",
                "reference_alias_declarations",
                "reference_alias_bindings",
                "page_portable_path_claims",
            ]
            .map(|table| {
                connection
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap()
            })
        };
        assert_eq!(
            derived_counts(&standalone.connection),
            derived_counts(&managed)
        );
        assert_eq!(derived_counts(&managed), [1, 1, 1, 1]);

        drop(standalone);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }
}
