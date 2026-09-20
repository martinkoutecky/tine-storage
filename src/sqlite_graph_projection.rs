//! Disposable Direct Files graph projection.
//!
//! This database owns only parser-derived graph facts and their indexes. A
//! Direct Files watcher/parser feeds the page replacement/delete transaction.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, TransactionBehavior};

use crate::sqlite_materialization::{
    self, ApplyChangeInstrumentation, MaterializationError, PhysicalAliasDeclaration,
    PhysicalGraphProjectionChange, SqliteGraphProjectionRead,
};
const PREPARED_STATEMENT_CACHE_STATEMENTS: usize = 64;
const SOURCE_REVISION_MAX_BYTES: usize = 4096;
const SOURCE_REVISIONS_DDL: &str = "CREATE TABLE direct_source_revisions (
    path TEXT PRIMARY KEY CHECK (length(CAST(path AS BLOB)) BETWEEN 1 AND 4194304),
    revision TEXT NOT NULL CHECK (length(CAST(revision AS BLOB)) BETWEEN 1 AND 4096),
    FOREIGN KEY (path) REFERENCES pages(path) ON DELETE CASCADE
) STRICT";

/// Exact application-authority revision for one disposable projection page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalGraphProjectionSourceRevision {
    pub path: String,
    pub revision: String,
}

#[cfg(test)]
mod compact_key_tests {
    use super::*;
    use crate::sqlite_materialization::{
        PhysicalAliasDeclaration, PhysicalBlock, PhysicalEntityId, PhysicalName, PhysicalPage,
        PhysicalProperty, PhysicalPropertyAtom, PhysicalReferencePosting, PhysicalReferenceTarget,
        PhysicalTag, PhysicalTask,
    };

    fn file(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "tine-storage-p3-{name}-{}.sqlite",
            uuid::Uuid::new_v4()
        ))
    }

    fn block(id: &str, parent: Option<&str>) -> PhysicalBlock {
        PhysicalBlock {
            result_id: id.into(),
            own_refs: vec![
                PhysicalName {
                    raw: "Foo".into(),
                    key: "foo".into(),
                },
                PhysicalName {
                    raw: "FOO".into(),
                    key: "foo".into(),
                },
                PhysicalName {
                    raw: "Only own".into(),
                    key: "only own".into(),
                },
            ],
            parent: parent.map(str::to_owned),
            order: if parent.is_some() {
                "b".into()
            } else {
                "a".into()
            },
            content: format!("content {id}"),
            search_tokens: format!("content {id}"),
            heading_level: None,
            collapsed: false,
            logseq_uuid: Some([7; 16]),
            logseq_identity_origin: Some(0),
            properties: vec![PhysicalProperty {
                name: "Priority".into(),
                normalized_name: "priority".into(),
                value: "A".into(),
            }],
            tags: vec![PhysicalTag {
                tag: "Tag".into(),
                tag_key: "tag".into(),
            }],
            task: Some(PhysicalTask {
                marker: "TODO".into(),
                priority: None,
                scheduled: None,
                deadline: None,
            }),
            planning: None,
            path_refs: vec![PhysicalName {
                raw: "Foo".into(),
                key: "foo".into(),
            }],
            property_atoms: vec![PhysicalPropertyAtom {
                name: "Priority".into(),
                normalized_name: "priority".into(),
                ordinal: 0,
                atom: "A".into(),
                atom_key: "a".into(),
                origin: 1,
                atom_num: None,
                atom_day: None,
            }],
        }
    }

    fn page(path: &str, name: &str, blocks: Vec<PhysicalBlock>) -> PhysicalPage {
        PhysicalPage {
            position: None,
            name: name.into(),
            name_key: name.to_lowercase(),
            path: path.into(),
            text_kind: 0,
            journal_day: None,
            preamble: None,
            search_tokens: name.to_lowercase(),
            properties: Vec::new(),
            tags: Vec::new(),
            property_atoms: Vec::new(),
            blocks,
        }
    }

    fn posting(path: &str, block: &str, raw: &str) -> PhysicalReferencePosting {
        PhysicalReferencePosting {
            source_page_path: path.into(),
            source_entity: PhysicalEntityId::Block(block.into()),
            source_locator: b"content".to_vec(),
            ordinal: if raw == "Foo" { 0 } else { 1 },
            kind: 0,
            target: PhysicalReferenceTarget::PageName {
                raw_name: raw.into(),
                normalized_name: "foo".into(),
            },
        }
    }

    fn change(
        page: PhysicalPage,
        postings: Vec<PhysicalReferencePosting>,
    ) -> PhysicalGraphProjectionChange {
        PhysicalGraphProjectionChange {
            replacements: vec![page],
            deletions: Vec::new(),
            reference_postings: postings,
        }
    }

    fn scalar(db: &PhysicalGraphProjectionDatabase, sql: &str) -> i64 {
        db.connection.query_row(sql, [], |row| row.get(0)).unwrap()
    }

    fn fts_rowids(db: &PhysicalGraphProjectionDatabase, expression: &str) -> Vec<i64> {
        db.connection
            .prepare("SELECT rowid FROM search_fts WHERE search_fts MATCH ?1 ORDER BY rowid")
            .unwrap()
            .query_map([expression], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    #[test]
    fn integer_coordinates_names_and_one_postings_representation() {
        let path = file("shape");
        let mut db = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        db.initialize_schema().unwrap();
        db.validate_schema().unwrap();
        let a = "pages/a.md";
        let mut c = change(
            page(
                a,
                "Café",
                vec![block("b-root", None), block("b-child", Some("b-root"))],
            ),
            vec![posting(a, "b-root", "Foo"), posting(a, "b-root", "FOO")],
        );
        c.reference_postings.push(PhysicalReferencePosting {
            source_page_path: a.into(),
            source_entity: PhysicalEntityId::Block("b-root".into()),
            source_locator: b"content".to_vec(),
            ordinal: 2,
            kind: 6,
            target: PhysicalReferenceTarget::ExternalUuid { raw_claim: [9; 16] },
        });
        db.apply_with_aliases(
            &c,
            &[PhysicalAliasDeclaration {
                source_page_path: a.into(),
                source_entity: PhysicalEntityId::Page(a.into()),
                source_locator: b"properties".to_vec(),
                ordinal: 0,
                raw_alias: "Café Alias".into(),
                normalized_alias: "café alias".into(),
            }],
        )
        .unwrap();

        let root = db.read().block("b-root").unwrap().unwrap();
        let child = db.read().block("b-child").unwrap().unwrap();
        assert_eq!(root.page_path, a);
        assert_eq!(child.parent.as_deref(), Some("b-root"));
        assert_eq!(
            db.read().blocks_by_logseq_uuid([7; 16], 10).unwrap().len(),
            2
        );
        assert_eq!(
            scalar(&db, "SELECT COUNT(*) FROM names WHERE key = 'foo'"),
            2
        );
        assert_eq!(
            scalar(&db, "SELECT COUNT(*) FROM reference_postings WHERE own = 1"),
            4
        );
        let raw_occurrences = db
            .connection
            .prepare(
                "SELECT n.raw FROM reference_postings r
                 JOIN names n ON n.name_id = r.target_name_id
                 JOIN blocks b ON b.block_id = r.source_entity_id
                 WHERE b.result_id = 'b-root' AND r.reference_kind < 8 AND n.key = 'foo'
                 ORDER BY n.raw",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(raw_occurrences, ["FOO", "Foo"]);
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM reference_postings r
                 JOIN names n ON n.name_id = r.target_name_id
                 JOIN blocks b ON b.block_id = r.source_entity_id
                 WHERE b.result_id = 'b-root' AND n.key = 'foo' AND r.own = 1"
            ),
            1
        );
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM reference_postings r
                 JOIN names n ON n.name_id = r.target_name_id
                 JOIN blocks b ON b.block_id = r.source_entity_id
                 WHERE b.result_id = 'b-root' AND n.key = 'foo'
                   AND r.reference_kind = 8"
            ),
            0
        );
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM reference_postings r
                 JOIN names n ON n.name_id = r.target_name_id
                 JOIN blocks b ON b.block_id = r.source_entity_id
                 WHERE b.result_id = 'b-root' AND n.key = 'only own'
                   AND r.reference_kind = 8 AND r.own = 1"
            ),
            1
        );
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM reference_postings WHERE reference_kind = 8"
            ),
            3
        );
        assert_eq!(scalar(&db, "SELECT COUNT(*) FROM block_path_refs"), 2);
        assert_eq!(scalar(&db, "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('block_own_refs','query_block_results','query_page_results','query_page_order')"), 0);
        let read = db.read();
        assert_eq!(
            read.navigation_pages_after_with_header_validation(None, None, 10, |_, _| Ok(()))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(read.navigation_aliases_after(None, 10).unwrap().len(), 1);
        assert_eq!(
            read.navigation_reference_names_after(None, 10)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            read.block_reference_counts_after(None, 10).unwrap()[0].raw_uuid_claim,
            [9; 16]
        );
        assert_eq!(
            read.block_referrer_candidates_after([9; 16], None, 10)
                .unwrap()[0]
                .source_block_id,
            "b-root"
        );
        assert_eq!(
            read.page_referrer_candidates_after("foo", None, 10)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            read.block_property_candidates_after("priority", None, 10)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            read.property_facet_rows_after(true, None, 10)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            read.properties(PhysicalEntityId::Block("b-root".into()), 10)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(read.tags("Tag", 10).unwrap().len(), 2);
        assert_eq!(read.tasks(Some("TODO"), 10).unwrap().len(), 2);
        assert_eq!(
            read.task_candidate_pages_after("TODO", None, 10)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            read.task_candidate_blocks_after("TODO", None, 10)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            read.task_candidate_locators_after("TODO", None, 10)
                .unwrap()
                .len(),
            2
        );
        for plan in [
            read.query_plan(
                "SELECT name_id FROM names WHERE key = ?1 AND raw = ?2",
                &[String::from("foo").into(), String::from("Foo").into()],
            )
            .unwrap(),
            read.query_plan(
                "SELECT name_id FROM names WHERE raw = ?1 AND key = ?2",
                &[String::from("Foo").into(), String::from("foo").into()],
            )
            .unwrap(),
        ] {
            assert!(plan.iter().any(|step| step.contains("SEARCH")), "{plan:?}");
        }
        db.quick_check().unwrap();
        drop(db);
        let db = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        db.validate_schema().unwrap();
        assert_eq!(db.read().page(a).unwrap().unwrap().name, "Café");
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn replacements_allocate_fresh_blocks_preserve_pages_and_reclaim_affected_names() {
        let path = file("replace");
        let mut db = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        db.initialize_schema().unwrap();
        let a = "pages/a.md";
        db.apply(&change(
            page(a, "Old unique", vec![block("live", None)]),
            vec![posting(a, "live", "Foo")],
        ))
        .unwrap();
        let page_id = scalar(&db, "SELECT page_id FROM pages WHERE path = 'pages/a.md'");
        let first_block = scalar(&db, "SELECT block_id FROM blocks WHERE result_id = 'live'");
        db.apply(&change(
            page(a, "New unique", vec![block("live", None)]),
            vec![],
        ))
        .unwrap();
        let second_block = scalar(&db, "SELECT block_id FROM blocks WHERE result_id = 'live'");
        assert_eq!(
            scalar(&db, "SELECT page_id FROM pages WHERE path = 'pages/a.md'"),
            page_id
        );
        assert!(second_block > first_block);
        assert_eq!(
            scalar(&db, "SELECT COUNT(*) FROM names WHERE raw = 'Old unique'"),
            0
        );

        let b = "pages/b.md";
        db.apply(&change(page(b, "B", vec![block("newest", None)]), vec![]))
            .unwrap();
        let newest = scalar(
            &db,
            "SELECT block_id FROM blocks WHERE result_id = 'newest'",
        );
        db.apply(&PhysicalGraphProjectionChange {
            replacements: Vec::new(),
            deletions: vec![b.into()],
            reference_postings: Vec::new(),
        })
        .unwrap();
        db.apply(&change(
            page(a, "New unique", vec![block("live", None)]),
            vec![],
        ))
        .unwrap();
        assert!(scalar(&db, "SELECT block_id FROM blocks WHERE result_id = 'live'") > newest);
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn source_paths_foreign_keys_off_and_reset_are_coherent() {
        let path = file("lifecycle");
        let mut db = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        db.initialize_schema().unwrap();
        let a = "pages/a.md";
        let c = change(page(a, "A", vec![block("one", None)]), vec![]);
        db.apply_with_source_revisions_aliases_and_page_order(
            &c,
            &[PhysicalGraphProjectionSourceRevision {
                path: a.into(),
                revision: "r1".into(),
            }],
            &[],
            &[a.into()],
        )
        .unwrap();
        assert_eq!(
            db.source_delta(&[PhysicalGraphProjectionSourceRevision {
                path: a.into(),
                revision: "r1".into()
            }])
            .unwrap(),
            PhysicalGraphProjectionSourceDelta::default()
        );
        let revision = scalar(&db, "SELECT revision FROM query_projection_state");
        db.apply_with_source_revisions_aliases_and_page_order(
            &PhysicalGraphProjectionChange {
                replacements: Vec::new(),
                deletions: Vec::new(),
                reference_postings: Vec::new(),
            },
            &[],
            &[],
            &[a.into()],
        )
        .unwrap();
        assert_eq!(
            scalar(&db, "SELECT revision FROM query_projection_state"),
            revision
        );
        db.connection
            .pragma_update(None, "foreign_keys", "OFF")
            .unwrap();
        db.apply(&PhysicalGraphProjectionChange {
            replacements: Vec::new(),
            deletions: vec![a.into()],
            reference_postings: Vec::new(),
        })
        .unwrap();
        assert_eq!(scalar(&db, "SELECT COUNT(*) FROM pages"), 0);
        assert_eq!(scalar(&db, "SELECT COUNT(*) FROM reference_postings"), 0);
        assert_eq!(scalar(&db, "SELECT COUNT(*) FROM search_fts"), 0);
        db.reset().unwrap();
        assert_eq!(scalar(&db, "SELECT COUNT(*) FROM names"), 0);
        assert_eq!(scalar(&db, "SELECT COUNT(*) FROM search_fts"), 0);
        assert_eq!(
            scalar(&db, "SELECT next_entity_id FROM query_projection_state"),
            1
        );
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn contentless_trigram_index_has_one_token_copy_and_transactional_lifecycle() {
        let path = file("contentless-fts");
        let mut db = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        db.initialize_schema().unwrap();
        db.validate_schema().unwrap();

        let old_tokens = "alpha oldneedle omega";
        let mut original = page("pages/a.md", "A", vec![block("one", None)]);
        original.search_tokens = old_tokens.into();
        original.blocks[0].search_tokens = old_tokens.into();
        db.apply_with_source_revisions_and_aliases(
            &change(original, vec![]),
            &[PhysicalGraphProjectionSourceRevision {
                path: "pages/a.md".into(),
                revision: "r1".into(),
            }],
            &[],
        )
        .unwrap();

        let old_expression =
            "\"old\" AND \"ldn\" AND \"dne\" AND \"nee\" AND \"eed\" AND \"edl\" AND \"dle\"";
        assert_eq!(fts_rowids(&db, old_expression).len(), 2);
        let stored_text: Option<String> = db
            .connection
            .query_row(
                "SELECT normalized_text FROM search_fts LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            stored_text, None,
            "a contentless FTS row must return NULL text"
        );
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM sqlite_schema
                 WHERE name IN ('search_substring_fts', 'search_fts_owners')",
            ),
            0
        );
        for (table, expected) in [
            ("page_text", vec!["page_id", "preamble"]),
            ("block_text", vec!["block_id", "content"]),
        ] {
            let columns = db
                .connection
                .prepare(&format!("PRAGMA table_info({table})"))
                .unwrap()
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(columns, expected);
        }
        assert_eq!(
            scalar(
                &db,
                "SELECT
                    (SELECT COUNT(*) FROM pragma_table_info('page_text')
                     WHERE name IN ('searchable_text', 'normalized_searchable_text'))
                  + (SELECT COUNT(*) FROM pragma_table_info('block_text')
                     WHERE name IN ('searchable_text', 'normalized_searchable_text', 'query_visible'))
                  + (SELECT COUNT(*) FROM pragma_table_info('blocks')
                     WHERE name = 'query_visible_folded')",
            ),
            0,
            "raw/visible/fold text must not survive outside raw owners and token postings"
        );
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM pages p JOIN blocks b ON b.block_id = p.page_id",
            ),
            0,
            "page and block IDs share one disjoint scalar namespace"
        );

        let mut replacement = page("pages/a.md", "A", vec![block("two", None)]);
        replacement.search_tokens = "alpha newneedle omega".into();
        replacement.blocks[0].search_tokens = "alpha newneedle omega".into();
        let failed = PhysicalGraphProjectionChange {
            replacements: vec![replacement.clone()],
            deletions: vec![],
            reference_postings: vec![PhysicalReferencePosting {
                source_page_path: "pages/a.md".into(),
                source_entity: PhysicalEntityId::Block("missing".into()),
                source_locator: b"content".to_vec(),
                ordinal: 0,
                kind: 0,
                target: PhysicalReferenceTarget::PageName {
                    raw_name: "Target".into(),
                    normalized_name: "target".into(),
                },
            }],
        };
        assert!(db
            .apply_with_source_revisions_and_aliases(
                &failed,
                &[PhysicalGraphProjectionSourceRevision {
                    path: "pages/a.md".into(),
                    revision: "r2".into(),
                }],
                &[],
            )
            .is_err());
        assert_eq!(fts_rowids(&db, old_expression).len(), 2);
        assert!(fts_rowids(
            &db,
            "\"new\" AND \"ewn\" AND \"wne\" AND \"nee\" AND \"eed\" AND \"edl\" AND \"dle\"",
        )
        .is_empty());
        assert_eq!(
            db.source_delta(&[PhysicalGraphProjectionSourceRevision {
                path: "pages/a.md".into(),
                revision: "r1".into(),
            }])
            .unwrap(),
            PhysicalGraphProjectionSourceDelta::default()
        );

        let cleanup = db.apply(&change(replacement, vec![])).unwrap();
        assert_eq!(cleanup.cleanup_fts_rowids, 2);
        assert!(fts_rowids(&db, old_expression).is_empty());
        assert_eq!(
            fts_rowids(
                &db,
                "\"new\" AND \"ewn\" AND \"wne\" AND \"nee\" AND \"eed\" AND \"edl\" AND \"dle\"",
            )
            .len(),
            2
        );
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM search_fts f
                 LEFT JOIN pages p ON p.page_id = f.rowid
                 LEFT JOIN blocks b ON b.block_id = f.rowid
                 WHERE p.page_id IS NULL AND b.block_id IS NULL",
            ),
            0
        );

        db.apply(&PhysicalGraphProjectionChange {
            replacements: vec![],
            deletions: vec!["pages/a.md".into()],
            reference_postings: vec![],
        })
        .unwrap();
        assert_eq!(scalar(&db, "SELECT COUNT(*) FROM search_fts"), 0);
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trigram_postings_admit_false_positives_and_sqlite_edge_inputs() {
        let path = file("fts-edge-inputs");
        let mut db = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        db.initialize_schema().unwrap();
        let mut fixture = page("pages/edge.md", "Edge", vec![block("edge", None)]);
        fixture.search_tokens = String::new();
        fixture.blocks[0].content = "raw\0content".into();
        fixture.blocks[0].search_tokens = "abc separated bcd\0xy".into();
        let mut empty = block("empty", None);
        empty.content.clear();
        empty.search_tokens.clear();
        fixture.blocks.push(empty);
        let mut short = block("short", None);
        short.content = "xy".into();
        short.search_tokens = "xy".into();
        fixture.blocks.push(short);
        db.apply(&change(fixture, vec![])).unwrap();

        assert_eq!(scalar(&db, "SELECT COUNT(*) FROM search_fts"), 4);
        assert_eq!(
            fts_rowids(&db, "\"abc\" AND \"bcd\"").len(),
            1,
            "storage returns trigram candidates; exact verification rejects this false positive"
        );
        assert_eq!(
            db.read().block("edge").unwrap().unwrap().content,
            "raw\0content"
        );
        db.quick_check().unwrap();
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn public_ids_are_not_query_expression_capped() {
        let path = file("long-public-ids");
        let mut db = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        db.initialize_schema().unwrap();
        let long_path = format!("pages/{}.md", "p".repeat(70_000));
        let long_block_id = "b".repeat(70_000);
        db.apply(&change(
            page(&long_path, "Long", vec![block(&long_block_id, None)]),
            vec![],
        ))
        .unwrap();
        assert_eq!(db.read().page(&long_path).unwrap().unwrap().path, long_path);
        let page_cursor = db
            .connection
            .query_row(
                "SELECT page_id FROM pages WHERE path = ?1",
                [&long_path],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert!(db
            .read()
            .navigation_pages_after_with_header_validation(
                Some(&long_path),
                Some(page_cursor),
                10,
                |_, _| Ok(()),
            )
            .unwrap()
            .is_empty());
        assert_eq!(
            db.read().block(&long_block_id).unwrap().unwrap().result_id,
            long_block_id
        );
        assert_eq!(
            db.read()
                .properties(PhysicalEntityId::Block(long_block_id), 10)
                .unwrap()
                .len(),
            1
        );
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}

/// Page IDs whose physical facts differ from an application's current source.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PhysicalGraphProjectionSourceDelta {
    pub replacements: Vec<String>,
    pub deletions: Vec<String>,
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
        if columns != ["path", "revision"] {
            return Err(MaterializationError::Schema(format!(
                "direct_source_revisions columns {columns:?} != [path, revision]"
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
        self.apply_inner(change, None, aliases, None)
    }

    /// Apply page/reference facts, exact source revisions, and aliases in one
    /// transaction. Source identity is the same public relative path used by
    /// page replacement; no derived UUID coordinate crosses this boundary.
    pub fn apply_with_source_revisions_and_aliases(
        &mut self,
        change: &PhysicalGraphProjectionChange,
        revisions: &[PhysicalGraphProjectionSourceRevision],
        aliases: &[PhysicalAliasDeclaration],
    ) -> Result<ApplyChangeInstrumentation, MaterializationError> {
        self.apply_inner(change, Some(revisions), aliases, None)
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
        page_order: &[String],
    ) -> Result<ApplyChangeInstrumentation, MaterializationError> {
        self.apply_inner(change, Some(revisions), aliases, Some(page_order))
    }

    fn apply_inner(
        &mut self,
        change: &PhysicalGraphProjectionChange,
        revisions: Option<&[PhysicalGraphProjectionSourceRevision]>,
        aliases: &[PhysicalAliasDeclaration],
        page_order: Option<&[String]>,
    ) -> Result<ApplyChangeInstrumentation, MaterializationError> {
        let replacement_ids = change
            .replacements
            .iter()
            .map(|page| page.path.clone())
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
        let page_order_plan = page_order
            .map(|order| prepare_query_page_order(&transaction, change, order))
            .transpose()?;
        let order_changed = page_order_plan
            .as_ref()
            .is_some_and(|plan| !plan.mismatched.is_empty());
        if !change.replacements.is_empty()
            || !change.deletions.is_empty()
            || !change.reference_postings.is_empty()
            || !aliases.is_empty()
            || order_changed
        {
            sqlite_materialization::advance_query_projection_revision(&transaction)?;
        }
        if let Some(plan) = &page_order_plan {
            plan.clear_insert_collisions(&transaction)?;
        }
        // Fresh-build route (GH #543): maintaining every secondary index once
        // per inserted fact makes a graph-sized build touch the whole index
        // set page by page and, past the page-cache ceiling, spill and re-read
        // it. Building the indexes once after the rows is an external sort.
        // Readers on this WAL file see the old snapshot until the
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
            change,
            aliases,
            None,
        )?;
        for path in &change.deletions {
            transaction.execute(
                "DELETE FROM direct_source_revisions WHERE path = ?1",
                rusqlite::params![path],
            )?;
        }
        match revisions {
            Some(revisions) => {
                for revision in revisions {
                    transaction.execute(
                        "INSERT INTO direct_source_revisions (path, revision)
                         VALUES (?1, ?2)
                         ON CONFLICT(path) DO UPDATE SET revision = excluded.revision",
                        rusqlite::params![&revision.path, &revision.revision],
                    )?;
                }
            }
            None => {
                for page in &change.replacements {
                    transaction.execute(
                        "DELETE FROM direct_source_revisions WHERE path = ?1",
                        rusqlite::params![&page.path],
                    )?;
                }
            }
        }
        if let Some(plan) = &page_order_plan {
            plan.reconcile(&transaction)?;
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
        let mut existing = BTreeMap::<String, Option<String>>::new();
        let mut statement = self.connection.prepare(
            "SELECT p.path, s.revision
             FROM pages AS p
             LEFT JOIN direct_source_revisions AS s ON s.path = p.path
             ORDER BY p.path",
        )?;
        for row in statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })? {
            let (path, revision) = row?;
            existing.insert(path, revision);
        }
        let mut output_budget = sqlite_materialization::MaterializationReadBudget::default();
        let mut replacements = Vec::new();
        for (path, revision) in &current {
            if existing.get(path).and_then(Option::as_ref) != Some(revision) {
                output_budget.add(sqlite_materialization::checked_output_bytes(
                    0,
                    [Some(path.as_str())],
                )?)?;
                replacements.push(path.clone());
            }
        }
        let mut deletions = Vec::new();
        for path in existing.keys().filter(|path| !current.contains_key(*path)) {
            output_budget.add(sqlite_materialization::checked_output_bytes(
                0,
                [Some(path.as_str())],
            )?)?;
            deletions.push(path.clone());
        }
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

struct QueryPageOrderPlan {
    /// Final positions whose post-replacement value differs from the complete
    /// inventory. Existing rows at these paths temporarily vacate their
    /// positions before replacement rows retain positions, then only these
    /// rows receive their final positions.
    mismatched: Vec<(String, i64)>,
}

impl QueryPageOrderPlan {
    fn clear_insert_collisions(&self, connection: &Connection) -> Result<(), MaterializationError> {
        let mut clear = connection.prepare_cached(
            "UPDATE pages SET position = NULL WHERE path = ?1 AND position IS NOT NULL",
        )?;
        for (path, _) in &self.mismatched {
            clear.execute([path])?;
        }
        Ok(())
    }

    fn reconcile(&self, connection: &Connection) -> Result<(), MaterializationError> {
        // Pre-materialization clearing makes mismatched retained positions
        // NULL. Clear conditionally again after insertion before assigning any
        // target so every reconciliation remains collision-free.
        let mut clear = connection.prepare_cached(
            "UPDATE pages SET position = NULL WHERE path = ?1 AND position IS NOT NULL",
        )?;
        for (path, _) in &self.mismatched {
            clear.execute([path])?;
        }
        let mut set = connection.prepare_cached(
            "UPDATE pages SET position = ?2 WHERE path = ?1 AND position IS NOT ?2",
        )?;
        for (path, position) in &self.mismatched {
            if set.execute(rusqlite::params![path, position])? != 1 {
                return Err(MaterializationError::Corrupt(
                    "query inventory page disappeared during reconciliation".into(),
                ));
            }
        }
        Ok(())
    }
}

fn prepare_query_page_order(
    connection: &Connection,
    change: &PhysicalGraphProjectionChange,
    order: &[String],
) -> Result<QueryPageOrderPlan, MaterializationError> {
    let mut desired = BTreeMap::new();
    for (position, path) in order.iter().enumerate() {
        let position = i64::try_from(position).map_err(|_| {
            MaterializationError::InvalidInput("query page position exceeds SQLite".into())
        })?;
        if desired.insert(path.clone(), position).is_some() {
            return Err(MaterializationError::InvalidInput(
                "duplicate page in query inventory".into(),
            ));
        }
    }

    let existing = connection
        .prepare("SELECT path, position FROM pages")?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
        })?
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let replacements = change
        .replacements
        .iter()
        .map(|page| (page.path.as_str(), page))
        .collect::<BTreeMap<_, _>>();
    let mut final_paths = existing.keys().cloned().collect::<BTreeSet<_>>();
    for path in &change.deletions {
        final_paths.remove(path);
    }
    final_paths.extend(change.replacements.iter().map(|page| page.path.clone()));
    if final_paths.len() != desired.len()
        || final_paths.iter().any(|path| !desired.contains_key(path))
    {
        return Err(MaterializationError::InvalidInput(
            "query inventory must exactly cover projected pages".into(),
        ));
    }

    for page in &change.replacements {
        if let Some(position) = page.position {
            let position = i64::try_from(position).map_err(|_| {
                MaterializationError::InvalidInput("query page position exceeds SQLite".into())
            })?;
            if desired.get(&page.path) != Some(&position) {
                return Err(MaterializationError::InvalidInput(
                    "page order differs from complete inventory".into(),
                ));
            }
        }
    }

    let mut mismatched = Vec::new();
    for (path, desired_position) in desired {
        let post_apply_position = match replacements.get(path.as_str()) {
            Some(page) => page
                .position
                .map(i64::try_from)
                .transpose()
                .map_err(|_| {
                    MaterializationError::InvalidInput("query page position exceeds SQLite".into())
                })?
                .or_else(|| existing.get(&path).copied().flatten()),
            None => existing.get(&path).copied().flatten(),
        };
        if post_apply_position != Some(desired_position) {
            mismatched.push((path, desired_position));
        }
    }
    Ok(QueryPageOrderPlan { mismatched })
}

fn validated_source_revisions(
    revisions: &[PhysicalGraphProjectionSourceRevision],
) -> Result<BTreeMap<String, String>, MaterializationError> {
    let mut validated = BTreeMap::new();
    for revision in revisions {
        if revision.revision.is_empty() || revision.revision.len() > SOURCE_REVISION_MAX_BYTES {
            return Err(MaterializationError::InvalidInput(
                "source revision must contain 1..=4096 bytes".into(),
            ));
        }
        if validated
            .insert(revision.path.clone(), revision.revision.clone())
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
/// disposable cache derived from the Markdown/Org tree, so a malformed
/// statement fails a read and can never corrupt truth. The authoritative tree
/// does not expose this seam.
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
    /// revision target and is reset by a fresh file.
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
        PhysicalAliasDeclaration, PhysicalBlock, PhysicalEntityId, PhysicalName, PhysicalPage,
        PhysicalPlanning, PhysicalReferencePosting, PhysicalReferenceTarget, PhysicalTask,
    };

    fn scalar(database: &PhysicalGraphProjectionDatabase, sql: &str) -> i64 {
        database
            .connection
            .query_row(sql, [], |row| row.get(0))
            .unwrap()
    }

    fn fts_rowids(database: &PhysicalGraphProjectionDatabase, expression: &str) -> Vec<i64> {
        database
            .connection
            .prepare("SELECT rowid FROM search_fts WHERE search_fts MATCH ?1 ORDER BY rowid")
            .unwrap()
            .query_map([expression], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    /// The GH #543 fixture shape: `pages` pages of 60 blocks, each block
    /// carrying one page link and one tag, exercising the same cardinality
    /// and secondary-index fanout as the reporter-scale build.
    fn gh543_snapshot(
        pages: usize,
    ) -> (
        PhysicalGraphProjectionChange,
        Vec<PhysicalGraphProjectionSourceRevision>,
        Vec<String>,
    ) {
        use crate::sqlite_materialization::PhysicalTag;
        let page_ids = (0..pages)
            .map(|page| format!("pages/主题-{page:05}.md"))
            .collect::<Vec<_>>();
        let mut replacements = Vec::with_capacity(pages);
        let mut postings = Vec::with_capacity(pages * 120);
        for (position, page_id) in page_ids.iter().enumerate() {
            let target = (position + 1) % pages;
            let target_name = format!("Topic {target} 你好");
            let normalized_target = target_name.to_lowercase();
            let mut blocks = Vec::with_capacity(60);
            for block in 0..60 {
                let block_id = format!("b-{position}-{block}");
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
                        source_page_path: page_id.clone(),
                        source_entity: PhysicalEntityId::Block(block_id.clone()),
                        source_locator: b"content".to_vec(),
                        ordinal: ordinal as u32,
                        kind: 0,
                        target: PhysicalReferenceTarget::PageName {
                            raw_name: raw.clone(),
                            normalized_name: normalized.clone(),
                        },
                    });
                }
                blocks.push(PhysicalBlock {
                    result_id: block_id,
                    own_refs: vec![
                        PhysicalName {
                            raw: target_name.clone(),
                            key: normalized_target.clone(),
                        },
                        PhysicalName {
                            raw: tag.clone(),
                            key: tag.clone(),
                        },
                    ],
                    parent: None,
                    order: format!("{block:04}"),
                    content: content.clone(),
                    search_tokens: content.to_lowercase(),
                    heading_level: None,
                    collapsed: false,
                    logseq_uuid: None,
                    logseq_identity_origin: None,
                    properties: Vec::new(),
                    tags: vec![PhysicalTag {
                        tag: tag.clone(),
                        tag_key: tag.clone(),
                    }],
                    task: None,
                    planning: None,
                    path_refs: vec![
                        PhysicalName {
                            raw: target_name.clone(),
                            key: normalized_target.clone(),
                        },
                        PhysicalName {
                            raw: tag.clone(),
                            key: tag,
                        },
                    ],
                    property_atoms: Vec::new(),
                });
            }
            let name = format!("Topic {position} 你好");
            replacements.push(PhysicalPage {
                position: Some(position as u64),
                name_key: name.to_lowercase(),
                path: page_id.clone(),
                name,
                text_kind: 0,
                journal_day: None,
                preamble: None,
                search_tokens: String::new(),
                properties: Vec::new(),
                tags: Vec::new(),
                property_atoms: Vec::new(),
                blocks,
            });
        }
        let revisions = page_ids
            .iter()
            .map(|page_id| PhysicalGraphProjectionSourceRevision {
                path: page_id.clone(),
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

    /// GH #543: an apply into an empty projection builds the 33 secondary
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
        assert_eq!(fresh_indexes, 33);
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
                .filter(|posting| posting.source_page_path == change.replacements[0].path)
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
            source_page_path: "pages/nowhere.md".into(),
            source_entity: PhysicalEntityId::Page("pages/nowhere.md".into()),
            source_locator: b"nowhere".to_vec(),
            ordinal: 0,
            kind: 0,
            target: PhysicalReferenceTarget::PageName {
                raw_name: "x".into(),
                normalized_name: "x".into(),
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
                        with_seed.push(seed_ids[0].clone());
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
                INSERT INTO payload VALUES (1, 'before');",
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
            position: Some(u64::from(page_id)),
            name: format!("Page {page_id}"),
            name_key: format!("page {page_id}"),
            path: format!("pages/page-{page_id}.md"),
            text_kind: 0,
            journal_day: None,
            preamble: None,
            search_tokens: content.to_lowercase(),
            properties: Vec::new(),
            tags: Vec::new(),
            property_atoms: Vec::new(),
            blocks: vec![PhysicalBlock {
                result_id: format!("block-{page_id}"),
                own_refs: Vec::new(),
                parent: None,
                order: "0001".into(),
                content: content.into(),
                search_tokens: content.to_lowercase(),
                heading_level: None,
                collapsed: false,
                logseq_uuid: None,
                logseq_identity_origin: None,
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
        invalid.position = None;
        assert!(database
            .apply_with_source_revisions_aliases_and_page_order(
                &PhysicalGraphProjectionChange {
                    replacements: vec![invalid],
                    deletions: vec![],
                    reference_postings: vec![]
                },
                &[PhysicalGraphProjectionSourceRevision {
                    path: "pages/page-1.md".into(),
                    revision: "bad".into()
                }],
                &[],
                &["pages/page-2.md".into()],
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
                &["pages/page-1.md".into()],
            )
            .unwrap();
        let order_revision = open().query_revision().unwrap();
        assert!(order_revision > revision);
        let before_reset = order_revision;
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
        let pages = (1..=8)
            .map(|index| {
                let mut value = page(index, "TODO", &format!("page {index}"));
                value.position = None;
                value
            })
            .collect::<Vec<_>>();
        let revisions = (1..=8)
            .map(|index| PhysicalGraphProjectionSourceRevision {
                path: format!("pages/page-{index}.md"),
                revision: format!("r{index}"),
            })
            .collect::<Vec<_>>();
        let original_order = (1..=8)
            .map(|index| format!("pages/page-{index}.md"))
            .collect::<Vec<_>>();
        database
            .apply_with_source_revisions_aliases_and_page_order(
                &PhysicalGraphProjectionChange {
                    replacements: pages,
                    deletions: vec![],
                    reference_postings: vec![],
                },
                &revisions,
                &[],
                &original_order,
            )
            .unwrap();
        let empty = PhysicalGraphProjectionChange {
            replacements: vec![],
            deletions: vec![],
            reference_postings: vec![],
        };
        database
            .connection
            .execute_batch(
                "CREATE TEMP TABLE position_writes(path TEXT NOT NULL);
                 CREATE TEMP TRIGGER track_position_writes AFTER UPDATE OF position ON pages
                 BEGIN INSERT INTO position_writes(path) VALUES (NEW.path); END;",
            )
            .unwrap();
        let mut permuted = original_order.clone();
        permuted.swap(2, 3);
        database
            .apply_with_source_revisions_aliases_and_page_order(&empty, &[], &[], &permuted)
            .unwrap();
        let writes = database
            .connection
            .prepare("SELECT path FROM position_writes ORDER BY path")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            writes,
            [
                "pages/page-3.md",
                "pages/page-3.md",
                "pages/page-4.md",
                "pages/page-4.md"
            ]
        );
        let order = database
            .connection
            .prepare("SELECT path FROM pages ORDER BY position")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(order, permuted);
        database
            .connection
            .execute("DELETE FROM position_writes", [])
            .unwrap();
        let unchanged_revision = scalar(&database, "SELECT revision FROM query_projection_state");
        database
            .apply_with_source_revisions_aliases_and_page_order(&empty, &[], &[], &permuted)
            .unwrap();
        assert_eq!(
            scalar(&database, "SELECT revision FROM query_projection_state"),
            unchanged_revision
        );
        assert_eq!(scalar(&database, "SELECT COUNT(*) FROM position_writes"), 0);

        let mut replacement = page(5, "DONE", "replacement unchanged order");
        replacement.position = None;
        database
            .apply_with_source_revisions_aliases_and_page_order(
                &PhysicalGraphProjectionChange {
                    replacements: vec![replacement],
                    deletions: vec![],
                    reference_postings: vec![],
                },
                &[PhysicalGraphProjectionSourceRevision {
                    path: "pages/page-5.md".into(),
                    revision: "replacement".into(),
                }],
                &[],
                &permuted,
            )
            .unwrap();
        assert_eq!(scalar(&database, "SELECT COUNT(*) FROM position_writes"), 0);
        assert_eq!(
            scalar(
                &database,
                "SELECT position FROM pages WHERE path = 'pages/page-5.md'"
            ),
            4
        );

        let mut ordinary_replacement = page(6, "DONE", "ordinary replacement");
        ordinary_replacement.position = None;
        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: vec![ordinary_replacement],
                deletions: vec![],
                reference_postings: vec![],
            })
            .unwrap();
        assert_eq!(
            scalar(
                &database,
                "SELECT position FROM pages WHERE path = 'pages/page-6.md'"
            ),
            5
        );
        assert!(database
            .apply_with_source_revisions_aliases_and_page_order(
                &empty,
                &[],
                &[],
                &["pages/page-1.md".into()]
            )
            .is_err());
        assert!(database
            .apply_with_source_revisions_aliases_and_page_order(
                &empty,
                &[],
                &[],
                &["pages/page-1.md".into(), "pages/page-1.md".into()]
            )
            .is_err());
        drop(database);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn mixed_explicit_and_retained_replacement_positions_are_collision_safe() {
        let path = std::env::temp_dir().join(format!(
            "tine-order-mixed-replacement-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        let mut first = page(1, "TODO", "first before");
        first.position = Some(0);
        let mut second = page(2, "TODO", "second before");
        second.position = Some(1);
        database
            .apply_with_source_revisions_aliases_and_page_order(
                &PhysicalGraphProjectionChange {
                    replacements: vec![first, second],
                    deletions: vec![],
                    reference_postings: vec![],
                },
                &[
                    PhysicalGraphProjectionSourceRevision {
                        path: "pages/page-1.md".into(),
                        revision: "before-1".into(),
                    },
                    PhysicalGraphProjectionSourceRevision {
                        path: "pages/page-2.md".into(),
                        revision: "before-2".into(),
                    },
                ],
                &[],
                &["pages/page-1.md".into(), "pages/page-2.md".into()],
            )
            .unwrap();

        let mut first = page(1, "DONE", "first after");
        first.position = Some(1);
        let mut second = page(2, "DONE", "second after");
        second.position = None;
        database
            .apply_with_source_revisions_aliases_and_page_order(
                &PhysicalGraphProjectionChange {
                    replacements: vec![first, second],
                    deletions: vec![],
                    reference_postings: vec![],
                },
                &[
                    PhysicalGraphProjectionSourceRevision {
                        path: "pages/page-1.md".into(),
                        revision: "after-1".into(),
                    },
                    PhysicalGraphProjectionSourceRevision {
                        path: "pages/page-2.md".into(),
                        revision: "after-2".into(),
                    },
                ],
                &[],
                &["pages/page-2.md".into(), "pages/page-1.md".into()],
            )
            .unwrap();

        let order = database
            .connection
            .prepare("SELECT path FROM pages ORDER BY position")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(order, ["pages/page-2.md", "pages/page-1.md"]);
        drop(database);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn incomplete_inventory_is_rejected_before_page_and_source_changes() {
        let path = std::env::temp_dir().join(format!(
            "tine-order-rollback-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        let mut first = page(1, "TODO", "before");
        first.position = None;
        let revisions = [PhysicalGraphProjectionSourceRevision {
            path: "pages/page-1.md".into(),
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
                &["pages/page-1.md".into()],
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
                    path: "pages/page-1.md".into(),
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
                .query_row(
                    "SELECT count(*) FROM pages WHERE position IS NOT NULL",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        drop(database);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn post_write_order_reconciliation_failure_rolls_back_everything() {
        let path = std::env::temp_dir().join(format!(
            "tine-order-post-write-rollback-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        let mut first = page(1, "TODO", "first before");
        first.position = Some(0);
        let mut second = page(2, "TODO", "second before");
        second.position = Some(1);
        let before_revisions = [
            PhysicalGraphProjectionSourceRevision {
                path: "pages/page-1.md".into(),
                revision: "before-1".into(),
            },
            PhysicalGraphProjectionSourceRevision {
                path: "pages/page-2.md".into(),
                revision: "before-2".into(),
            },
        ];
        database
            .apply_with_source_revisions_aliases_and_page_order(
                &PhysicalGraphProjectionChange {
                    replacements: vec![first, second],
                    deletions: vec![],
                    reference_postings: vec![],
                },
                &before_revisions,
                &[],
                &["pages/page-1.md".into(), "pages/page-2.md".into()],
            )
            .unwrap();
        let before_expression = "\"bef\" AND \"efo\" AND \"for\" AND \"ore\"";
        let after_expression = "\"aft\" AND \"fte\" AND \"ter\"";
        let before_fts_rowids = fts_rowids(&database, before_expression);
        assert_eq!(before_fts_rowids.len(), 4);
        assert!(fts_rowids(&database, after_expression).is_empty());
        let before_projection_revision =
            scalar(&database, "SELECT revision FROM query_projection_state");
        database
            .connection
            .execute_batch(
                "CREATE TEMP TRIGGER abort_final_position_reconciliation
                 BEFORE UPDATE OF position ON pages
                 WHEN OLD.position IS NULL AND NEW.position IS NOT NULL
                 BEGIN
                     SELECT RAISE(ABORT, 'test abort during final position reconciliation');
                 END;",
            )
            .unwrap();

        let mut first = page(1, "DONE", "first after");
        first.position = None;
        let mut second = page(2, "DONE", "second after");
        second.position = None;
        let error = database
            .apply_with_source_revisions_aliases_and_page_order(
                &PhysicalGraphProjectionChange {
                    replacements: vec![first, second],
                    deletions: vec![],
                    reference_postings: vec![],
                },
                &[
                    PhysicalGraphProjectionSourceRevision {
                        path: "pages/page-1.md".into(),
                        revision: "after-1".into(),
                    },
                    PhysicalGraphProjectionSourceRevision {
                        path: "pages/page-2.md".into(),
                        revision: "after-2".into(),
                    },
                ],
                &[],
                &["pages/page-2.md".into(), "pages/page-1.md".into()],
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("test abort during final position reconciliation"),
            "{error}"
        );

        let order = database
            .connection
            .prepare("SELECT path FROM pages ORDER BY position")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(order, ["pages/page-1.md", "pages/page-2.md"]);
        let content = database
            .connection
            .prepare(
                "SELECT block_text.content
                 FROM pages
                 JOIN blocks USING (page_id)
                 JOIN block_text USING (block_id)
                 ORDER BY pages.position",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(content, ["first before", "second before"]);
        assert_eq!(fts_rowids(&database, before_expression), before_fts_rowids);
        assert!(fts_rowids(&database, after_expression).is_empty());
        assert_eq!(scalar(&database, "SELECT COUNT(*) FROM search_fts"), 4);
        assert_eq!(
            scalar(
                &database,
                "SELECT COUNT(*) FROM search_fts f
                 LEFT JOIN pages p ON p.page_id = f.rowid
                 LEFT JOIN blocks b ON b.block_id = f.rowid
                 WHERE (p.page_id IS NULL AND b.block_id IS NULL)
                    OR (p.page_id IS NOT NULL AND b.block_id IS NOT NULL)",
            ),
            0,
            "each contentless FTS row must retain exactly one page or block owner"
        );
        let source_revisions = database
            .connection
            .prepare("SELECT path, revision FROM direct_source_revisions ORDER BY path")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            source_revisions,
            [
                ("pages/page-1.md".into(), "before-1".into()),
                ("pages/page-2.md".into(), "before-2".into())
            ]
        );
        assert_eq!(
            scalar(&database, "SELECT revision FROM query_projection_state"),
            before_projection_revision
        );
        assert!(database
            .source_delta(&before_revisions)
            .unwrap()
            .replacements
            .is_empty());
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
                "SELECT COUNT(*) FROM pages p JOIN names n ON n.name_id = p.name_id WHERE n.key = ?1",
                &[PhysicalQueryValue::Text("absent".into())],
            )
            .unwrap();
        assert_eq!(rows, vec![vec![PhysicalQueryValue::Integer(0)]]);

        // Every write shape is refused by SQLite itself.
        for write in [
            "DELETE FROM pages",
            "INSERT INTO pages (page_id, name_id, path, text_kind, journal_day, position, estimated_bytes, property_count)
             VALUES (1, 1, 'x', 0, NULL, NULL, 0, 0)",
            "UPDATE pages SET text_kind = 0",
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
                "SELECT COUNT(*) FROM pages p JOIN names n ON n.name_id = p.name_id WHERE n.key = ?1",
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
                "SELECT p.page_id FROM pages p JOIN names n ON n.name_id = p.name_id WHERE n.key = ?1",
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

        let obsolete_tables = database
            .connection
            .prepare(
                "SELECT name FROM sqlite_master
                 WHERE type = 'table'
                   AND name IN (
                     'materialization_stamp', 'materialization_batches',
                     'block_home_claims', 'logseq_uuid_introductions',
                     'page_name_identity_records', 'portable_path_identity_records',
                     'page_portable_path_claims', 'reference_alias_bindings',
                     'search_fts_outbox', 'search_fts_build', 'refs'
                   )
                 ORDER BY name",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            obsolete_tables,
            Vec::<String>::new(),
            "the Direct projection must not create obsolete Managed tables"
        );

        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: vec![page(1, "TODO", "Needle first")],
                deletions: Vec::new(),
                reference_postings: Vec::new(),
            })
            .unwrap();
        assert_eq!(database.read().tasks(Some("TODO"), 10).unwrap().len(), 1);
        assert_eq!(
            scalar(
                &database,
                "SELECT COUNT(*) FROM search_fts WHERE search_fts MATCH '\"nee\" AND \"eed\" AND \"edl\" AND \"dle\"'",
            ),
            2
        );

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
                deletions: vec!["pages/page-1.md".into()],
                reference_postings: Vec::new(),
            })
            .unwrap();
        assert!(database.read().tasks(None, 10).unwrap().is_empty());
        assert_eq!(
            scalar(
                &database,
                "SELECT COUNT(*) FROM search_fts WHERE search_fts MATCH '\"nee\" AND \"eed\" AND \"edl\" AND \"dle\"'",
            ),
            0
        );
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
            source_page_path: "pages/page-1.md".into(),
            source_entity: PhysicalEntityId::Page("pages/page-1.md".into()),
            source_locator: b"preamble".to_vec(),
            ordinal: 0,
            kind: 0,
            target: PhysicalReferenceTarget::PageName {
                raw_name: raw_name.into(),
                normalized_name: normalized_name.into(),
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
        orphan.source_page_path = "pages/page-2.md".into();
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
                deletions: vec!["pages/page-1.md".into()],
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
                "SCAN d USING COVERING INDEX ",
            ),
            (
                NAVIGATION_ALIASES_AFTER_SQL,
                &[0i64.into(), 0i64.into(), limit.clone()],
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
                "SELECT DISTINCT r.source_page_id, p.path, n.raw, n.key
                 FROM reference_postings r JOIN pages p ON p.page_id = r.source_page_id
                 JOIN names n ON n.name_id = r.target_name_id
                 WHERE r.target_type = 0 AND r.reference_kind <= 4
                   AND (p.path > ?1 OR (p.path = ?1 AND n.raw > ?2))
                 ORDER BY p.path, n.raw, n.key, r.source_page_id LIMIT ?3",
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
                source_page_path: format!("pages/page-{page}.md"),
                source_entity: PhysicalEntityId::Page(format!("pages/page-{page}.md")),
                source_locator: b"preamble".to_vec(),
                ordinal,
                kind: 0,
                target: PhysicalReferenceTarget::PageName {
                    raw_name: raw_name.into(),
                    normalized_name: normalized_name.into(),
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
            source_page_path: "pages/page-1.md".into(),
            source_entity: PhysicalEntityId::Page("pages/page-1.md".into()),
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
        orphan.source_page_path = "pages/page-2.md".into();
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
                deletions: vec!["pages/page-1.md".into()],
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
                .map(|row| row.result_id)
                .collect::<Vec<_>>()
        };

        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.initialize_schema().unwrap();
        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: vec![
                    with_claim(1, "first"),
                    with_claim(2, "second"),
                    page(3, "TODO", "referrer"),
                ],
                deletions: Vec::new(),
                reference_postings: vec![PhysicalReferencePosting {
                    source_page_path: "pages/page-3.md".into(),
                    source_entity: PhysicalEntityId::Block("block-3".into()),
                    source_locator: b"content".to_vec(),
                    ordinal: 0,
                    kind: 6,
                    target: PhysicalReferenceTarget::ExternalUuid { raw_claim: claim },
                }],
            })
            .unwrap();
        assert_eq!(claimant_ids(&database), vec!["block-1", "block-2"]);
        assert_eq!(
            database
                .read()
                .block_referrer_candidates_after(claim, None, 3)
                .unwrap()[0]
                .source_block_id,
            "block-3"
        );
        drop(database);

        let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
        database.validate_schema().unwrap();
        assert_eq!(claimant_ids(&database), vec!["block-1", "block-2"]);
        database
            .apply(&PhysicalGraphProjectionChange {
                replacements: vec![page(1, "DONE", "claim removed")],
                deletions: vec!["pages/page-2.md".into()],
                reference_postings: Vec::new(),
            })
            .unwrap();
        assert!(claimant_ids(&database).is_empty());
        assert_eq!(
            database
                .read()
                .block_referrer_candidates_after(claim, None, 3)
                .unwrap()[0]
                .source_block_id,
            "block-3",
            "raw UUID reference evidence remains resolvable independently of claimant edits"
        );
        database.quick_check().unwrap();
        drop(database);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }
}
