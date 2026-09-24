# Changelog

All notable changes to `tine-storage` are recorded here. The crate's semantic
version describes its Rust API; persistent byte formats are versioned
independently in `src/formats.rs` and summarized in
`FORMAT-COMPATIBILITY.md`.

## Unreleased

## [0.28.2] - 2026-09-25

### Fixed

- `checkpoint_passive_at` empties the WAL once every frame is in the image,
  when that needs no waiting (no write transaction, no reader on the WAL).
  Before, a copied WAL kept its frames until the writer next restarted it; a
  process that exited first left them behind, and the next open copied them
  all again (Tine GH #543: 13-23 s on every reopen after a rename on a hosted
  Windows disk, with the first search waiting on it).

## [0.28.1] - 2026-09-24

Write-side cost of incremental updates (Tine GH #543). A 261-page rename on a
10,000-page graph wrote 603 MB; with Tine's matching changes, 200 MB.

### Added

- `PhysicalGraphProjectionDatabase::begin_turn` and
  `PhysicalGraphProjectionTurn`: several applies committed as one transaction,
  so an index page every apply touches is written to the WAL once.
- `disable_automatic_checkpoints`, `keep_temporary_files_in_memory` and
  `checkpoint_passive_at`: a writer whose commits never checkpoint (and whose
  statement journals stay in memory), and a checkpoint on a connection of its
  own that the caller runs where nothing interactive waits.

### Changed

- The referenced-names navigation reader is driven by `names` in index order
  and probes the postings per name. As a `DISTINCT` join, a real graph's
  statistics made SQLite sort every posting in a temporary B-tree per call
  (2.66 MB of temp file per Tine edit). The plan guard now also runs under a
  real graph's `sqlite_stat1` and rejects any temporary B-tree except the
  alias readers' batch-bounded `DISTINCT`.

## [0.28.0] - 2026-09-24

### Added

- A second contentless FTS5 table, `short_word_fts`, answers one- and
  two-character CJK searches that the trigram index cannot (Tine ADR 0069).
  `PhysicalPage` and `PhysicalBlock` gain an application-owned
  `short_word_tokens` field: space-separated whole tokens, indexed with the
  `ascii` tokenizer so every non-ASCII scalar (combining marks included) stays
  inside its token. An entity whose field is empty writes no row, so a graph
  without such text pays nothing. Rows share the page/block rowids of
  `search_fts` and are written, replaced, deleted and reset with them.

### Changed

- `SQLITE_SCHEMA_VERSION` moves 30 → 31 for the new table. An existing
  projection file is rebuilt from the graph, never reinterpreted.
- Breaking: the two new public fields make exhaustive struct literals of
  `PhysicalPage` / `PhysicalBlock` fail to compile until they set
  `short_word_tokens`.

## [0.27.1] - 2026-09-21

### Changed

- A fresh projection build is about 45% faster on a 10,000-page graph
  (47.5 s to 26.2 s, 600,062 blocks, Linux): the per-row name, page and
  block lookups now go through the prepared-statement cache instead of
  compiling their SQL on every call, `intern_name` looks a name up before
  attempting to insert it, and a fresh append resolves its reference
  postings from the coordinates it has just allocated instead of three
  queries each. No format, schema or API change; the refusals are the same.

## [0.27.0] - 2026-09-20

Completes the compact-projection storage work: schema 30 stores one raw text
copy, fresh rebuilds use an unpublished staged database, and publication moves
the completed file into place without copying its bytes.

### Added

- `PhysicalGraphProjectionDatabase::create_fresh_build` exclusively creates an
  unpublished projection with journal and synchronous writes disabled, and
  `optimize` completes SQLite's bounded post-build maintenance. Any failed
  staged write invalidates that disposable image; normal published writers
  remain WAL/NORMAL with transactional rollback.
- `DurableDirectoryPublication::replace_from_staged_regular_single_writer`
  flushes and atomically installs a same-directory staged regular cache file,
  creating or replacing a regular destination through the existing native
  name-operation and durability policy. It preserves file identity without a
  whole-database byte load or second copy and never removes an installed
  destination on an outcome-ambiguous error.

Unit cost: none per edit. The new path applies only to a whole-projection
rebuild and writes the staged SQLite image once; publication is a metadata
replacement with no reserialization or retained snapshot.

### Changed

- Schema 30 now keeps exactly one raw document-text copy: page preamble in
  `page_text` and block source in `block_text`. `PhysicalPage` and
  `PhysicalBlock` replace the persisted searchable/visible variants with one
  ephemeral `search_tokens` input owned by the application.
- Search postings now live in one contentless `search_fts` table keyed directly
  by the projection's disjoint page/block scalar IDs. It uses case-sensitive
  trigram tokenization with `detail=none`; replacement and deletion remove rows
  by rowid without retaining old token bodies or an owner mapping table.

### Removed

- **Breaking.** `PhysicalGraphProjectionDatabase::set_build_durability`; active projections
  remain WAL/NORMAL, while relaxed durability is confined by construction to
  the unpublished database returned by `create_fresh_build`.
- The unicode61 word index, legacy trigram substring index,
  `search_fts_owners`, and the storage-owned plain-text, fuzzy-subsequence, and
  ranked-search readers. Callers plan candidates through the read-only
  projection query seam and perform exact matching in the application.
- Derived `searchable_text` fields from page/block reads and the stored
  `query_visible` / `query_visible_folded` columns.

Schema identity remains 30 because this is the same unreleased private rebuild.
Unit cost: one contentless posting delete/insert for each entity on a replaced
page; no stored token text and no graph-wide reindex on edit.

## [0.26.0] - 2026-09-19

Completes the compact-projection P1 deliverable: the public surface is what
Tine's Direct Files path uses, plus the named keeps.

### Removed

- 35 public methods on `PhysicalProjectionQuerySnapshot`,
  `PhysicalGraphProjectionDatabase` and `SqliteGraphProjectionRead` that no
  Tine production code calls (the `*_with_header_validation` family, the
  `pages_by_*` lookups, `open_managed`, the `apply_with_*` variants, the
  source-revision reuse path, `query_block_preorder`, `ensure_stamp`,
  `search_index_building_horizon`, `build_durability_relaxed`, …) together
  with the row types and helpers only they reached (1,224 lines). 0.25.0 had
  removed the unreachable modules; the crate-level dead-code lint cannot see
  an unused `pub` method, so this pass was made against Tine's actual imports.
- Five tests that exercised only removed methods.

### Unchanged

- `api.txt` (52 exported names): every removed method lived on a type that
  stays exported. `SQLITE_SCHEMA_VERSION` = 29 and `SQLITE_APPLICATION_ID`
  are unchanged: no projection rebuild. The exported names Tine does not
  reference directly are the named keeps (the two SQLite identity constants),
  the crate's own certification tooling (`api_surface`, `formats`), and types
  that appear in the signatures of methods Tine does call.

## [0.25.0] - 2026-09-19

Continues the patch line Tine pins (v0.24.0). The crate is now the Direct Files
durability and projection crate only; this is the compact-projection campaign's
P1 packet (Tine ADR 0066 removed Managed Storage on 2026-09-15).

### Removed

- **Breaking.** The Managed Storage spine: the frontier-stamped SQLite database
  (`PhysicalSqliteDatabase`, `StoredFrontier`, `PhysicalApplyRequest`,
  `SqliteMaterializedRead`, the apply/preflight/terminal-construction family),
  the SQLite file set and checkpoint fingerprints, the projection query
  progress types, local journals v1 and v2, durable batches, digest-sealed
  payloads, the sealed accepted-history index (`sealed_accepted_index`), the
  managed layout vocabulary, and every `test-support` seam that fed them.
  Nothing in Tine imported any of it: the deletion is a reachability census
  from Tine's imports, verified by rustc's dead-code lint at a fixed point,
  and `api.txt` now lists exactly the 52 names Tine's production and test
  code reaches. `formats::FORMAT_MANIFEST` keeps two rows,
  `SQLITE_APPLICATION_ID` and `SQLITE_SCHEMA_VERSION`; the Direct projection
  DDL and schema version 29 are untouched, so no projection file is rebuilt.
  Also removed from the facade: row types and reads Tine never named
  (`PhysicalBlockHomeClaim`, the identity-record and UUID-introduction rows,
  `PhysicalSearchIndexStatus`, `query_block_preorder`, …) and the unused
  filesystem re-exports (`open_dir_nofollow`, `publish_immutable_exact`,
  `ExactImmutablePublicationBatch`, …). The `test-support` feature stays
  declared, and empty, so a consumer manifest that enables it still resolves.
- The `fs2` dependency.

## [0.24.0] - 2026-09-17

Continues the patch line Tine pins (v0.20.3). The sealed-history work on `main`
already took 0.21–0.23, so those numbers are skipped rather than reused, and the
change below is breaking — for a `0.x` crate `cargo semver-checks` requires the
MINOR to move, which is why this is not v0.20.4.

### Changed

- **Breaking.** `navigation_reference_names_after` returns one row per distinct
  spelling graph-wide instead of one per (page, spelling). Its cursor becomes
  `Option<(&str, &str)>` — `(normalized_name, raw_name)` — and
  `PhysicalNavigationReferenceNameRow` drops `source_page_id` and `owner_path`;
  the one consumer folds the rows by name and read neither field. The statement
  also drops its `pages` join. With the index below, draining every spelling on
  a 10,000-page graph (1.2M postings) goes **1.276 s → 0.022 s**, 110,000 rows
  in 215 batches → 10,010 in 20, and the plan is `SCAN`/`SEARCH … USING COVERING
  INDEX` with no join and no temp B-tree for either the `DISTINCT` or the
  `ORDER BY`. The returned name set is unchanged, verified by set comparison
  rather than row counts (GH tine#543).

### Added

- `reference_postings_navigation_names_idx` on
  `reference_postings(normalized_name, raw_name, reference_kind)
  WHERE target_type = 0`, which covers that read entirely. Column order is
  load-bearing: putting `reference_kind` first puts a range predicate ahead of
  the sort keys, costs `USE TEMP B-TREE FOR ORDER BY`, and measures 1.765 s —
  slower than no index at all.
  Unit cost: 88.4 MB on a 1.64 GB projection (5.4%, measured with `dbstat`, not
  by differencing a vacuumed file), ~1.8 s to build. Per edit, the
  `reference_postings` replacement step of a save costs +0.007 ms on a
  1-posting page and +0.057 ms on a 60-posting page (interleaved A/B/A/B, 60
  cycles per arm).

### Changed (schema)

- `SQLITE_SCHEMA_VERSION` moves 28 → 29 for the index above, so every existing
  projection is refused and rebuilt once on first open after the update.
  Martin accepted that cost explicitly on 2026-09-17 while the projection
  schema is still being tuned; it is not a precedent for adding an index
  casually once it settles.

## [0.20.3] - 2026-09-17

Patch line from v0.20.2, the revision Tine pins; the sealed-history work on
`main` (0.21–0.23) is not included.

### Added

- `PhysicalGraphProjectionDatabase::set_build_durability(relaxed)` and
  `build_durability_relaxed()`: a bulk build can run its per-batch commits
  under `PRAGMA synchronous = OFF` and restore `NORMAL` before it closes.
  The projection is a disposable cache; an application crash leaves WAL mode
  consistent, and a torn file after power loss is what `quick_check` already
  rebuilds. Tine's warm stream at 10,000 pages is hundreds of commits and
  checkpoints whose fsyncs are pure waiting on Windows (GH tine#543).
  Unit cost: none per edit; single-page deltas keep `NORMAL`.

## [0.20.2] - 2026-09-17

Patch line from v0.20.1, the revision Tine pins; the sealed-history work on
`main` (0.21–0.23) is not included.

### Fixed

- `navigation_reference_names_after` and `navigation_aliases_after` page on
  the columns of an existing index (`reference_postings_normalized_name_idx`,
  `reference_alias_declarations_source_idx`) with a row-value keyset, so each
  batch is one bounded index range. They ordered by the joined page path,
  which no index serves: every 512-row batch scanned and sorted the whole
  join, O(N²) over the graph. Tine drains the reference names on every
  launch; at 10,000 pages (600,000 postings, 110,000 distinct rows) that was
  215 batches × 1.3 s and ~11 GB of reads before search would answer
  (GH tine#543). Measured on that projection: full drain 65 s → 1.3 s.
  Signatures and the cursor tuple are unchanged; only the order of rows
  changes, and the one consumer builds an order-independent set.

### Added (test-only)

- `paged_navigation_readers_use_an_index_range` pins the plan shape of the
  four navigation readers (`EXPLAIN QUERY PLAN`: an index range or an
  index-ordered scan, never a whole-result sort), with the v0.20.1 query as
  the rejected counterexample; `navigation_reference_names_page_without_gaps_or_repeats`
  drains at batch sizes 1–3 against the unbounded read.

### Unit cost

- No persisted record or index changes; the schema text is identical.

## [0.20.1] - 2026-09-17

Patch line from v0.20.0, the revision Tine pins; the sealed-history work on
`main` (0.21–0.23) is not included.

### Added

- `PhysicalGraphProjectionDatabase::set_page_cache_budget`,
  `shrink_page_cache_budget`, `page_cache_budget` and
  `MIN_PAGE_CACHE_BUDGET_BYTES`: the consumer sizes the writer's SQLite page
  cache to a bulk build and hands the memory back after the commit (GH
  tine#543 — at SQLite's ~2 MiB default a 600,000-block build spilled and
  re-read every dirty page: 8.8M cache misses, 27 GB read for 49 MB of
  Markdown; the same build at 512 MiB took 8,896 misses).
- `PhysicalGraphProjectionDatabase::last_apply_deferred_indexes` reports which
  route the most recent apply took.

### Changed

- An `apply*` into an empty graph projection (fresh, rebuilt, or just
  `reset()`) drops the 35 secondary indexes before its rows and recreates
  them from the same DDL before the commit, inside the one existing
  transaction — an external sort instead of a random leaf write per row. The
  committed schema text is identical; a rollback restores the indexes; an
  apply into a populated projection is unchanged. Measured (`gh543_build_probe`,
  release, Linux): 13% faster and 4.6% smaller at 10,000 pages, 20% fewer
  cache misses under a tight budget.

### Unit cost

- No persisted record changes. The build's page-cache working set is ~12
  bytes per byte of projected text (the knee of the miss curve in
  `gh543_build_probe`: 1,000 pages / 5.1 MB text — 2 MiB: 445k misses,
  32 MiB: 14k, 64 MiB: 13k, 512 MiB: 1); the projection file itself stays
  ~32× its text.

## [0.20.0] - 2026-09-08

### Added

- Shared process-local projection progress targets, cancellable fixed-target
  waits and check/wait observations, with explicit pending/failure/recovery.

- Snapshot `query_revision()` and schema 28 transactional local projection image
  revision metadata, distinct from saved-edit coverage and authority frontiers.

- Disposable SQLite schema 28 adds a partial parent-first covering index for
  bounded subtree completeness validation. Earlier projections must be rebuilt;
  authority formats are unchanged. The Direct metadata compatibility marker also
  advances to 28 so previous readers reject the newer disposable cache.
- `query_page_results` stores the page construction estimate and property count
  from existing page inputs, allowing bounded page Display payload reads.

- Fixed reader/snapshot `tine_query_rank` callback registration for application
  compiled matching with lossless BLOB ranking keys and NULL nonmatches. Exact
  text, owned snapshot consistency, cancellation and error release retain the
  existing read-only boundary. No schema or authority format changes.

## [0.19.0] - 2026-09-08

### Changed

- The one shared authenticated map now keys entries by bounded canonical key
  bytes supplied by the domain owner instead of fixed 16-byte identifiers.
  `sealed_accepted_index::AuthenticatedMapKey` holds 1..=48 bytes
  (`formats::MAX_AUTHENTICATED_MAP_KEY_BYTES`), is `Copy`/`Eq`/`Hash`, orders
  lexicographically over its meaningful bytes, and serializes canonically with a
  validated length; its fixed-width buffer never affects equality, ordering,
  hashing or the bytes written. `From<[u8; 16]>` keeps every existing 16-byte
  identity expressible, so the batch, status, causal-clock and causal-tip maps
  and every UUID-typed API (`status`, `causal`, `causal_clock_counter_digest`,
  `AcceptedStatusRecordV2`, `CausalTipRecordV2`, the accepted-sequence tree,
  `SealedAcceptedIndexRootsV2`, `PhysicalCheckpointGenerationBinding`'s covered
  root keys) are unchanged. `map_value`, `upsert_map` and `remove_map` take
  `impl Into<AuthenticatedMapKey>`.
- The SQLite document frontier carries the same full keys.
  `PhysicalFrontierDocument.document_id: [u8; 16]` becomes
  `document_key: AuthenticatedMapKey`; `PhysicalFrontierRoot` and
  `PhysicalCheckpointFrontierRoot` widen `document_map_root_key` only — their
  batch-map root fields stay `[u8; 16]`. `frontier_document` takes an
  `AuthenticatedMapKey`. `frontier_documents` remains one table with one node
  kind: no second tree, no second serializer, no hash adapter, and no raw SQL
  surface is widened.

### Persistent format

- `SEALED_ACCEPTED_INDEX_SCHEMA_VERSION` and
  `SEALED_ACCEPTED_MAP_NODE_SCHEMA_VERSION` 2 -> 3; `SQLITE_SCHEMA_VERSION`
  26 -> 27; new manifest row `MAX_AUTHENTICATED_MAP_KEY_BYTES` = 48
  (`WriterBound`).
- The shared node digest length-frames every key (`length ‖ bytes`) under the new
  `tine/oplog/authenticated-map/v2/node` domain, which makes its preimage
  injective for arbitrary caller key spaces rather than imposing an undocumented
  prefix-free obligation on callers. `authenticated_map_priority` keeps its raw
  v1 fold, so existing 16-byte maps keep their treap shape; their node digests
  and derived roots are rebuilt. `authenticated_map_empty_digest` is unchanged.
  `accepted_causal_record_digest` frames its clock-root key for the same reason.
- **Old-data behavior:** no migration and no dual reader. Sealed checkpoint
  generation directories and SQLite projection caches are pre-0.7 private derived
  state; an old sealed generation is backed up and rebuilt, and an unrecognized
  SQLite `user_version` or DDL census is refused by `validate_schema_and_claim`
  so the caller rebuilds the disposable projection from the oplog. This crate's
  derived golden vectors changed and were regenerated in the same change.

## [0.18.0] - 2026-09-08

### Changed

- The existing durable batch contract now supplies `CausalPeerKey` independently
  from `DeviceId`. `CausalPeerId::from_key` and `key` replace the device-specific
  accessors. Products can preserve enrolled device authority while minting a new
  sequential writer incarnation after actual private-state loss, without reusing
  an earlier causal dot. Allocation and accepted ownership remain product policy;
  the shared codec and physical encoding are unchanged.

## [0.17.0] - 2026-09-07

### Added

- `SealedAcceptedIndexWriter::remove_map` removes a live authenticated-map entry
  with path copying and the existing canonical priority order. Historical roots
  remain readable; absent-key removal publishes no nodes. Search and subtree-join
  paths bound work independently of retained history. This supplies active-roster
  retirement without a second map implementation in Tine. Persistent formats and
  existing APIs are unchanged.

## [0.16.0] - 2026-09-06

### Added

- Owned read-only query snapshots with in-transaction Managed frontier checks,
  Direct acquisition guards, streaming bound-parameter reads, sticky cancellation
  and SQLite interrupts. Selection and payload reads share one snapshot while
  WAL writers continue. Errors, completion and drop release the read transaction.
  Fixed regex-ID callbacks retain the application's existing compiled semantics.
- Schema 26 query result metadata and own-reference facts, shared preorder and
  byte-estimate helpers, and Direct session page positions. Metadata is produced
  in the page transaction and explicitly removed on replacement, deletion and
  reset even with foreign keys disabled. No raw text duplication or authority
  input format change. Ordered Direct inventories reconcile in the existing
  transaction without rewriting unchanged pages or unchanged order metadata.
  A Direct source-table shape marker also forces older readers to reject the
  newer disposable schema. Query reads reject malformed UTF-8 rather than
  silently substitute result text. Tine result-reader rollout follows separately.

## [0.15.0] - 2026-09-06

### Changed

- SQLite projection schema 25 keeps query/structural rows narrow and stores
  document text in keyed `page_text` and `block_text` tables. Typed reads,
  incremental replacements and both search indexes preserve their existing
  semantics. Previous disposable projections rebuild through the existing app
  lifecycle; no old-schema reader or migration is added.

## [0.14.0] - 2026-09-05

### Added

- `block_planning`, holding each block's `[#A]` / `SCHEDULED:` / `DEADLINE:`
  facets INDEPENDENTLY of its task marker, with the five lookup indexes
  `(priority, page_id, block_id)`, `(scheduled_day, ...)`, `(deadline_day,
  ...)`, `(scheduled, ...)` and `(deadline, ...)`. `tasks` cannot answer these
  questions: a row is written there only under a marker, so a markerless
  `SCHEDULED:` block is absent from it entirely. The `*_day` columns hold the
  `yyyymmdd` ordinal and are NULL when the timestamp text is not a calendar
  day, so presence survives a malformed date -- which is why the two presence
  indexes exist beside the two day indexes rather than being folded into them.
  `PhysicalBlock` carries `planning: Option<PhysicalPlanning>`.
- `blocks.query_visible` and `blocks.query_visible_folded`: the block's exact
  visible text and that text canonically folded. `searchable_text` cannot serve
  the query engine because both producers collapse whitespace in it for the
  existing search consumers, and a content predicate has to be able to tell
  `a  b` from `a b`. Those columns and their FTS are unchanged.
- `tags.tag_key`, the page-name key `tag('x')` compares on, supplied by the
  caller because this crate does not know Tine's page-identity normalization.
  `PhysicalPage`/`PhysicalBlock` now carry `Vec<PhysicalTag>` rather than
  `Vec<String>`.
- `pages.journal_day` plus `pages_journal_day_idx`: the `yyyymmdd` ordinal of a
  journal page, NULL for every other page.
- `PhysicalProjectionQueryReader`, a read-only statement seam over the graph
  projection: `run_projection_query(sql, &[PhysicalQueryValue])` and
  `explain_query_plan(sql, &[PhysicalQueryValue])`. Raw SQL crosses this
  boundary; authority does not. The projection is a disposable cache derived
  from the oplog, so a malformed statement fails a read and can never corrupt
  truth -- which is precisely why the projection may have a statement seam
  while the oplog, the frontier and the Markdown/Org tree keep their curated
  typed boundaries and must never gain one.
  The restriction is the ENGINE's: the type owns a connection opened
  `SQLITE_OPEN_READ_ONLY` and no constructor accepts an existing writable
  handle, so it cannot be reached from one. There is deliberately no SQL-text
  parser or "single SELECT only" check -- SQLite already refuses every write
  through a read-only connection, and a redundant text check would be a runtime
  refusal with no in-scope failure to name that could also reject a legitimate
  statement. Values travel as bound parameters in the signature, so an
  interpolated statement is not expressible. `explain_query_plan` binds the same
  parameters as the query it explains, because with `sqlite_stat4` present an
  unbound explain can report a plan for a statement the caller never runs.

### Changed

- `tags_lookup_idx` moves from `(tag, ...)` to `(tag_key, page_id, owner_type,
  owner_id)`. A case-insensitive tag probe cannot search an index led by the
  original spelling.
- SQLite projection schema 23 -> 24, and `PhysicalBlock`/`PhysicalPage` change
  shape. As with every prior schema change there is no older-schema reader and
  no migration: an unrecognized store is preserved as a backup and rebuilt from
  the untouched Markdown/Org tree.

## [0.13.0] - 2026-09-05

### Added

- `block_path_refs` and `property_atoms`, the two derived materialization
  tables Tine's query engine needs, with their six lookup indexes. The physical
  layer stores what it is handed and never atomizes: `PhysicalBlock` carries
  `path_refs`, and `PhysicalBlock`/`PhysicalPage` carry `property_atoms`
  (`PhysicalPropertyAtom`), keyed `(owner_type, owner_id, normalized_name,
  ordinal)` `WITHOUT ROWID`.
- `materialization_stamp.parse_config_hash`: the digest of the graph parse
  config the derived rows were lowered under. `initialize_schema` stamps it and
  `stamped_parse_config_hash` reads it back, so an open route can tell a stale
  projection from a damaged one.
- Test-support seams `materialization_row_digests_by_table_for_test` and
  `seed_terminal_chunk_for_test`, which let a consumer compare a genesis-built
  and a delta-built store table by table.

### Changed

- SQLite projection schema 22 -> 23. There is no older-schema reader and no
  migration: an unrecognized store is preserved as a backup and rebuilt from
  the untouched Markdown/Org tree.
- `PhysicalBlock`, `PhysicalPage`, `PhysicalMaterializationChange`,
  `PhysicalGraphProjectionChange` and `PhysicalTerminalMaterializationChunk` no
  longer derive `Eq`; a property atom carries an optional `REAL`, and `f64` is
  not `Eq`. `PartialEq` is unchanged.

## [0.12.2] - 2026-09-02

### Fixed

- Immutable certification receipts now name the required macOS and iOS compile
  jobs alongside Linux, Windows, Android, and API/semver certification. The
  v0.12.1 code passed those jobs, but its published receipt omitted their names.

## [0.12.1] - 2026-09-02

### Fixed

- Package-store recovery now classifies a package as torn only when a required
  regular file is missing, preserving complete packages that contain unrelated
  extra entries. Crash-cut tests now pin the authoritative publish and reclaim
  boundaries.
- The package-store second-writer test is now named for the sequential refusal
  it actually proves; the attempted two-process harness was not deterministic
  enough to become certification evidence.
- Linux and Windows directory moves now reuse their certified no-replace and
  write-through name-operation bodies, with source guards against duplication.
- Certification now compiles both the macOS and iOS Apple publication arms in
  addition to the existing Linux, Windows, and Android gates.

## [0.12.0] - 2026-09-02

### Added

- Added an audited immutable-package protocol with durably flushed staged
  files, five-target no-clobber whole-directory publication, exact-byte
  idempotent retries, retire-then-reclaim removal, and reopen recovery for
  staged, retired, and incomplete package residues.

## [0.11.0] - 2026-08-31

### Fixed

- iOS now uses the same no-clobber hard-link publication and interrupted-move
  recovery as macOS, fixing page creation and Direct Files saves on iPhone and
  iPad.

### Removed

- Removed the unused scratch store and obsolete engine-history layout
  vocabulary after Tine retired their final production consumers. Managed
  Storage remains pre-0.7 with one current private format: unrecognized state
  is preserved as a backup and rebuilt from Markdown/Org rather than migrated.

## [0.10.0] - 2026-08-31

### Removed

- Removed the unused generic Patricia index, packed Patricia publication,
  test-only head-transition API, and their obsolete private-layout vocabulary
  after Tine retired its final consumers. This is an API and private-format
  break on the pre-1.0 line: Tine preserves unrecognized Managed Storage state
  as a backup and rebuilds its one current representation from Markdown/Org.
- Removed the remaining zero-consumer pre-0.7 layout vocabulary for detached
  bootstrap publication, resume points, promoted-runtime state, projection-work
  indexes, reconciliation, shadow/migration staging, legacy lazy-genesis packs,
  and their obsolete claims, proofs, receipts, and temporary names. Current
  engine history, clean source capture, enrollment, journal, projection-receipt,
  scratch, SQLite, and provider layouts remain unchanged. As above, Tine backs
  up unrecognized Managed Storage state and rebuilds from Markdown/Org rather
  than carrying a migration for unreleased private layouts.

## [0.9.2] - 2026-08-31

### Fixed

- Repeated durable publications on Windows now reuse the successful
  write-through capability proof for the same retained directory identity.
  Each open still validates its own no-follow directory capability, while
  ordinary Direct Files saves no longer create and retire four probe files
  before every authority update. The process cache is bounded; uncached
  directories retain the conservative prove-on-open behavior.

## [0.9.1] - 2026-08-31

### Added

- `DurableDirectoryPublication::move_exact_no_replace` durably moves a
  caller-owned staged or recovery file to a previously absent same-directory
  name. On Windows it uses the already certified write-through name operation;
  on every platform it verifies exact bytes, preserves no-replace races, and
  supports an idempotent retry after the source name has disappeared.

## [0.9.0] - 2026-08-30

### Added

- SQLite frontier APIs now expose an explicit checkpoint-generation binding,
  anchor, and candidate frontier plus read-only hot-plus-sealed membership,
  authentication, and causal-containment entry points. Missing covered objects
  remain corruption; the injection seam cannot publish or enumerate sealed
  history.
- The one current SQLite schema includes the checkpoint-generation anchor table
  used by a separately constructed candidate while keeping active
  `applied_batches` and materialization rows tail-relative.

### Changed

- The SQLite schema advances to 22 as a pre-0.7 blank-slate change. The crate
  implements no older schema reader, compatibility fixture, or migration path;
  Tine preserves unrecognized private state and rebuilds from Markdown/Org.
- A checkpoint candidate is a separate disposable file under the same current
  schema. This release adds no production checkpoint marker, selector, or
  cutover path, and a regression proves candidate construction never mutates
  the live file.
- Removed the obsolete public legacy-journal inspector and its compatibility
  error. Pre-0.7 Managed Storage has one current journal format; Tine backs up
  and rebuilds unrecognized private state from Markdown/Org.

## [0.8.13] - 2026-08-30

### Fixed

- Certification now checks Rust API compatibility against the latest published
  storage release rather than an uncertified tag left by a failed attempt.

## [0.8.12] - 2026-08-30

### Fixed

- The API inventory now records the new `sealed_accepted_index` facade without
  adding a variant to its pre-existing public exhaustive `ExportPath` enum, so
  the accepted-index addition remains a semver-compatible patch release.

## [0.8.11] - 2026-08-30

### Added

- A public `sealed_accepted_index` module now owns the frozen V1 authenticated
  map algorithm and the canonical V2 accepted-status, acceptance-sequence, and
  causal-record formats used by checkpoint generations. Its shared reader and
  writer cross-check the one-based sequence, status, batch-map, causal, and
  caller-decoded exact-evidence bindings without depending on Tine engine types
  or a physical filesystem layout.

### Changed

- SQLite's existing accepted-frontier validation and Tine's clean/scratch
  derivations now import the shared V1 map and causal primitives. Golden fixtures
  prove the refactor is byte-, root-, and causal-address-identical to prior
  releases.

## [0.8.10] - 2026-08-29

### Changed

- Disposable SQLite projections now use WAL `synchronous=NORMAL`, and fresh
  schema construction is one atomic transaction. Accepted history remains the
  recovery authority; explicit checkpointing and atomic file-set publication
  establish durable projection snapshots without a sync for every cache
  transaction or DDL statement.
- Managed terminal projections now publish before either FTS family is built.
  A bounded, crash-resumable background builder catches up live edits through
  a transaction-local outbox and flips one readiness marker atomically. Ready
  projections maintain both the Unicode and trigram indexes by entity delta,
  so a one-block edit no longer rewrites the page's complete search surface.

### Fixed

- Exact immutable publication batches on Linux and Android now keep final names
  absent until all staged bytes are durable, then install no-replace and flush
  every distinct destination directory before reporting completion. Interrupted
  retries verify exact existing winners, Android retains its capability-refusal
  fallback, and abandoned or raced staged files leave no temporary residue.

## [0.8.9] - 2026-08-23

### Added

- Logical page-name point lookups can return the same lightweight navigation
  rows as namespace seeks, avoiding page-body reads during rename planning.

## [0.8.8] - 2026-08-23

### Added

- SQLite materialized reads can seek and paginate one logical page namespace
  through the existing `(name_key, page_id)` index, without enumerating the
  complete page inventory or loading page bodies.

## [0.8.7] - 2026-08-23

### Added

- Adaptive authenticated-tree traversals can hold one scratch page-file read
  session while discovering child nodes. Every page retains its canonical
  decode, digest, binding, and accounting checks, without repeating the file
  lock, append-buffer flush, and end-position refresh for every immutable node.

## [0.8.6] - 2026-08-22

### Fixed

- Android app-private journal-v2 segment, frontier, and selector publication
  can use the existing sole-writer atomic-rename fallback when hard links are
  unavailable, while strict shared/provider publication remains unchanged.

## [0.8.5] - 2026-08-15

### Fixed

- Android app-private single-writer immutable publication now falls back from
  a denied hard-link installation to an ordinary same-directory atomic rename
  after proving the target name is absent. Shared/provider publication retains
  the strict no-replace protocol.

## [0.8.4] - 2026-08-15

### Fixed

- Android immutable publications now retain exact byte verification and file
  synchronization when the platform refuses directory synchronization. Real
  I/O failures remain fatal, while managed-storage edits no longer enter a
  retry loop after an otherwise successful no-replace manifest publication.

## [0.8.3] - 2026-08-15

### Added

- Fresh SQLite rebuilds can now seed the exact sparse terminal overlay of a
  lazy-genesis frontier. Immutable baseline documents remain outside SQLite,
  while accepted history and changed-document frontier rows are reconstructed
  without pretending that the sparse map contains every logical document.

## [0.8.2] - 2026-08-14

### Added

- SQLite frontiers now support an immutable lazy-genesis baseline: baseline
  dependencies remain in the external pack while SQLite stores only later
  accepted-document overlays. Sequence-zero installation and the first edit of
  an existing baseline document therefore require neither copied baseline rows
  nor a fabricated accepted batch.

## [0.8.1] - 2026-08-14

### Added

- A fresh SQLite candidate can atomically install an authenticated
  sequence-zero genesis frontier and its document map without fabricating an
  accepted bootstrap batch. The primitive refuses nonempty history and is the
  physical foundation for Tine's operation-free managed-storage activation.

## [0.8.0] - 2026-08-14

### Changed

- SQLite schema 20 preserves every block that claims the same external Logseq
  UUID and exposes those claimants through one bounded, canonical multi-row
  read. Ambiguous source graphs are now application-visible input rather than
  a projection-construction failure or an arbitrary physical owner.
- The same schema retains append-only block-ID/home-document claims derived
  from accepted history, including their accepted batch and optional causal
  dot. Deleting a live block no longer erases the evidence needed to classify
  sequential versus concurrent identity reuse; all candidates are exposed
  through a bounded canonical read.
- The disposable projection now also retains application-owned causal
  page-name and portable-path ownership records behind bounded SQLite point
  reads, plus append-only external-UUID introductions with baseline or
  accepted-batch provenance. These tables are the physical replacement for
  Tine's custom Patricia identity indexes; storage does not interpret their
  domain semantics.
- The former singular `block_by_logseq_uuid` API is replaced by
  `blocks_by_logseq_uuid(logseq_uuid, limit)`. Callers must classify ambiguity
  explicitly.

## [0.7.0] - 2026-08-14

### Changed

- SQLite schema 18 makes reference postings and aliases ordinary
  parser-derived projection facts. The disposable database now has one
  frontier stamp and one regime-neutral apply path instead of retaining an
  unused second reference-catalog authority.

### Removed

- Removed the authenticated reference-catalog change, stamp, coverage, and
  terminal-catalog APIs, together with their three authority tables and five
  indexes. Direct Files and managed storage now feed the same physical
  reference projection surface.

## [0.6.5] - 2026-08-14

### Added

- Managed materialization changes can now publish parser-derived reference
  postings, aliases, and portable-path claims directly in the accepted-frontier
  transaction. These are disposable current-state facts and no longer require
  clients to manufacture a second authenticated reference-catalog authority.

### Fixed

- Ordinary parser-derived page replacements now refresh the affected alias
  candidate bindings in the same transaction as their declarations. Alias
  navigation no longer waits for a terminal rebuild after an edit.

## [0.6.4] - 2026-08-14

### Added

- The certified managed-storage layout now names the content-addressed lazy
  genesis archive and its manifest, commit, catalog-update, and segment files.
  This is the physical publication vocabulary for Tine's new activation
  format; authority and semantic validation remain owned by Tine core.
- The disposable SQLite graph projection now retains caller-derived portable
  path keys as a bounded, non-unique page-candidate index. Direct Files and
  managed storage can share path-identity lookup while preserving every
  case/Unicode-equivalent conflict for application-level diagnosis.
- Page home-document ownership is now a bounded, non-unique candidate lookup,
  so duplicate CRDT-home claims remain diagnosable without a handwritten
  application index.

### Changed

- The disposable SQLite projection schema is now 17. Older projections rebuild
  from authoritative graph/history input rather than being reinterpreted.

## [0.6.3] - 2026-08-14

### Changed

- Fresh terminal bootstrap now builds ordinary SQLite secondary indexes once
  after inserting the complete row set. The exact normal schema is restored
  inside the unpublished candidate transaction before its authenticated stamp
  advances; Direct Files, ordinary managed updates, and reopen behavior are
  unchanged.

## [0.6.2] - 2026-08-14

### Fixed

- Private Patricia construction now divides a sorted update range larger than
  one resident bulk sink into consecutive canonical bulk publications. Dense
  reference indices no longer fall back to hundreds of thousands of loose
  per-key immutable files merely because the complete range exceeds one
  bounded construction buffer.
- Packed Patricia catalogs admit up to 128 MiB of exact immutable pack data.
  This remains a hard decode and construction bound, while covering the
  derived reverse-reference index of a representative 130,000-block graph
  without forcing the otherwise valid tail into loose per-node files.

## [0.6.1] - 2026-08-13

### Added

- Regime-neutral graph-projection changes can transactionally replace the
  parser-derived aliases owned by each replaced page. Replacement, deletion,
  and reopen now leave no stale alias declarations in the disposable SQLite
  projection.
- The complete managed-storage path vocabulary is now exported through the
  certified format manifest and pinned to the exact pre-migration Tine values.
  This changes ownership and reviewability only; no persisted path changes.

## [0.6.0] - 2026-08-13

### Added

- Regime-neutral graph-projection changes can transactionally replace the raw
  page-reference spellings owned by each replaced page. Direct Files can now
  populate the same disposable `reference_postings` facts and bounded
  navigation-name read used by managed storage, without a second application
  cache or a whole-graph referenced-name scan.
- Disposable projections now maintain a SQLite trigram index and expose
  bounded page candidates for exact normalized literal-substring matching,
  including matches in the middle of tokens. One- and two-character needles
  use the bounded page inventory; the application parser remains the final
  semantic matcher for every query.
- Disposable projections expose bounded page candidates for Unicode-normalized
  ordered-subsequence fuzzy matching. This is a candidate superset: the
  application parser remains the final owner of fuzzy semantics and ordering.

### Changed

- The disposable SQLite projection schema is now 16. Older projections rebuild
  from authoritative graph/history input rather than being reinterpreted.

## [0.5.0] - 2026-08-13

### Added

- Disposable SQLite projections can return bounded task-candidate structural
  locators without copying block content or public UUIDs. Applications that
  already retain the exact parser document can recover and identity-check the
  corresponding parser block while managed storage keeps the existing full-row
  physical API.
- Standalone disposable projections can transactionally retain each page's
  caller-owned source revision and report only changed, missing, or deleted
  pages on reopen. Direct Files can therefore reuse exact SQLite facts after a
  clean restart and lower only externally changed parser documents; these
  revisions are adapter metadata and do not alter managed-storage formats.

### Fixed

- A refused legacy-journal recovery scan now explicitly releases its retained
  writer lock before returning, so an immediate retry reports the original
  corruption instead of an intermittent false “already open” refusal.

## [0.4.0] - 2026-08-13

### Added

- A regime-neutral disposable SQLite graph projection with one typed page
  replace/delete transaction and the existing bounded page, block, task,
  property, reference, and search reads. Direct Files and managed storage can
  now feed the same physical graph-fact tables without exposing an oplog
  frontier or raw SQLite connection to Direct Files.

### Changed

- Managed materialization delegates its graph-row work to the same extracted
  projection engine while retaining accepted-frontier stamps and reference
  authentication in its managed-only adapter. The on-disk managed format is
  unchanged.

## [0.3.1] - 2026-08-13

### Fixed

- Android immutable bootstrap batches now fall back from unavailable
  filesystem-wide synchronization to exact per-file synchronization, while
  retaining strict failure for ordinary I/O errors.

## [0.3.0] - 2026-08-10

### Added

- Bounded SQLite reads for exact-marker task-candidate blocks and block-only
  structure. Candidate pagination seeks by `(page_id, block_id)` and returns
  only the raw block/page fields needed for application-owned parsing;
  structural point reads exclude content, search text, and public UUIDs.

## [0.2.0] - 2026-08-10

### Added

- Local-journal protocol v2 with a checksummed segment identity and an ordered,
  separately durable frontier. A returned append is selected exactly once;
  an unreturned physical suffix is discarded on reopen without weakening a
  previously committed frontier.
- A typed durable-directory publication API for exact create, replacement, and
  authority retirement. Windows proves the capability in an owned namespace
  and uses `MoveFileExW` write-through publication with exact byte and file
  identity verification.

### Changed

- Legacy v1 journal rollover now inspects ambiguous suffixes without mutating
  them before a migration decision.
- The ordinary Patricia certification suite separates a 4,096-record semantic
  differential from a 96-record physical publish/reopen proof; the complete
  4,096-record physical journey remains a required release burn-in.
- The Rust API intentionally adds variants to exhaustive storage and journal
  error enums. This requires the `0.2.0` compatibility boundary.

## [0.1.1] - 2026-08-10

### Fixed

- Local-journal recovery now preserves the segment and fails closed when a
  fully sized final frame fails validation or a damaged length field makes its
  extent beyond EOF ambiguous. Only a byte tail too short to contain any
  complete frame is truncated, preventing corruption from silently discarding
  a previously durable commit.

## [0.1.0] - 2026-08-10

### Added

- Exact immutable filesystem publication with no-follow and durability checks.
- Durable batch codecs, local journal recovery, scratch storage, and packed
  authenticated Patricia indices.
- Disposable SQLite frontier and graph materialization behind a typed facade.
- Generated public-API inventory and a production/test-support boundary gate.
- Machine-readable persistent-format manifest.

[Unreleased]: https://github.com/martinkoutecky/tine-storage/compare/v0.27.0...HEAD
[0.27.0]: https://github.com/martinkoutecky/tine-storage/compare/v0.26.0...v0.27.0
[0.26.0]: https://github.com/martinkoutecky/tine-storage/compare/v0.25.0...v0.26.0
[0.10.0]: https://github.com/martinkoutecky/tine-storage/compare/v0.9.2...v0.10.0
[0.9.2]: https://github.com/martinkoutecky/tine-storage/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/martinkoutecky/tine-storage/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/martinkoutecky/tine-storage/compare/v0.8.13...v0.9.0
[0.8.7]: https://github.com/martinkoutecky/tine-storage/compare/v0.8.6...v0.8.7
[0.8.6]: https://github.com/martinkoutecky/tine-storage/compare/v0.8.5...v0.8.6
[0.8.5]: https://github.com/martinkoutecky/tine-storage/compare/v0.8.4...v0.8.5
[0.8.4]: https://github.com/martinkoutecky/tine-storage/compare/v0.8.3...v0.8.4
[0.8.3]: https://github.com/martinkoutecky/tine-storage/compare/v0.8.2...v0.8.3
[0.8.2]: https://github.com/martinkoutecky/tine-storage/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/martinkoutecky/tine-storage/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/martinkoutecky/tine-storage/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/martinkoutecky/tine-storage/compare/v0.6.5...v0.7.0
[0.6.5]: https://github.com/martinkoutecky/tine-storage/compare/v0.6.4...v0.6.5
[0.6.4]: https://github.com/martinkoutecky/tine-storage/compare/v0.6.3...v0.6.4
[0.6.3]: https://github.com/martinkoutecky/tine-storage/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/martinkoutecky/tine-storage/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/martinkoutecky/tine-storage/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/martinkoutecky/tine-storage/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/martinkoutecky/tine-storage/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/martinkoutecky/tine-storage/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/martinkoutecky/tine-storage/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/martinkoutecky/tine-storage/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/martinkoutecky/tine-storage/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/martinkoutecky/tine-storage/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/martinkoutecky/tine-storage/releases/tag/v0.1.0
