# Changelog

All notable changes to `tine-storage` are recorded here. The crate's semantic
version describes its Rust API; persistent byte formats are versioned
independently in `src/formats.rs` and summarized in
`FORMAT-COMPATIBILITY.md`.

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

[Unreleased]: https://github.com/martinkoutecky/tine-storage/compare/v0.10.0...HEAD
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
