# tine-storage

[![certify](https://github.com/martinkoutecky/tine-storage/actions/workflows/certify.yml/badge.svg)](https://github.com/martinkoutecky/tine-storage/actions/workflows/certify.yml)

`tine-storage` owns physical persistence mechanisms. The dependency direction
is `src-tauri -> tine-core -> tine-storage`: core supplies policy, authority,
validation, and domain meaning, while this crate supplies storage operations.
It has no dependency on `tine-core`, `lsdoc`, Tauri, or UI crates.

SQLite is a disposable local projection. In managed storage the oplog/archive
is authoritative; in Direct Files the Markdown/Org tree is authoritative. Both
can feed the same regime-neutral graph-fact tables through
`PhysicalGraphProjectionDatabase`, while managed storage layers its accepted
frontier stamps beside those tables. Consumers use the curated
`tine_storage::sqlite` facade; it exposes typed physical operations without raw
connections or DDL construction details.

Checkpoint-generation accepted history uses the separate
`tine_storage::sealed_accepted_index` facade. It owns canonical logical map and
sequence algorithms, V2 immutable object codecs, and a storage-agnostic shared
reader/writer. Tine supplies engine policy and the physical content-addressed
object store; this crate validates canonical bytes, logical addresses, bounded
tree shape, and the one-based sequence/status/batch/causal cross-check. A
caller-supplied decoder validates Tine's exact current evidence bytes and lets
the shared reader bind their batch, sequence, manifest, and event fields
without making this physical crate depend on the engine crate.

## Immutable package publication

`publish_package_noclobber`, `retire_package`, and `recover_package_store`
compose the crate's no-follow and durable-name primitives into one
staged-directory protocol for app-private immutable packages:

- every staged regular file is written and synchronized before the staging
  directory is synchronized;
- the complete staging directory is moved from the store root to its
  package-id directory with a native no-replace operation on Linux, Windows,
  macOS, iOS, and Android;
- Windows requests write-through name operations, while Unix synchronizes both
  changed parent directories after publication or retirement;
- an existing version is idempotent when every required file has the expected
  exact bytes; unrelated extra entries neither invalidate nor get deleted from
  an otherwise complete package;
- removal first moves the active package to a caller-supplied `.retired-*`
  root name, then reclaims it; and
- reopen removes `.install-*`, `.retired-*`, and active packages missing a
  required regular file, so every crash cut converges to one complete package
  or absence.

Callers define the package identity grammar, transient suffixes, required file
set, and semantic validation. Transient names are deliberately supplied rather
than invented here so the application contract can pin and test its grammar.

### Package recovery and refusal scenarios

| Boundary outcome | In-scope scenario | Contract |
| --- | --- | --- |
| Recovery reclaims an active package missing a required regular file | A crash or power loss interrupted publication before the complete staged directory became authoritative | Reclaim the torn package idempotently. Extra entries alone are not crash evidence and are preserved. |
| `PackageStoreError::TransientNameCollision` | An honest concurrent Tine process claimed the caller-supplied `.install-*` or `.retired-*` name after recovery | Retry with a fresh transient name; this refusal is retryable and never permits replacement. |
| `PackageStoreError::ImmutableVersionCollision` | Honest concurrent publishers supplied different bytes for one immutable package identity | Refuse the second publication and preserve the first complete winner. |

## Persistent-format identity

`tine_storage::formats` collects every constant that describes bytes already on
disk — envelope/schema versions, on-disk names and layout, the bounds a writer
may legally have produced, and checkpoint fingerprint geometry — and exposes
them as `FORMAT_MANIFEST`.

**On-disk format versions are independent of this crate's semver.** The crate
version tracks the Rust API; these constants track the bytes. An API-breaking
refactor that reads and writes identical bytes changes nothing in the manifest,
and a one-field change to a stored envelope changes it even in a patch release.

A storage release receipt and Tine's storage pin receipt should be generated
from `FORMAT_MANIFEST` rather than transcribing values by hand, because a
hand-copied receipt drifts silently and the drift is invisible exactly when it
matters. `formats::tests::format_identity_is_pinned` asserts the exact current
values, so changing an on-disk format cannot pass CI without a deliberate edit
a reviewer sees; when that test fails, update it together with the migration
story for existing graphs, not on its own.

In-memory budgets and read-path limits are deliberately excluded from the
manifest: they bound one process's work, not the bytes it leaves behind.

Package-local test ownership is intentionally divided as follows:

- Persistent-format invariants: `durable_batch::tests` and `digest_sealed::tests`.
- Durability and filesystem publication invariants: `filesystem::tests`.
- Immutable package crash-cut and recovery invariants: `package_store::tests`.
- Sealed accepted-history index invariants:
  `sealed_accepted_index_impl::tests`.
- SQLite transaction and schema invariants: `sqlite_frontier::tests` and
  `sqlite_materialization::tests`.
- SQLite facade, connection ownership, and test-support-gate invariants:
  `sqlite_database::tests` and `sqlite_graph_projection::tests`.

## Public API surface

`api.txt` records every publicly reachable name, its export path, and whether it
is gated behind `test-support`. It is generated, not hand-maintained:

```
TINE_STORAGE_BLESS_API=1 cargo test -p tine-storage api_surface
```

A change to the surface fails `api_surface::tests::api_surface_matches_the_recorded_golden`
until `api.txt` is regenerated in the same commit, so a version can be cut
against a surface someone actually reviewed. It records names, not signatures —
rustdoc JSON would give signatures but is nightly-only, and this crate builds on
the pinned stable toolchain.

Two rules the tests enforce, both of which exist because this crate is becoming
an independently versioned package with an exact Tine pin:

- **A persistent-format constant has exactly one export path**,
  `tine_storage::formats::NAME`. A receipt generated from `FORMAT_MANIFEST`
  claims to state the format surface a build commits to; a second path would let
  a consumer bind to a constant the receipt never mentions.
- **`test-support` never reaches a release build.** It holds today only because
  the feature is dev-dependency-only *and* the workspace uses resolver 2;
  `scripts/check-storage-test-support.mjs` resolves the app's real feature graph
  rather than trusting either fact to stay true.

`tests/public_boundary.rs` is compiled as a separate crate against the built
library, so it can only reach `pub` paths with default features. Its compiling
is the assertion: the production API is self-sufficient for someone outside this
crate.
# Owned query snapshots

`PhysicalProjectionQuerySnapshot` owns a read-only SQLite connection and pinned
read transaction. `open_managed` checks acceptance sequence and frontier digest
inside that transaction; `open_direct` calls the projection owner's instance/
readiness validator before and after establishing it. Ordinary later edits do
not invalidate a coherent snapshot. Query, explain and streaming visitor methods
share the transaction and accept bound parameters. No connection is exposed.

Acquire worker capacity before opening snapshots. A snapshot can move to a worker;
keep selection/payload execution off the actor. Its cancellation handle combines
a sticky flag, SQLite interrupt and progress checks. SQL/visitor errors release
the connection; consumers finish or drop successful snapshots. Calling cancel
signals the owner: graph replacement must also drain workers/handles, including
idle cancelled jobs, before removing the projection. Do not infer bounded WAL
bytes from bounded worker count; measure reader duration and retained WAL.

Schema 28 adds `query_page_results` with rebuildable page construction estimates
and property counts, plus `blocks_parent_page_idx` for bounded descendant lookup.
The existing page transaction produces and deletes these facts; raw text is not
duplicated. Both main and Direct projections reject their previous disposable
schemas so callers rebuild once. This extends schema 27 without changing its
authenticated-map encoding or the source authority.

Schema 27 widens the authenticated document frontier from a fixed 16-byte
identifier to the domain owner's complete key bytes
(`sealed_accepted_index::AuthenticatedMapKey`, 1..=48 bytes), keying one table
with one node kind. The sealed writer/reader, the SQLite treap and the Cartesian
root builder share one key codec, one length-framed node digest and one root
representation, so their roots are comparable bit for bit. Batch, status and
causal identities stay 16-byte UUIDs. Everything schema 26 added is unchanged.

Schema 26 adds `query_block_results`, `query_page_order`, and `block_own_refs`.
The page transaction derives preorder, construction estimates and facet counts
from its physical inputs; producers supply their existing public identity and
own references. Direct session positions are optional for Managed pages, which
use path ordering. `query_block_preorder` and `query_result_estimated_bytes`
are shared helpers for producers/consumers. Incomplete trees and empty public
identities reject a replacement transaction. Raw text remains in block_text.

`set_query_regex_predicate` installs the fixed `tine_query_regex(id, visible_text)`
function over an application-owned compiled-regex registry. Bound IDs select
existing compiled expressions; storage introduces no regex grammar or per-row
compilation.

`set_query_rank_function` installs the fixed `tine_query_rank(id, visible_text)`
function on that reader. The application supplies immutable compiled matching
semantics and a lossless lexicographic BLOB ordering key; NULL means no match.
Storage does not define a ranking grammar or convert the key to a numeric score.
The callback sees exact text and is isolated to its connection. Both fixed
functions are deterministic and direct-only; errors release an owned snapshot's
transaction through the same read guard as ordinary SQL errors. Callbacks must
finish promptly because SQLite cannot interrupt Rust code inside a callback.
The ranking callback does not change authority formats. The current disposable
projection schema and its rebuild behavior are documented in FORMAT-COMPATIBILITY.md.

`PhysicalProjectionQuerySnapshot::query_revision()` reads a local image counter
from the pinned transaction. Pair it with the owner's projection-instance
identity for result memoization; it does not certify coverage of a saved edit.
Schema 28's `query_projection_state` is a rebuildable singleton advanced in the
existing transaction for graph apply (including order-only changes), reset and
terminal construction. Failed transactions roll it back. Reset within a file
advances it; replacement files start at zero and require lifecycle invalidation.
It is excluded from deterministic graph-fact digests because construction history
is not authority. Missing/exhausted state is a projection error for normal rebuild
recovery. The revision accessor shares snapshot cancellation/error release.
