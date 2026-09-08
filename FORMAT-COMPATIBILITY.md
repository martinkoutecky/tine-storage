# Persistent-format compatibility

Crate semver and persistent-format identity are deliberately independent.
`tine-storage` 0.1.0 writes and reads the format family below. The authoritative
machine-readable values are `tine_storage::formats::FORMAT_MANIFEST`; this table
is a review aid, not a second source of truth.

| Artifact | Format identity in 0.1.0 | Compatibility rule |
| --- | --- | --- |
| Oplog manifest/object protocol | protocol 2; object envelope 2; manifest encoding 4 | Existing versions must remain readable or receive an explicit migration before a writer changes these values. |
| Local journal | frame schema 1; segment/frontier protocol 2; `TINEJNL2`/`TINEFRT2`; 136-byte header; 240-byte `.frontier-v2` | This is the only current pre-0.7 format. Recovery accepts only the exact selected header and frontier and treats bytes beyond a valid old frontier as uncommitted suffix. Tine backs up and rebuilds unrecognized private state rather than migrating or dual-reading it. |
| SQLite projection | application ID `0x54494e45`; current schema 27 | One exact `PRAGMA user_version` plus one frozen table/index/DDL census. This is pre-0.7 private state: the crate contains no older-schema reader or migration path. Tine preserves an unrecognized private store as a backup and rebuilds the current store from Markdown/Org. Live and separately built checkpoint-candidate files use this same schema; no production marker selects a candidate in this release. |
| Sealed accepted-history index | family schema 3; map-node schema 3; status/sequence/causal schemas 2; authenticated-map key 1..=48 bytes; sequence fanout 32; leaf capacity 1 | No production Tine marker names these objects yet. These numbers identify the one current canonical encoding; they do not imply a supported earlier encoding or migration path. After 0.7 compatibility begins, a change must preserve released readers or use a new object namespace/schema rather than replace bytes at an existing address. |
| Checkpoint fingerprints | 64 KiB edges; 16 KiB interior ranges; 1 MiB interior sampling interval | Stored and freshly computed fingerprints are comparable only with identical geometry. |
| Managed-storage layout | Current shared-provider, archive object/batch, projection-receipt, enrollment, source-capture, runtime/journal, and SQLite path vocabulary frozen in `managed-layout-v1.txt` | Zero-consumer Patricia, detached-bootstrap, engine-history, scratch, reconciliation, projection-work, resume-point, and migration-staging names were removed before 0.7. Tine preserves unrecognized private state as a backup and rebuilds from Markdown/Org; future post-0.7 changes must preserve old readers/writers or carry an explicit migration/rebuild rule. |

Writer bounds are also part of compatibility because lowering them may strand
already-written data. The exact released bounds are pinned by
`formats::tests::format_identity_is_pinned` and included in every certification
receipt.

Schema 26 adds `query_block_results` (public identity, preorder, estimate and
counts), `query_page_order` (Direct session positions), and `block_own_refs`
(names before closure). These share the page transaction, are explicitly deleted
on replacement/deletion/reset even with foreign keys disabled, and contain no
serialized DTO or duplicate raw text. Producers must supply complete trees;
preorder expands parent-local order with physical-ID ties. No authority input
format changes. Older/newer disposable schemas are rejected and rebuilt.
Direct `direct_source_revisions` includes `query_metadata_schema`, constrained to
26. Its exact three-column shape and DDL are validated: old readers that only
check known table shapes also reject this cache, rather than overlook the new
tables. This marker carries no authority data or migration behavior.
Full Direct inventory reconciliation updates only `query_page_order` when its
order changes, atomically with any source/page deltas. An identical inventory
performs no order writes; unchanged pages are not re-materialized.

The schema also separates query rows from document text. `pages` stores identity,
name and routing fields; `page_text`, keyed by `page_id`, stores `preamble`,
`searchable_text` and `normalized_searchable_text`. `blocks` stores identity,
structure, metadata and `query_visible_folded`; `block_text`, keyed by
`block_id`, stores `content`, `searchable_text`, `normalized_searchable_text`
and `query_visible`. The text rows reference their owners with `ON DELETE
CASCADE`. Shared inserts and replacements write both parts in the same
transaction; explicit reset/cleanup also supports caller-owned connections.
Typed document reads retain their existing fields and limits. Payload reads
preserve the owner row and report missing required text as cache damage rather
than silently omitting an entity. Index-only queries need no payload join.
The previous projection schema is rebuilt through the existing app lifecycle;
this change adds no migration or compatibility reader.

Any change to `FORMAT_MANIFEST` requires all of the following in the same
storage release:

1. a changelog entry explaining the old-data behavior;
2. old-version fixtures or an explicit rebuild/migration proof;
3. an updated pinned-format test;
4. a new certification receipt and a new Tine pin receipt.

Version 0.18 separates the Rust types for an enrolled device and a causal writer
key through `DurableBatchContract::CausalPeerKey`. The same manifest field and
canonical codec serialize the product-selected key; the shared physical format
numbers do not change. A product changing its interpretation of causal identity
must update its one current operation schema and admission contract. The codec
does not infer ownership or allocate identities. Typed round-trip tests cover two
writer incarnations with the same enrolled author and preserve the existing byte
golden when a contract selects the same serialized key representation.

Version 0.19 widens the one shared authenticated map from fixed 16-byte
identifiers to **bounded canonical key bytes** supplied by the domain owner:
`sealed_accepted_index::AuthenticatedMapKey`, 1 to
`formats::MAX_AUTHENTICATED_MAP_KEY_BYTES` (48) bytes. There is exactly one
current key codec, one node digest and one root representation, shared by the
sealed writer/reader, the SQLite document frontier and the Cartesian root
builder; `tine-storage` never parses a key. A 16-byte identity is one such key
(`From<[u8; 16]>`), so the batch, status, causal-clock and causal-tip maps and
every UUID-typed API keep their existing shapes and their original durable
object bytes.

**Length-framed digest semantics.** The shared node digest
`authenticated_map_node_digest` now folds every key — the node's own key and
each child link key — as `length ‖ bytes`, under the new domain
`tine/oplog/authenticated-map/v2/node\0`. A one-byte length prefix suffices
because the bound is 48. Framing makes the preimage injective for *any* caller
key space, including keys where one is a strict prefix of another: without it, a
one-byte-longer key can absorb the first byte of the fixed-width digest that
follows it and let a different `(key, value, child)` triple hash to the same
preimage. The alternative — requiring every caller's key space to be
self-delimiting — would impose an undocumented obligation on library callers
merely to preserve obsolete private-state goldens, so it was rejected.
`authenticated_map_priority` deliberately keeps its raw single-key fold and its
v1 domain: its only variable-width input is the last field, so a single key
already determines that preimage. The treap *shape* of an existing 16-byte map
is therefore unchanged; only node digests, and the roots derived from them, are
rebuilt. `authenticated_map_empty_digest` is unchanged and stays the canonical
empty pairing (`root_key = None`, digest = empty digest). Because the causal
record's clock-root key became variable-width and is followed by a fixed-width
digest, `accepted_causal_record_digest` frames that key for the same reason;
that record's address is in the rebuild set regardless, since the clock root
digest itself changes.

**Format identity moved, and what happens to existing state.**
`SEALED_ACCEPTED_INDEX_SCHEMA_VERSION` and
`SEALED_ACCEPTED_MAP_NODE_SCHEMA_VERSION` move 2 -> 3 (the node wire gains a
length-prefixed key). `SQLITE_SCHEMA_VERSION` moves 26 -> 27
(`frontier_documents.document_id` and both child-key columns now CHECK
`length BETWEEN 1 AND 48`). `MAX_AUTHENTICATED_MAP_KEY_BYTES` is a new manifest
row of kind `WriterBound`: keys up to that width are on disk in sealed map nodes
and in the SQLite overlay, so lowering it would strand stored data, and the two
changed schema versions identify the encoding but do not by themselves state the
bound a reader must accept. `SQLITE_APPLICATION_ID`, `OPLOG_PROTOCOL_VERSION`,
`OBJECT_ENVELOPE_SCHEMA_VERSION`, `MANIFEST_ENCODING_VERSION`, the local-journal
constants and the status/sequence/causal schema numbers are unchanged.

There is **no migration and no dual reader**. Existing sealed checkpoint
generation directories and existing SQLite projection caches are pre-0.7 private
derived state: an old sealed generation is backed up and rebuilt, and an
unrecognized SQLite `user_version` or DDL census is refused by
`validate_schema_and_claim` so the caller rebuilds the disposable projection
from the oplog rather than migrating or dually reading it. Derived goldens in
this crate's pinned vectors changed accordingly and were regenerated in the same
change.
