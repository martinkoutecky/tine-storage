# Persistent-format compatibility

Crate semver and persistent-format identity are deliberately independent.
Since 0.25.0 `tine-storage` writes and reads one persistent artifact: the
Direct Files SQLite projection. The authoritative machine-readable values are
`tine_storage::formats::FORMAT_MANIFEST`; this table is a review aid, not a
second source of truth.

| Artifact | Format identity | Compatibility rule |
| --- | --- | --- |
| SQLite projection | application ID `0x54494e45`; schema 31 | SQLite is disposable. A file whose `user_version` differs is rebuilt from the graph's Markdown/Org files, never reinterpreted under a new schema. |

Schema 30 is the single unreleased compact projection format: entity
coordinates are monotonic integer rowids, public identity remains path/result
ID, and reference/property/tag names are spelling-preserving dictionary rows.
Page preamble and block source are the only stored raw document text. Search is
one contentless, detail-free, case-sensitive trigram FTS5 table whose rowids are
those same disjoint page/block coordinates; application-folded `search_tokens`
are indexed but cannot be read back as text.

Schema 31 (0.28.0) adds `short_word_fts`, a second contentless, detail-free
FTS5 table over application `short_word_tokens` (whole tokens, `ascii`
tokenizer), keyed by the same page/block rowids. Only entities with tokens own
a row. A schema-30 file is rebuilt, never migrated.

Fresh projection construction may use an unpublished journal-OFF staging file
which is atomically installed only after completion. That changes build and
publication mechanics, not schema 30 or its compatibility identity. Ordinary
published writers reopen the installed file in WAL/NORMAL mode; a failed
staging write is discarded rather than recovered or interpreted as authority.

The Managed Storage formats (oplog manifest/object protocol, local journal v1
and v2, sealed accepted-history index, engine scratch, checkpoint fingerprints,
and the managed on-disk layout vocabulary) were deleted in 0.25.0 together
with the code that read and wrote them. Their last definitions are in the
v0.24.0 tag. A graph directory that still carries `.tine-sync/` from that era
is not read or modified by this crate.

Any change to `FORMAT_MANIFEST` requires all of the following in the same
storage release:

1. a changelog entry explaining the old-data behavior;
2. an explicit rebuild proof for existing projection files;
3. an updated pinned-format test (`formats::tests::format_identity_is_pinned`);
4. a new certification receipt and a new Tine pin receipt.
