# Persistent-format compatibility

Crate semver and persistent-format identity are deliberately independent.
Since 0.25.0 `tine-storage` writes and reads one persistent artifact: the
Direct Files SQLite projection. The authoritative machine-readable values are
`tine_storage::formats::FORMAT_MANIFEST`; this table is a review aid, not a
second source of truth.

| Artifact | Format identity | Compatibility rule |
| --- | --- | --- |
| SQLite projection | application ID `0x54494e45`; schema 29 | SQLite is disposable. A file whose `user_version` differs is rebuilt from the graph's Markdown/Org files, never reinterpreted under a new schema. |

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
