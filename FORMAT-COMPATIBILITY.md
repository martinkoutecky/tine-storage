# Persistent-format compatibility

Crate semver and persistent-format identity are deliberately independent.
`tine-storage` 0.1.0 writes and reads the format family below. The authoritative
machine-readable values are `tine_storage::formats::FORMAT_MANIFEST`; this table
is a review aid, not a second source of truth.

| Artifact | Format identity in 0.1.0 | Compatibility rule |
| --- | --- | --- |
| Oplog manifest/object protocol | protocol 2; object envelope 2; manifest encoding 4 | Existing versions must remain readable or receive an explicit migration before a writer changes these values. |
| Local journal | frame schema 1; segment/frontier protocol 2; `TINEJNL2`/`TINEFRT2`; 136-byte header; 240-byte `.frontier-v2` | This is the only current pre-0.7 format. Recovery accepts only the exact selected header and frontier and treats bytes beyond a valid old frontier as uncommitted suffix. Tine backs up and rebuilds unrecognized private state rather than migrating or dual-reading it. |
| Engine scratch | run schema 13; page schema 1; 32 LSM levels; `engine-scratch-v2` layout | Scratch is reconstructible, but an interrupted run must either resume safely or be rejected and rebuilt. |
| SQLite projection | application ID `0x54494e45`; current schema 22 | One exact `PRAGMA user_version` plus one frozen table/index/DDL census. This is pre-0.7 private state: the crate contains no older-schema reader or migration path. Tine preserves an unrecognized private store as a backup and rebuilds the current store from Markdown/Org. Live and separately built checkpoint-candidate files use this same schema; no production marker selects a candidate in this release. |
| Sealed accepted-history index | family/map/status/sequence/causal schemas 2; sequence fanout 32; leaf capacity 1 | No production Tine marker names these objects yet. These numbers identify the one current canonical encoding; they do not imply a supported earlier encoding or migration path. After 0.7 compatibility begins, a change must preserve released readers or use a new object namespace/schema rather than replace bytes at an existing address. |
| Checkpoint fingerprints | 64 KiB edges; 16 KiB interior ranges; 1 MiB interior sampling interval | Stored and freshly computed fingerprints are comparable only with identical geometry. |
| Managed-storage layout | Shared-provider, archive, lazy-genesis, projection, reconciliation, enrollment, bootstrap, migration, and source-capture path vocabulary frozen in `managed-layout-v1.txt` | The lazy-genesis names are additive before 0.7 and old crates simply ignore them. Future changes must preserve old readers/writers or carry an explicit migration/rebuild rule. |

Writer bounds are also part of compatibility because lowering them may strand
already-written data. The exact released bounds are pinned by
`formats::tests::format_identity_is_pinned` and included in every certification
receipt.

Any change to `FORMAT_MANIFEST` requires all of the following in the same
storage release:

1. a changelog entry explaining the old-data behavior;
2. old-version fixtures or an explicit rebuild/migration proof;
3. an updated pinned-format test;
4. a new certification receipt and a new Tine pin receipt.
