//! What an external consumer can actually reach.
//!
//! Every other test in this crate is a unit test compiled *inside* it, so it
//! sees private items and whatever features the surrounding build enabled.
//! That is precisely the wrong vantage point for the question a package split
//! turns into a contract: is the published API self-sufficient from outside,
//! without `test-support`?
//!
//! An integration test is compiled as a separate crate against the built
//! library, so it can only use `pub` paths — which makes this file's *compiling*
//! the assertion. `cargo test -p tine-storage` builds it with default features,
//! so a production path that secretly needs a test seam fails here.
//!
//! This is the fixture `tine-core` would become after extraction, in miniature:
//! when the crate moves out of tree, its consumers see exactly this much.

use tine_storage::formats::{self, FormatKind, FormatValue};
use tine_storage::sqlite::{
    MaterializationError, PhysicalGraphProjectionChange, PhysicalGraphProjectionDatabase,
    SqliteGraphProjectionRead,
};
use tine_storage::{
    publish_package_noclobber, recover_package_store, retire_package, ContentDigest,
    DurableDirectoryPublication, PackageFile, PackagePublishOutcome,
};
use uuid::Uuid;

#[test]
fn owned_ranked_query_is_usable_from_an_external_worker() {
    use tine_storage::sqlite::{PhysicalProjectionQuerySnapshot, PhysicalQueryValue};
    let path = std::env::temp_dir().join(format!("tine-public-rank-{}.sqlite", Uuid::new_v4()));
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE texts(value TEXT); INSERT INTO texts VALUES ('exact');").unwrap();
    let mut snapshot = PhysicalProjectionQuerySnapshot::open_direct(&path, || Ok(())).unwrap();
    snapshot
        .set_query_rank_function(|id, text| Ok((id == 42 && text == "exact").then(|| vec![0, 255])))
        .unwrap();
    let rows = std::thread::spawn(move || {
        snapshot.run_projection_query(
            "SELECT tine_query_rank(?1, value) FROM texts",
            &[PhysicalQueryValue::Integer(42)],
        )
    })
    .join()
    .unwrap()
    .unwrap();
    assert_eq!(rows, vec![vec![PhysicalQueryValue::Blob(vec![0, 255])]]);
    drop(writer);
    std::fs::remove_file(path).unwrap();
}

/// A content digest is constructible and inspectable from outside.
#[test]
fn content_digests_are_usable_from_outside_the_crate() {
    let digest = ContentDigest::of(b"bytes");
    assert_eq!(digest.as_bytes().len(), 32);
    assert_eq!(digest, ContentDigest::of(b"bytes"));
    assert_ne!(digest, ContentDigest::of(b"other bytes"));
}

#[test]
fn immutable_package_protocol_is_usable_from_the_public_api() {
    let root = std::env::temp_dir().join(format!("tine-storage-public-package-{}", Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let store = root.join("plugins");
    let files = [
        PackageFile {
            name: "manifest.json",
            bytes: br#"{"id":"dev.tine.example","version":"1.0.0"}"#,
        },
        PackageFile {
            name: "plugin.wasm",
            bytes: b"\0asm\x01\0\0\0",
        },
    ];
    assert_eq!(
        publish_package_noclobber(
            &store,
            "dev.tine.example",
            "1.0.0",
            ".install-dev.tine.example-1.0.0-1-1",
            &files,
        )
        .unwrap(),
        PackagePublishOutcome::Published
    );
    recover_package_store(&store, &["manifest.json", "plugin.wasm"]).unwrap();
    assert!(retire_package(
        &store,
        "dev.tine.example",
        "1.0.0",
        ".retired-dev.tine.example-1.0.0-1-2",
        &["manifest.json", "plugin.wasm"],
    )
    .unwrap());
    assert!(!store.join("dev.tine.example").exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn standalone_graph_projection_is_usable_without_managed_storage_types() {
    fn compile_read(read: &SqliteGraphProjectionRead<'_>) -> Result<usize, MaterializationError> {
        read.pages(None, 8).map(|rows| rows.len())
    }

    let path = std::env::temp_dir().join(format!(
        "tine-storage-public-graph-projection-{}.sqlite",
        Uuid::new_v4()
    ));
    let mut database = PhysicalGraphProjectionDatabase::open_writable(&path).unwrap();
    database.initialize_schema().unwrap();
    database.validate_schema().unwrap();
    database
        .apply(&PhysicalGraphProjectionChange {
            replacements: Vec::new(),
            deletions: Vec::new(),
            reference_postings: Vec::new(),
        })
        .unwrap();
    assert_eq!(compile_read(&database.read()).unwrap(), 0);
    let mut snapshot =
        tine_storage::sqlite::PhysicalProjectionQuerySnapshot::open_direct(&path, || Ok(()))
            .unwrap();
    let revision: u64 = snapshot.query_revision().unwrap();
    assert_eq!(revision, 0, "an empty apply is inventory-idempotent");
    drop(snapshot);
    drop(database);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[test]
fn fresh_projection_build_is_usable_from_the_public_api() {
    let path = std::env::temp_dir().join(format!(
        "tine-storage-public-fresh-projection-{}.sqlite",
        Uuid::new_v4()
    ));
    let database = PhysicalGraphProjectionDatabase::create_fresh_build(&path).unwrap();
    database.initialize_schema().unwrap();
    database.optimize().unwrap();
    database.quick_check().unwrap();
    drop(database);
    std::fs::remove_file(path).unwrap();
}

/// The whole point of `formats`: a release or pin receipt is *generated* from
/// the manifest by someone outside this crate. If the manifest's row type is
/// not fully public, that consumer has to hand-transcribe values instead —
/// which is the failure mode the module exists to prevent.
#[test]
fn a_receipt_can_be_generated_from_the_public_manifest() {
    assert!(
        !formats::FORMAT_MANIFEST.is_empty(),
        "the manifest is empty; a generated receipt would claim nothing"
    );

    let mut lines = Vec::new();
    for row in formats::FORMAT_MANIFEST {
        let kind = match row.kind {
            FormatKind::Identity => "identity",
            FormatKind::Layout => "layout",
            FormatKind::WriterBound => "writer-bound",
            FormatKind::CheckpointGeometry => "checkpoint-geometry",
        };
        let value = match row.value {
            FormatValue::Number(number) => number.to_string(),
            FormatValue::Name(name) => name.to_string(),
        };
        lines.push(format!(
            "{} {} {} = {}",
            row.artifact, kind, row.name, value
        ));
    }

    assert_eq!(lines.len(), formats::FORMAT_MANIFEST.len());
    assert!(
        lines
            .iter()
            .any(|line| line.contains("SQLITE_SCHEMA_VERSION")),
        "the generated receipt is missing a known format constant"
    );
}

#[test]
#[cfg(any(unix, windows))]
fn durable_publication_exposes_create_replace_staged_move_and_retire_to_a_consumer() {
    let root = std::env::temp_dir().join(format!("tine-storage-public-durable-{}", Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let dir = cap_std::fs::Dir::open_ambient_dir(&root, cap_std::ambient_authority()).unwrap();
    let publication = DurableDirectoryPublication::open(&dir).unwrap();
    publication
        .publish_new_exact("schema-2-anchor", b"old")
        .unwrap();
    publication
        .publish_new_exact_single_writer("private-schema-2-anchor", b"private")
        .unwrap();
    publication
        .replace_exact("schema-2-anchor", b"old", b"new")
        .unwrap();
    dir.write("staged", b"staged").unwrap();
    publication
        .move_exact_no_replace("staged", "published", b"staged")
        .unwrap();
    dir.write("staged-cache", b"cache replacement").unwrap();
    publication
        .replace_from_staged_regular_single_writer("staged-cache", "published")
        .unwrap();
    publication
        .retire_exact("schema-2-anchor", ".retired-schema-2-anchor", b"new")
        .unwrap();
    assert!(!root.join("schema-2-anchor").exists());
    assert_eq!(
        std::fs::read(root.join(".retired-schema-2-anchor")).unwrap(),
        b"new"
    );
    assert_eq!(
        std::fs::read(root.join("private-schema-2-anchor")).unwrap(),
        b"private"
    );
    assert_eq!(
        std::fs::read(root.join("published")).unwrap(),
        b"cache replacement"
    );
    drop(publication);
    drop(dir);
    std::fs::remove_dir_all(root).unwrap();
}

/// The recorded surface is itself public, so a consumer or a release process
/// can enumerate what it is pinning without parsing this crate's source.
#[test]
fn the_api_surface_is_enumerable_by_a_consumer() {
    let names = tine_storage::api_surface::exported_names();
    assert!(names.len() > 40, "the published surface looks truncated");
    // Since 0.25.0 no seam is gated behind `test-support`; the inventory still
    // records the flag so a consumer can tell a future seam from production API.
    assert!(
        names.iter().all(|name| !name.test_support_only),
        "an ungated export is recorded as a test-support seam"
    );
}
