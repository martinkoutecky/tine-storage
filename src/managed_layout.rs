//! Persistent managed-storage path vocabulary.
//!
//! These are names only. Tine owns authority, lifecycle, and interpretation;
//! this crate owns the exact path grammar so its release receipt certifies the
//! layout consumed by every writer and reader.

// Shared graph-local provider: `<graph>/.tine-sync/v2/shared/`.
pub const SHARED_ENROLLMENT_DESCRIPTOR_PATH: &str = "enrollment/shared-enrollment-v1.json";
pub const SHARED_FRONTIER_HEADS_DIR: &str = "frontier-heads-v1";
pub const SHARED_PUBLICATION_INTENTS_DIR: &str = "publication-intents-v1";
pub const SHARED_MANIFEST_RECOVERY_LINKS_DIR: &str = "manifest-recovery-links-v1";
pub const SHARED_MANIFEST_RECOVERY_BLOBS_DIR: &str = "manifest-recovery-blobs-v1";
pub const SHARED_OBJECTS_DIR: &str = "objects";
pub const SHARED_MANIFESTS_DIR: &str = "manifests";
pub const SHARED_ENROLLMENT_DIR: &str = "enrollment";
pub const SHARED_TEMP_DIR: &str = ".part";
pub const SHARED_REMOVED_DIR: &str = "removed";
pub const SHARED_RENAME_EVIDENCE_DIR: &str = "rename-evidence";
pub const PROVIDER_PENDING_PUBLICATION_DIR: &str = "pending-publication-v1";
pub const PROVIDER_INBOX_DIR: &str = "inbox";
pub const PROVIDER_OUTBOX_DIR: &str = "outbox";

// App-private binding and recovery roots.
pub const PRIVATE_BINDING_DIR: &str = "sparse-v2";
pub const PRIVATE_BINDING_FILE: &str = "binding.json";
pub const PRIVATE_RECOVERY_DIR: &str = "sparse-v2-recovery";

// Archive object store.
pub const ARCHIVE_OBJECTS_DIR: &str = "objects";
pub const ARCHIVE_BATCHES_DIR: &str = "batches";
pub const LAZY_GENESIS_MANIFEST_FILE: &str = "manifest.postcard";
pub const LAZY_GENESIS_COMMIT_FILE: &str = "commit.postcard";
pub const LINEAGE_CLAIM_FILE: &str = "lineage.claim";
// Derived receipts/work indexes.
pub const PROJECTION_STORE_CLAIM_FILE: &str = "projection-receipts.claim";
pub const PROJECTION_STORE_INIT_FILE: &str = "projection-receipts.init";
pub const PROJECTION_BASES_DIR: &str = "bases";
pub const PROJECTION_INTENTS_DIR: &str = "intents";
pub const PROJECTION_COMPLETIONS_DIR: &str = "completions";
pub const PROJECTION_ATTEMPTS_DIR: &str = "attempts";
pub const PROJECTION_FORENSICS_DIR: &str = "forensics";
pub const PROJECTION_PENDING_CLEANUP_SUFFIX: &str = ".projection-cleanup";
pub const PROJECTION_PENDING_CLEANUP_DIR: &str = ".pending-cleanup";
pub const PROJECTION_PENDING_CLEANUP_AUTHORITY_FILE: &str = ".pending-cleanup.authority";
pub const PROJECTION_CLEANUP_ROUND_STATE_FILE: &str = "round-robin.state";
pub const PROJECTION_CLEANUP_ROUND_0_DIR: &str = "round-0";
pub const PROJECTION_CLEANUP_ROUND_1_DIR: &str = "round-1";
pub const MUTATION_AUTHORITY_SUFFIX: &str = ".mutation-authority";
pub const MUTATION_AUTHORITY_LEASE_SUFFIX: &str = ".mutation-authority.lock";

// Device-private runtime, scratch, and journal state.
pub const SQLITE_RUNTIME_DIR: &str = ".tine-runtime";
pub const SQLITE_WORKSPACES_DIR: &str = "sqlite-workspaces";
pub const SQLITE_APPLIER_LOCK_FILE: &str = "sqlite-applier.lock";
pub const MANAGED_LOCAL_JOURNAL_DIR: &str = "managed-local-journal-v1";

// Enrollment.
pub const ENROLLMENT_STORAGE_DIR: &str = "sparse-storage";
pub const ENROLLMENT_VERSION_DIR: &str = "v2";
pub const ENROLLMENT_LOCAL_DIR: &str = "local";
pub const ENROLLMENT_DIR: &str = "enrollment";
pub const ENROLLMENT_RECORDS_DIR: &str = "records";
pub const ENROLLMENT_LEASE_FILE: &str = "lease";
pub const ENROLLMENT_AUTHORITY_FILE: &str = "authority-v1.claim";
pub const ENROLLMENT_HEAD_FILE: &str = "head";
pub const ENROLLMENT_RECORD_SUFFIX: &str = ".enrollment";
pub const ENROLLMENT_HEAD_TEMP_PREFIX: &str = ".head-tmp-";
pub const ENROLLMENT_AUTHORITY_TEMP_PREFIX: &str = ".authority-tmp-";
pub const LOCAL_ACTIVATION_RESERVATION_FILE: &str = "local-activation-v1.reservation";

// Direct-to-managed source capture.
pub const BOOTSTRAP_SOURCE_CAPTURE_DIR: &str = "bootstrap-source-capture-v1";
pub const BOOTSTRAP_SOURCE_CAPTURE_CHUNKS_DIR: &str = "source-chunks";
pub const BOOTSTRAP_SOURCE_CAPTURE_MANIFEST_FILE: &str = "capture-manifest.bin";
pub const BOOTSTRAP_SOURCE_INVENTORY_FILE: &str = "inventory.sorted";
pub const BOOTSTRAP_SOURCE_ENTRIES_FILE: &str = "entries.sorted";
pub const BOOTSTRAP_SOURCE_CHUNKS_FILE: &str = "chunks.sorted";
pub const PROVIDER_DEVICE_AUTHORITY_FILE: &str = "provider-transaction.authority";
