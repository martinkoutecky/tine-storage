//! Durable staged-directory publication for immutable app-private packages.
//!
//! Callers own package identity and payload validation. This module owns the
//! physical protocol: durable staged files, a no-clobber directory transition,
//! exact-byte retry classification, retire-then-reclaim, and reopen cleanup.

use crate::filesystem::{
    ensure_directory_nofollow, move_directory_no_replace, open_dir_nofollow,
    open_existing_dir_nofollow, read_required_regular, require_regular_entry, sync_dir_required,
    FilesystemError,
};
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use std::collections::BTreeSet;
use std::fmt;
use std::io::{self, ErrorKind, Write};
use std::path::Path;
use std::sync::{Mutex, OnceLock};

const INSTALL_PREFIX: &str = ".install-";
const RETIRED_PREFIX: &str = ".retired-";

/// One exact regular file in an immutable package.
#[derive(Clone, Copy, Debug)]
pub struct PackageFile<'a> {
    pub name: &'a str,
    pub bytes: &'a [u8],
}

/// The result of a no-clobber immutable package publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PackagePublishOutcome {
    Published,
    AlreadyPresentExact,
}

/// A failure at the staged-directory package boundary.
#[derive(Debug)]
pub enum PackageStoreError {
    Filesystem(FilesystemError),
    Io(io::Error),
    UnsafeName(String),
    TransientNameCollision,
    ImmutableVersionCollision,
}

impl fmt::Display for PackageStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Filesystem(error) => error.fmt(f),
            Self::Io(error) => error.fmt(f),
            Self::UnsafeName(message) => message.fmt(f),
            Self::TransientNameCollision => {
                f.write_str("transient package-store name already exists")
            }
            Self::ImmutableVersionCollision => {
                f.write_str("immutable package version already exists with different bytes")
            }
        }
    }
}

impl std::error::Error for PackageStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Filesystem(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<FilesystemError> for PackageStoreError {
    fn from(error: FilesystemError) -> Self {
        Self::Filesystem(error)
    }
}

impl From<io::Error> for PackageStoreError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

fn store_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn lock_store() -> std::sync::MutexGuard<'static, ()> {
    store_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn validate_component(name: &str, label: &str) -> Result<(), PackageStoreError> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.starts_with('.')
        || name.contains(['/', '\\', '\0'])
    {
        return Err(PackageStoreError::UnsafeName(format!(
            "{label} is not one non-hidden package component: {name:?}"
        )));
    }
    Ok(())
}

fn validate_entry_name(name: &str, label: &str) -> Result<(), PackageStoreError> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\', '\0']) {
        return Err(PackageStoreError::UnsafeName(format!(
            "{label} is not one package entry: {name:?}"
        )));
    }
    Ok(())
}

fn validate_transient(name: &str, prefix: &str) -> Result<(), PackageStoreError> {
    validate_entry_name(name, "transient name")?;
    if !name.starts_with(prefix) || name.len() == prefix.len() {
        return Err(PackageStoreError::UnsafeName(format!(
            "transient package name must start with {prefix:?}"
        )));
    }
    Ok(())
}

fn validate_files(files: &[PackageFile<'_>]) -> Result<(), PackageStoreError> {
    if files.is_empty() {
        return Err(PackageStoreError::UnsafeName(
            "an immutable package must contain at least one file".into(),
        ));
    }
    let mut names = BTreeSet::new();
    for file in files {
        validate_component(file.name, "package filename")?;
        if !names.insert(file.name) {
            return Err(PackageStoreError::UnsafeName(format!(
                "duplicate package filename: {:?}",
                file.name
            )));
        }
    }
    Ok(())
}

fn validate_required_files(required_files: &[&str]) -> Result<(), PackageStoreError> {
    if required_files.is_empty() {
        return Err(PackageStoreError::UnsafeName(
            "package recovery requires at least one filename".into(),
        ));
    }
    let mut names = BTreeSet::new();
    for name in required_files {
        validate_component(name, "required package filename")?;
        if !names.insert(*name) {
            return Err(PackageStoreError::UnsafeName(format!(
                "duplicate required package filename: {name:?}"
            )));
        }
    }
    Ok(())
}

fn open_store_root(root: &Path) -> Result<Dir, PackageStoreError> {
    let parent = root
        .parent()
        .ok_or_else(|| PackageStoreError::UnsafeName("package-store root has no parent".into()))?;
    let name = root
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| PackageStoreError::UnsafeName("package-store name is not UTF-8".into()))?;
    validate_component(name, "package-store directory")?;
    let parent = Dir::open_ambient_dir(parent, ambient_authority())?;
    ensure_directory_nofollow(&parent, name)?;
    open_dir_nofollow(&parent, name).map_err(Into::into)
}

fn entry_names(dir: &Dir) -> Result<Vec<String>, PackageStoreError> {
    dir.entries()?
        .map(|entry| {
            entry.map_err(PackageStoreError::from).and_then(|entry| {
                entry.file_name().into_string().map_err(|_| {
                    PackageStoreError::UnsafeName("package-store entry name is not UTF-8".into())
                })
            })
        })
        .collect()
}

fn reclaim_entry(parent: &Dir, name: &str) -> Result<(), PackageStoreError> {
    let metadata = match parent.symlink_metadata(name) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        parent.remove_file(name)?;
    } else {
        parent.remove_dir_all(name)?;
    }
    sync_dir_required(parent)?;
    Ok(())
}

fn package_has_required_shape(
    package: &Dir,
    required_files: &[&str],
) -> Result<bool, PackageStoreError> {
    for name in required_files {
        let metadata = match package.symlink_metadata(name) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        if require_regular_entry(&metadata.file_type(), &name).is_err() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn recover_locked(root: &Dir, required_files: &[&str]) -> Result<(), PackageStoreError> {
    validate_required_files(required_files)?;
    for name in entry_names(root)? {
        if name.starts_with(INSTALL_PREFIX) || name.starts_with(RETIRED_PREFIX) {
            reclaim_entry(root, &name)?;
            continue;
        }
        if name.starts_with('.') {
            continue;
        }
        let Some(id_dir) = open_existing_dir_nofollow(root, &name)? else {
            continue;
        };
        for version in entry_names(&id_dir)? {
            let valid = match open_existing_dir_nofollow(&id_dir, &version) {
                Ok(Some(package)) => package_has_required_shape(&package, required_files)?,
                Ok(None) | Err(FilesystemError::UnsafeEntry(_)) => false,
                Err(error) => return Err(error.into()),
            };
            if !valid {
                reclaim_entry(&id_dir, &version)?;
            }
        }
        if entry_names(&id_dir)?.is_empty() {
            drop(id_dir);
            match root.remove_dir(&name) {
                Ok(()) => sync_dir_required(root)?,
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}

/// Reclaim interrupted staging, retired names, and incomplete active packages.
///
/// A package is physically complete when every `required_files` entry exists
/// as a regular file. Extra entries do not prove a crash-torn publication and
/// are left for the caller's semantic validation and policy.
pub fn recover_package_store(
    root: &Path,
    required_files: &[&str],
) -> Result<(), PackageStoreError> {
    let _guard = lock_store();
    let root = open_store_root(root)?;
    recover_locked(&root, required_files)
}

fn existing_package_exact(
    id_dir: &Dir,
    version: &str,
    files: &[PackageFile<'_>],
) -> Result<Option<bool>, PackageStoreError> {
    let Some(package) = open_existing_dir_nofollow(id_dir, version)? else {
        return Ok(None);
    };
    for file in files {
        let existing = match read_required_regular(
            &package,
            file.name,
            file.bytes.len() as u64,
            Some(file.bytes.len() as u64),
        ) {
            Ok(existing) => existing,
            Err(
                FilesystemError::StoredLengthMismatch { .. }
                | FilesystemError::StoredFileTooLarge { .. }
                | FilesystemError::UnsafeEntry(_),
            ) => return Ok(Some(false)),
            Err(FilesystemError::Io(error)) if error.kind() == ErrorKind::NotFound => {
                return Ok(Some(false))
            }
            Err(error) => return Err(error.into()),
        };
        if existing != file.bytes {
            return Ok(Some(false));
        }
    }
    Ok(Some(true))
}

fn publish_locked<F>(
    root_path: &Path,
    id: &str,
    version: &str,
    staging_name: &str,
    files: &[PackageFile<'_>],
    mut after_step: F,
) -> Result<PackagePublishOutcome, PackageStoreError>
where
    F: FnMut() -> io::Result<()>,
{
    validate_component(id, "package id")?;
    validate_component(version, "package version")?;
    validate_transient(staging_name, INSTALL_PREFIX)?;
    validate_files(files)?;
    let required = files.iter().map(|file| file.name).collect::<Vec<_>>();
    let root = open_store_root(root_path)?;
    recover_locked(&root, &required)?;
    after_step()?;

    ensure_directory_nofollow(&root, id)?;
    let id_dir = open_dir_nofollow(&root, id)?;
    after_step()?;

    match root.create_dir(staging_name) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            return Err(PackageStoreError::TransientNameCollision)
        }
        Err(error) => return Err(error.into()),
    }
    sync_dir_required(&root)?;
    after_step()?;
    let staging = open_dir_nofollow(&root, staging_name)?;
    for file in files {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        let mut output = staging.open_with(file.name, &options)?;
        output.write_all(file.bytes)?;
        output.sync_all()?;
        after_step()?;
    }
    sync_dir_required(&staging)?;
    after_step()?;
    drop(staging);

    match move_directory_no_replace(&root, staging_name, &id_dir, version) {
        Ok(()) => {
            after_step()?;
            sync_dir_required(&root)?;
            after_step()?;
            sync_dir_required(&id_dir)?;
            after_step()?;
            Ok(PackagePublishOutcome::Published)
        }
        Err(FilesystemError::Io(error)) if error.kind() == ErrorKind::AlreadyExists => {
            let exact = existing_package_exact(&id_dir, version, files)?.unwrap_or(false);
            reclaim_entry(&root, staging_name)?;
            if exact {
                Ok(PackagePublishOutcome::AlreadyPresentExact)
            } else {
                Err(PackageStoreError::ImmutableVersionCollision)
            }
        }
        Err(error) => Err(error.into()),
    }
}

/// Publish one immutable version from a durable staged directory without
/// replacing a concurrent destination.
pub fn publish_package_noclobber(
    root: &Path,
    id: &str,
    version: &str,
    staging_name: &str,
    files: &[PackageFile<'_>],
) -> Result<PackagePublishOutcome, PackageStoreError> {
    let _guard = lock_store();
    publish_locked(root, id, version, staging_name, files, || Ok(()))
}

fn retire_locked<F>(
    root_path: &Path,
    id: &str,
    version: &str,
    retired_name: &str,
    required_files: &[&str],
    mut after_step: F,
) -> Result<bool, PackageStoreError>
where
    F: FnMut() -> io::Result<()>,
{
    validate_component(id, "package id")?;
    validate_component(version, "package version")?;
    validate_transient(retired_name, RETIRED_PREFIX)?;
    validate_required_files(required_files)?;
    let root = open_store_root(root_path)?;
    recover_locked(&root, required_files)?;
    after_step()?;
    let id_dir = open_existing_dir_nofollow(&root, id)?
        .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "package id is not installed"))?;
    let package = open_existing_dir_nofollow(&id_dir, version)?
        .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "package version is not installed"))?;
    debug_assert!(package_has_required_shape(&package, required_files)?);
    drop(package);

    match move_directory_no_replace(&id_dir, version, &root, retired_name) {
        Ok(()) => {}
        Err(FilesystemError::Io(error)) if error.kind() == ErrorKind::AlreadyExists => {
            return Err(PackageStoreError::TransientNameCollision)
        }
        Err(error) => return Err(error.into()),
    }
    after_step()?;
    sync_dir_required(&id_dir)?;
    after_step()?;
    sync_dir_required(&root)?;
    after_step()?;
    reclaim_entry(&root, retired_name)?;
    after_step()?;

    let no_versions_remain = entry_names(&id_dir)?.is_empty();
    drop(id_dir);
    if no_versions_remain {
        root.remove_dir(id)?;
        sync_dir_required(&root)?;
        after_step()?;
    }
    Ok(no_versions_remain)
}

/// Durably retire one immutable package name, then reclaim its bytes.
///
/// The returned boolean is true when the package-id directory became empty and
/// was removed too.
pub fn retire_package(
    root: &Path,
    id: &str,
    version: &str,
    retired_name: &str,
    required_files: &[&str],
) -> Result<bool, PackageStoreError> {
    let _guard = lock_store();
    retire_locked(root, id, version, retired_name, required_files, || Ok(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem::PACKAGE_DIRECTORY_MOVE_SUPPORTED_TARGETS;
    use std::path::PathBuf;
    use std::sync::{Arc, Barrier};
    use uuid::Uuid;

    const REQUIRED: &[&str] = &["manifest.json", "plugin.wasm"];
    const MANIFEST: &[u8] = br#"{"id":"dev.tine.example","version":"1.0.0"}"#;
    const WASM: &[u8] = b"\0asm\x01\0\0\0";

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir()
                .join(format!("tine-storage-package-{label}-{}", Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn files(manifest: &'static [u8]) -> [PackageFile<'static>; 2] {
        [
            PackageFile {
                name: "manifest.json",
                bytes: manifest,
            },
            PackageFile {
                name: "plugin.wasm",
                bytes: WASM,
            },
        ]
    }

    fn assert_no_transients(root: &Path) {
        if !root.exists() {
            return;
        }
        assert!(std::fs::read_dir(root).unwrap().all(|entry| {
            let name = entry.unwrap().file_name();
            let name = name.to_string_lossy();
            !name.starts_with(INSTALL_PREFIX) && !name.starts_with(RETIRED_PREFIX)
        }));
    }

    fn assert_present_exact(root: &Path, manifest: &[u8]) {
        let package = root.join("dev.tine.example/1.0.0");
        assert_eq!(
            std::fs::read(package.join("manifest.json")).unwrap(),
            manifest
        );
        assert_eq!(std::fs::read(package.join("plugin.wasm")).unwrap(), WASM);
    }

    fn normalized_cfg_predicates_before_package_moves(source: &str) -> Vec<String> {
        let marker = "pub(crate) fn move_directory_no_replace";
        let chunks = source.split(marker).collect::<Vec<_>>();
        assert_eq!(
            chunks.len() - 1,
            5,
            "I-16: staged-directory publication must have exactly four shipped-target arms and one unsupported-target stub; imitate src/filesystem.rs::move_directory_no_replace"
        );
        chunks[..chunks.len() - 1]
            .iter()
            .map(|chunk| {
                let start = chunk.rfind("#[cfg(").expect(
                    "I-16: every move_directory_no_replace definition needs an immediately preceding cfg predicate; imitate src/filesystem.rs",
                );
                chunk[start..]
                    .chars()
                    .filter(|character| !character.is_whitespace())
                    .collect::<String>()
            })
            .collect()
    }

    fn assert_package_directory_move_cfgs(source: &str) {
        assert_eq!(
            normalized_cfg_predicates_before_package_moves(source),
            [
                "#[cfg(target_os=\"linux\")]",
                "#[cfg(target_os=\"android\")]",
                "#[cfg(any(target_os=\"macos\",target_os=\"ios\"))]",
                "#[cfg(windows)]",
                "#[cfg(not(any(target_os=\"linux\",target_os=\"macos\",target_os=\"ios\",target_os=\"android\",windows)))]",
            ],
            "I-16: package directory publication must name Linux, Android, macOS+iOS, Windows, then only the genuinely unsupported remainder; imitate src/filesystem.rs::move_directory_no_replace"
        );
    }

    #[test]
    fn package_directory_move_cfg_names_every_shipped_target() {
        assert_eq!(
            PACKAGE_DIRECTORY_MOVE_SUPPORTED_TARGETS,
            ["linux", "macos", "ios", "android", "windows"]
        );
        assert_package_directory_move_cfgs(include_str!("filesystem.rs"));
    }

    #[test]
    fn package_directory_move_cfg_guard_rejects_a_missing_arm() {
        let source = include_str!("filesystem.rs");
        let apple = source
            .find("#[cfg(any(target_os = \"macos\", target_os = \"ios\"))]")
            .unwrap();
        let windows = source[apple..].find("#[cfg(windows)]").unwrap() + apple;
        let scratch = format!("{}{}", &source[..apple], &source[windows..]);
        let result = std::panic::catch_unwind(|| assert_package_directory_move_cfgs(&scratch));
        assert!(
            result.is_err(),
            "I-16: the cfg guard accepted a scratch source with its Apple arm deleted"
        );
    }

    #[test]
    fn exact_retry_is_idempotent_and_different_bytes_never_clobber() {
        let temp = TestRoot::new("exact");
        let root = temp.path().join("plugins");
        assert_eq!(
            publish_package_noclobber(
                &root,
                "dev.tine.example",
                "1.0.0",
                ".install-dev.tine.example-1.0.0-1-1",
                &files(MANIFEST),
            )
            .unwrap(),
            PackagePublishOutcome::Published
        );
        assert_eq!(
            publish_package_noclobber(
                &root,
                "dev.tine.example",
                "1.0.0",
                ".install-dev.tine.example-1.0.0-1-2",
                &files(MANIFEST),
            )
            .unwrap(),
            PackagePublishOutcome::AlreadyPresentExact
        );
        let different = br#"{ "id": "dev.tine.example", "version": "1.0.0" }"#;
        assert!(matches!(
            publish_package_noclobber(
                &root,
                "dev.tine.example",
                "1.0.0",
                ".install-dev.tine.example-1.0.0-1-3",
                &files(different),
            ),
            Err(PackageStoreError::ImmutableVersionCollision)
        ));
        assert_present_exact(&root, MANIFEST);
        assert_no_transients(&root);
    }

    #[test]
    fn sequential_second_writer_refuses_without_clobbering_the_complete_winner() {
        let temp = TestRoot::new("sequential-second-writer");
        let root = Arc::new(temp.path().join("plugins"));
        let barrier = Arc::new(Barrier::new(3));
        let manifests: [&'static [u8]; 2] = [
            MANIFEST,
            br#"{ "id": "dev.tine.example", "version": "1.0.0" }"#,
        ];
        let workers = manifests
            .into_iter()
            .enumerate()
            .map(|(index, manifest)| {
                let root = Arc::clone(&root);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    publish_package_noclobber(
                        &root,
                        "dev.tine.example",
                        "1.0.0",
                        &format!(".install-dev.tine.example-1.0.0-2-{index}"),
                        &files(manifest),
                    )
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let outcomes = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Ok(PackagePublishOutcome::Published)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(
                    outcome,
                    Err(PackageStoreError::ImmutableVersionCollision)
                ))
                .count(),
            1
        );
        let stored = std::fs::read(root.join("dev.tine.example/1.0.0/manifest.json")).unwrap();
        assert!(manifests.contains(&stored.as_slice()));
        assert_eq!(
            std::fs::read(root.join("dev.tine.example/1.0.0/plugin.wasm")).unwrap(),
            WASM
        );
        assert_no_transients(&root);
    }

    #[test]
    fn reopen_reclaims_staged_retired_and_wedged_half_packages() {
        let temp = TestRoot::new("recovery");
        let root = temp.path().join("plugins");
        std::fs::create_dir_all(root.join(".install-dev.tine.example-1.0.0-3-1")).unwrap();
        std::fs::create_dir_all(root.join(".retired-dev.tine.example-1.0.0-3-2")).unwrap();
        let wedged = root.join("dev.tine.example/1.0.0");
        std::fs::create_dir_all(&wedged).unwrap();
        std::fs::write(wedged.join("manifest.json"), MANIFEST).unwrap();

        recover_package_store(&root, REQUIRED).unwrap();

        assert!(!root.join("dev.tine.example/1.0.0").exists());
        assert_no_transients(&root);
        assert_eq!(
            publish_package_noclobber(
                &root,
                "dev.tine.example",
                "1.0.0",
                ".install-dev.tine.example-1.0.0-3-3",
                &files(MANIFEST),
            )
            .unwrap(),
            PackagePublishOutcome::Published
        );
    }

    #[test]
    fn recovery_preserves_complete_package_with_extra_regular_file() {
        let temp = TestRoot::new("recovery-extra");
        let root = temp.path().join("plugins");
        publish_package_noclobber(
            &root,
            "dev.tine.example",
            "1.0.0",
            ".install-dev.tine.example-1.0.0-extra",
            &files(MANIFEST),
        )
        .unwrap();
        let package = root.join("dev.tine.example/1.0.0");
        std::fs::write(package.join(".DS_Store"), b"finder metadata").unwrap();

        recover_package_store(&root, REQUIRED).unwrap();

        assert_present_exact(&root, MANIFEST);
        assert_eq!(
            std::fs::read(package.join(".DS_Store")).unwrap(),
            b"finder metadata"
        );
    }

    #[test]
    fn every_publish_crash_cut_reopens_to_exact_or_absent() {
        // Store-ready, id-ready, stage-create, two file syncs, stage sync,
        // publish, source-parent sync, destination-parent sync.
        for cut in 0..9 {
            let temp = TestRoot::new("publish-cut");
            let root = temp.path().join("plugins");
            let mut step = 0_usize;
            let result = publish_locked(
                &root,
                "dev.tine.example",
                "1.0.0",
                &format!(".install-dev.tine.example-1.0.0-4-{cut}"),
                &files(MANIFEST),
                || {
                    let current = step;
                    step += 1;
                    if current == cut {
                        Err(io::Error::new(ErrorKind::Interrupted, "injected crash cut"))
                    } else {
                        Ok(())
                    }
                },
            );
            assert!(result.is_err(), "publish cut {cut} was not reached");
            recover_package_store(&root, REQUIRED).unwrap();
            let package = root.join("dev.tine.example/1.0.0");
            if cut >= 6 {
                assert!(
                    package.exists(),
                    "publish cut {cut} occurred after no-replace publication"
                );
                assert_present_exact(&root, MANIFEST);
            } else {
                assert!(!package.exists(), "publish cut {cut} preceded publication");
            }
            assert_no_transients(&root);
        }
    }

    #[test]
    fn every_retire_crash_cut_reopens_to_exact_or_absent() {
        // Store-ready, retire rename, id-parent sync, root sync, reclaim,
        // empty-id reclaim.
        for cut in 0..6 {
            let temp = TestRoot::new("retire-cut");
            let root = temp.path().join("plugins");
            publish_package_noclobber(
                &root,
                "dev.tine.example",
                "1.0.0",
                ".install-dev.tine.example-1.0.0-5-0",
                &files(MANIFEST),
            )
            .unwrap();
            let mut step = 0_usize;
            let result = retire_locked(
                &root,
                "dev.tine.example",
                "1.0.0",
                &format!(".retired-dev.tine.example-1.0.0-5-{cut}"),
                REQUIRED,
                || {
                    let current = step;
                    step += 1;
                    if current == cut {
                        Err(io::Error::new(ErrorKind::Interrupted, "injected crash cut"))
                    } else {
                        Ok(())
                    }
                },
            );
            assert!(result.is_err(), "retire cut {cut} was not reached");
            recover_package_store(&root, REQUIRED).unwrap();
            let package = root.join("dev.tine.example/1.0.0");
            if cut >= 4 {
                assert!(
                    !package.exists(),
                    "retire cut {cut} occurred after retired-entry reclamation"
                );
            } else if package.exists() {
                assert_present_exact(&root, MANIFEST);
            }
            assert_no_transients(&root);
        }
    }
}
