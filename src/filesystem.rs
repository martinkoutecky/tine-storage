#[cfg(windows)]
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _, OpenOptionsMaybeDirExt as _};
use cap_std::fs::{Dir, OpenOptions};
#[cfg(unix)]
use std::ffi::CString;
use std::fmt;
use std::fs;
use std::io::{self, ErrorKind, Read, Write};
#[cfg(unix)]
use std::os::fd::{AsFd as _, AsRawFd as _, FromRawFd as _};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;
#[cfg(windows)]
use std::os::windows::ffi::{OsStrExt as _, OsStringExt as _};
#[cfg(windows)]
use std::os::windows::fs::MetadataExt as _;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle as _;
#[cfg(windows)]
use std::path::PathBuf;
#[cfg(windows)]
use std::sync::{Mutex, OnceLock};
use uuid::Uuid;

/// A failure at the generic physical-filesystem boundary.
#[derive(Debug)]
pub enum FilesystemError {
    Io(io::Error),
    /// The platform could not prove the documented write-through name
    /// operations needed for a replaceable authority. Callers must leave the
    /// caller-owned authority untouched and refuse activation instead of
    /// silently falling back to an ordinary rename.
    DurableNameOperationUnavailable(String),
    UnsafeEntry(String),
    StoredLengthMismatch {
        path: String,
        expected: u64,
        actual: u64,
    },
    StoredFileTooLarge {
        path: String,
        length: u64,
        limit: u64,
    },
    ByteCollision,
}

impl fmt::Display for FilesystemError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::DurableNameOperationUnavailable(message) => {
                write!(
                    f,
                    "durable write-through name operation unavailable: {message}"
                )
            }
            Self::UnsafeEntry(message) => message.fmt(f),
            Self::StoredLengthMismatch {
                path,
                expected,
                actual,
            } => write!(
                f,
                "stored file length mismatch for {path}: expected {expected}, got {actual}"
            ),
            Self::StoredFileTooLarge {
                path,
                length,
                limit,
            } => write!(
                f,
                "stored file is too large for {path}: {length} bytes exceeds {limit}"
            ),
            Self::ByteCollision => f.write_str("immutable byte collision"),
        }
    }
}

impl std::error::Error for FilesystemError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for FilesystemError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

// `LockFileEx(..., LOCKFILE_FAIL_IMMEDIATELY, ...)` reports this Win32 code
// when another handle owns an overlapping byte-range lock. Keep the numeric
// value available to platform-neutral unit tests; the Windows SDK defines
// `ERROR_LOCK_VIOLATION` as 33.
/// A directory capability validated for a durable name-operation publication.
#[cfg(windows)]
pub struct ValidatedDirectorySync {
    // Retain the exact validated object for the whole publication. cap-std
    // opens directory capabilities without FILE_SHARE_DELETE, so this object
    // cannot be renamed or deleted underneath the operation.
    _capability: fs::File,
    entry_durability: WindowsDirectoryEntryDurability,
}

/// A directory capability validated for a durable name-operation publication.
#[cfg(not(windows))]
pub struct ValidatedDirectorySync<'a>(&'a Dir);

#[cfg(any(test, windows))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WindowsDirectoryEntryDurability {
    UnsupportedAfterValidation,
}

#[cfg(windows)]
impl ValidatedDirectorySync {
    /// Validate and retain `dir` for the duration of a publication.
    pub fn open(dir: &Dir) -> io::Result<Self> {
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

        let capability = dir.try_clone()?.into_std_file();
        let metadata = capability.metadata()?;
        let entry_durability = validated_windows_directory_entry_durability(
            metadata.is_dir(),
            metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0,
        )?;

        Ok(Self {
            _capability: capability,
            entry_durability,
        })
    }

    /// Synchronize the directory entry or report the platform durability limit.
    pub fn sync(&self) -> io::Result<()> {
        match self.entry_durability {
            WindowsDirectoryEntryDurability::UnsupportedAfterValidation => Ok(()),
        }
    }
}

#[cfg(unix)]
impl<'a> ValidatedDirectorySync<'a> {
    /// Validate and retain `dir` for the duration of a publication.
    pub fn open(dir: &'a Dir) -> io::Result<Self> {
        Ok(Self(dir))
    }

    /// Validate that directory synchronization can proceed.
    pub fn preflight(&self) -> io::Result<()> {
        Ok(())
    }

    /// Synchronize the directory entry.
    pub fn sync(&self) -> io::Result<()> {
        // cap-std may retain an O_PATH capability, which is suitable for openat
        // but cannot itself be fsynced. Open `.` as a real directory descriptor.
        let fd = unsafe {
            libc::openat(
                self.0.as_fd().as_raw_fd(),
                c".".as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: openat returned one newly owned directory descriptor.
        unsafe { fs::File::from_raw_fd(fd) }.sync_all()
    }
}

#[cfg(not(any(unix, windows)))]
impl<'a> ValidatedDirectorySync<'a> {
    /// Validate and retain `dir` for the duration of a publication.
    pub fn open(dir: &'a Dir) -> io::Result<Self> {
        Ok(Self(dir))
    }

    /// Validate that directory synchronization can proceed.
    pub fn preflight(&self) -> io::Result<()> {
        Ok(())
    }

    /// Synchronize the directory entry.
    pub fn sync(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "directory durability is unsupported on this target",
        ))
    }
}

/// Synchronize `dir` after a required durable directory-entry update.
pub fn sync_dir_required(dir: &Dir) -> io::Result<()> {
    ValidatedDirectorySync::open(dir)?.sync()
}

/// A retained directory capability which has proved the platform's
/// write-through create, replacement, reopen, and retirement operations in a
/// private same-directory namespace.
///
/// This is intentionally a typed boundary rather than a boolean capability
/// check: callers can only mutate a replaceable authority through the object
/// that retained the exact no-follow directory capability used by the probe.
/// On Windows, [`DurableDirectoryPublication::open`] refuses if the documented
/// `MoveFileExW(..., MOVEFILE_WRITE_THROUGH)` protocol cannot be demonstrated;
/// it never falls back to `std::fs::rename`. The first retained capability for
/// one exact directory proves the protocol; later opens of that same live
/// directory identity reuse the process-local proof while still revalidating
/// their own retained no-follow capability.
pub struct DurableDirectoryPublication {
    dir: Dir,
    #[cfg(windows)]
    windows: WindowsWriteThroughDirectory,
}

impl DurableDirectoryPublication {
    /// Retain `dir` and prove the durable name-operation capability before any
    /// caller-owned authority is created, replaced, or retired.
    pub fn open(dir: &Dir) -> Result<Self, FilesystemError> {
        #[cfg(windows)]
        {
            let publication = Self {
                dir: dir.try_clone()?,
                windows: WindowsWriteThroughDirectory::open(dir)?,
            };
            publication.probe_windows_write_through_once_per_directory()?;
            return Ok(publication);
        }

        #[cfg(not(windows))]
        {
            // Preserve the pre-v2 Unix durability contract while retaining a
            // typed API shared with the Windows implementation.
            ValidatedDirectorySync::open(dir)?.preflight()?;
            Ok(Self {
                dir: dir.try_clone()?,
            })
        }
    }

    /// Create one previously absent authority name from exact bytes.
    ///
    /// If the name already names the same exact bytes, this is idempotent. A
    /// different existing file is a collision and is never overwritten.
    pub fn publish_new_exact(&self, name: &str, bytes: &[u8]) -> Result<(), FilesystemError> {
        validate_single_entry_name(name)?;
        #[cfg(windows)]
        {
            self.windows.validate()?;
            return self.windows.publish_new_exact(&self.dir, name, bytes);
        }
        #[cfg(not(windows))]
        {
            publish_immutable_exact(&self.dir, name, bytes)
        }
    }

    /// Create one previously absent authority name while the caller holds the
    /// sole writer lease for this private namespace.
    ///
    /// This has the same exact-byte and no-overwrite contract as
    /// [`Self::publish_new_exact`]. On Android only, a denied hard-link based
    /// no-replace installation may fall back to an ordinary same-directory
    /// atomic rename after proving that the target is absent. Shared/provider
    /// namespaces must continue to use [`Self::publish_new_exact`].
    pub fn publish_new_exact_single_writer(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<(), FilesystemError> {
        validate_single_entry_name(name)?;
        #[cfg(windows)]
        {
            self.windows.validate()?;
            return self.windows.publish_new_exact(&self.dir, name, bytes);
        }
        #[cfg(not(windows))]
        {
            publish_immutable_exact_single_writer(&self.dir, name, bytes)
        }
    }

    /// Replace `name` only when it still contains `expected`, then reopen and
    /// verify the exact replacement.
    ///
    /// The caller supplies its single-writer/authority lease. A current target
    /// already equal to `replacement` is accepted as an idempotent retry;
    /// every other current value fails closed as [`FilesystemError::ByteCollision`].
    pub fn replace_exact(
        &self,
        name: &str,
        expected: &[u8],
        replacement: &[u8],
    ) -> Result<(), FilesystemError> {
        validate_single_entry_name(name)?;
        #[cfg(windows)]
        {
            self.windows.validate()?;
            return self
                .windows
                .replace_exact(&self.dir, name, expected, replacement);
        }
        #[cfg(not(windows))]
        {
            replace_regular_exact_unix(&self.dir, name, expected, replacement)
        }
    }

    /// Move one existing exact regular file to a previously absent name in the
    /// same retained directory, without replacing a concurrent target.
    ///
    /// The source bytes and identity are verified before and after the move.
    /// A retry after the source has disappeared accepts the destination only
    /// when it contains the exact expected bytes. The caller owns the source
    /// name as a single writer for the duration of the call, and destination
    /// names must be content-determined: an existing destination with the exact
    /// expected bytes is accepted as the same completed move. This is the
    /// generic durable name-transition primitive for caller-owned staged and
    /// recovery files; unlike [`Self::retire_exact`], the destination need not
    /// be a retired authority name.
    pub fn move_exact_no_replace(
        &self,
        source_name: &str,
        destination_name: &str,
        expected: &[u8],
    ) -> Result<(), FilesystemError> {
        validate_single_entry_name(source_name)?;
        validate_single_entry_name(destination_name)?;
        if source_name == destination_name {
            return Err(FilesystemError::UnsafeEntry(
                "source and destination names must differ".into(),
            ));
        }
        #[cfg(windows)]
        {
            self.windows.validate()?;
            return self.windows.move_exact_no_replace(
                &self.dir,
                source_name,
                destination_name,
                expected,
            );
        }
        #[cfg(not(windows))]
        {
            move_regular_exact_no_replace_unix(&self.dir, source_name, destination_name, expected)
        }
    }

    /// Atomically install a caller-owned staged cache file under an active
    /// name in this retained directory, creating or replacing one regular
    /// destination file.
    ///
    /// The caller must hold the namespace's exclusive writer lease. For a
    /// SQLite projection it must also drain readers and writers, checkpoint
    /// the old WAL, close every conflicting handle, and resolve sidecars
    /// before this call; this filesystem operation does not manage SQLite
    /// state. The staged file is flushed, closed, moved without reading or
    /// copying its contents, and the published regular-file identity is
    /// verified. Source and destination are single entry names and may not be
    /// equal; symlinks and other non-regular entries are refused.
    ///
    /// An error after the native replacement can mean the staged identity is
    /// already installed. This method deliberately does not delete the
    /// destination on any error; the caller must reopen and validate or rebuild
    /// the disposable cache.
    pub fn replace_from_staged_regular_single_writer(
        &self,
        source_name: &str,
        destination_name: &str,
    ) -> Result<(), FilesystemError> {
        validate_single_entry_name(source_name)?;
        validate_single_entry_name(destination_name)?;
        if source_name == destination_name {
            return Err(FilesystemError::UnsafeEntry(
                "source and destination names must differ".into(),
            ));
        }
        validate_staged_regular_replacement_entries(&self.dir, source_name, destination_name)?;

        #[cfg(windows)]
        {
            self.windows.validate()?;
            return self.windows.replace_from_staged_regular_single_writer(
                &self.dir,
                source_name,
                destination_name,
            );
        }
        #[cfg(unix)]
        {
            replace_from_staged_regular_unix(&self.dir, source_name, destination_name)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = (source_name, destination_name);
            Err(FilesystemError::DurableNameOperationUnavailable(
                "staged regular-file replacement is unsupported on this target".into(),
            ))
        }
    }

    /// Retire an authority by a no-replace same-directory rename to a fresh
    /// name outside that authority's selector grammar.
    ///
    /// The method verifies the old authority bytes and identity, then verifies
    /// the retired name and the active-name absence. It is deliberately not a
    /// delete API: a failed retirement must leave a recoverable authority.
    pub fn retire_exact(
        &self,
        active_name: &str,
        retired_name: &str,
        expected: &[u8],
    ) -> Result<(), FilesystemError> {
        self.move_exact_no_replace(active_name, retired_name, expected)
    }
}

fn validate_single_entry_name(name: &str) -> Result<(), FilesystemError> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\', '\0']) {
        return Err(FilesystemError::UnsafeEntry(format!(
            "durable publication name is not one safe directory entry: {name:?}"
        )));
    }
    Ok(())
}

fn validate_staged_regular_replacement_entries(
    dir: &Dir,
    source_name: &str,
    destination_name: &str,
) -> Result<(), FilesystemError> {
    let source = dir
        .symlink_metadata(source_name)
        .map_err(FilesystemError::from)?;
    if source.file_type().is_symlink() || !source.is_file() {
        return Err(FilesystemError::UnsafeEntry(format!(
            "staged source is not a regular no-follow file: {source_name}"
        )));
    }
    match dir.symlink_metadata(destination_name) {
        Ok(destination) if destination.file_type().is_symlink() || !destination.is_file() => {
            Err(FilesystemError::UnsafeEntry(format!(
                "publication destination is not a regular no-follow file: {destination_name}"
            )))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
type UnixFileIdentity = (u64, u64);

#[cfg(unix)]
fn open_flushed_staged_regular_unix(
    dir: &Dir,
    name: &str,
) -> Result<(fs::File, UnixFileIdentity), FilesystemError> {
    let name_c = CString::new(name)
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "invalid staged filename"))?;
    // SAFETY: `name_c` and the retained directory descriptor remain live for
    // the call. O_NOFOLLOW binds regular-file validation and flushing to the
    // exact staged identity which will be renamed after this handle closes.
    let fd = unsafe {
        libc::openat(
            dir.as_fd().as_raw_fd(),
            name_c.as_ptr(),
            libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: openat returned one newly owned descriptor.
    let file = unsafe { fs::File::from_raw_fd(fd) };
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(FilesystemError::UnsafeEntry(format!(
            "staged source is not a regular no-follow file: {name}"
        )));
    }
    file.sync_all()?;
    Ok((file, (metadata.dev(), metadata.ino())))
}

#[cfg(unix)]
fn verify_unix_regular_identity(
    dir: &Dir,
    name: &str,
    expected: UnixFileIdentity,
) -> Result<(), FilesystemError> {
    let file = open_file_nofollow(dir, name)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || (metadata.dev(), metadata.ino()) != expected {
        return Err(FilesystemError::ByteCollision);
    }
    Ok(())
}

#[cfg(target_os = "android")]
fn finish_staged_regular_replacement_sync(
    dir: &Dir,
    destination_name: &str,
    expected: UnixFileIdentity,
    result: io::Result<()>,
) -> Result<(), FilesystemError> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if android_durability_capability_refusal(&error) => {
            verify_unix_regular_identity(dir, destination_name, expected)?;
            let (published, identity) = open_flushed_staged_regular_unix(dir, destination_name)?;
            drop(published);
            if identity != expected {
                return Err(FilesystemError::ByteCollision);
            }
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(all(unix, not(target_os = "android")))]
fn finish_staged_regular_replacement_sync(
    _dir: &Dir,
    _destination_name: &str,
    _expected: UnixFileIdentity,
    result: io::Result<()>,
) -> Result<(), FilesystemError> {
    result.map_err(Into::into)
}

#[cfg(unix)]
fn replace_from_staged_regular_unix(
    dir: &Dir,
    source_name: &str,
    destination_name: &str,
) -> Result<(), FilesystemError> {
    let publication_sync = ValidatedDirectorySync::open(dir)?;
    publication_sync.preflight()?;
    let (source, source_identity) = open_flushed_staged_regular_unix(dir, source_name)?;
    drop(source);

    // This is the same cap-std native same-directory replacement already used
    // by replace_regular_exact_unix; no second OS rename implementation exists.
    dir.rename(source_name, dir, destination_name)?;
    finish_staged_regular_replacement_sync(
        dir,
        destination_name,
        source_identity,
        publication_sync.sync(),
    )?;
    verify_unix_regular_identity(dir, destination_name, source_identity)?;
    match dir.symlink_metadata(source_name) {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(FilesystemError::ByteCollision),
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(windows))]
fn read_regular_for_transition(
    dir: &Dir,
    name: &str,
    expected_or_replacement_limit: usize,
) -> Result<Option<Vec<u8>>, FilesystemError> {
    match read_optional_regular(
        dir,
        name,
        expected_or_replacement_limit.saturating_add(1) as u64,
        None,
    ) {
        Err(FilesystemError::StoredFileTooLarge { .. }) => Err(FilesystemError::ByteCollision),
        result => result,
    }
}

#[cfg(not(windows))]
fn replace_regular_exact_unix(
    dir: &Dir,
    name: &str,
    expected: &[u8],
    replacement: &[u8],
) -> Result<(), FilesystemError> {
    let limit = expected.len().max(replacement.len());
    let current = read_regular_for_transition(dir, name, limit)?;
    if current.as_deref() == Some(replacement) {
        sync_dir_required(dir)?;
        return Ok(());
    }
    if current.as_deref() != Some(expected) {
        return Err(FilesystemError::ByteCollision);
    }

    let temp_name = format!(".tmp-{}", Uuid::new_v4());
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    let mut temp = dir.open_with(&temp_name, &options)?;
    let result = (|| {
        temp.write_all(replacement)?;
        temp.sync_all()?;
        drop(temp);
        dir.rename(&temp_name, dir, name)?;
        sync_dir_required(dir)?;
        verify_existing(dir, name, replacement)
    })();
    let cleanup = dir.remove_file(&temp_name);
    if let Err(error) = result {
        let _ = cleanup;
        return Err(error);
    }
    if cleanup
        .as_ref()
        .is_err_and(|error| error.kind() != ErrorKind::NotFound)
    {
        cleanup?;
    }
    Ok(())
}

#[cfg(not(windows))]
fn move_regular_exact_no_replace_unix(
    dir: &Dir,
    source_name: &str,
    destination_name: &str,
    expected: &[u8],
) -> Result<(), FilesystemError> {
    match read_regular_for_transition(dir, source_name, expected.len())? {
        Some(active) if active == expected => {
            match rename_noreplace(dir, source_name, destination_name) {
                Ok(()) => {}
                #[cfg(any(target_os = "macos", target_os = "ios", target_os = "android"))]
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                    finish_interrupted_hard_link_move(
                        dir,
                        source_name,
                        destination_name,
                        expected,
                    )?;
                }
                Err(error) => return Err(error.into()),
            }
            sync_dir_required(dir)?;
        }
        Some(_) => return Err(FilesystemError::ByteCollision),
        None => {
            // An interrupted caller may retry after the durable rename
            // completed but before it observed the result.
            verify_existing(dir, destination_name, expected)?;
            sync_dir_required(dir)?;
            return Ok(());
        }
    }
    verify_existing(dir, destination_name, expected)?;
    if read_regular_for_transition(dir, source_name, expected.len())?.is_some() {
        return Err(FilesystemError::ByteCollision);
    }
    Ok(())
}

/// Apple platforms and Android implement no-replace as hard-link then unlink. If the
/// process stops between those calls, both names identify the same exact file.
/// Completing that interrupted move is safe under the public single-writer,
/// content-determined-name contract; any different inode or bytes fail closed.
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    all(test, unix)
))]
fn finish_interrupted_hard_link_move(
    dir: &Dir,
    source_name: &str,
    destination_name: &str,
    expected: &[u8],
) -> Result<(), FilesystemError> {
    let source = open_file_nofollow(dir, source_name)?;
    let destination = open_file_nofollow(dir, destination_name)?;
    let source_metadata = source.metadata()?;
    let destination_metadata = destination.metadata()?;
    if !source_metadata.is_file()
        || !destination_metadata.is_file()
        || source_metadata.len() != expected.len() as u64
        || destination_metadata.len() != expected.len() as u64
        || source_metadata.dev() != destination_metadata.dev()
        || source_metadata.ino() != destination_metadata.ino()
    {
        return Err(FilesystemError::ByteCollision);
    }
    drop(source);
    drop(destination);
    verify_existing(dir, source_name, expected)?;
    verify_existing(dir, destination_name, expected)?;
    dir.remove_file(source_name)?;
    Ok(())
}

#[cfg(windows)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct WindowsFileIdentity {
    volume_serial: u32,
    file_index: u64,
}

#[cfg(windows)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WindowsDirectoryProbeKey {
    path: PathBuf,
    identity: WindowsFileIdentity,
}

#[cfg(windows)]
struct WindowsWriteThroughDirectory {
    // This must outlive every MoveFileExW call. cap-std opens directory
    // capabilities without FILE_SHARE_DELETE, so the validated directory
    // object cannot be renamed/deleted between capability proof and publish.
    capability: fs::File,
    path: PathBuf,
    identity: WindowsFileIdentity,
}

#[cfg(windows)]
impl WindowsWriteThroughDirectory {
    fn probe_key(&self) -> WindowsDirectoryProbeKey {
        WindowsDirectoryProbeKey {
            path: self.path.clone(),
            identity: self.identity,
        }
    }

    fn open(dir: &Dir) -> Result<Self, FilesystemError> {
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

        let capability = dir.try_clone()?.into_std_file();
        let metadata = capability.metadata()?;
        validated_windows_directory_entry_durability(
            metadata.is_dir(),
            metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0,
        )
        .map_err(|error| FilesystemError::UnsafeEntry(error.to_string()))?;
        let identity = windows_file_identity(&capability)?;
        let path = windows_final_path(&capability)?;
        Ok(Self {
            capability,
            path,
            identity,
        })
    }

    fn validate(&self) -> Result<(), FilesystemError> {
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

        let metadata = self.capability.metadata()?;
        if !metadata.is_dir()
            || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
            || windows_file_identity(&self.capability)? != self.identity
        {
            return Err(FilesystemError::UnsafeEntry(
                "retained durable directory capability no longer proves the same real directory"
                    .into(),
            ));
        }
        Ok(())
    }

    fn path_for(&self, name: &str) -> Result<Vec<u16>, FilesystemError> {
        validate_single_entry_name(name)?;
        let path = self.path.join(name);
        Ok(path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect())
    }

    fn create_flushed_temp(
        &self,
        dir: &Dir,
        label: &str,
        bytes: &[u8],
    ) -> Result<(String, WindowsFileIdentity), FilesystemError> {
        let temp_name = format!(".tine-storage-{label}-{}", Uuid::new_v4().simple());
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        let mut temp = dir.open_with(&temp_name, &options)?.into_std();
        let result = (|| {
            temp.write_all(bytes)?;
            temp.sync_all()?;
            let metadata = temp.metadata()?;
            if !metadata.is_file() || metadata.len() != bytes.len() as u64 {
                return Err(FilesystemError::StoredLengthMismatch {
                    path: temp_name.clone(),
                    expected: bytes.len() as u64,
                    actual: metadata.len(),
                });
            }
            windows_file_identity(&temp).map_err(FilesystemError::from)
        })();
        drop(temp);
        result.map(|identity| (temp_name, identity))
    }

    fn move_write_through(
        &self,
        from: &str,
        to: &str,
        replace_existing: bool,
    ) -> Result<(), FilesystemError> {
        self.move_to_write_through(from, self, to, replace_existing)
    }

    fn move_to_write_through(
        &self,
        from: &str,
        destination: &Self,
        to: &str,
        replace_existing: bool,
    ) -> Result<(), FilesystemError> {
        use windows_sys::Win32::Storage::FileSystem::{
            MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        };

        self.validate()?;
        destination.validate()?;
        let from = self.path_for(from)?;
        let to = destination.path_for(to)?;
        let flags = MOVEFILE_WRITE_THROUGH
            | if replace_existing {
                MOVEFILE_REPLACE_EXISTING
            } else {
                0
            };
        // SAFETY: both zero-terminated paths are derived from the retained
        // no-follow directory capability and validated single-entry names.
        if unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), flags) } == 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(())
    }

    fn publish_new_exact(
        &self,
        dir: &Dir,
        name: &str,
        bytes: &[u8],
    ) -> Result<(), FilesystemError> {
        let (temp_name, identity) = self.create_flushed_temp(dir, "new", bytes)?;
        let result = match self.move_write_through(&temp_name, name, false) {
            Ok(()) => verify_windows_regular_exact(dir, name, bytes, Some(identity)),
            Err(FilesystemError::Io(error)) if error.kind() == ErrorKind::AlreadyExists => {
                verify_windows_regular_exact(dir, name, bytes, None)
            }
            Err(error) => Err(error),
        };
        cleanup_temp(dir, &temp_name);
        result
    }

    fn replace_exact(
        &self,
        dir: &Dir,
        name: &str,
        expected: &[u8],
        replacement: &[u8],
    ) -> Result<(), FilesystemError> {
        let current =
            read_windows_regular_with_limit(dir, name, expected.len().max(replacement.len()))?;
        if current.as_deref() == Some(replacement) {
            return Ok(());
        }
        if current.as_deref() != Some(expected) {
            return Err(FilesystemError::ByteCollision);
        }
        let (temp_name, identity) = self.create_flushed_temp(dir, "replace", replacement)?;
        // Any error after this documented replacement call is intentionally
        // returned to the journal as outcome-ambiguous; callers must reopen.
        let result = self
            .move_write_through(&temp_name, name, true)
            .and_then(|()| verify_windows_regular_exact(dir, name, replacement, Some(identity)));
        cleanup_temp(dir, &temp_name);
        result
    }

    fn move_exact_no_replace(
        &self,
        dir: &Dir,
        source_name: &str,
        destination_name: &str,
        expected: &[u8],
    ) -> Result<(), FilesystemError> {
        match read_windows_regular_with_identity(dir, source_name, expected.len())? {
            Some((bytes, identity)) if bytes == expected => {
                self.move_write_through(source_name, destination_name, false)?;
                verify_windows_regular_exact(dir, destination_name, expected, Some(identity))?;
                if read_windows_regular_with_identity(dir, source_name, expected.len())?.is_some() {
                    return Err(FilesystemError::ByteCollision);
                }
                Ok(())
            }
            Some(_) => Err(FilesystemError::ByteCollision),
            None => {
                // Idempotent retry after a successful write-through retirement.
                verify_windows_regular_exact(dir, destination_name, expected, None)
            }
        }
    }

    fn replace_from_staged_regular_single_writer(
        &self,
        dir: &Dir,
        source_name: &str,
        destination_name: &str,
    ) -> Result<(), FilesystemError> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).follow(FollowSymlinks::No);
        let source = dir.open_with(source_name, &options)?.into_std();
        reject_windows_reparse(&source, source_name)?;
        let metadata = source.metadata()?;
        if !metadata.is_file() {
            return Err(FilesystemError::UnsafeEntry(format!(
                "staged source is not a regular no-follow file: {source_name}"
            )));
        }
        source.sync_all()?;
        let source_identity = windows_file_identity(&source)?;
        drop(source);

        validate_windows_existing_regular_destination(dir, destination_name)?;
        self.move_write_through(source_name, destination_name, true)?;
        verify_windows_regular_identity(dir, destination_name, source_identity)?;
        match dir.symlink_metadata(source_name) {
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Ok(_) => Err(FilesystemError::ByteCollision),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(windows)]
impl DurableDirectoryPublication {
    fn probe_windows_write_through_once_per_directory(&self) -> Result<(), FilesystemError> {
        const MAX_CACHED_DIRECTORIES: usize = 1_024;
        static PROBED_DIRECTORIES: OnceLock<
            Mutex<std::collections::HashSet<WindowsDirectoryProbeKey>>,
        > = OnceLock::new();
        let directories =
            PROBED_DIRECTORIES.get_or_init(|| Mutex::new(std::collections::HashSet::new()));
        let mut directories = directories
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.windows.validate()?;
        let key = self.windows.probe_key();
        if directories.contains(&key) {
            return Ok(());
        }
        self.probe_windows_write_through()?;
        // The cache is only an optimization. Once bounded capacity is reached,
        // keep proving new directories on every open rather than allowing a
        // long-running multi-graph process to grow without limit.
        if directories.len() < MAX_CACHED_DIRECTORIES {
            directories.insert(key);
        }
        Ok(())
    }

    fn probe_windows_write_through(&self) -> Result<(), FilesystemError> {
        #[cfg(test)]
        {
            let probes = WINDOWS_WRITE_THROUGH_PROBES.get_or_init(|| {
                Mutex::new(std::collections::HashMap::<WindowsDirectoryProbeKey, usize>::new())
            });
            *probes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .entry(self.windows.probe_key())
                .or_default() += 1;
        }
        let source = format!(
            ".tine-storage-write-through-probe-source-{}",
            Uuid::new_v4()
        );
        let target = format!(
            ".tine-storage-write-through-probe-target-{}",
            Uuid::new_v4()
        );
        let retired = format!(
            ".tine-storage-write-through-probe-retired-{}",
            Uuid::new_v4()
        );
        let result = (|| {
            let (temp, first_identity) =
                self.windows
                    .create_flushed_temp(&self.dir, "probe-create", b"create")?;
            // The first write-through move is a no-replace creation proof.
            self.windows.move_write_through(&temp, &source, false)?;
            cleanup_temp(&self.dir, &temp);
            verify_windows_regular_exact(&self.dir, &source, b"create", Some(first_identity))?;

            // Move an independent source into the target to prove a second
            // no-replace name operation (the target begins absent).
            self.windows.move_write_through(&source, &target, false)?;
            verify_windows_regular_exact(&self.dir, &target, b"create", Some(first_identity))?;

            let (replacement, replacement_identity) =
                self.windows
                    .create_flushed_temp(&self.dir, "probe-replace", b"replace")?;
            self.windows
                .move_write_through(&replacement, &target, true)?;
            cleanup_temp(&self.dir, &replacement);
            verify_windows_regular_exact(
                &self.dir,
                &target,
                b"replace",
                Some(replacement_identity),
            )?;

            self.windows.move_write_through(&target, &retired, false)?;
            verify_windows_regular_exact(
                &self.dir,
                &retired,
                b"replace",
                Some(replacement_identity),
            )?;
            if read_windows_regular_with_identity(&self.dir, &target, 7)?.is_some() {
                return Err(FilesystemError::ByteCollision);
            }
            Ok(())
        })();
        cleanup_temp(&self.dir, &source);
        cleanup_temp(&self.dir, &target);
        cleanup_temp(&self.dir, &retired);
        result.map_err(|error| match error {
            FilesystemError::UnsafeEntry(_) => error,
            FilesystemError::DurableNameOperationUnavailable(_) => error,
            error => FilesystemError::DurableNameOperationUnavailable(error.to_string()),
        })
    }
}

#[cfg(all(test, windows))]
static WINDOWS_WRITE_THROUGH_PROBES: OnceLock<
    Mutex<std::collections::HashMap<WindowsDirectoryProbeKey, usize>>,
> = OnceLock::new();

#[cfg(windows)]
fn cleanup_temp(dir: &Dir, name: &str) {
    let _ = dir.remove_file(name);
}

#[cfg(windows)]
fn windows_final_path(file: &fs::File) -> io::Result<PathBuf> {
    use windows_sys::Win32::Storage::FileSystem::GetFinalPathNameByHandleW;

    let handle = file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
    // The zero-buffer call returns the required UTF-16 capacity. Allocate one
    // additional element because providers differ on whether the terminator is
    // included in that returned count.
    let needed = unsafe { GetFinalPathNameByHandleW(handle, std::ptr::null_mut(), 0, 0) };
    if needed == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut wide = vec![0_u16; needed as usize + 1];
    let written =
        unsafe { GetFinalPathNameByHandleW(handle, wide.as_mut_ptr(), wide.len() as u32, 0) };
    if written == 0 || written as usize >= wide.len() {
        return Err(io::Error::last_os_error());
    }
    wide.truncate(written as usize);
    Ok(PathBuf::from(std::ffi::OsString::from_wide(&wide)))
}

#[cfg(windows)]
fn windows_file_identity(file: &fs::File) -> io::Result<WindowsFileIdentity> {
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    };

    let handle = file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
    let mut information: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: `information` is valid writable storage and `handle` is owned by
    // the live file object for the duration of the call.
    if unsafe { GetFileInformationByHandle(handle, &mut information) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(WindowsFileIdentity {
        volume_serial: information.dwVolumeSerialNumber,
        file_index: ((information.nFileIndexHigh as u64) << 32) | information.nFileIndexLow as u64,
    })
}

#[cfg(windows)]
fn read_windows_regular_with_limit(
    dir: &Dir,
    name: &str,
    limit: usize,
) -> Result<Option<Vec<u8>>, FilesystemError> {
    read_windows_regular_with_identity(dir, name, limit).map(|entry| entry.map(|(bytes, _)| bytes))
}

#[cfg(windows)]
fn read_windows_regular_with_identity(
    dir: &Dir,
    name: &str,
    limit: usize,
) -> Result<Option<(Vec<u8>, WindowsFileIdentity)>, FilesystemError> {
    let mut file = match open_file_nofollow(dir, name) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > limit as u64 {
        return Err(FilesystemError::ByteCollision);
    }
    let identity = windows_file_identity(&file)?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut bytes)?;
    if bytes.len() as u64 != metadata.len() {
        return Err(FilesystemError::StoredLengthMismatch {
            path: name.into(),
            expected: metadata.len(),
            actual: bytes.len() as u64,
        });
    }
    Ok(Some((bytes, identity)))
}

#[cfg(windows)]
fn verify_windows_regular_exact(
    dir: &Dir,
    name: &str,
    expected: &[u8],
    expected_identity: Option<WindowsFileIdentity>,
) -> Result<(), FilesystemError> {
    let Some((bytes, identity)) = read_windows_regular_with_identity(dir, name, expected.len())?
    else {
        return Err(FilesystemError::Io(io::Error::new(
            ErrorKind::NotFound,
            format!("missing published file {name}"),
        )));
    };
    if bytes != expected || expected_identity.is_some_and(|expected| expected != identity) {
        return Err(FilesystemError::ByteCollision);
    }
    Ok(())
}

#[cfg(windows)]
fn verify_windows_regular_identity(
    dir: &Dir,
    name: &str,
    expected_identity: WindowsFileIdentity,
) -> Result<(), FilesystemError> {
    let file = open_file_nofollow(dir, name)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || windows_file_identity(&file)? != expected_identity {
        return Err(FilesystemError::ByteCollision);
    }
    Ok(())
}

pub fn ensure_directory_nofollow(root: &Dir, name: &str) -> Result<(), FilesystemError> {
    match root.symlink_metadata(name) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(FilesystemError::UnsafeEntry(format!(
                "{name} is not a real no-follow directory"
            )));
        }
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    root.create_dir(name)?;
    sync_dir_required(root)?;
    Ok(())
}

pub fn open_existing_dir_nofollow(root: &Dir, name: &str) -> Result<Option<Dir>, FilesystemError> {
    match root.symlink_metadata(name) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(
            FilesystemError::UnsafeEntry(format!("{name} is not a real no-follow directory")),
        ),
        Ok(_) => open_dir_nofollow(root, name).map(Some),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
pub fn open_file_nofollow(dir: &Dir, path: &str) -> io::Result<fs::File> {
    let path = CString::new(path)
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "invalid stored filename"))?;
    // SAFETY: `path` is a live NUL-terminated string and `dir` is an opened
    // directory capability. O_NOFOLLOW binds validation and reading to the
    // same opened regular-file handle.
    let fd = unsafe {
        libc::openat(
            dir.as_fd().as_raw_fd(),
            path.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: `openat` returned a newly owned descriptor.
        Ok(unsafe { fs::File::from_raw_fd(fd) })
    }
}

#[cfg(windows)]
pub fn open_file_nofollow(dir: &Dir, path: &str) -> io::Result<fs::File> {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = dir.open_with(path, &options)?.into_std();
    reject_windows_reparse(&file, path)?;
    Ok(file)
}

#[cfg(not(any(unix, windows)))]
pub fn open_file_nofollow(_dir: &Dir, _path: &str) -> io::Result<fs::File> {
    Err(io::Error::new(
        ErrorKind::Unsupported,
        "atomic no-follow reads are unsupported on this target",
    ))
}

#[cfg(unix)]
pub fn open_dir_nofollow(dir: &Dir, path: &str) -> Result<Dir, FilesystemError> {
    let path = CString::new(path)
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "invalid directory name"))?;
    // SAFETY: as in `open_file_nofollow`; O_DIRECTORY rejects non-directories
    // and O_NOFOLLOW rejects a final-component symlink in the same operation.
    let fd = unsafe {
        libc::openat(
            dir.as_fd().as_raw_fd(),
            path.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: `openat` returned one newly owned directory descriptor.
    Ok(Dir::from_std_file(unsafe { fs::File::from_raw_fd(fd) }))
}

#[cfg(windows)]
pub fn open_dir_nofollow(dir: &Dir, path: &str) -> Result<Dir, FilesystemError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .follow(FollowSymlinks::No)
        .maybe_dir(true);
    let file = dir.open_with(path, &options)?.into_std();
    let metadata = file.metadata()?;
    if metadata.file_attributes()
        & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT
        != 0
        || !metadata.is_dir()
    {
        return Err(FilesystemError::UnsafeEntry(format!(
            "{path} is not a real no-follow directory"
        )));
    }
    Ok(Dir::from_std_file(file))
}

#[cfg(windows)]
fn reject_windows_reparse(file: &fs::File, path: &str) -> io::Result<()> {
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    reject_windows_reparse_classification(
        file.metadata()?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0,
        path,
    )
}

#[cfg(windows)]
fn validate_windows_existing_regular_destination(
    dir: &Dir,
    path: &str,
) -> Result<(), FilesystemError> {
    let destination = match open_file_nofollow(dir, path) {
        Ok(destination) => destination,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !destination.metadata()?.is_file() {
        return Err(FilesystemError::UnsafeEntry(format!(
            "publication destination is not a regular no-follow file: {path}"
        )));
    }
    Ok(())
}

#[cfg(any(test, windows))]
fn reject_windows_reparse_classification(is_reparse: bool, path: &str) -> io::Result<()> {
    if is_reparse {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("opened path is a reparse point: {path}"),
        ));
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
pub fn open_dir_nofollow(_dir: &Dir, _path: &str) -> Result<Dir, FilesystemError> {
    Err(io::Error::new(
        ErrorKind::Unsupported,
        "atomic no-follow directory opens are unsupported on this target",
    )
    .into())
}

pub fn require_regular_entry(
    file_type: &cap_std::fs::FileType,
    name: &str,
) -> Result<(), FilesystemError> {
    if file_type.is_symlink() || !file_type.is_file() {
        Err(FilesystemError::UnsafeEntry(format!(
            "namespace entry is not a regular no-follow file: {name}"
        )))
    } else {
        Ok(())
    }
}

pub fn read_optional_regular(
    dir: &Dir,
    path: &str,
    limit: u64,
    expected_length: Option<u64>,
) -> Result<Option<Vec<u8>>, FilesystemError> {
    // Windows refuses to open a directory through the file-only capability
    // before we can classify its handle. Preclassify an existing non-file,
    // then still validate the opened handle below so a concurrent replacement
    // cannot turn this check into authority.
    match dir.symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(FilesystemError::UnsafeEntry(format!(
                "stored path is not a regular no-follow file: {path}"
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let mut file = match open_file_nofollow(dir, path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(FilesystemError::UnsafeEntry(format!(
            "stored path is not a regular no-follow file: {path}"
        )));
    }
    let length = metadata.len();
    if let Some(expected) = expected_length {
        if length != expected {
            return Err(FilesystemError::StoredLengthMismatch {
                path: path.into(),
                expected,
                actual: length,
            });
        }
    }
    if length > limit {
        return Err(FilesystemError::StoredFileTooLarge {
            path: path.into(),
            length,
            limit,
        });
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(FilesystemError::StoredFileTooLarge {
            path: path.into(),
            length: bytes.len() as u64,
            limit,
        });
    }
    if bytes.len() as u64 != length {
        return Err(FilesystemError::StoredLengthMismatch {
            path: path.into(),
            expected: length,
            actual: bytes.len() as u64,
        });
    }
    Ok(Some(bytes))
}

pub fn read_required_regular(
    dir: &Dir,
    path: &str,
    limit: u64,
    expected_length: Option<u64>,
) -> Result<Vec<u8>, FilesystemError> {
    read_optional_regular(dir, path, limit, expected_length)?.ok_or_else(|| {
        FilesystemError::Io(io::Error::new(
            ErrorKind::NotFound,
            format!("missing stored file {path}"),
        ))
    })
}

#[cfg(not(windows))]
pub fn publish_immutable_exact(
    dir: &Dir,
    filename: &str,
    bytes: &[u8],
) -> Result<(), FilesystemError> {
    publish_immutable_exact_impl(dir, filename, bytes, false)
}

#[cfg(not(windows))]
/// Publish exact immutable bytes while the caller holds the sole writer lease
/// for this private namespace.
///
/// On Android, some app-private filesystems permit ordinary atomic renames but
/// deny the hard-link operation used by the portable no-replace protocol. A
/// caller that owns the namespace's single-writer lease may therefore fall
/// back to an ordinary same-directory atomic rename after proving the target
/// name is absent. Shared/provider namespaces must continue to use
/// [`publish_immutable_exact`], because another process may legitimately race
/// their publication.
pub fn publish_immutable_exact_single_writer(
    dir: &Dir,
    filename: &str,
    bytes: &[u8],
) -> Result<(), FilesystemError> {
    publish_immutable_exact_impl(dir, filename, bytes, true)
}

#[cfg(not(windows))]
fn publish_immutable_exact_impl(
    dir: &Dir,
    filename: &str,
    bytes: &[u8],
    allow_android_single_writer_install: bool,
) -> Result<(), FilesystemError> {
    // Windows clones, retains, and validates the exact directory capability
    // before inserting an immutable target name. Win32 exposes no documented
    // directory-entry flush, so that validated state explicitly records the
    // platform limitation; it never classifies an I/O error as success.
    let publication_sync = ValidatedDirectorySync::open(dir)?;
    publication_sync.preflight()?;
    let temp_name = format!(".tmp-{}", Uuid::new_v4());
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    let mut temp = dir.open_with(&temp_name, &options)?;
    let result = (|| {
        temp.write_all(bytes)?;
        temp.sync_all()?;
        drop(temp);
        match install_immutable_name(
            dir,
            &temp_name,
            filename,
            allow_android_single_writer_install,
        ) {
            // A post-insertion sync error can leave the correct immutable
            // target present. Retrying verifies bytes and retries the barrier.
            Ok(()) => {
                finish_immutable_publication_sync(dir, filename, bytes, publication_sync.sync())
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                verify_existing(dir, filename, bytes)?;
                finish_immutable_publication_sync(dir, filename, bytes, publication_sync.sync())
            }
            Err(error) => Err(error.into()),
        }
    })();
    let cleanup = dir.remove_file(&temp_name);
    if let Err(error) = result {
        let _ = cleanup;
        return Err(error);
    }
    if cleanup
        .as_ref()
        .is_err_and(|error| error.kind() != ErrorKind::NotFound)
    {
        cleanup?;
    }
    Ok(())
}

#[cfg(not(windows))]
fn install_immutable_name(
    dir: &Dir,
    from: &str,
    to: &str,
    allow_android_single_writer_install: bool,
) -> io::Result<()> {
    let result = rename_noreplace(dir, from, to);
    #[cfg(target_os = "android")]
    if allow_android_single_writer_install {
        return finish_android_single_writer_install(dir, from, to, result);
    }
    #[cfg(not(target_os = "android"))]
    let _ = allow_android_single_writer_install;
    result
}

#[cfg(target_os = "android")]
fn finish_immutable_publication_sync(
    dir: &Dir,
    filename: &str,
    bytes: &[u8],
    result: io::Result<()>,
) -> Result<(), FilesystemError> {
    finish_android_immutable_publication_sync(dir, filename, bytes, result)
}

#[cfg(not(any(target_os = "android", windows)))]
fn finish_immutable_publication_sync(
    _dir: &Dir,
    _filename: &str,
    _bytes: &[u8],
    result: io::Result<()>,
) -> Result<(), FilesystemError> {
    result.map_err(FilesystemError::from)
}

#[cfg(not(windows))]
fn verify_existing(dir: &Dir, filename: &str, expected: &[u8]) -> Result<(), FilesystemError> {
    let existing = match read_required_regular(
        dir,
        filename,
        expected.len() as u64,
        Some(expected.len() as u64),
    ) {
        Ok(existing) => existing,
        Err(
            FilesystemError::StoredLengthMismatch { .. }
            | FilesystemError::StoredFileTooLarge { .. },
        ) => return Err(FilesystemError::ByteCollision),
        Err(error) => return Err(error),
    };
    if existing == expected {
        Ok(())
    } else {
        Err(FilesystemError::ByteCollision)
    }
}

#[cfg(test)]
const RENAME_NOREPLACE_SUPPORTED_TARGETS: &[&str] =
    &["linux", "macos", "ios", "android", "windows"];

#[cfg(test)]
const STAGED_REGULAR_REPLACEMENT_SUPPORTED_TARGETS: &[&str] =
    &["linux", "macos", "ios", "android", "windows"];

#[cfg(test)]
pub(crate) const PACKAGE_DIRECTORY_MOVE_SUPPORTED_TARGETS: &[&str] =
    &["linux", "macos", "ios", "android", "windows"];

#[cfg(target_os = "linux")]
fn linux_renameat2_noreplace(
    source_parent: &Dir,
    source_name: &str,
    destination_parent: &Dir,
    destination_name: &str,
) -> io::Result<()> {
    let source = CString::new(source_name)
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "invalid source name"))?;
    let destination = CString::new(destination_name)
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "invalid destination name"))?;
    // SAFETY: both directory descriptors and C strings remain live for the
    // call; RENAME_NOREPLACE forbids replacement of an existing destination.
    let result = unsafe {
        libc::renameat2(
            source_parent.as_fd().as_raw_fd(),
            source.as_ptr(),
            destination_parent.as_fd().as_raw_fd(),
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Move one real directory between two retained parent capabilities without
/// replacing a concurrent destination.
///
/// Package-store staging lives at the store root while immutable versions live
/// below their package-id directory, so this is deliberately a cross-parent
/// primitive. Every shipped target uses its native no-replace name operation;
/// Windows additionally requests write-through completion. Callers own the
/// directory-content validation and synchronize both parents after success.
#[cfg(target_os = "linux")]
pub(crate) fn move_directory_no_replace(
    source_parent: &Dir,
    source_name: &str,
    destination_parent: &Dir,
    destination_name: &str,
) -> Result<(), FilesystemError> {
    validate_single_entry_name(source_name)?;
    validate_single_entry_name(destination_name)?;
    linux_renameat2_noreplace(
        source_parent,
        source_name,
        destination_parent,
        destination_name,
    )
    .map_err(Into::into)
}

#[cfg(target_os = "android")]
pub(crate) fn move_directory_no_replace(
    source_parent: &Dir,
    source_name: &str,
    destination_parent: &Dir,
    destination_name: &str,
) -> Result<(), FilesystemError> {
    validate_single_entry_name(source_name)?;
    validate_single_entry_name(destination_name)?;
    let source = CString::new(source_name)
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "invalid source directory name"))?;
    let destination = CString::new(destination_name).map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidInput,
            "invalid destination directory name",
        )
    })?;
    // Android's libc does not expose a stable renameat2 wrapper on every NDK,
    // but the shipped arm64 target provides the Linux-compatible syscall.
    // SAFETY: both directory descriptors and C strings remain live for the
    // call; RENAME_NOREPLACE forbids replacement of an existing destination.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            source_parent.as_fd().as_raw_fd(),
            source.as_ptr(),
            destination_parent.as_fd().as_raw_fd(),
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error().into())
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
pub(crate) fn move_directory_no_replace(
    source_parent: &Dir,
    source_name: &str,
    destination_parent: &Dir,
    destination_name: &str,
) -> Result<(), FilesystemError> {
    validate_single_entry_name(source_name)?;
    validate_single_entry_name(destination_name)?;
    let source = CString::new(source_name)
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "invalid source directory name"))?;
    let destination = CString::new(destination_name).map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidInput,
            "invalid destination directory name",
        )
    })?;
    // SAFETY: the retained parent descriptors and C strings remain live for
    // renameatx_np; RENAME_EXCL supplies the Darwin no-replace contract.
    let result = unsafe {
        libc::renameatx_np(
            source_parent.as_fd().as_raw_fd(),
            source.as_ptr(),
            destination_parent.as_fd().as_raw_fd(),
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error().into())
    }
}

#[cfg(windows)]
pub(crate) fn move_directory_no_replace(
    source_parent: &Dir,
    source_name: &str,
    destination_parent: &Dir,
    destination_name: &str,
) -> Result<(), FilesystemError> {
    validate_single_entry_name(source_name)?;
    validate_single_entry_name(destination_name)?;
    let source = WindowsWriteThroughDirectory::open(source_parent)?;
    let destination = WindowsWriteThroughDirectory::open(destination_parent)?;
    source.move_to_write_through(source_name, &destination, destination_name, false)
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    windows
)))]
pub(crate) fn move_directory_no_replace(
    _source_parent: &Dir,
    _source_name: &str,
    _destination_parent: &Dir,
    _destination_name: &str,
) -> Result<(), FilesystemError> {
    Err(FilesystemError::DurableNameOperationUnavailable(
        "staged-directory publication is unsupported on this target".into(),
    ))
}

#[cfg(target_os = "linux")]
fn rename_noreplace(dir: &Dir, from: &str, to: &str) -> io::Result<()> {
    linux_renameat2_noreplace(dir, from, dir, to)
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "android"))]
fn rename_noreplace(dir: &Dir, from: &str, to: &str) -> io::Result<()> {
    dir.hard_link(from, dir, to)?;
    dir.remove_file(from)
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    windows
)))]
fn rename_noreplace(_dir: &Dir, _from: &str, _to: &str) -> io::Result<()> {
    Err(io::Error::new(
        ErrorKind::Unsupported,
        "atomic no-clobber publication is unsupported on this target",
    ))
}

#[cfg(any(target_os = "android", all(test, unix)))]
fn android_durability_capability_refusal(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::PermissionDenied | ErrorKind::Unsupported | ErrorKind::InvalidInput
    ) || matches!(
        error.raw_os_error(),
        Some(libc::EACCES) | Some(libc::EPERM) | Some(libc::EINVAL) | Some(libc::EOPNOTSUPP)
    )
}

#[cfg(any(target_os = "android", all(test, unix)))]
fn finish_android_immutable_publication_sync(
    dir: &Dir,
    filename: &str,
    bytes: &[u8],
    result: io::Result<()>,
) -> Result<(), FilesystemError> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if android_durability_capability_refusal(&error) => {
            // Android kernels and provider-backed filesystems can allow the
            // exact create, file flush, and no-replace rename while refusing
            // directory fsync. Re-open the published immutable name, prove its
            // bytes, and flush the file again. This is the strongest available
            // crash boundary on that platform; every ordinary I/O error stays
            // fatal and non-Android targets retain required directory fsync.
            verify_existing(dir, filename, bytes)?;
            open_file_nofollow(dir, filename)?.sync_all()?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(any(target_os = "android", all(test, unix)))]
fn finish_android_single_writer_install(
    dir: &Dir,
    from: &str,
    to: &str,
    result: io::Result<()>,
) -> io::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Err(error),
        Err(error) if android_durability_capability_refusal(&error) => {
            // This namespace is app-private and the caller holds its sole
            // writer lease. Android may nevertheless reject hard-link based
            // no-replace installation. Never overwrite an observed target;
            // once absence is proved, use the ordinary same-directory atomic
            // rename that Direct Files and Android's storage stack support.
            match dir.symlink_metadata(to) {
                Ok(_) => Err(io::Error::from(ErrorKind::AlreadyExists)),
                Err(target_error) if target_error.kind() == ErrorKind::NotFound => {
                    dir.rename(from, dir, to)
                }
                Err(target_error) => Err(target_error),
            }
        }
        Err(error) => Err(error),
    }
}

#[cfg(any(test, windows))]
fn validated_windows_directory_entry_durability(
    is_dir: bool,
    is_reparse: bool,
) -> io::Result<WindowsDirectoryEntryDurability> {
    if !is_dir || is_reparse {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "directory durability handle is not a real no-follow directory",
        ));
    }
    Ok(WindowsDirectoryEntryDurability::UnsupportedAfterValidation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cap_std::ambient_authority;
    #[cfg(not(windows))]
    use std::sync::{Arc, Barrier};
    #[cfg(not(windows))]
    use std::thread;

    struct TestDirectory {
        path: std::path::PathBuf,
        dir: Dir,
    }

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("tine-storage-{label}-{}", Uuid::new_v4()));
            fs::create_dir(&path).unwrap();
            let dir = Dir::open_ambient_dir(&path, ambient_authority()).unwrap();
            Self { path, dir }
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[cfg(unix)]
    fn test_file_identity(dir: &Dir, name: &str) -> (u64, u64) {
        let metadata = open_file_nofollow(dir, name).unwrap().metadata().unwrap();
        (metadata.dev(), metadata.ino())
    }

    #[cfg(windows)]
    fn test_file_identity(dir: &Dir, name: &str) -> WindowsFileIdentity {
        windows_file_identity(&open_file_nofollow(dir, name).unwrap()).unwrap()
    }

    #[cfg(not(windows))]
    fn temporary_entries(dir: &Dir) -> Vec<String> {
        dir.entries()
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".tmp-"))
            .collect()
    }

    #[cfg(not(windows))]
    fn assert_persisted_entries(fixture: &TestDirectory, entries: &[(&str, &[u8])]) {
        for (filename, bytes) in entries {
            assert_eq!(fixture.dir.read(filename).unwrap(), *bytes);
        }
        assert!(temporary_entries(&fixture.dir).is_empty());
    }

    #[cfg(not(windows))]
    #[test]
    fn exact_publish_retries_identically_without_temporary_residue() {
        let fixture = TestDirectory::new("exact-retry");
        publish_immutable_exact(&fixture.dir, "entry", b"exact bytes").unwrap();
        publish_immutable_exact(&fixture.dir, "entry", b"exact bytes").unwrap();
        assert_persisted_entries(&fixture, &[("entry", b"exact bytes")]);
    }

    #[cfg(not(windows))]
    #[test]
    #[cfg(unix)]
    fn android_exact_publish_accepts_only_directory_sync_capability_refusal() {
        let fixture = TestDirectory::new("android-exact-publish-fallback");
        publish_immutable_exact(&fixture.dir, "entry", b"exact bytes").unwrap();

        finish_android_immutable_publication_sync(
            &fixture.dir,
            "entry",
            b"exact bytes",
            Err(io::Error::new(
                ErrorKind::PermissionDenied,
                "simulated Android directory fsync refusal",
            )),
        )
        .unwrap();
        assert_persisted_entries(&fixture, &[("entry", b"exact bytes")]);

        assert!(matches!(
            finish_android_immutable_publication_sync(
                &fixture.dir,
                "entry",
                b"different bytes",
                Err(io::Error::new(
                    ErrorKind::PermissionDenied,
                    "simulated Android directory fsync refusal",
                )),
            ),
            Err(FilesystemError::ByteCollision)
        ));
        assert!(matches!(
            finish_android_immutable_publication_sync(
                &fixture.dir,
                "entry",
                b"exact bytes",
                Err(io::Error::new(ErrorKind::WriteZero, "real I/O failure")),
            ),
            Err(FilesystemError::Io(error)) if error.kind() == ErrorKind::WriteZero
        ));
    }

    #[test]
    #[cfg(unix)]
    fn android_single_writer_install_falls_back_without_overwriting() {
        let fixture = TestDirectory::new("android-single-writer-install");
        fixture.dir.write("temporary", b"exact bytes").unwrap();
        finish_android_single_writer_install(
            &fixture.dir,
            "temporary",
            "final",
            Err(io::Error::from(ErrorKind::PermissionDenied)),
        )
        .unwrap();
        assert_eq!(fixture.dir.read("final").unwrap(), b"exact bytes");
        assert!(fixture.dir.symlink_metadata("temporary").is_err());

        fixture
            .dir
            .write("other-temporary", b"replacement")
            .unwrap();
        let error = finish_android_single_writer_install(
            &fixture.dir,
            "other-temporary",
            "final",
            Err(io::Error::from(ErrorKind::PermissionDenied)),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::AlreadyExists);
        assert_eq!(fixture.dir.read("final").unwrap(), b"exact bytes");
        assert_eq!(fixture.dir.read("other-temporary").unwrap(), b"replacement");
    }

    #[test]
    fn durable_directory_single_writer_publication_is_exact_and_never_overwrites() {
        let fixture = TestDirectory::new("durable-directory-single-writer");
        let publication = DurableDirectoryPublication::open(&fixture.dir).unwrap();
        publication
            .publish_new_exact_single_writer("entry", b"exact bytes")
            .unwrap();
        publication
            .publish_new_exact_single_writer("entry", b"exact bytes")
            .unwrap();
        assert_eq!(fixture.dir.read("entry").unwrap(), b"exact bytes");

        assert!(matches!(
            publication.publish_new_exact_single_writer("entry", b"replacement"),
            Err(FilesystemError::ByteCollision)
        ));
        assert_eq!(fixture.dir.read("entry").unwrap(), b"exact bytes");
    }

    #[test]
    #[cfg(windows)]
    fn durable_directory_reuses_the_exact_directory_write_through_probe() {
        let fixture = TestDirectory::new("durable-directory-probe-cache");
        let first = DurableDirectoryPublication::open(&fixture.dir).unwrap();
        let key = first.windows.probe_key();
        let probe_count = || {
            WINDOWS_WRITE_THROUGH_PROBES
                .get()
                .and_then(|probes| {
                    probes
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .get(&key)
                        .copied()
                })
                .unwrap_or(0)
        };
        assert_eq!(probe_count(), 1);

        let second = DurableDirectoryPublication::open(&fixture.dir).unwrap();
        assert_eq!(second.windows.probe_key(), key);
        assert_eq!(probe_count(), 1);
        second
            .publish_new_exact_single_writer("cached-publication", b"exact bytes")
            .unwrap();
        assert_eq!(
            fixture.dir.read("cached-publication").unwrap(),
            b"exact bytes"
        );

        let other = TestDirectory::new("durable-directory-distinct-probe");
        let distinct = DurableDirectoryPublication::open(&other.dir).unwrap();
        let distinct_key = distinct.windows.probe_key();
        assert_ne!(distinct_key, key);
        assert_eq!(
            WINDOWS_WRITE_THROUGH_PROBES
                .get()
                .and_then(|probes| {
                    probes
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .get(&distinct_key)
                        .copied()
                })
                .unwrap_or(0),
            1
        );
    }

    #[test]
    fn durable_exact_move_preserves_the_winner_and_is_idempotent() {
        let fixture = TestDirectory::new("durable-exact-move");
        let publication = DurableDirectoryPublication::open(&fixture.dir).unwrap();
        fixture.dir.write("source", b"exact bytes").unwrap();

        publication
            .move_exact_no_replace("source", "destination", b"exact bytes")
            .unwrap();
        assert!(fixture.dir.symlink_metadata("source").is_err());
        assert_eq!(fixture.dir.read("destination").unwrap(), b"exact bytes");

        publication
            .move_exact_no_replace("source", "destination", b"exact bytes")
            .unwrap();
        fixture.dir.write("other", b"replacement").unwrap();
        assert!(matches!(
            publication.move_exact_no_replace("other", "destination", b"replacement"),
            Err(FilesystemError::Io(error)) if error.kind() == ErrorKind::AlreadyExists
        ));
        assert_eq!(fixture.dir.read("other").unwrap(), b"replacement");
        assert_eq!(fixture.dir.read("destination").unwrap(), b"exact bytes");

        fixture.dir.write("wrong-source", b"wrong bytes").unwrap();
        assert!(matches!(
            publication.move_exact_no_replace("wrong-source", "unused", b"expected"),
            Err(FilesystemError::ByteCollision)
        ));
        assert_eq!(fixture.dir.read("wrong-source").unwrap(), b"wrong bytes");
        assert!(fixture.dir.symlink_metadata("unused").is_err());
        assert!(matches!(
            publication.move_exact_no_replace("missing", "also-missing", b"expected"),
            Err(FilesystemError::Io(error)) if error.kind() == ErrorKind::NotFound
        ));
        assert!(matches!(
            publication.move_exact_no_replace("same", "same", b"expected"),
            Err(FilesystemError::UnsafeEntry(_))
        ));
    }

    #[test]
    fn staged_regular_publication_creates_or_replaces_by_atomic_identity() {
        let fixture = TestDirectory::new("staged-regular-replacement");
        let publication = DurableDirectoryPublication::open(&fixture.dir).unwrap();

        fixture.dir.write("staged-new", b"new bytes").unwrap();
        let new_identity = test_file_identity(&fixture.dir, "staged-new");
        publication
            .replace_from_staged_regular_single_writer("staged-new", "active-new")
            .unwrap();
        assert!(fixture.dir.symlink_metadata("staged-new").is_err());
        assert_eq!(fixture.dir.read("active-new").unwrap(), b"new bytes");
        assert_eq!(test_file_identity(&fixture.dir, "active-new"), new_identity);

        fixture.dir.write("active", b"old bytes").unwrap();
        let old_identity = test_file_identity(&fixture.dir, "active");
        fixture.dir.write("staged", b"replacement bytes").unwrap();
        let staged_identity = test_file_identity(&fixture.dir, "staged");
        publication
            .replace_from_staged_regular_single_writer("staged", "active")
            .unwrap();
        assert!(fixture.dir.symlink_metadata("staged").is_err());
        assert_eq!(fixture.dir.read("active").unwrap(), b"replacement bytes");
        assert_eq!(test_file_identity(&fixture.dir, "active"), staged_identity);
        assert_ne!(test_file_identity(&fixture.dir, "active"), old_identity);
    }

    #[test]
    fn staged_regular_publication_refuses_invalid_entries_without_clobbering() {
        let fixture = TestDirectory::new("staged-regular-refusals");
        let publication = DurableDirectoryPublication::open(&fixture.dir).unwrap();
        fixture.dir.write("active", b"old bytes").unwrap();
        fixture.dir.write("staged", b"replacement bytes").unwrap();

        for (source, destination) in [
            ("../staged", "active"),
            ("staged", "../active"),
            ("staged", "staged"),
            ("missing", "active"),
        ] {
            assert!(publication
                .replace_from_staged_regular_single_writer(source, destination)
                .is_err());
            assert_eq!(fixture.dir.read("active").unwrap(), b"old bytes");
            assert_eq!(fixture.dir.read("staged").unwrap(), b"replacement bytes");
        }

        fixture.dir.create_dir("directory").unwrap();
        assert!(publication
            .replace_from_staged_regular_single_writer("staged", "directory")
            .is_err());
        assert_eq!(fixture.dir.read("active").unwrap(), b"old bytes");
        assert_eq!(fixture.dir.read("staged").unwrap(), b"replacement bytes");
    }

    #[cfg(unix)]
    #[test]
    fn staged_regular_publication_refuses_symlinks_without_touching_targets() {
        let fixture = TestDirectory::new("staged-regular-symlinks");
        let publication = DurableDirectoryPublication::open(&fixture.dir).unwrap();
        fixture.dir.write("target", b"target bytes").unwrap();
        fixture.dir.write("staged", b"replacement bytes").unwrap();
        fixture.dir.symlink("target", "destination-link").unwrap();
        fixture.dir.symlink("target", "source-link").unwrap();

        assert!(publication
            .replace_from_staged_regular_single_writer("staged", "destination-link")
            .is_err());
        assert!(publication
            .replace_from_staged_regular_single_writer("source-link", "active")
            .is_err());
        assert_eq!(fixture.dir.read("target").unwrap(), b"target bytes");
        assert_eq!(fixture.dir.read("staged").unwrap(), b"replacement bytes");
        assert!(fixture.dir.symlink_metadata("active").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn staged_regular_publication_preserves_installed_identity_on_post_rename_sync_error() {
        let fixture = TestDirectory::new("staged-regular-post-rename-sync-error");
        fixture.dir.write("active", b"old bytes").unwrap();
        fixture.dir.write("staged", b"replacement bytes").unwrap();
        let staged_identity = test_file_identity(&fixture.dir, "staged");

        fixture
            .dir
            .rename("staged", &fixture.dir, "active")
            .unwrap();
        let error = finish_staged_regular_replacement_sync(
            &fixture.dir,
            "active",
            staged_identity,
            Err(io::Error::from_raw_os_error(libc::EIO)),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            FilesystemError::Io(error) if error.raw_os_error() == Some(libc::EIO)
        ));
        assert!(fixture.dir.symlink_metadata("staged").is_err());
        assert_eq!(fixture.dir.read("active").unwrap(), b"replacement bytes");
        assert_eq!(test_file_identity(&fixture.dir, "active"), staged_identity);
    }

    #[test]
    #[cfg(unix)]
    fn interrupted_hard_link_move_finishes_only_for_the_same_exact_inode() {
        let fixture = TestDirectory::new("interrupted-hard-link-move");
        fixture.dir.write("source", b"exact bytes").unwrap();
        fixture
            .dir
            .hard_link("source", &fixture.dir, "destination")
            .unwrap();
        finish_interrupted_hard_link_move(&fixture.dir, "source", "destination", b"exact bytes")
            .unwrap();
        assert!(fixture.dir.symlink_metadata("source").is_err());
        assert_eq!(fixture.dir.read("destination").unwrap(), b"exact bytes");

        fixture.dir.write("foreign-source", b"same bytes").unwrap();
        fixture
            .dir
            .write("foreign-destination", b"same bytes")
            .unwrap();
        assert!(matches!(
            finish_interrupted_hard_link_move(
                &fixture.dir,
                "foreign-source",
                "foreign-destination",
                b"same bytes",
            ),
            Err(FilesystemError::ByteCollision)
        ));
        assert_eq!(fixture.dir.read("foreign-source").unwrap(), b"same bytes");
        assert_eq!(
            fixture.dir.read("foreign-destination").unwrap(),
            b"same bytes"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn divergent_existing_bytes_collide_without_clobbering() {
        let fixture = TestDirectory::new("collision");
        publish_immutable_exact(&fixture.dir, "entry", b"winner").unwrap();
        assert!(matches!(
            publish_immutable_exact(&fixture.dir, "entry", b"different"),
            Err(FilesystemError::ByteCollision)
        ));
        assert_eq!(fixture.dir.read("entry").unwrap(), b"winner");
        assert!(temporary_entries(&fixture.dir).is_empty());
    }

    #[test]
    fn bounded_optional_and_required_reads_reject_invalid_entries() {
        let fixture = TestDirectory::new("bounded-read");
        fixture.dir.write("entry", b"12345").unwrap();
        assert_eq!(
            read_optional_regular(&fixture.dir, "entry", 5, Some(5)).unwrap(),
            Some(b"12345".to_vec())
        );
        assert!(matches!(
            read_optional_regular(&fixture.dir, "entry", 4, None),
            Err(FilesystemError::StoredFileTooLarge {
                path,
                length: 5,
                limit: 4,
            }) if path == "entry"
        ));
        assert!(matches!(
            read_optional_regular(&fixture.dir, "entry", 5, Some(4)),
            Err(FilesystemError::StoredLengthMismatch {
                path,
                expected: 4,
                actual: 5,
            }) if path == "entry"
        ));
        assert_eq!(
            read_optional_regular(&fixture.dir, "absent", 5, None).unwrap(),
            None
        );
        assert!(matches!(
            read_required_regular(&fixture.dir, "absent", 5, None),
            Err(FilesystemError::Io(error)) if error.kind() == ErrorKind::NotFound
        ));
        fixture.dir.create_dir("unsafe").unwrap();
        assert!(matches!(
            read_optional_regular(&fixture.dir, "unsafe", 5, None),
            Err(FilesystemError::UnsafeEntry(message))
                if message == "stored path is not a regular no-follow file: unsafe"
        ));
    }

    #[cfg(not(windows))]
    #[test]
    fn concurrent_publishers_converge_and_preserve_one_divergent_winner() {
        let fixture = TestDirectory::new("concurrent");
        let path = Arc::new(fixture.path.clone());
        let barrier = Arc::new(Barrier::new(8));
        let threads = (0..8)
            .map(|_| {
                let path = Arc::clone(&path);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let dir = Dir::open_ambient_dir(path.as_ref(), ambient_authority()).unwrap();
                    barrier.wait();
                    publish_immutable_exact(&dir, "identical", b"same")
                })
            })
            .collect::<Vec<_>>();
        assert!(threads
            .into_iter()
            .all(|thread| thread.join().unwrap().is_ok()));
        assert_eq!(fixture.dir.read("identical").unwrap(), b"same");

        let barrier = Arc::new(Barrier::new(2));
        let threads = [b"first".as_slice(), b"second".as_slice()]
            .into_iter()
            .map(|bytes| {
                let path = Arc::clone(&path);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let dir = Dir::open_ambient_dir(path.as_ref(), ambient_authority()).unwrap();
                    barrier.wait();
                    publish_immutable_exact(&dir, "divergent", bytes)
                })
            })
            .collect::<Vec<_>>();
        let results = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(FilesystemError::ByteCollision)))
                .count(),
            1
        );
        let winner = fixture.dir.read("divergent").unwrap();
        assert!(winner == b"first" || winner == b"second");
        assert!(temporary_entries(&fixture.dir).is_empty());
    }

    #[test]
    fn validated_real_directory_has_explicit_windows_durability_limit() {
        assert_eq!(
            validated_windows_directory_entry_durability(true, false).unwrap(),
            WindowsDirectoryEntryDurability::UnsupportedAfterValidation
        );
    }

    #[test]
    fn windows_directory_validation_rejects_reparse_and_non_directory_handles() {
        assert_eq!(
            validated_windows_directory_entry_durability(false, false)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            validated_windows_directory_entry_durability(true, true)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn windows_existing_destination_rejects_honest_provider_reparse_file() {
        let error = reject_windows_reparse_classification(true, "active").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(reject_windows_reparse_classification(false, "active").is_ok());
    }

    #[test]
    fn no_replace_supported_target_set_is_pinned() {
        assert_eq!(
            RENAME_NOREPLACE_SUPPORTED_TARGETS,
            ["linux", "macos", "ios", "android", "windows"]
        );
    }

    #[test]
    fn staged_regular_replacement_supported_target_set_is_pinned() {
        assert_eq!(
            STAGED_REGULAR_REPLACEMENT_SUPPORTED_TARGETS,
            ["linux", "macos", "ios", "android", "windows"]
        );
    }

    #[test]
    fn native_name_operation_bodies_are_single_sourced() {
        let source = include_str!("filesystem.rs");
        let linux_call = ["libc::rename", "at2("].concat();
        assert_eq!(
            source.matches(&linux_call).count(),
            1,
            "I-12: Linux no-replace name operations must reuse linux_renameat2_noreplace; imitate that helper"
        );
        let windows_call = [
            "if unsafe { MoveFileExW(from.as_ptr(), ",
            "to.as_ptr(), flags) }",
        ]
        .concat();
        assert_eq!(
            source.matches(&windows_call).count(),
            1,
            "I-12: Windows name moves must reuse WindowsWriteThroughDirectory::move_to_write_through; imitate that method"
        );
    }
}
