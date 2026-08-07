//! Durable Ed25519 signing-key files.
//!
//! Loading and initialization are deliberately separate operations. Ordinary
//! loading never creates a key or falls back to an ephemeral identity. Explicit
//! initialization writes a mode-0600 temporary file beside the destination,
//! makes its contents durable, and installs it atomically without replacing an
//! existing winner.

use std::env;
use std::error::Error as StdError;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use rand::RngCore;
use zeroize::Zeroizing;

#[cfg(unix)]
use std::ffi::CString;
#[cfg(unix)]
use std::mem::MaybeUninit;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

/// Environment variable consulted when no explicit key path is supplied.
pub const KEY_PATH_ENV: &str = "TRIBLESPACE_KEY";

/// Default key-file name beside a pile.
pub const DEFAULT_KEY_FILE_NAME: &str = "self.key";

const ENCODED_SEED_LEN: usize = 64;
const TEMP_NONCE_LEN: usize = 16;
const TEMP_ATTEMPTS: u64 = 128;

/// Filesystem operation associated with an [`Error::Io`] failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Inspect,
    Open,
    Read,
    CreateTemporary,
    SetPermissions,
    WriteTemporary,
    SyncTemporary,
    Install,
    RemoveTemporary,
    OpenParent,
    SyncParent,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Inspect => "inspect",
            Self::Open => "open",
            Self::Read => "read",
            Self::CreateTemporary => "create temporary file for",
            Self::SetPermissions => "set permissions on temporary file for",
            Self::WriteTemporary => "write temporary file for",
            Self::SyncTemporary => "sync temporary file for",
            Self::Install => "install",
            Self::RemoveTemporary => "remove temporary file for",
            Self::OpenParent => "open parent directory of",
            Self::SyncParent => "sync parent directory of",
        };
        f.write_str(label)
    }
}

/// Failure to load or explicitly initialize a durable signing-key file.
///
/// Diagnostics contain paths and error classes, never seed bytes or encoded
/// key contents.
#[derive(Debug)]
pub enum Error {
    /// A filesystem operation failed.
    Io {
        operation: Operation,
        path: PathBuf,
        source: io::Error,
    },
    /// The path names something other than a regular file, including a symlink.
    NotRegularFile { path: PathBuf },
    /// A Unix key file grants any group or world permission.
    InsecurePermissions { path: PathBuf, mode: u32 },
    /// The file is not exactly 64 ASCII hexadecimal seed characters.
    InvalidFormat { path: PathBuf },
    /// The destination has no usable final path component.
    InvalidPath { path: PathBuf },
    /// The operating-system random source failed.
    Entropy { source: rand::Error },
    /// All bounded same-directory temporary names already existed.
    TemporaryNameExhausted { path: PathBuf },
}

impl Error {
    fn io(operation: Operation, path: &Path, source: io::Error) -> Self {
        Self::Io {
            operation,
            path: path.to_owned(),
            source,
        }
    }

    fn is_not_found(&self) -> bool {
        matches!(
            self,
            Self::Io {
                source,
                ..
            } if source.kind() == io::ErrorKind::NotFound
        )
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                source,
            } => write!(f, "failed to {operation} {}: {source}", path.display()),
            Self::NotRegularFile { path } => {
                write!(
                    f,
                    "signing-key path {} is not a regular file",
                    path.display()
                )
            }
            Self::InsecurePermissions { path, mode } => write!(
                f,
                "signing-key file {} has group or world permissions ({mode:#05o})",
                path.display()
            ),
            Self::InvalidFormat { path } => write!(
                f,
                "signing-key file {} is not exactly 64 hexadecimal seed characters",
                path.display()
            ),
            Self::InvalidPath { path } => {
                write!(f, "signing-key path {} has no file name", path.display())
            }
            Self::Entropy { source } => {
                write!(f, "failed to generate a signing-key seed: {source}")
            }
            Self::TemporaryNameExhausted { path } => write!(
                f,
                "could not reserve a temporary file beside {}",
                path.display()
            ),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Entropy { source } => Some(source),
            Self::NotRegularFile { .. }
            | Self::InsecurePermissions { .. }
            | Self::InvalidFormat { .. }
            | Self::InvalidPath { .. }
            | Self::TemporaryNameExhausted { .. } => None,
        }
    }
}

/// Resolve a key path without touching the filesystem or canonicalizing it.
///
/// Precedence is an explicit path, then [`KEY_PATH_ENV`], then `self.key` in
/// the pile path's lexical parent. An environment value is interpreted as an
/// OS-native path, so non-UTF-8 paths remain representable on Unix.
pub fn resolve_path(explicit: Option<&Path>, pile: &Path) -> PathBuf {
    if let Some(path) = explicit {
        return path.to_owned();
    }
    if let Some(path) = env::var_os(KEY_PATH_ENV) {
        return PathBuf::from(path);
    }
    pile.parent()
        .unwrap_or_else(|| Path::new(""))
        .join(DEFAULT_KEY_FILE_NAME)
}

/// Load an existing durable Ed25519 signing key.
///
/// The file must itself be a regular file (not a symlink), contain exactly 64
/// ASCII hexadecimal characters with no surrounding whitespace, and, on Unix,
/// grant no permissions to group or world. Missing files are ordinary typed
/// I/O errors; this function never creates or substitutes a key.
pub fn load_existing(path: &Path) -> Result<SigningKey, Error> {
    let (parent_path, file_name) = destination_parts(path)?;
    let parent = ParentDirectory::open(parent_path, path)?;
    parent.load_existing(file_name, path)
}

fn decode_signing_key(mut file: File, path: &Path) -> Result<SigningKey, Error> {
    let metadata = file
        .metadata()
        .map_err(|source| Error::io(Operation::Inspect, path, source))?;
    if !metadata.file_type().is_file() {
        return Err(Error::NotRegularFile {
            path: path.to_owned(),
        });
    }

    #[cfg(unix)]
    {
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(Error::InsecurePermissions {
                path: path.to_owned(),
                mode,
            });
        }
    }

    if metadata.len() != ENCODED_SEED_LEN as u64 {
        return Err(Error::InvalidFormat {
            path: path.to_owned(),
        });
    }

    let mut encoded = Zeroizing::new([0u8; ENCODED_SEED_LEN + 1]);
    let mut length = 0;
    while length < encoded.len() {
        let read = file
            .read(&mut encoded[length..])
            .map_err(|source| Error::io(Operation::Read, path, source))?;
        if read == 0 {
            break;
        }
        length += read;
    }
    if length != ENCODED_SEED_LEN {
        return Err(Error::InvalidFormat {
            path: path.to_owned(),
        });
    }

    let mut seed = Zeroizing::new([0u8; 32]);
    if hex::decode_to_slice(&encoded[..ENCODED_SEED_LEN], &mut seed[..]).is_err() {
        return Err(Error::InvalidFormat {
            path: path.to_owned(),
        });
    }
    let key = SigningKey::from_bytes(&seed);
    Ok(key)
}

/// Explicitly initialize a durable signing-key file, or load its valid winner.
///
/// Initialization never replaces an existing path. A newly generated seed is
/// written to a same-directory mode-0600 temporary file and synced before an
/// atomic hard-link installation. Concurrent initializers race only at that
/// no-replace installation point; losers discard their temporary file, sync
/// the parent directory, and strictly load the winner. On Unix, initialization
/// opens the lexical parent once and performs every child operation relative to
/// that stable directory handle, so a concurrent rename or symlink retarget
/// cannot redirect later stages of the transaction.
pub fn init(path: &Path) -> Result<SigningKey, Error> {
    init_with_hook(path, |_| {})
}

fn init_with_hook<F>(path: &Path, before_install: F) -> Result<SigningKey, Error>
where
    F: FnOnce(&OsStr),
{
    let (parent_path, file_name) = destination_parts(path)?;
    let parent = ParentDirectory::open(parent_path, path)?;

    match parent.load_existing(file_name, path) {
        Ok(key) => {
            parent
                .sync()
                .map_err(|source| Error::io(Operation::SyncParent, path, source))?;
            return Ok(key);
        }
        Err(error) if error.is_not_found() => {}
        Err(error) => return Err(error),
    }

    let mut seed = Zeroizing::new([0u8; 32]);
    OsRng
        .try_fill_bytes(&mut seed[..])
        .map_err(|source| Error::Entropy { source })?;
    let mut encoded = Zeroizing::new([0u8; ENCODED_SEED_LEN]);
    encode_hex(&seed, &mut encoded);
    drop(seed);

    let (mut temporary, guard) = create_temporary(path, &parent, file_name)?;
    let write_result = temporary
        .write_all(&encoded[..])
        .map_err(|source| Error::io(Operation::WriteTemporary, path, source));
    drop(encoded);
    if let Err(error) = write_result {
        drop(temporary);
        return cleanup_after_error(error, guard, &parent, path);
    }

    if let Err(source) = temporary.sync_all() {
        drop(temporary);
        return cleanup_after_error(
            Error::io(Operation::SyncTemporary, path, source),
            guard,
            &parent,
            path,
        );
    }
    drop(temporary);

    before_install(guard.name());

    match parent.install(guard.name(), file_name) {
        Ok(()) => {}
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {}
        Err(source) => {
            return cleanup_after_error(
                Error::io(Operation::Install, path, source),
                guard,
                &parent,
                path,
            );
        }
    };

    cleanup_temporary(guard, &parent, path)?;

    // Reload through the same parent handle for both the creator and a racing
    // loser. Besides applying the strict loader uniformly, this ensures the
    // returned key is the one actually installed at the stable destination.
    parent.load_existing(file_name, path)
}

/// An opened parent directory used as the stable authority for child names.
///
/// On Unix, every child lookup and mutation is relative to `file`, so replacing
/// or retargeting the lexical parent after this handle is opened cannot redirect
/// any later stage of initialization.
struct ParentDirectory {
    #[cfg(unix)]
    file: File,
    #[cfg(not(unix))]
    path: PathBuf,
}

impl ParentDirectory {
    fn open(parent: &Path, destination: &Path) -> Result<Self, Error> {
        let directory_path = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };

        #[cfg(unix)]
        {
            let mut options = OpenOptions::new();
            options
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC);
            let file = options
                .open(directory_path)
                .map_err(|source| Error::io(Operation::OpenParent, destination, source))?;
            Ok(Self { file })
        }

        #[cfg(not(unix))]
        {
            let metadata = fs::metadata(directory_path)
                .map_err(|source| Error::io(Operation::OpenParent, destination, source))?;
            if !metadata.is_dir() {
                return Err(Error::io(
                    Operation::OpenParent,
                    destination,
                    io::Error::new(
                        io::ErrorKind::NotADirectory,
                        "parent path is not a directory",
                    ),
                ));
            }
            Ok(Self {
                path: directory_path.to_owned(),
            })
        }
    }

    fn load_existing(&self, file_name: &OsStr, destination: &Path) -> Result<SigningKey, Error> {
        #[cfg(unix)]
        {
            let file_name = unix_name(file_name)
                .map_err(|source| Error::io(Operation::Inspect, destination, source))?;
            let mut status = MaybeUninit::<libc::stat>::uninit();
            // SAFETY: `file_name` is NUL-terminated, `status` points to writable
            // storage, and `self.file` owns a live directory descriptor.
            let result = unsafe {
                libc::fstatat(
                    self.file.as_raw_fd(),
                    file_name.as_ptr(),
                    status.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if result == -1 {
                return Err(Error::io(
                    Operation::Inspect,
                    destination,
                    io::Error::last_os_error(),
                ));
            }
            // SAFETY: a successful `fstatat` initialized `status`.
            let status = unsafe { status.assume_init() };
            if status.st_mode & libc::S_IFMT != libc::S_IFREG {
                return Err(Error::NotRegularFile {
                    path: destination.to_owned(),
                });
            }

            // SAFETY: the arguments remain valid for the duration of the call.
            let descriptor = unsafe {
                libc::openat(
                    self.file.as_raw_fd(),
                    file_name.as_ptr(),
                    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
                )
            };
            if descriptor == -1 {
                let source = io::Error::last_os_error();
                if source.raw_os_error() == Some(libc::ELOOP) {
                    return Err(Error::NotRegularFile {
                        path: destination.to_owned(),
                    });
                }
                return Err(Error::io(Operation::Open, destination, source));
            }
            // SAFETY: `openat` returned a new owned descriptor.
            let file = unsafe { File::from_raw_fd(descriptor) };
            decode_signing_key(file, destination)
        }

        #[cfg(not(unix))]
        {
            let path = self.path.join(file_name);
            let metadata = fs::symlink_metadata(&path)
                .map_err(|source| Error::io(Operation::Inspect, destination, source))?;
            if !metadata.file_type().is_file() {
                return Err(Error::NotRegularFile {
                    path: destination.to_owned(),
                });
            }
            let file = OpenOptions::new()
                .read(true)
                .open(path)
                .map_err(|source| Error::io(Operation::Open, destination, source))?;
            decode_signing_key(file, destination)
        }
    }

    fn create_new(&self, file_name: &OsStr) -> io::Result<File> {
        #[cfg(unix)]
        {
            let file_name = unix_name(file_name)?;
            // SAFETY: the arguments remain valid for the duration of the call.
            let descriptor = unsafe {
                libc::openat(
                    self.file.as_raw_fd(),
                    file_name.as_ptr(),
                    libc::O_WRONLY
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_CLOEXEC
                        | libc::O_NOFOLLOW,
                    0o600,
                )
            };
            if descriptor == -1 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: `openat` returned a new owned descriptor.
            Ok(unsafe { File::from_raw_fd(descriptor) })
        }

        #[cfg(not(unix))]
        {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.path.join(file_name))
        }
    }

    fn install(&self, temporary_name: &OsStr, destination_name: &OsStr) -> io::Result<()> {
        #[cfg(unix)]
        {
            let temporary_name = unix_name(temporary_name)?;
            let destination_name = unix_name(destination_name)?;
            // SAFETY: both names are NUL-terminated and the directory
            // descriptor stays live across the call.
            let result = unsafe {
                libc::linkat(
                    self.file.as_raw_fd(),
                    temporary_name.as_ptr(),
                    self.file.as_raw_fd(),
                    destination_name.as_ptr(),
                    0,
                )
            };
            if result == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }

        #[cfg(not(unix))]
        {
            fs::hard_link(
                self.path.join(temporary_name),
                self.path.join(destination_name),
            )
        }
    }

    fn remove(&self, file_name: &OsStr) -> io::Result<()> {
        #[cfg(unix)]
        {
            let file_name = unix_name(file_name)?;
            // SAFETY: `file_name` is NUL-terminated and the directory
            // descriptor stays live across the call.
            let result = unsafe { libc::unlinkat(self.file.as_raw_fd(), file_name.as_ptr(), 0) };
            if result == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }

        #[cfg(not(unix))]
        {
            fs::remove_file(self.path.join(file_name))
        }
    }

    fn sync(&self) -> io::Result<()> {
        #[cfg(unix)]
        {
            self.file.sync_all()
        }

        #[cfg(not(unix))]
        {
            File::open(&self.path)?.sync_all()
        }
    }
}

#[cfg(unix)]
fn unix_name(name: &OsStr) -> io::Result<CString> {
    CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file name contains NUL"))
}

fn destination_parts(path: &Path) -> Result<(&Path, &OsStr), Error> {
    let Some(file_name) = path.file_name() else {
        return Err(Error::InvalidPath {
            path: path.to_owned(),
        });
    };
    #[cfg(unix)]
    if file_name.as_bytes().contains(&0) {
        return Err(Error::InvalidPath {
            path: path.to_owned(),
        });
    }
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    Ok((parent, file_name))
}

fn create_temporary<'a>(
    destination: &Path,
    parent: &'a ParentDirectory,
    file_name: &OsStr,
) -> Result<(File, TemporaryGuard<'a>), Error> {
    for _ in 0..TEMP_ATTEMPTS {
        let mut nonce = Zeroizing::new([0u8; TEMP_NONCE_LEN]);
        OsRng
            .try_fill_bytes(&mut nonce[..])
            .map_err(|source| Error::Entropy { source })?;
        let mut temporary_name = OsString::from(".");
        temporary_name.push(file_name);
        temporary_name.push(format!(".tmp.{}.", std::process::id()));
        temporary_name.push(hex::encode(&nonce[..]));

        match parent.create_new(&temporary_name) {
            Ok(file) => {
                let mut guard = TemporaryGuard::new(parent, temporary_name);
                #[cfg(unix)]
                if let Err(source) = file.set_permissions(fs::Permissions::from_mode(0o600)) {
                    drop(file);
                    let _ = guard.remove();
                    drop(guard);
                    let _ = parent.sync();
                    return Err(Error::io(Operation::SetPermissions, destination, source));
                }
                return Ok((file, guard));
            }
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(Error::io(Operation::CreateTemporary, destination, source));
            }
        }
    }
    Err(Error::TemporaryNameExhausted {
        path: destination.to_owned(),
    })
}

fn cleanup_after_error<T>(
    original: Error,
    guard: TemporaryGuard<'_>,
    parent: &ParentDirectory,
    destination: &Path,
) -> Result<T, Error> {
    match cleanup_temporary(guard, parent, destination) {
        Ok(()) => Err(original),
        Err(cleanup) => Err(cleanup),
    }
}

fn cleanup_temporary(
    mut guard: TemporaryGuard<'_>,
    parent: &ParentDirectory,
    destination: &Path,
) -> Result<(), Error> {
    let removal = guard
        .remove()
        .map_err(|source| Error::io(Operation::RemoveTemporary, destination, source));

    // If the reported removal failed, let Drop retry while the same directory
    // handle is still live. Sync only after that retry so any successful
    // cleanup is itself durable.
    drop(guard);
    let sync = parent
        .sync()
        .map_err(|source| Error::io(Operation::SyncParent, destination, source));

    removal.and(sync)
}

fn encode_hex(seed: &[u8; 32], encoded: &mut [u8; ENCODED_SEED_LEN]) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for (index, byte) in seed.iter().copied().enumerate() {
        encoded[index * 2] = DIGITS[usize::from(byte >> 4)];
        encoded[index * 2 + 1] = DIGITS[usize::from(byte & 0x0f)];
    }
}

struct TemporaryGuard<'a> {
    parent: &'a ParentDirectory,
    name: OsString,
    armed: bool,
}

impl<'a> TemporaryGuard<'a> {
    fn new(parent: &'a ParentDirectory, name: OsString) -> Self {
        Self {
            parent,
            name,
            armed: true,
        }
    }

    fn name(&self) -> &OsStr {
        &self.name
    }

    fn remove(&mut self) -> io::Result<()> {
        self.parent.remove(&self.name)?;
        self.armed = false;
        Ok(())
    }
}

impl Drop for TemporaryGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.parent.remove(&self.name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvRestore(Option<OsString>);

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            match self.0.take() {
                Some(value) => env::set_var(KEY_PATH_ENV, value),
                None => env::remove_var(KEY_PATH_ENV),
            }
        }
    }

    fn private_write(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    #[test]
    fn path_resolution_is_explicit_then_environment_then_lexical_parent() {
        let _lock = ENV_LOCK.lock().unwrap();
        let restore = EnvRestore(env::var_os(KEY_PATH_ENV));
        env::set_var(KEY_PATH_ENV, "from-env.key");

        let pile = Path::new("some/lexical/pile.db");
        assert_eq!(
            resolve_path(Some(Path::new("explicit.key")), pile),
            PathBuf::from("explicit.key")
        );
        assert_eq!(resolve_path(None, pile), PathBuf::from("from-env.key"));

        env::remove_var(KEY_PATH_ENV);
        assert_eq!(
            resolve_path(None, pile),
            PathBuf::from("some/lexical/self.key")
        );
        drop(restore);
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_pile_parent_is_not_canonicalized() {
        use std::os::unix::fs::symlink;

        let _lock = ENV_LOCK.lock().unwrap();
        let restore = EnvRestore(env::var_os(KEY_PATH_ENV));
        env::remove_var(KEY_PATH_ENV);
        let directory = tempfile::tempdir().unwrap();
        let real = directory.path().join("real");
        let lexical = directory.path().join("lexical");
        fs::create_dir(&real).unwrap();
        symlink(&real, &lexical).unwrap();

        let pile = lexical.join("data.pile");
        assert_eq!(resolve_path(None, &pile), lexical.join("self.key"));
        drop(restore);
    }

    #[test]
    fn strict_load_accepts_exact_hex_and_rejects_invalid_formats() {
        let directory = tempfile::tempdir().unwrap();
        let valid = directory.path().join("valid.key");
        private_write(
            &valid,
            b"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        );
        assert_eq!(
            load_existing(&valid).unwrap().to_bytes(),
            [
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31,
            ]
        );

        for (name, contents) in [
            ("short.key", b"00".as_slice()),
            (
                "newline.key",
                b"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n".as_slice(),
            ),
            (
                "nonhex.key",
                b"g00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f".as_slice(),
            ),
        ] {
            let path = directory.path().join(name);
            private_write(&path, contents);
            assert!(matches!(
                load_existing(&path),
                Err(Error::InvalidFormat { .. })
            ));
        }
    }

    #[test]
    fn strict_load_rejects_non_regular_paths() {
        let directory = tempfile::tempdir().unwrap();
        assert!(matches!(
            load_existing(directory.path()),
            Err(Error::NotRegularFile { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn strict_load_rejects_symlinks_and_group_or_world_permissions() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let key = directory.path().join("key");
        private_write(
            &key,
            b"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        );
        fs::set_permissions(&key, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(matches!(
            load_existing(&key),
            Err(Error::InsecurePermissions { mode: 0o640, .. })
        ));

        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        let link = directory.path().join("link");
        symlink(&key, &link).unwrap();
        assert!(matches!(
            load_existing(&link),
            Err(Error::NotRegularFile { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn explicit_init_creates_mode_0600_and_roundtrips() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("new.key");
        let initialized = init(&path).unwrap();
        let metadata = fs::metadata(&path).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert_eq!(metadata.len(), ENCODED_SEED_LEN as u64);
        assert_eq!(
            load_existing(&path).unwrap().to_bytes(),
            initialized.to_bytes()
        );
        assert_eq!(
            fs::read_dir(directory.path())
                .unwrap()
                .filter_map(Result::ok)
                .count(),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn init_keeps_one_parent_when_lexical_symlink_is_retargeted() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let original = directory.path().join("original");
        let redirected = directory.path().join("redirected");
        let lexical = directory.path().join("lexical");
        fs::create_dir(&original).unwrap();
        fs::create_dir(&redirected).unwrap();
        symlink(&original, &lexical).unwrap();

        let lexical_key = lexical.join("retargeted.key");
        let initialized = init_with_hook(&lexical_key, |_| {
            fs::remove_file(&lexical).unwrap();
            symlink(&redirected, &lexical).unwrap();
        })
        .unwrap();

        let original_key = original.join("retargeted.key");
        assert_eq!(
            initialized.to_bytes(),
            load_existing(&original_key).unwrap().to_bytes()
        );
        assert!(!redirected.join("retargeted.key").exists());
        assert!(!lexical_key.exists());

        for parent in [&original, &redirected] {
            assert!(fs::read_dir(parent).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(".tmp.")
            }));
        }
    }

    #[test]
    fn init_does_not_clobber_an_existing_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("existing.key");
        let contents = b"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        private_write(&path, contents);
        let before = fs::read(&path).unwrap();

        let key = init(&path).unwrap();
        assert_eq!(fs::read(&path).unwrap(), before);
        assert_eq!(key.to_bytes(), load_existing(&path).unwrap().to_bytes());

        private_write(&path, b"invalid");
        assert!(matches!(init(&path), Err(Error::InvalidFormat { .. })));
        assert_eq!(fs::read(&path).unwrap(), b"invalid");
    }

    #[test]
    fn concurrent_initializers_load_one_winner() {
        const THREADS: usize = 8;
        let directory = tempfile::tempdir().unwrap();
        let path = Arc::new(directory.path().join("raced.key"));
        let barrier = Arc::new(Barrier::new(THREADS));
        let mut workers = Vec::new();
        for _ in 0..THREADS {
            let path = Arc::clone(&path);
            let barrier = Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                barrier.wait();
                init(&path).unwrap().to_bytes()
            }));
        }

        let winners: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert!(winners.iter().all(|key| key == &winners[0]));
        assert_eq!(load_existing(&path).unwrap().to_bytes(), winners[0]);
        assert_eq!(
            fs::read_dir(directory.path())
                .unwrap()
                .filter_map(Result::ok)
                .count(),
            1
        );
    }

    #[test]
    fn ordinary_load_never_creates_a_missing_key() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("missing.key");
        assert!(matches!(
            load_existing(&path),
            Err(Error::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound
        ));
        assert!(!path.exists());
    }
}
