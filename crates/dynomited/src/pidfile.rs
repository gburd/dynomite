//! Pid-file management for `dynomited`.
//!
//! The reference engine writes the daemon's PID into the file named
//! by `--pid-file` and removes it during teardown. We extend the
//! contract slightly: the file holds an exclusive `flock(2)` for the
//! lifetime of the [`PidFile`] guard so a second `dynomited` instance
//! cannot silently overwrite a running daemon's pid file. The
//! `flock` is released and the file unlinked on drop.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process;

use nix::fcntl::{Flock, FlockArg};

/// RAII guard for a pid file.
///
/// When dropped, the guard releases the exclusive lock and unlinks
/// the file. The unlink ignores ENOENT (someone removed the file
/// already) and reports any other error via `tracing::warn!` so
/// shutdown stays infallible. Recreate via [`PidFile::create`].
///
/// # Examples
///
/// ```
/// use dynomited::pidfile::PidFile;
/// let dir = tempfile::tempdir().unwrap();
/// let path = dir.path().join("d.pid");
/// let _guard = PidFile::create(&path).unwrap();
/// assert!(path.exists());
/// ```
#[derive(Debug)]
pub struct PidFile {
    path: PathBuf,
    // The lock holds the file open for the lifetime of the guard.
    // Wrapped in Option so [`Drop`] can swap it out without
    // panicking.
    lock: Option<Flock<File>>,
}

impl PidFile {
    /// Open or create the pid file at `path`, write the current
    /// process id, and acquire an exclusive non-blocking flock.
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] when the file is
    /// already locked by another process; the caller should
    /// surface this as a fatal startup error.
    ///
    /// # Errors
    /// Forwarded from `open(2)`, `write(2)`, and `flock(2)`.
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Self::create_with_pid(path, process::id())
    }

    /// Variant of [`Self::create`] that lets tests inject a pid.
    ///
    /// # Errors
    /// Same as [`Self::create`].
    pub fn create_with_pid<P: AsRef<Path>>(path: P, pid: u32) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(&path)?;

        let lock = Flock::lock(file, FlockArg::LockExclusiveNonblock).map_err(|(_, errno)| {
            io::Error::new(
                if errno == nix::errno::Errno::EWOULDBLOCK {
                    io::ErrorKind::WouldBlock
                } else {
                    io::ErrorKind::Other
                },
                format!("flock {}: {errno}", path.display()),
            )
        })?;

        // Write the pid through a `&File` reference so we keep the
        // ownership inside the Flock wrapper.
        let mut handle: &File = &lock;
        write!(handle, "{pid}")?;
        handle.flush()?;

        Ok(Self {
            path,
            lock: Some(lock),
        })
    }

    /// Path the pid was written to.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomited::pidfile::PidFile;
    /// let dir = tempfile::tempdir().unwrap();
    /// let p = dir.path().join("d.pid");
    /// let g = PidFile::create(&p).unwrap();
    /// assert_eq!(g.path(), p.as_path());
    /// ```
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for PidFile {
    fn drop(&mut self) {
        // Release the lock first by dropping the wrapper; the
        // flock ends with the close(2) the wrapper performs.
        self.lock.take();
        if let Err(e) = std::fs::remove_file(&self.path) {
            if e.kind() != io::ErrorKind::NotFound {
                tracing::warn!(path = %self.path.display(), error = %e, "remove pid file");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_writes_pid_and_unlinks_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.pid");
        {
            let _g = PidFile::create_with_pid(&path, 12345).unwrap();
            assert!(path.exists());
            let s = std::fs::read_to_string(&path).unwrap();
            assert_eq!(s.trim(), "12345");
        }
        assert!(!path.exists());
    }

    #[test]
    fn second_lock_attempt_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.pid");
        let _g1 = PidFile::create_with_pid(&path, 1).unwrap();
        let err = PidFile::create_with_pid(&path, 2).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn drop_after_external_unlink_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.pid");
        let g = PidFile::create_with_pid(&path, 1).unwrap();
        std::fs::remove_file(&path).unwrap();
        drop(g);
        assert!(!path.exists());
    }
}
