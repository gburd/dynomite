//! Pid-file management for `dynomited`.
//!
//! The reference engine writes the daemon's PID into the file named
//! by `--pid-file` and removes it during teardown. We extend the
//! contract slightly: the file holds an exclusive `flock(2)` for the
//! lifetime of the [`PidFile`] guard so a second `dynomited` instance
//! cannot silently overwrite a running daemon's pid file. The
//! `flock` is released and the file unlinked on drop.
//!
//! Acquiring the flock retries briefly on `EAGAIN` / `EWOULDBLOCK`
//! to absorb the kernel-level race that occurs when an operator
//! (or a chaos injector) restarts dynomited within milliseconds of
//! a `SIGKILL`: the killed process is still being reaped and its
//! flock entry has not yet been released. The retry budget is
//! bounded ([`DEFAULT_FLOCK_ATTEMPTS`] x [`DEFAULT_FLOCK_DELAY`])
//! so a genuine duplicate-instance error still surfaces quickly.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process;
use std::time::Duration;

use nix::fcntl::{Flock, FlockArg};

/// Default number of `flock(2)` retries before reporting
/// [`io::ErrorKind::WouldBlock`].
pub const DEFAULT_FLOCK_ATTEMPTS: u32 = 10;

/// Default delay between `flock(2)` retries.
pub const DEFAULT_FLOCK_DELAY: Duration = Duration::from_millis(100);

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
    /// Retries the flock on `EAGAIN` / `EWOULDBLOCK` up to
    /// [`DEFAULT_FLOCK_ATTEMPTS`] times spaced by
    /// [`DEFAULT_FLOCK_DELAY`]. Returns
    /// [`io::ErrorKind::WouldBlock`] when every attempt fails;
    /// the caller should surface this as a fatal startup error.
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
        Self::create_with_retry(path, pid, DEFAULT_FLOCK_ATTEMPTS, DEFAULT_FLOCK_DELAY)
    }

    /// Variant of [`Self::create_with_pid`] that lets the caller
    /// pick the retry budget. Mostly useful in tests that
    /// deliberately race two callers.
    ///
    /// # Errors
    /// Same as [`Self::create`].
    pub fn create_with_retry<P: AsRef<Path>>(
        path: P,
        pid: u32,
        max_attempts: u32,
        delay: Duration,
    ) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let attempts = max_attempts.max(1);
        let mut last_err: Option<io::Error> = None;
        for attempt in 0..attempts {
            // Reopen the file each iteration: the `Flock::lock`
            // call below consumes the `File` on failure, so we
            // cannot reuse it across attempts.
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o644)
                .open(&path)?;

            match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
                Ok(lock) => {
                    // Write the pid through a `&File` reference so we keep the
                    // ownership inside the Flock wrapper.
                    let mut handle: &File = &lock;
                    write!(handle, "{pid}")?;
                    handle.flush()?;
                    return Ok(Self {
                        path,
                        lock: Some(lock),
                    });
                }
                Err((_, errno)) => {
                    let kind = if errno == nix::errno::Errno::EWOULDBLOCK
                        || errno == nix::errno::Errno::EAGAIN
                    {
                        io::ErrorKind::WouldBlock
                    } else {
                        io::ErrorKind::Other
                    };
                    last_err = Some(io::Error::new(
                        kind,
                        format!("flock {}: {errno}", path.display()),
                    ));
                    if kind != io::ErrorKind::WouldBlock || attempt + 1 == attempts {
                        break;
                    }
                    std::thread::sleep(delay);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("flock {}: unknown error", path.display()),
            )
        }))
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
        // Use a tiny retry budget so the test does not stall.
        let err = PidFile::create_with_retry(&path, 2, 2, Duration::from_millis(5)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn flock_retry_succeeds_when_holder_drops_during_window() {
        // Simulates the SIGKILL+restart race: thread A holds the
        // flock briefly, thread B retries during the window. With
        // retries enabled, B should eventually win.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.pid");
        let path_a = path.clone();
        let holder = std::thread::spawn(move || {
            let g = PidFile::create_with_pid(&path_a, 1).unwrap();
            std::thread::sleep(Duration::from_millis(80));
            drop(g);
        });
        // Give the holder thread a head start.
        std::thread::sleep(Duration::from_millis(10));
        // Retry budget large enough to outlast the holder.
        let path_b = path.clone();
        let g = PidFile::create_with_retry(&path_b, 2, 20, Duration::from_millis(10)).unwrap();
        // We won the race after retrying.
        let s = std::fs::read_to_string(&path).unwrap();
        assert_eq!(s.trim(), "2");
        drop(g);
        holder.join().unwrap();
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
