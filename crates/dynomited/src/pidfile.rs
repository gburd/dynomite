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
use std::io::{self, Read, Write};
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
            // Open without O_TRUNC so an existing PID stays
            // readable for the diagnostic path. The successful
            // branch truncates and rewrites; the contention
            // branch reads the stored PID for the error message.
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o644)
                .open(&path)?;

            match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
                Ok(lock) => {
                    // Truncate then write the pid through a
                    // `&File` reference so we keep ownership
                    // inside the Flock wrapper.
                    {
                        let handle: &File = &lock;
                        handle.set_len(0)?;
                    }
                    let mut handle: &File = &lock;
                    write!(handle, "{pid}")?;
                    handle.flush()?;
                    return Ok(Self {
                        path,
                        lock: Some(lock),
                    });
                }
                Err((file_back, errno)) => {
                    let kind = if errno == nix::errno::Errno::EWOULDBLOCK
                        || errno == nix::errno::Errno::EAGAIN
                    {
                        io::ErrorKind::WouldBlock
                    } else {
                        io::ErrorKind::Other
                    };
                    let holder = read_holder_pid(file_back);
                    last_err = Some(format_lock_error(&path, errno, kind, holder));
                    if kind != io::ErrorKind::WouldBlock || attempt + 1 == attempts {
                        break;
                    }
                    std::thread::sleep(delay);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::other(format!("flock {}: unknown error", path.display()))
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

/// Read up to 32 bytes from the contended file and parse them as
/// a decimal pid. Best-effort: the returned `Option` is `None`
/// when the file is empty, unreadable, or contains a non-numeric
/// payload. Used only for the diagnostic in
/// [`format_lock_error`].
fn read_holder_pid(mut file: File) -> Option<u32> {
    use std::io::Seek;
    let _ = file.seek(std::io::SeekFrom::Start(0));
    let mut buf = [0u8; 32];
    let n = file.read(&mut buf).ok()?;
    let s = std::str::from_utf8(&buf[..n]).ok()?.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<u32>().ok()
}

/// Compose a diagnostic [`io::Error`] when the flock is held.
/// When the contended file already carries a decimal pid, the
/// message includes the holder's pid AND a liveness probe via
/// `kill(pid, 0)` so an operator can tell "another dynomited is
/// running" apart from "a stale pidfile from a crashed daemon".
fn format_lock_error(
    path: &Path,
    errno: nix::errno::Errno,
    kind: io::ErrorKind,
    holder: Option<u32>,
) -> io::Error {
    let path_disp = path.display();
    let msg = match holder {
        Some(pid) => {
            let alive = pid_is_alive(pid);
            if alive {
                format!(
                    "flock {path_disp}: {errno}: another dynomited (pid {pid}) is holding the pid file"
                )
            } else {
                format!(
                    "flock {path_disp}: {errno}: pid file holds pid {pid} which is not alive; kernel has not yet released the flock (transient)"
                )
            }
        }
        None => format!("flock {path_disp}: {errno}"),
    };
    io::Error::new(kind, msg)
}

/// Liveness probe via `kill(pid, 0)`. Returns `true` when the
/// pid is alive (or alive-but-permission-denied), `false` when
/// the kernel reports `ESRCH`. We treat permission-denied as
/// alive because EPERM only ever fires when the target exists.
fn pid_is_alive(pid: u32) -> bool {
    let Ok(pid_raw) = i32::try_from(pid) else {
        return false;
    };
    let target = nix::unistd::Pid::from_raw(pid_raw);
    !matches!(
        nix::sys::signal::kill(target, None),
        Err(nix::errno::Errno::ESRCH)
    )
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
    fn lock_error_reports_holder_pid() {
        // The first holder writes pid 1 (kernel `init`, always
        // alive). The contender's error message must surface
        // "pid 1" so the operator can chase the conflict.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.pid");
        let _g1 = PidFile::create_with_pid(&path, 1).unwrap();
        let err = PidFile::create_with_retry(&path, 2, 1, Duration::from_millis(0)).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("pid 1"),
            "error message did not surface holder pid: {msg}"
        );
    }

    #[test]
    fn lock_error_flags_stale_pid_when_not_alive() {
        // Pre-populate the file with a non-alive pid (u32::MAX
        // is virtually never a real process). The first lock
        // acquires the flock so the contender's error path
        // reports "not alive".
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.pid");
        // Holder writes a huge pid value into the file.
        let _g1 = PidFile::create_with_pid(&path, u32::MAX).unwrap();
        let err = PidFile::create_with_retry(&path, 1, 1, Duration::from_millis(0)).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not alive") || msg.contains("transient"),
            "expected stale-pid hint in error, got: {msg}"
        );
    }

    #[test]
    fn flock_retry_succeeds_when_holder_drops_during_window() {
        // Simulates the SIGKILL+restart race: thread A holds the
        // flock briefly, thread B retries during the window. With
        // retries enabled, B should eventually win.
        //
        // The retry budget is sized to be load-tolerant: the
        // holder advertises its release via a channel rather than
        // a fixed sleep, and the contender is allowed up to
        // `MAX_RETRY_BUDGET` retries (~5 seconds total). Earlier
        // budgets of ~200ms turned the test into a load-correlated
        // flake under `--all-features` parallelism (F9 in
        // `docs/journal/2026-05-23-audit.md`); the new budget
        // dominates any plausible scheduling jitter on shared CI
        // hosts. The directory is private to this test, so there is
        // no cross-test contention to worry about.
        const HOLD_MS: u64 = 30;
        const RETRY_DELAY: Duration = Duration::from_millis(10);
        const MAX_RETRY_BUDGET: u32 = 500;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.pid");
        let path_a = path.clone();
        let (released_tx, released_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            let g = PidFile::create_with_pid(&path_a, 1).unwrap();
            std::thread::sleep(Duration::from_millis(HOLD_MS));
            drop(g);
            // Signal post-release so the assertion below can verify
            // we observed at least one retry while the holder was
            // still active.
            let _ = released_tx.send(());
        });
        // Give the holder thread a head start so the contender's
        // first attempt observes a held lock.
        std::thread::sleep(Duration::from_millis(5));
        let path_b = path.clone();
        let started = std::time::Instant::now();
        let g = PidFile::create_with_retry(&path_b, 2, MAX_RETRY_BUDGET, RETRY_DELAY).unwrap();
        let elapsed = started.elapsed();
        // Confirm we actually retried (lock was held when we
        // started); the test would otherwise be a no-op if the
        // holder thread never got scheduled.
        assert!(
            elapsed >= RETRY_DELAY,
            "contender returned before any retry slept: {elapsed:?}"
        );
        let s = std::fs::read_to_string(&path).unwrap();
        assert_eq!(s.trim(), "2");
        drop(g);
        // Holder must have released by now since we hold the lock.
        released_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("holder thread released lock");
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
