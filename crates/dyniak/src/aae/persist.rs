//! Snapshot persistence for the Tictac AAE tree.
//!
//! At process start the AAE driver attempts to load the
//! previous run's snapshot via [`Tree::load_snapshot`]; on
//! success the driver picks up where it left off and avoids a
//! full datastore rescan. On a periodic interval (default
//! `ConfAae::snapshot_interval_seconds`) the driver writes a
//! fresh snapshot via [`Tree::save_snapshot`].
//!
//! # File format (version 1)
//!
//! The snapshot is a single binary file with a fixed-shape
//! header followed by length-prefixed body sections. All
//! integers are big-endian.
//!
//! ```text
//! Header (24 bytes):
//!   4 bytes   magic              0x41 0x41 0x45 0x31  ("AAE1")
//!   4 bytes   version            u32 = 1
//!   4 bytes   n_time_buckets     u32
//!   4 bytes   n_segments         u32
//!   8 bytes   time_window_secs   u64
//!
//! Roots section:
//!   4 bytes   roots_count        u32  (must equal n_time_buckets)
//!   for each: 4 bytes time_bucket u32, 8 bytes root_hash u64
//!
//! Segments section:
//!   4 bytes   segments_count     u32  (number of non-empty
//!                                      (time_bucket, segment)
//!                                      pairs that follow)
//!   for each:
//!     4 bytes time_bucket        u32
//!     4 bytes segment            u32
//!     8 bytes segment_hash       u64
//!     4 bytes keys_count         u32
//!     for each key entry:
//!       4 bytes bucket_len       u32
//!       bucket_len bytes         bucket bytes
//!       4 bytes key_len          u32
//!       key_len bytes            key bytes
//!       4 bytes vclock_len       u32
//!       vclock_len bytes         vclock bytes
//!
//! Trailer (4 bytes):
//!   4 bytes   magic              0x41 0x41 0x45 0x31  ("AAE1")
//! ```
//!
//! No compression is applied in v1; the snapshot is small
//! (a 24-bucket, 1024-segment, 1M-key tree fits in a few
//! tens of MiB) and the cost of compressing/decompressing on
//! every snapshot interval outweighs the disk savings.
//!
//! # Atomicity
//!
//! [`Tree::save_snapshot`] writes to `<path>.tmp` and atomic-
//! renames to `<path>` on success. A torn write at process
//! crash leaves the previous good snapshot intact; the
//! `.tmp` file may linger and is safe to delete out of band.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use crate::aae::tictac::KeyEntry;
use crate::aae::tictac::Tree;
use crate::aae::tictac::TreeShape;

/// Magic word that opens and closes every snapshot file.
/// "AAE1" in big-endian ASCII.
pub const SNAPSHOT_MAGIC: u32 = 0x4141_4531;

/// Current snapshot format version. A snapshot that does not
/// match this version is rejected with [`PersistError::VersionSkew`];
/// the driver treats version skew as "rebuild from scratch."
pub const SNAPSHOT_VERSION: u32 = 1;

/// Hard ceiling on a single length-prefixed byte string in
/// the body. Frames longer than this are rejected as
/// corrupted to keep a malformed file from triggering a
/// multi-gigabyte allocation.
pub const MAX_FIELD_LEN: u32 = 16 * 1024 * 1024;

/// Header size in bytes (magic + version + n_time_buckets +
/// n_segments + time_window_seconds).
const HEADER_BYTES: usize = 24;

/// Trailer size in bytes (closing magic).
const TRAILER_BYTES: usize = 4;

/// Errors raised by snapshot save/load.
///
/// On the load path, every variant other than
/// [`PersistError::Io`] for `NotFound` indicates the
/// snapshot is unusable; the AAE scheduler treats these as
/// "rebuild from scratch."
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PersistError {
    /// Underlying filesystem error (open, read, write,
    /// rename, mkdir). The error text carries the failing
    /// path and the OS-level reason.
    #[error("aae snapshot io: {0}")]
    Io(#[from] io::Error),

    /// The snapshot file's magic word does not match
    /// [`SNAPSHOT_MAGIC`], or the trailer does not close
    /// with the same magic, or the field-length sanity
    /// checks failed. The carried string describes the
    /// concrete reason so an operator can triage without
    /// re-reading the file.
    #[error("aae snapshot corrupted: {0}")]
    Corrupted(String),

    /// The snapshot's recorded version field does not match
    /// [`SNAPSHOT_VERSION`]. Surfaces separately from
    /// [`PersistError::Corrupted`] because operators upgrading
    /// across a snapshot-format-bump want to see "old
    /// version, rebuild" rather than "corrupt, panic".
    #[error("aae snapshot version skew: file v{file}, expected v{expected}")]
    VersionSkew {
        /// Version recorded in the snapshot file.
        file: u32,
        /// Version this build understands.
        expected: u32,
    },

    /// The snapshot's recorded shape parameters cross-
    /// validate inconsistently (zero fan-out, segment count
    /// past the body) and the file is not safe to load.
    #[error("aae snapshot bad shape: {0}")]
    BadShape(String),
}

impl Tree {
    /// Persist the tree's segment hashes and per-segment key
    /// directory to `path`.
    ///
    /// Writes go through a `<path>.tmp` sibling and an
    /// `fs::rename` so a crash mid-write cannot leave a
    /// torn file under `path`. Parent directories are
    /// created on demand with mode `0700` (Unix; on other
    /// platforms the default `create_dir_all` mode is used).
    ///
    /// # Errors
    ///
    /// [`PersistError::Io`] on filesystem failure.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    /// use dyniak::aae::tictac::{Tree, TreeShape};
    /// let mut t = Tree::new(TreeShape {
    ///     n_time_buckets: 4,
    ///     n_segments: 16,
    ///     time_window_seconds: 60,
    /// });
    /// t.insert(b"users", b"alice", b"vc1", 0);
    /// t.save_snapshot(Path::new("/tmp/aae.snapshot"))
    ///     .expect("snapshot");
    /// ```
    pub fn save_snapshot(&self, path: &Path) -> Result<(), PersistError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                ensure_dir(parent)?;
            }
        }

        let tmp_path = tmp_companion_path(path);
        let buf = self.encode_to_bytes();

        // Open with truncate so a previous incomplete .tmp
        // does not leak its tail into this snapshot.
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        f.write_all(&buf)?;
        f.sync_all()?;
        drop(f);

        fs::rename(&tmp_path, path)?;
        Ok(())
    }

    /// Reload a previously-saved snapshot from `path`.
    ///
    /// Returns a freshly-built [`Tree`] whose roots, segment
    /// hashes, and per-segment directories match what
    /// [`Tree::save_snapshot`] wrote. Empty segments (those
    /// that were not present in the snapshot's body) start
    /// at zero hash with empty directory.
    ///
    /// # Errors
    ///
    /// * [`PersistError::Io`] on filesystem failure (the
    ///   common case is `NotFound`, which the AAE scheduler
    ///   treats as "no prior snapshot, rebuild").
    /// * [`PersistError::VersionSkew`] when the recorded
    ///   version field does not match [`SNAPSHOT_VERSION`].
    /// * [`PersistError::Corrupted`] when the magic, body
    ///   layout, or trailer fail to validate.
    /// * [`PersistError::BadShape`] when the recorded shape
    ///   parameters are cross-inconsistent with the body.
    pub fn load_snapshot(path: &Path) -> Result<Self, PersistError> {
        let mut f = fs::File::open(path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        Self::decode_from_bytes(&buf)
    }

    /// Encode this tree to the v1 snapshot byte layout.
    /// Exposed at crate visibility so tests can corrupt the
    /// bytes without touching the filesystem.
    pub(crate) fn encode_to_bytes(&self) -> Vec<u8> {
        let shape = self.shape();
        let mut buf = Vec::new();

        // Header.
        buf.extend_from_slice(&SNAPSHOT_MAGIC.to_be_bytes());
        buf.extend_from_slice(&SNAPSHOT_VERSION.to_be_bytes());
        buf.extend_from_slice(&shape.n_time_buckets.to_be_bytes());
        buf.extend_from_slice(&shape.n_segments.to_be_bytes());
        buf.extend_from_slice(&shape.time_window_seconds.to_be_bytes());

        // Roots.
        let roots = self.roots();
        let roots_count = u32::try_from(roots.len()).unwrap_or(u32::MAX);
        buf.extend_from_slice(&roots_count.to_be_bytes());
        for (tb, hash) in &roots {
            buf.extend_from_slice(&tb.to_be_bytes());
            buf.extend_from_slice(&hash.to_be_bytes());
        }

        // Segments. Walk every (tb, seg) with a non-empty
        // directory and emit the segment hash + its key set.
        // Segments whose directory is empty have hash 0 by
        // construction (empty XOR aggregate) and are
        // reconstructed implicitly on load.
        let payload = self.collect_nonempty_segments();
        let seg_count = u32::try_from(payload.len()).unwrap_or(u32::MAX);
        buf.extend_from_slice(&seg_count.to_be_bytes());
        for (tb, seg, hash, entries) in &payload {
            buf.extend_from_slice(&tb.to_be_bytes());
            buf.extend_from_slice(&seg.to_be_bytes());
            buf.extend_from_slice(&hash.to_be_bytes());
            let entries_len = u32::try_from(entries.len()).unwrap_or(u32::MAX);
            buf.extend_from_slice(&entries_len.to_be_bytes());
            for e in entries {
                write_lp(&mut buf, &e.bucket);
                write_lp(&mut buf, &e.key);
                write_lp(&mut buf, &e.vclock);
            }
        }

        // Trailer.
        buf.extend_from_slice(&SNAPSHOT_MAGIC.to_be_bytes());
        buf
    }

    /// Inverse of [`Tree::encode_to_bytes`]. Crate-visible so
    /// tests can drive a synthesised buffer through the
    /// decoder.
    pub(crate) fn decode_from_bytes(buf: &[u8]) -> Result<Self, PersistError> {
        if buf.len() < HEADER_BYTES + TRAILER_BYTES {
            return Err(PersistError::Corrupted(format!(
                "buffer too short: {} bytes",
                buf.len()
            )));
        }
        let mut p = 0usize;
        let shape = decode_header(buf, &mut p)?;
        let mut tree = Tree::new(shape);

        let expected_roots = decode_roots_section(buf, &mut p, shape.n_time_buckets)?;
        let accum_roots = decode_segments_section(buf, &mut p, &shape, &mut tree)?;

        // Trailer.
        let trailer = read_u32(buf, &mut p)?;
        if trailer != SNAPSHOT_MAGIC {
            return Err(PersistError::Corrupted(format!(
                "bad trailer magic: 0x{trailer:08x}"
            )));
        }
        if p != buf.len() {
            return Err(PersistError::Corrupted(format!(
                "trailing bytes: {} unread of {}",
                buf.len() - p,
                buf.len()
            )));
        }

        // Cross-check recomputed roots against what the file
        // recorded.
        for (tb, recorded) in &expected_roots {
            let recomputed = accum_roots.get(tb).copied().unwrap_or(0);
            if recomputed != *recorded {
                return Err(PersistError::Corrupted(format!(
                    "root mismatch on tb {tb}: file 0x{recorded:016x}, segments 0x{recomputed:016x}"
                )));
            }
        }

        Ok(tree)
    }
}

fn decode_header(buf: &[u8], p: &mut usize) -> Result<TreeShape, PersistError> {
    let magic = read_u32(buf, p)?;
    if magic != SNAPSHOT_MAGIC {
        return Err(PersistError::Corrupted(format!("bad magic: 0x{magic:08x}")));
    }
    let version = read_u32(buf, p)?;
    if version != SNAPSHOT_VERSION {
        return Err(PersistError::VersionSkew {
            file: version,
            expected: SNAPSHOT_VERSION,
        });
    }
    let n_time_buckets = read_u32(buf, p)?;
    let n_segments = read_u32(buf, p)?;
    let time_window_seconds = read_u64(buf, p)?;

    if n_time_buckets == 0 {
        return Err(PersistError::BadShape("n_time_buckets == 0".to_string()));
    }
    if n_segments == 0 {
        return Err(PersistError::BadShape("n_segments == 0".to_string()));
    }
    if time_window_seconds == 0 {
        return Err(PersistError::BadShape(
            "time_window_seconds == 0".to_string(),
        ));
    }

    Ok(TreeShape {
        n_time_buckets,
        n_segments,
        time_window_seconds,
    })
}

fn decode_roots_section(
    buf: &[u8],
    p: &mut usize,
    n_time_buckets: u32,
) -> Result<BTreeMap<u32, u64>, PersistError> {
    let roots_count = read_u32(buf, p)?;
    if roots_count != n_time_buckets {
        return Err(PersistError::Corrupted(format!(
            "roots_count {roots_count} != n_time_buckets {n_time_buckets}"
        )));
    }
    let mut expected_roots: BTreeMap<u32, u64> = BTreeMap::new();
    for _ in 0..roots_count {
        let tb = read_u32(buf, p)?;
        let hash = read_u64(buf, p)?;
        if tb >= n_time_buckets {
            return Err(PersistError::Corrupted(format!(
                "roots tb {tb} out of range (n_time_buckets={n_time_buckets})"
            )));
        }
        expected_roots.insert(tb, hash);
    }
    Ok(expected_roots)
}

fn decode_segments_section(
    buf: &[u8],
    p: &mut usize,
    shape: &TreeShape,
    tree: &mut Tree,
) -> Result<BTreeMap<u32, u64>, PersistError> {
    let seg_count = read_u32(buf, p)?;
    let mut accum_roots: BTreeMap<u32, u64> = BTreeMap::new();
    let mut seen: BTreeSet<(u32, u32)> = BTreeSet::new();
    for _ in 0..seg_count {
        let tb = read_u32(buf, p)?;
        let seg = read_u32(buf, p)?;
        let hash = read_u64(buf, p)?;
        let keys_count = read_u32(buf, p)?;
        if tb >= shape.n_time_buckets {
            return Err(PersistError::Corrupted(format!(
                "segment tb {tb} out of range (n_time_buckets={n_time_buckets})",
                n_time_buckets = shape.n_time_buckets
            )));
        }
        if seg >= shape.n_segments {
            return Err(PersistError::Corrupted(format!(
                "segment seg {seg} out of range (n_segments={n_segments})",
                n_segments = shape.n_segments
            )));
        }
        if !seen.insert((tb, seg)) {
            return Err(PersistError::Corrupted(format!(
                "duplicate segment record (tb={tb}, seg={seg})"
            )));
        }

        let mut entries = Vec::with_capacity(keys_count as usize);
        for _ in 0..keys_count {
            let bucket = read_lp(buf, p)?;
            let key = read_lp(buf, p)?;
            let vclock = read_lp(buf, p)?;
            entries.push(KeyEntry {
                bucket,
                key,
                vclock,
            });
        }
        tree.install_segment(tb, seg, hash, entries)
            .map_err(|e| PersistError::Corrupted(format!("install_segment: {e}")))?;
        *accum_roots.entry(tb).or_default() ^= hash;
    }
    Ok(accum_roots)
}

/// Append a length-prefixed byte string to `buf`.
fn write_lp(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(bytes);
}

fn read_u32(buf: &[u8], p: &mut usize) -> Result<u32, PersistError> {
    if *p + 4 > buf.len() {
        return Err(PersistError::Corrupted(format!(
            "short read u32 at offset {p}"
        )));
    }
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&buf[*p..*p + 4]);
    *p += 4;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(buf: &[u8], p: &mut usize) -> Result<u64, PersistError> {
    if *p + 8 > buf.len() {
        return Err(PersistError::Corrupted(format!(
            "short read u64 at offset {p}"
        )));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&buf[*p..*p + 8]);
    *p += 8;
    Ok(u64::from_be_bytes(bytes))
}

fn read_lp(buf: &[u8], p: &mut usize) -> Result<Vec<u8>, PersistError> {
    let len = read_u32(buf, p)?;
    if len > MAX_FIELD_LEN {
        return Err(PersistError::Corrupted(format!(
            "length-prefixed field {len} exceeds MAX_FIELD_LEN {MAX_FIELD_LEN}"
        )));
    }
    let len_us = len as usize;
    if *p + len_us > buf.len() {
        return Err(PersistError::Corrupted(format!(
            "short read of {len_us} bytes at offset {p}"
        )));
    }
    let out = buf[*p..*p + len_us].to_vec();
    *p += len_us;
    Ok(out)
}

/// Build the temp companion path used by the atomic save.
fn tmp_companion_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Create the parent directory of a snapshot path with mode
/// 0700 on Unix. On other platforms the default permissions
/// from `create_dir_all` are used.
fn ensure_dir(parent: &Path) -> io::Result<()> {
    if parent.exists() {
        return Ok(());
    }
    fs::create_dir_all(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o700);
        // Ignore the result of set_permissions on the
        // outer parent: a shared /var/lib path may already
        // be present and not owned by us.
        let _ = fs::set_permissions(parent, perms);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aae::tictac::TreeShape;
    use tempfile::TempDir;

    fn shape() -> TreeShape {
        TreeShape {
            n_time_buckets: 4,
            n_segments: 64,
            time_window_seconds: 60,
        }
    }

    fn populated_tree() -> Tree {
        let mut t = Tree::new(shape());
        for i in 0..100u32 {
            let key = format!("k{i:04}");
            let vc = format!("vc{i}");
            t.insert(b"users", key.as_bytes(), vc.as_bytes(), u64::from(i % 240));
        }
        t
    }

    #[test]
    fn snapshot_round_trip_preserves_roots_and_segments() {
        let t = populated_tree();
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("tree.snapshot");
        t.save_snapshot(&path).expect("save");

        let loaded = Tree::load_snapshot(&path).expect("load");
        assert_eq!(t.shape(), loaded.shape());
        assert_eq!(t.roots(), loaded.roots());
        for tb in 0..shape().n_time_buckets {
            assert_eq!(
                t.segments(tb).expect("orig segments"),
                loaded.segments(tb).expect("loaded segments"),
                "segments differ for tb {tb}"
            );
            for seg in 0..shape().n_segments {
                let a = t.keys_in_segment(tb, seg).expect("orig keys");
                let b = loaded.keys_in_segment(tb, seg).expect("loaded keys");
                assert_eq!(a, b, "keys differ at (tb={tb}, seg={seg})");
            }
        }
    }

    #[test]
    fn snapshot_creates_parent_dir() {
        let t = Tree::new(shape());
        let dir = TempDir::new().expect("tempdir");
        let nested = dir.path().join("nested").join("aae");
        let path = nested.join("tree.snapshot");
        t.save_snapshot(&path).expect("save");
        assert!(path.exists(), "snapshot file should exist");
        assert!(nested.exists(), "parent directory should exist");
    }

    #[test]
    fn empty_tree_round_trips() {
        let t = Tree::new(shape());
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("tree.snapshot");
        t.save_snapshot(&path).expect("save");
        let loaded = Tree::load_snapshot(&path).expect("load");
        assert_eq!(t.roots(), loaded.roots());
    }

    #[test]
    fn truncated_snapshot_returns_corrupted() {
        let t = populated_tree();
        let mut buf = t.encode_to_bytes();
        buf.truncate(100);
        let err = Tree::decode_from_bytes(&buf).unwrap_err();
        assert!(
            matches!(err, PersistError::Corrupted(_)),
            "expected Corrupted, got {err:?}"
        );
    }

    #[test]
    fn bad_magic_returns_corrupted() {
        let t = populated_tree();
        let mut buf = t.encode_to_bytes();
        // Flip the first magic byte.
        buf[0] = 0xff;
        let err = Tree::decode_from_bytes(&buf).unwrap_err();
        assert!(
            matches!(err, PersistError::Corrupted(_)),
            "expected Corrupted, got {err:?}"
        );
    }

    #[test]
    fn version_skew_returns_version_skew() {
        let t = populated_tree();
        let mut buf = t.encode_to_bytes();
        // Bump version field (bytes 4..8) to v999.
        buf[4..8].copy_from_slice(&999u32.to_be_bytes());
        let err = Tree::decode_from_bytes(&buf).unwrap_err();
        match err {
            PersistError::VersionSkew { file, expected } => {
                assert_eq!(file, 999);
                assert_eq!(expected, SNAPSHOT_VERSION);
            }
            other => panic!("expected VersionSkew, got {other:?}"),
        }
    }

    #[test]
    fn bad_trailer_returns_corrupted() {
        let t = populated_tree();
        let mut buf = t.encode_to_bytes();
        let last4 = buf.len() - 4;
        buf[last4] = 0xde;
        buf[last4 + 1] = 0xad;
        let err = Tree::decode_from_bytes(&buf).unwrap_err();
        assert!(
            matches!(err, PersistError::Corrupted(_)),
            "expected Corrupted, got {err:?}"
        );
    }

    #[test]
    fn missing_file_returns_io_not_found() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("nope.snapshot");
        let err = Tree::load_snapshot(&path).unwrap_err();
        match err {
            PersistError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::NotFound),
            other => panic!("expected Io(NotFound), got {other:?}"),
        }
    }

    #[test]
    fn atomic_save_replaces_prior_snapshot() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("tree.snapshot");

        let mut a = Tree::new(shape());
        a.insert(b"users", b"alice", b"v1", 0);
        a.save_snapshot(&path).expect("save 1");

        let mut b = Tree::new(shape());
        b.insert(b"users", b"bob", b"v2", 0);
        b.save_snapshot(&path).expect("save 2");

        let loaded = Tree::load_snapshot(&path).expect("load");
        assert_eq!(loaded.roots(), b.roots());
        // The .tmp companion should not be left behind.
        let tmp = tmp_companion_path(&path);
        assert!(!tmp.exists(), ".tmp should not linger after rename");
    }
}
