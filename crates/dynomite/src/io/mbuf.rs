//! Pooled fixed-size byte buffers.
//!
//! The C engine uses chained `struct mbuf` chunks for every connection's
//! read and write paths. Each chunk has a read cursor (`pos`), a write
//! cursor (`last`), a writable end boundary (`end`), and an extra
//! trailing region (`end_extra`) that downstream code uses for crypto
//! padding and overflow guards. Chunks are recycled through a global
//! free list so steady-state traffic does not call into the allocator.
//!
//! This module reproduces the same shape in safe Rust:
//!
//! * [`Mbuf`] owns a [`Box<[u8]>`] of `chunk_size` bytes plus the four
//!   cursors that track readable, writable, and reserved regions.
//! * [`MbufQueue`] is a tail-queue of chunks. The C list head is
//!   `struct mhdr`; the Rust container is a [`VecDeque`].
//! * [`MbufPool`] owns the recycled buffers behind a parking-lot
//!   mutex and is shared across worker tasks via [`Arc`].
//!
//! The default chunk size is [`MBUF_SIZE`] (16 KiB), tunable through
//! [`MbufPool::new`]. The configurable range is
//! `[MBUF_MIN_SIZE, MBUF_MAX_SIZE]`. The trailing [`MBUF_ESIZE`] bytes
//! of every chunk are reserved for the crypto MAC region used by Stage
//! 13 traffic; normal writes stop at [`Mbuf::capacity`].
//!
//! # Examples
//!
//! ```
//! use dynomite::io::mbuf::{Mbuf, MbufPool, MbufQueue};
//!
//! let pool = MbufPool::default();
//! let mut buf = pool.get();
//! assert_eq!(buf.recv(b"hello"), 5);
//! assert_eq!(buf.len(), 5);
//!
//! let mut out = [0u8; 5];
//! assert_eq!(buf.send(&mut out), 5);
//! assert_eq!(&out, b"hello");
//!
//! let mut q = MbufQueue::new();
//! q.push_back(buf);
//! assert_eq!(q.len(), 1);
//! ```

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;

/// Default mbuf chunk size in bytes. Mirrors the reference `MBUF_SIZE`.
pub const MBUF_SIZE: usize = 16384;

/// Minimum permitted mbuf chunk size. Mirrors the reference
/// `MBUF_MIN_SIZE`.
pub const MBUF_MIN_SIZE: usize = 512;

/// Maximum permitted mbuf chunk size. Mirrors the reference
/// `MBUF_MAX_SIZE`.
pub const MBUF_MAX_SIZE: usize = 512_000;

/// Bytes reserved at the tail of every chunk for crypto padding and
/// overflow guards. Mirrors the reference `MBUF_ESIZE`.
pub const MBUF_ESIZE: usize = 16;

/// Maximum number of free chunks the pool retains before dropping
/// returned buffers on the floor.
///
/// The C engine has no hard cap on the free list because the worker
/// loop bounds the working set implicitly. The Rust port adds an
/// upper bound so a misbehaving caller cannot drive unbounded memory
/// growth. The default tracks the connection-budget order-of-magnitude
/// the reference docs cite (4096 connections * 4 chunks per direction).
pub const MBUF_POOL_MAX_FREE: usize = 16384;

/// Connection identifier carried by an mbuf to mark the conn that owns
/// it. The reactor stamps this when it hands a chunk to a connection
/// state machine; the chunk pool itself never inspects the value.
pub type OwnerId = u64;

/// A single chunk of a connection's I/O buffer chain.
///
/// An `Mbuf` exposes three regions:
///
/// * `[0, pos)` - already-consumed bytes (drained by the parser).
/// * `[pos, last)` - readable bytes (parser input or pre-flight write).
/// * `[last, end)` - writable bytes (target of [`recv`](Self::recv)).
///
/// The slice `[end, end_extra)` of length [`MBUF_ESIZE`] is reserved
/// for crypto MAC and overflow guards. It is not visible through the
/// normal write API.
pub struct Mbuf {
    buf: Box<[u8]>,
    pos: usize,
    last: usize,
    end: usize,
    flags: u32,
    owner: Option<OwnerId>,
}

/// Bit flag set when the buffer has been flipped from write to read
/// orientation. Mirrors `MBUF_FLAGS_READ_FLIP`.
pub const MBUF_FLAG_READ_FLIP: u32 = 0x0000_0001;

/// Bit flag set when the contents have just been decrypted. Mirrors
/// `MBUF_FLAGS_JUST_DECRYPTED`.
pub const MBUF_FLAG_JUST_DECRYPTED: u32 = 0x0000_0002;

impl Mbuf {
    /// Allocate a fresh chunk of the given total size. Used by the
    /// pool to fill its free list and by callers that need a one-off
    /// non-pooled buffer (the Stage 13 entropy path is a planned
    /// consumer).
    ///
    /// `chunk_size` must be in `[MBUF_MIN_SIZE, MBUF_MAX_SIZE]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::{Mbuf, MBUF_SIZE};
    /// let buf = Mbuf::with_chunk_size(MBUF_SIZE);
    /// assert_eq!(buf.chunk_size(), MBUF_SIZE);
    /// assert!(buf.is_empty());
    /// ```
    pub fn with_chunk_size(chunk_size: usize) -> Self {
        assert!(
            (MBUF_MIN_SIZE..=MBUF_MAX_SIZE).contains(&chunk_size),
            "mbuf chunk_size {chunk_size} outside [{MBUF_MIN_SIZE}, {MBUF_MAX_SIZE}]"
        );
        let buf = vec![0u8; chunk_size].into_boxed_slice();
        let end = chunk_size - MBUF_ESIZE;
        Self {
            buf,
            pos: 0,
            last: 0,
            end,
            flags: 0,
            owner: None,
        }
    }

    /// Total byte length of the chunk allocation.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::{Mbuf, MBUF_SIZE};
    /// let buf = Mbuf::with_chunk_size(MBUF_SIZE);
    /// assert_eq!(buf.chunk_size(), MBUF_SIZE);
    /// ```
    pub fn chunk_size(&self) -> usize {
        self.buf.len()
    }

    /// Number of bytes that can be written before [`is_full`](Self::is_full)
    /// trips, equal to `chunk_size - MBUF_ESIZE`. Mirrors `mbuf_data_size`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::{Mbuf, MBUF_SIZE, MBUF_ESIZE};
    /// let buf = Mbuf::with_chunk_size(MBUF_SIZE);
    /// assert_eq!(buf.data_size(), MBUF_SIZE - MBUF_ESIZE);
    /// ```
    pub fn data_size(&self) -> usize {
        self.end
    }

    /// Total byte addressable region including the trailing extra
    /// area, equal to `chunk_size`. Mirrors `mbuf_full_data_size` from
    /// the Stage 2 plan and the underlying `end_extra` boundary in the
    /// C struct.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::{Mbuf, MBUF_SIZE};
    /// let buf = Mbuf::with_chunk_size(MBUF_SIZE);
    /// assert_eq!(buf.usable_capacity(), MBUF_SIZE);
    /// ```
    pub fn usable_capacity(&self) -> usize {
        self.buf.len()
    }

    /// Bytes currently available to read from the buffer
    /// (`last - pos`). Mirrors `mbuf_length`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::Mbuf;
    /// let mut buf = Mbuf::with_chunk_size(1024);
    /// buf.recv(b"abc");
    /// assert_eq!(buf.len(), 3);
    /// ```
    pub fn len(&self) -> usize {
        self.last - self.pos
    }

    /// Maximum number of bytes the buffer can ever hold (the writable
    /// region size, equal to [`data_size`](Self::data_size)). Mirrors
    /// `mbuf_size` in the reference's higher-level usage.
    pub fn capacity(&self) -> usize {
        self.end
    }

    /// Bytes still writable into the buffer (`end - last`). Mirrors
    /// `mbuf_remaining_space`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::{Mbuf, MBUF_SIZE, MBUF_ESIZE};
    /// let buf = Mbuf::with_chunk_size(MBUF_SIZE);
    /// assert_eq!(buf.remaining(), MBUF_SIZE - MBUF_ESIZE);
    /// ```
    pub fn remaining(&self) -> usize {
        self.end - self.last
    }

    /// True when no readable bytes remain. Mirrors `mbuf_empty`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::Mbuf;
    /// let buf = Mbuf::with_chunk_size(1024);
    /// assert!(buf.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.pos == self.last
    }

    /// True when the writable region is full. Mirrors `mbuf_full`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::Mbuf;
    /// let mut buf = Mbuf::with_chunk_size(1024);
    /// while buf.remaining() > 0 {
    ///     buf.recv(&[0]);
    /// }
    /// assert!(buf.is_full());
    /// ```
    pub fn is_full(&self) -> bool {
        self.last == self.end
    }

    /// Discard all buffered bytes and rewind both cursors to the
    /// origin. Mirrors `mbuf_rewind`.
    pub fn rewind(&mut self) {
        self.pos = 0;
        self.last = 0;
    }

    /// Read the buffer's flag bits.
    pub fn flags(&self) -> u32 {
        self.flags
    }

    /// Set or clear individual flag bits.
    pub fn set_flag(&mut self, flag: u32, on: bool) {
        if on {
            self.flags |= flag;
        } else {
            self.flags &= !flag;
        }
    }

    /// Return the optional owner connection id stamped on the buffer.
    pub fn owner(&self) -> Option<OwnerId> {
        self.owner
    }

    /// Stamp or clear the owner connection id.
    pub fn set_owner(&mut self, owner: Option<OwnerId>) {
        self.owner = owner;
    }

    /// Borrow the readable region (`[pos, last)`).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::Mbuf;
    /// let mut buf = Mbuf::with_chunk_size(1024);
    /// buf.recv(b"abc");
    /// assert_eq!(buf.readable(), b"abc");
    /// ```
    pub fn readable(&self) -> &[u8] {
        &self.buf[self.pos..self.last]
    }

    /// Borrow the writable region (`[last, end)`).
    pub fn writable(&mut self) -> &mut [u8] {
        &mut self.buf[self.last..self.end]
    }

    /// Copy bytes from `src` into the writable region. Stops when the
    /// buffer fills. Returns the number of bytes copied. Mirrors
    /// `mbuf_recv` / `mbuf_copy` for the inbound direction.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::Mbuf;
    /// let mut buf = Mbuf::with_chunk_size(1024);
    /// assert_eq!(buf.recv(b"hello"), 5);
    /// assert_eq!(buf.readable(), b"hello");
    /// ```
    pub fn recv(&mut self, src: &[u8]) -> usize {
        let n = src.len().min(self.remaining());
        self.buf[self.last..self.last + n].copy_from_slice(&src[..n]);
        self.last += n;
        n
    }

    /// Drain bytes from the readable region into `dst`. Returns the
    /// number of bytes copied. Mirrors `mbuf_send` for the outbound
    /// direction.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::Mbuf;
    /// let mut buf = Mbuf::with_chunk_size(1024);
    /// buf.recv(b"hello");
    /// let mut out = [0u8; 8];
    /// let n = buf.send(&mut out);
    /// assert_eq!(n, 5);
    /// assert_eq!(&out[..n], b"hello");
    /// ```
    pub fn send(&mut self, dst: &mut [u8]) -> usize {
        let n = dst.len().min(self.len());
        dst[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        n
    }

    /// Copy `n` bytes from `src` into the writable region without
    /// bounds-checking against partial copy. Panics if `n` exceeds
    /// [`remaining`](Self::remaining). Mirrors `mbuf_copy`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::Mbuf;
    /// let mut buf = Mbuf::with_chunk_size(1024);
    /// buf.copy_from_slice(b"abc");
    /// assert_eq!(buf.readable(), b"abc");
    /// ```
    pub fn copy_from_slice(&mut self, src: &[u8]) {
        assert!(
            src.len() <= self.remaining(),
            "mbuf copy of {} bytes exceeds remaining {}",
            src.len(),
            self.remaining()
        );
        let end = self.last + src.len();
        self.buf[self.last..end].copy_from_slice(src);
        self.last = end;
    }

    /// Split the buffer at offset `at` (relative to `pos`). The bytes
    /// in `[pos+at, last)` are moved into a new mbuf taken from `pool`
    /// and returned. The original keeps `[pos, pos+at)`. Mirrors
    /// `mbuf_split` (without the precopy callback - callers that
    /// previously injected a header into the new buffer can do so on
    /// the returned `Mbuf` before chaining it).
    ///
    /// Returns `None` if `at` is greater than [`len`](Self::len) or if
    /// the moved tail would not fit in a fresh pool buffer.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::MbufPool;
    /// let pool = MbufPool::default();
    /// let mut head = pool.get();
    /// head.recv(b"hello world");
    /// let tail = head.split_off(5, &pool).unwrap();
    /// assert_eq!(head.readable(), b"hello");
    /// assert_eq!(tail.readable(), b" world");
    /// ```
    pub fn split_off(&mut self, at: usize, pool: &MbufPool) -> Option<Mbuf> {
        if at > self.len() {
            return None;
        }
        let mut tail = pool.get();
        let cut = self.pos + at;
        let moved = self.last - cut;
        if moved > tail.remaining() {
            pool.put(tail);
            return None;
        }
        tail.copy_from_slice(&self.buf[cut..self.last]);
        self.last = cut;
        Some(tail)
    }

    /// Append the readable region of `other` into this buffer.
    /// Returns the number of bytes appended (which may be less than
    /// `other.len()` if this buffer fills first).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::Mbuf;
    /// let mut a = Mbuf::with_chunk_size(1024);
    /// let mut b = Mbuf::with_chunk_size(1024);
    /// a.recv(b"foo");
    /// b.recv(b"bar");
    /// a.append(&b);
    /// assert_eq!(a.readable(), b"foobar");
    /// ```
    pub fn append(&mut self, other: &Mbuf) -> usize {
        self.recv(other.readable())
    }

    /// Mark `n` bytes of the readable region as consumed by advancing
    /// `pos`. Useful when downstream code wrote directly into
    /// [`writable`](Self::writable). Panics if `n` exceeds
    /// [`len`](Self::len).
    pub fn advance_pos(&mut self, n: usize) {
        assert!(
            n <= self.len(),
            "advance_pos {n} exceeds len {}",
            self.len()
        );
        self.pos += n;
    }

    /// Mark `n` bytes of the writable region as filled by advancing
    /// `last`. Used after writing into [`writable`](Self::writable)
    /// directly (for example through an `AsyncRead` call). Panics if
    /// `n` exceeds [`remaining`](Self::remaining).
    pub fn advance_last(&mut self, n: usize) {
        assert!(
            n <= self.remaining(),
            "advance_last {n} exceeds remaining {}",
            self.remaining()
        );
        self.last += n;
    }
}

impl std::fmt::Debug for Mbuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mbuf")
            .field("chunk_size", &self.buf.len())
            .field("pos", &self.pos)
            .field("last", &self.last)
            .field("end", &self.end)
            .field("flags", &self.flags)
            .field("owner", &self.owner)
            .finish()
    }
}

/// Tail-queue of mbufs. Mirrors the C `struct mhdr` head and the
/// `STAILQ_*` macros that operated on it.
///
/// The container is a [`VecDeque`]; insertion at either end is O(1).
///
/// # Examples
///
/// ```
/// use dynomite::io::mbuf::{MbufPool, MbufQueue};
/// let pool = MbufPool::default();
/// let mut q = MbufQueue::new();
/// q.push_back(pool.get());
/// q.push_back(pool.get());
/// assert_eq!(q.len(), 2);
/// let _ = q.pop_front();
/// assert_eq!(q.len(), 1);
/// ```
#[derive(Debug, Default)]
pub struct MbufQueue {
    inner: VecDeque<Mbuf>,
}

impl MbufQueue {
    /// Create an empty queue.
    pub fn new() -> Self {
        Self {
            inner: VecDeque::new(),
        }
    }

    /// Number of buffers currently chained.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// True when the queue holds no buffers.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Append `mbuf` at the tail. Mirrors `mbuf_insert`.
    pub fn push_back(&mut self, mbuf: Mbuf) {
        self.inner.push_back(mbuf);
    }

    /// Insert `mbuf` at the head. Mirrors `mbuf_insert_head`.
    pub fn push_front(&mut self, mbuf: Mbuf) {
        self.inner.push_front(mbuf);
    }

    /// Remove and return the head buffer. Mirrors `mbuf_remove`
    /// applied to the head; the C API allowed removing from any
    /// position, but every reference call site removed the head.
    pub fn pop_front(&mut self) -> Option<Mbuf> {
        self.inner.pop_front()
    }

    /// Remove and return the tail buffer.
    pub fn pop_back(&mut self) -> Option<Mbuf> {
        self.inner.pop_back()
    }

    /// Borrow the tail buffer mutably without removing it. Used by
    /// the `mbuf_split` consumers that want to operate on the latest
    /// chunk.
    pub fn back_mut(&mut self) -> Option<&mut Mbuf> {
        self.inner.back_mut()
    }

    /// Borrow the head buffer mutably without removing it.
    pub fn front_mut(&mut self) -> Option<&mut Mbuf> {
        self.inner.front_mut()
    }

    /// Iterate the queued buffers in head-to-tail order.
    pub fn iter(&self) -> std::collections::vec_deque::Iter<'_, Mbuf> {
        self.inner.iter()
    }
}

impl<'a> IntoIterator for &'a MbufQueue {
    type Item = &'a Mbuf;
    type IntoIter = std::collections::vec_deque::Iter<'a, Mbuf>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl MbufQueue {
    /// Total readable bytes across the chain.
    pub fn total_len(&self) -> usize {
        self.inner.iter().map(Mbuf::len).sum()
    }

    /// Drain the queue into the pool, recycling every chunk.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::{MbufPool, MbufQueue};
    /// let pool = MbufPool::default();
    /// let mut q = MbufQueue::new();
    /// q.push_back(pool.get());
    /// q.push_back(pool.get());
    /// q.recycle(&pool);
    /// assert!(q.is_empty());
    /// ```
    pub fn recycle(&mut self, pool: &MbufPool) {
        while let Some(buf) = self.inner.pop_front() {
            pool.put(buf);
        }
    }
}

/// Free-list backed mbuf pool.
///
/// The pool keeps a parking-lot mutex around a stash of recyclable
/// chunk allocations. New chunks are taken from the stash if available
/// and freshly allocated otherwise. [`MbufPool::put`] returns chunks
/// to the stash up to [`MBUF_POOL_MAX_FREE`]; chunks beyond that cap
/// are dropped.
///
/// The pool tracks total live and free counts for diagnostics, mirroring
/// the C `mbuf_alloc_get_count` / `mbuf_free_queue_size` accessors.
#[derive(Clone)]
pub struct MbufPool {
    inner: Arc<MbufPoolInner>,
}

struct MbufPoolInner {
    chunk_size: usize,
    max_free: usize,
    state: Mutex<MbufPoolState>,
}

struct MbufPoolState {
    free: Vec<Box<[u8]>>,
    total_allocated: u64,
}

impl Default for MbufPool {
    fn default() -> Self {
        Self::new(MBUF_SIZE, MBUF_POOL_MAX_FREE)
    }
}

impl MbufPool {
    /// Construct a new pool with `chunk_size` byte chunks and a free
    /// list capped at `max_free`. Mirrors `mbuf_init` plus the Rust-
    /// only free-list bound.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::{MbufPool, MBUF_SIZE, MBUF_POOL_MAX_FREE};
    /// let pool = MbufPool::new(MBUF_SIZE, MBUF_POOL_MAX_FREE);
    /// let buf = pool.get();
    /// assert_eq!(buf.chunk_size(), MBUF_SIZE);
    /// pool.put(buf);
    /// ```
    pub fn new(chunk_size: usize, max_free: usize) -> Self {
        assert!(
            (MBUF_MIN_SIZE..=MBUF_MAX_SIZE).contains(&chunk_size),
            "mbuf chunk_size {chunk_size} outside [{MBUF_MIN_SIZE}, {MBUF_MAX_SIZE}]"
        );
        Self {
            inner: Arc::new(MbufPoolInner {
                chunk_size,
                max_free,
                state: Mutex::new(MbufPoolState {
                    free: Vec::new(),
                    total_allocated: 0,
                }),
            }),
        }
    }

    /// Configured chunk size, in bytes.
    pub fn chunk_size(&self) -> usize {
        self.inner.chunk_size
    }

    /// Maximum number of chunks the free list retains.
    pub fn max_free(&self) -> usize {
        self.inner.max_free
    }

    /// Take a fresh or recycled chunk from the pool. Mirrors
    /// `mbuf_get`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::MbufPool;
    /// let pool = MbufPool::default();
    /// let buf = pool.get();
    /// assert!(buf.is_empty());
    /// ```
    pub fn get(&self) -> Mbuf {
        let buf = self.alloc_buffer();
        let chunk_size = buf.len();
        let end = chunk_size - MBUF_ESIZE;
        Mbuf {
            buf,
            pos: 0,
            last: 0,
            end,
            flags: 0,
            owner: None,
        }
    }

    /// Return a chunk to the free list. Resets cursors and flags
    /// before storing. Mirrors `mbuf_put`. Chunks past
    /// [`max_free`](Self::max_free) are dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::MbufPool;
    /// let pool = MbufPool::default();
    /// let buf = pool.get();
    /// pool.put(buf);
    /// assert_eq!(pool.free_count(), 1);
    /// ```
    pub fn put(&self, mut mbuf: Mbuf) {
        if mbuf.buf.len() != self.inner.chunk_size {
            // Off-size buffers (e.g. from [`Mbuf::with_chunk_size`])
            // are simply dropped rather than poisoning the free list.
            return;
        }
        mbuf.rewind();
        mbuf.flags = 0;
        mbuf.owner = None;
        let mut state = self.inner.state.lock();
        if state.free.len() < self.inner.max_free {
            state.free.push(mbuf.buf);
        }
    }

    /// Number of chunks currently sitting in the free list. Mirrors
    /// `mbuf_free_queue_size`.
    pub fn free_count(&self) -> usize {
        self.inner.state.lock().free.len()
    }

    /// Lifetime count of fresh allocations performed by the pool.
    /// Mirrors `mbuf_alloc_get_count`. Useful for tests asserting
    /// that recycling avoids the allocator path.
    pub fn total_allocated(&self) -> u64 {
        self.inner.state.lock().total_allocated
    }

    fn alloc_buffer(&self) -> Box<[u8]> {
        let mut state = self.inner.state.lock();
        if let Some(buf) = state.free.pop() {
            return buf;
        }
        state.total_allocated += 1;
        drop(state);
        vec![0u8; self.inner.chunk_size].into_boxed_slice()
    }
}

impl std::fmt::Debug for MbufPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.inner.state.lock();
        f.debug_struct("MbufPool")
            .field("chunk_size", &self.inner.chunk_size)
            .field("max_free", &self.inner.max_free)
            .field("free_count", &state.free.len())
            .field("total_allocated", &state.total_allocated)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mbuf_recv_send_round_trip() {
        let mut buf = Mbuf::with_chunk_size(1024);
        assert_eq!(buf.recv(b"abc"), 3);
        let mut out = [0u8; 8];
        assert_eq!(buf.send(&mut out), 3);
        assert_eq!(&out[..3], b"abc");
        assert!(buf.is_empty());
    }

    #[test]
    fn mbuf_recv_truncates_to_remaining() {
        let mut buf = Mbuf::with_chunk_size(MBUF_MIN_SIZE);
        let payload = vec![7u8; MBUF_MIN_SIZE * 2];
        let n = buf.recv(&payload);
        assert_eq!(n, MBUF_MIN_SIZE - MBUF_ESIZE);
        assert!(buf.is_full());
    }

    #[test]
    fn mbuf_split_then_append_reconstructs() {
        let pool = MbufPool::default();
        let mut head = pool.get();
        head.recv(b"hello world");
        let tail = head.split_off(5, &pool).unwrap();
        assert_eq!(head.readable(), b"hello");
        assert_eq!(tail.readable(), b" world");
        head.append(&tail);
        assert_eq!(head.readable(), b"hello world");
    }

    #[test]
    fn mbuf_split_off_out_of_range_returns_none() {
        let pool = MbufPool::default();
        let mut head = pool.get();
        head.recv(b"abc");
        assert!(head.split_off(99, &pool).is_none());
    }

    #[test]
    fn mbuf_pool_recycles_buffers_without_reallocation() {
        let pool = MbufPool::default();
        let n = 4;
        let mut taken = Vec::new();
        for _ in 0..n {
            taken.push(pool.get());
        }
        assert_eq!(pool.total_allocated(), n as u64);
        for buf in taken.drain(..) {
            pool.put(buf);
        }
        assert_eq!(pool.free_count(), n);
        for _ in 0..n {
            taken.push(pool.get());
        }
        // Reused; allocation count is unchanged.
        assert_eq!(pool.total_allocated(), n as u64);
    }

    #[test]
    fn mbuf_pool_drops_chunks_beyond_cap() {
        let cap = 4;
        let pool = MbufPool::new(MBUF_SIZE, cap);
        let n = cap * 2;
        let mut taken = Vec::with_capacity(n);
        for _ in 0..n {
            taken.push(pool.get());
        }
        for buf in taken.drain(..) {
            pool.put(buf);
        }
        // The pool refuses to grow past `max_free`; the surplus
        // buffers are dropped on return rather than poisoning the
        // free list.
        assert_eq!(pool.free_count(), cap);
    }

    #[test]
    fn mbuf_queue_push_pop_fifo() {
        let pool = MbufPool::default();
        let mut q = MbufQueue::new();
        for i in 0..3u8 {
            let mut buf = pool.get();
            buf.recv(&[i]);
            q.push_back(buf);
        }
        for i in 0..3u8 {
            let buf = q.pop_front().unwrap();
            assert_eq!(buf.readable(), &[i]);
        }
        assert!(q.is_empty());
    }

    #[test]
    fn mbuf_flags_round_trip() {
        let mut buf = Mbuf::with_chunk_size(1024);
        buf.set_flag(MBUF_FLAG_READ_FLIP, true);
        assert_eq!(buf.flags() & MBUF_FLAG_READ_FLIP, MBUF_FLAG_READ_FLIP);
        buf.set_flag(MBUF_FLAG_READ_FLIP, false);
        assert_eq!(buf.flags(), 0);
    }

    #[test]
    fn mbuf_owner_round_trip() {
        let mut buf = Mbuf::with_chunk_size(1024);
        assert!(buf.owner().is_none());
        buf.set_owner(Some(42));
        assert_eq!(buf.owner(), Some(42));
    }

    #[test]
    #[should_panic(expected = "outside")]
    fn mbuf_too_small_panics() {
        let _ = Mbuf::with_chunk_size(MBUF_MIN_SIZE - 1);
    }

    #[test]
    #[should_panic(expected = "outside")]
    fn mbuf_too_large_panics() {
        let _ = Mbuf::with_chunk_size(MBUF_MAX_SIZE + 1);
    }

    #[test]
    fn mbuf_put_drops_offsize_chunks() {
        let pool = MbufPool::default();
        // Use Mbuf::with_chunk_size to construct a buffer of a
        // different size and confirm the pool refuses to admit it.
        let stray = Mbuf::with_chunk_size(MBUF_MIN_SIZE);
        pool.put(stray);
        assert_eq!(pool.free_count(), 0);
    }
}
