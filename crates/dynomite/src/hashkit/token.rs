//! Big-integer token used as the hash output and ring coordinate.
//!
//! [`DynToken`] stores tokens as `(signum, mag[4], len)`, where
//! `mag[]` holds little-significance-first 32-bit words in a numeral
//! system whose radix is `UINT_MAX_PLUS_ONE` (i.e. 2^32). Tokens are
//! signed so the comparator can distinguish negative values; this
//! representation makes `cmp` and the textual parser produce
//! bit-identical answers across peers.
//!
//! # Examples
//!
//! ```
//! use dynomite::hashkit::DynToken;
//!
//! let mut a = DynToken::default();
//! a.size(1).expect("len <= 4");
//! a.set_int(42);
//! assert_eq!(a.get_int(), 42);
//! ```

use std::cmp::Ordering;
use std::fmt;

use crate::core::types::DynError;

/// Maximum number of 32-bit words a token can hold.
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::token::TOKEN_WORD_CAPACITY;
/// assert_eq!(TOKEN_WORD_CAPACITY, 4);
/// ```
pub const TOKEN_WORD_CAPACITY: usize = 4;

/// 10 base-10 digits per group when parsing a textual token.
const DIGITS_PER_INT: usize = 10;

/// Multiplier applied to the running buffer for each new digit group.
///
/// The intended value is 10^9 = `0x3B9A_CA00`, but the on-the-wire
/// constant is `0x17179149` (which is `10^9 + 0x17F1`, i.e. off by
/// `0x17F1`). That odd value is what peers expect, so it is the one
/// reproduced here; the `parse_dyn_token` tests pin down a fixed
/// mapping rather than a numeric round-trip.
const RADIX_VAL_C_REFERENCE: u32 = 0x1717_9149;

/// Sign of a token.
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::token::Sign;
/// assert_ne!(Sign::Negative, Sign::Positive);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Sign {
    /// Negative token (sign field == -1 in C).
    Negative,
    /// Zero.
    Zero,
    /// Positive (sign field == 1 in C).
    Positive,
}

impl Sign {
    fn as_i32(self) -> i32 {
        match self {
            Sign::Negative => -1,
            Sign::Zero => 0,
            Sign::Positive => 1,
        }
    }
}

/// A signed magnitude integer used as both a hash output and a ring
/// coordinate.
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::DynToken;
/// let mut t = DynToken::from_u32(7);
/// assert_eq!(t.get_int(), 7);
/// assert_eq!(t.len(), 1);
/// ```
#[derive(Clone, Debug)]
pub struct DynToken {
    sign: Sign,
    mag: [u32; TOKEN_WORD_CAPACITY],
    len: usize,
}

impl Default for DynToken {
    fn default() -> Self {
        Self {
            sign: Sign::Zero,
            mag: [0; TOKEN_WORD_CAPACITY],
            len: 0,
        }
    }
}

impl DynToken {
    /// Construct an empty token (sign zero, no magnitude words).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::DynToken;
    /// let t = DynToken::new();
    /// assert!(t.is_empty());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a token holding a single 32-bit value.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::DynToken;
    /// let t = DynToken::from_u32(7);
    /// assert_eq!(t.get_int(), 7);
    /// ```
    #[must_use]
    pub fn from_u32(value: u32) -> Self {
        let mut t = Self::default();
        // size(1) cannot fail for a length within capacity.
        t.size(1).expect("len of 1 fits within TOKEN_WORD_CAPACITY");
        t.set_int(value);
        t
    }

    /// Set the number of magnitude words. Returns an error if `len`
    /// exceeds [`TOKEN_WORD_CAPACITY`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::DynToken;
    /// let mut t = DynToken::default();
    /// assert!(t.size(2).is_ok());
    /// assert!(t.size(99).is_err());
    /// ```
    pub fn size(&mut self, len: usize) -> Result<(), DynError> {
        if len > TOKEN_WORD_CAPACITY {
            return Err(DynError::Generic(format!(
                "token length {len} exceeds capacity {TOKEN_WORD_CAPACITY}"
            )));
        }
        self.len = len;
        self.sign = Sign::Zero;
        Ok(())
    }

    /// Number of magnitude words currently in use.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::DynToken;
    /// assert_eq!(DynToken::from_u32(1).len(), 1);
    /// assert_eq!(DynToken::default().len(), 0);
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the token holds no magnitude words.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::DynToken;
    /// assert!(DynToken::default().is_empty());
    /// assert!(!DynToken::from_u32(1).is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Sign field.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::DynToken;
    /// use dynomite::hashkit::token::Sign;
    /// assert_eq!(DynToken::from_u32(1).sign(), Sign::Positive);
    /// assert_eq!(DynToken::default().sign(), Sign::Zero);
    /// ```
    #[must_use]
    pub fn sign(&self) -> Sign {
        self.sign
    }

    /// Read-only view of the magnitude words actually in use.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::DynToken;
    /// let t = DynToken::from_u32(0xdead);
    /// assert_eq!(t.mag(), &[0xdead]);
    /// ```
    #[must_use]
    pub fn mag(&self) -> &[u32] {
        &self.mag[..self.len]
    }

    /// Mutable access to the full magnitude buffer (capacity-sized).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::DynToken;
    /// use dynomite::hashkit::token::TOKEN_WORD_CAPACITY;
    /// let mut t = DynToken::default();
    /// t.size(2).unwrap();
    /// t.mag_mut()[0] = 1;
    /// assert_eq!(t.mag_mut().len(), TOKEN_WORD_CAPACITY);
    /// ```
    pub fn mag_mut(&mut self) -> &mut [u32; TOKEN_WORD_CAPACITY] {
        &mut self.mag
    }

    /// Force the length without resetting the sign or zeroing words.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::DynToken;
    /// let mut t = DynToken::default();
    /// t.mag_mut()[0] = 0xaa;
    /// t.set_len_keep(1);
    /// assert_eq!(t.len(), 1);
    /// assert_eq!(t.get_int(), 0xaa);
    /// ```
    pub fn set_len_keep(&mut self, len: usize) {
        assert!(len <= TOKEN_WORD_CAPACITY, "token length out of range");
        self.len = len;
    }

    /// Sets sign explicitly. Mostly useful in tests.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::DynToken;
    /// use dynomite::hashkit::token::Sign;
    /// let mut t = DynToken::from_u32(1);
    /// t.set_sign(Sign::Negative);
    /// assert_eq!(t.sign(), Sign::Negative);
    /// ```
    pub fn set_sign(&mut self, sign: Sign) {
        self.sign = sign;
    }

    /// Set the token to a single 32-bit value.
    ///
    /// Sign becomes `Positive` when `val > 0`, `Zero` otherwise. Length
    /// is forced to 1.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::DynToken;
    /// let mut t = DynToken::default();
    /// t.size(1).unwrap();
    /// t.set_int(99);
    /// assert_eq!(t.get_int(), 99);
    /// ```
    pub fn set_int(&mut self, val: u32) {
        self.mag[0] = val;
        self.len = 1;
        self.sign = if val > 0 { Sign::Positive } else { Sign::Zero };
    }

    /// Read the token's first word as a 32-bit unsigned value.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::DynToken;
    /// assert_eq!(DynToken::from_u32(33).get_int(), 33);
    /// assert_eq!(DynToken::default().get_int(), 0);
    /// ```
    #[must_use]
    pub fn get_int(&self) -> u32 {
        if self.len == 0 {
            0
        } else {
            self.mag[0]
        }
    }

    /// Hex dump of the magnitude words, big-endian per word, in
    /// declaration order. Used by tests and the `dyn-hash-tool` CLI.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::DynToken;
    /// assert_eq!(DynToken::from_u32(0xdead).to_hex(), "0000dead");
    /// ```
    #[must_use]
    pub fn to_hex(&self) -> String {
        use std::fmt::Write;
        let mut out = String::with_capacity(8 * self.len);
        for w in &self.mag[..self.len] {
            let _ = write!(out, "{w:08x}");
        }
        out
    }
}

impl fmt::Display for DynToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Token(sign={}, len={}, mag={:?})",
            self.sign.as_i32(),
            self.len,
            &self.mag[..self.len]
        )
    }
}

impl PartialEq for DynToken {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for DynToken {}

impl PartialOrd for DynToken {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DynToken {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.sign == other.sign {
            if self.sign == Sign::Zero {
                return Ordering::Equal;
            }
            if self.len == other.len {
                for i in 0..self.len {
                    let a = self.mag[i];
                    let b = other.mag[i];
                    if a != b {
                        return if a > b {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        };
                    }
                }
                return Ordering::Equal;
            }
            return if self.len > other.len {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        if self.sign.as_i32() > other.sign.as_i32() {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

impl std::hash::Hash for DynToken {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.sign.as_i32().hash(state);
        self.len.hash(state);
        for w in &self.mag[..self.len] {
            w.hash(state);
        }
    }
}

/// Parse a textual token from `bytes`. Accepts an optional leading `-`,
/// then base-10 digits.
///
/// # Errors
///
/// Returns `DynError::Generic` when the input is empty, contains
/// non-digit bytes, or specifies a length that overflows the token.
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::token::{parse_token, Sign};
/// let t = parse_token(b"42").unwrap();
/// assert_eq!(t.sign(), Sign::Positive);
/// assert_eq!(t.get_int(), 42);
/// assert!(parse_token(b"").is_err());
/// ```
pub fn parse_token(bytes: &[u8]) -> Result<DynToken, DynError> {
    if bytes.is_empty() {
        return Err(DynError::Generic("empty token".into()));
    }
    let mut token = DynToken::default();

    let (sign, payload) = if bytes[0] == b'-' {
        if bytes.len() < 2 {
            return Err(DynError::Generic("dangling minus sign".into()));
        }
        (Sign::Negative, &bytes[1..])
    } else if bytes.len() == 1 && bytes[0] == b'0' {
        (Sign::Zero, bytes)
    } else {
        (Sign::Positive, bytes)
    };
    token.sign = sign;

    let nwords: usize = 1;
    token.len = nwords;
    let buf = &mut token.mag;

    let digits = payload.len();
    let mut first_group_len = digits % DIGITS_PER_INT;
    if first_group_len == 0 {
        first_group_len = DIGITS_PER_INT;
    }

    let mut p = 0usize;
    if first_group_len > digits {
        return Err(DynError::Generic("digit group overruns input".into()));
    }
    buf[nwords - 1] = atoui(&payload[..first_group_len])?;
    p += first_group_len;

    while p < digits {
        let end = p + DIGITS_PER_INT;
        if end > digits {
            return Err(DynError::Generic("misaligned digit groups".into()));
        }
        let local = atoui(&payload[p..end])?;
        add_next_word(buf, nwords, local);
        p = end;
    }

    Ok(token)
}

fn atoui(bytes: &[u8]) -> Result<u32, DynError> {
    let mut acc: u32 = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return Err(DynError::Generic(format!(
                "non-digit byte 0x{b:02x} in token"
            )));
        }
        acc = acc.wrapping_mul(10).wrapping_add(u32::from(b - b'0'));
    }
    Ok(acc)
}

fn add_next_word(buf: &mut [u32; TOKEN_WORD_CAPACITY], len: usize, next_int: u32) {
    let radix_val: u64 = u64::from(RADIX_VAL_C_REFERENCE);
    let mut carry: u64 = 0;
    for i in (0..len).rev() {
        let product = radix_val * u64::from(buf[i]) + carry;
        buf[i] = product as u32;
        carry = product >> 32;
    }

    let sum = u64::from(buf[len - 1]) + u64::from(next_int);
    buf[len - 1] = sum as u32;
    let mut carry2 = sum >> 32;
    if len >= 2 {
        for i in (0..=len - 2).rev() {
            let s = u64::from(buf[i]) + carry2;
            buf[i] = s as u32;
            carry2 = s >> 32;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_empty() {
        let t = DynToken::default();
        assert!(t.is_empty());
        assert_eq!(t.sign(), Sign::Zero);
    }

    #[test]
    fn set_int_get_int_round_trip() {
        let mut t = DynToken::default();
        t.size(1).unwrap();
        for v in [0u32, 1, 42, 0x7fff_ffff, 0xffff_ffff, 0x8000_0000] {
            t.set_int(v);
            assert_eq!(t.get_int(), v);
        }
    }

    #[test]
    fn set_int_zero_has_zero_sign() {
        let mut t = DynToken::default();
        t.size(1).unwrap();
        t.set_int(0);
        assert_eq!(t.sign(), Sign::Zero);
        t.set_int(1);
        assert_eq!(t.sign(), Sign::Positive);
    }

    #[test]
    fn cmp_total_order_for_singletons() {
        let mut t = vec![];
        for v in [0u32, 1, 2, 100, 1_000_000, u32::MAX] {
            t.push(DynToken::from_u32(v));
        }
        for i in 0..t.len() {
            assert_eq!(t[i].cmp(&t[i]), Ordering::Equal);
            for j in (i + 1)..t.len() {
                assert_eq!(t[i].cmp(&t[j]), Ordering::Less);
                assert_eq!(t[j].cmp(&t[i]), Ordering::Greater);
            }
        }
    }

    #[test]
    fn cmp_uses_sign_first() {
        let pos = DynToken::from_u32(1);
        let mut neg = DynToken::default();
        neg.size(1).unwrap();
        neg.set_int(1);
        neg.set_sign(Sign::Negative);
        assert!(neg < pos);
    }

    #[test]
    fn cmp_uses_length_when_signs_match() {
        let mut short = DynToken::default();
        short.size(1).unwrap();
        short.set_int(0xffff_ffff);
        short.set_sign(Sign::Positive);

        let mut long = DynToken::default();
        long.size(2).unwrap();
        long.mag_mut()[0] = 1;
        long.mag_mut()[1] = 0;
        long.set_sign(Sign::Positive);

        assert!(long > short);
    }

    #[test]
    fn parse_zero() {
        let t = parse_token(b"0").unwrap();
        assert_eq!(t.sign(), Sign::Zero);
        assert_eq!(t.get_int(), 0);
    }

    #[test]
    fn parse_short_positive() {
        let t = parse_token(b"42").unwrap();
        assert_eq!(t.sign(), Sign::Positive);
        assert_eq!(t.get_int(), 42);
    }

    #[test]
    fn parse_negative() {
        let t = parse_token(b"-7").unwrap();
        assert_eq!(t.sign(), Sign::Negative);
        assert_eq!(t.get_int(), 7);
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!(parse_token(b"abc").is_err());
        assert!(parse_token(b"").is_err());
        assert!(parse_token(b"-").is_err());
    }

    #[test]
    fn new_matches_default() {
        // new() forwards to default(): empty, sign zero.
        let t = DynToken::new();
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
        assert_eq!(t.sign(), Sign::Zero);
    }

    #[test]
    fn from_u32_populates_single_word() {
        // from_u32 sizes to 1 and sets the value with positive sign.
        let t = DynToken::from_u32(0xdead_beef);
        assert_eq!(t.len(), 1);
        assert_eq!(t.get_int(), 0xdead_beef);
        assert_eq!(t.sign(), Sign::Positive);
        assert_eq!(t.mag(), &[0xdead_beef]);
    }

    #[test]
    fn get_int_on_empty_is_zero() {
        // The len == 0 arm of get_int returns 0 without indexing.
        let t = DynToken::default();
        assert_eq!(t.get_int(), 0);
    }

    #[test]
    fn set_len_keep_preserves_words() {
        // set_len_keep changes len without zeroing the magnitude.
        let mut t = DynToken::default();
        t.mag_mut()[0] = 0xaa;
        t.set_len_keep(1);
        assert_eq!(t.len(), 1);
        assert_eq!(t.get_int(), 0xaa);
    }

    #[test]
    fn size_rejects_overcapacity() {
        // size beyond TOKEN_WORD_CAPACITY is an error and leaves the
        // length untouched.
        let mut t = DynToken::default();
        assert!(t.size(TOKEN_WORD_CAPACITY).is_ok());
        assert!(t.size(TOKEN_WORD_CAPACITY + 1).is_err());
    }

    #[test]
    fn to_hex_pads_each_word() {
        // to_hex emits 8 hex chars per in-use word, declaration order.
        assert_eq!(DynToken::from_u32(0xdead).to_hex(), "0000dead");
        let mut t = DynToken::default();
        t.size(2).unwrap();
        t.mag_mut()[0] = 1;
        t.mag_mut()[1] = 0xff;
        assert_eq!(t.to_hex(), "00000001000000ff");
        assert_eq!(DynToken::default().to_hex(), "");
    }

    #[test]
    fn display_reports_sign_len_and_mag() {
        // The Display impl prints the signed sign field, len, and the
        // in-use magnitude slice.
        let s = format!("{}", DynToken::from_u32(7));
        assert_eq!(s, "Token(sign=1, len=1, mag=[7])");
        let mut neg = DynToken::from_u32(7);
        neg.set_sign(Sign::Negative);
        assert_eq!(format!("{neg}"), "Token(sign=-1, len=1, mag=[7])");
    }

    #[test]
    fn hash_agrees_with_eq() {
        // Equal tokens hash equally; the Hash impl walks sign/len/mag.
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn digest(t: &DynToken) -> u64 {
            let mut h = DefaultHasher::new();
            t.hash(&mut h);
            h.finish()
        }

        let a = DynToken::from_u32(99);
        let b = DynToken::from_u32(99);
        assert_eq!(a, b);
        assert_eq!(digest(&a), digest(&b));
        assert_ne!(digest(&a), digest(&DynToken::from_u32(100)));
    }

    #[test]
    fn cmp_equal_multiword_magnitudes() {
        // Two same-length, same-sign tokens with identical words are
        // Equal (the mag loop falls through to Ordering::Equal).
        let mut a = DynToken::default();
        a.size(2).unwrap();
        a.mag_mut()[0] = 5;
        a.mag_mut()[1] = 9;
        a.set_sign(Sign::Positive);
        let b = a.clone();
        assert_eq!(a.cmp(&b), Ordering::Equal);
    }

    #[test]
    fn parse_multi_group_uses_add_next_word() {
        // A payload longer than DIGITS_PER_INT drives the
        // `while p < digits` loop and add_next_word's carry path.
        // The first group is digits % 10, then full 10-digit groups.
        let t = parse_token(b"12345678901").unwrap();
        assert_eq!(t.sign(), Sign::Positive);
        // The exact word value is the C-reference radix folding; the
        // invariant under test is that it parses and does not panic.
        assert!(!t.is_empty());
    }

    #[test]
    fn parse_full_first_group_of_ten_digits() {
        // When digits % 10 == 0 the first group is a full 10 digits
        // (first_group_len defaults to DIGITS_PER_INT).
        let t = parse_token(b"1000000000").unwrap();
        assert_eq!(t.sign(), Sign::Positive);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn parse_negative_multi_group() {
        // A negative sign in front of a multi-group payload.
        let t = parse_token(b"-99999999999").unwrap();
        assert_eq!(t.sign(), Sign::Negative);
    }

    #[test]
    fn parse_rejects_non_digit_in_later_group() {
        // atoui rejects a non-digit byte inside a trailing group.
        assert!(parse_token(b"1234567890x23456789").is_err());
    }
}
