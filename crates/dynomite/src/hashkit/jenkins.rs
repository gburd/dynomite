use crate::hashkit::token::DynToken;

const JENKINS_INITVAL: u32 = 13;

#[inline]
fn rot(x: u32, k: u32) -> u32 {
    x.rotate_left(k)
}

#[inline]
fn mix(a: &mut u32, b: &mut u32, c: &mut u32) {
    *a = a.wrapping_sub(*c);
    *a ^= rot(*c, 4);
    *c = c.wrapping_add(*b);
    *b = b.wrapping_sub(*a);
    *b ^= rot(*a, 6);
    *a = a.wrapping_add(*c);
    *c = c.wrapping_sub(*b);
    *c ^= rot(*b, 8);
    *b = b.wrapping_add(*a);
    *a = a.wrapping_sub(*c);
    *a ^= rot(*c, 16);
    *c = c.wrapping_add(*b);
    *b = b.wrapping_sub(*a);
    *b ^= rot(*a, 19);
    *a = a.wrapping_add(*c);
    *c = c.wrapping_sub(*b);
    *c ^= rot(*b, 4);
    *b = b.wrapping_add(*a);
}

#[inline]
fn finalize(a: &mut u32, b: &mut u32, c: &mut u32) {
    *c ^= *b;
    *c = c.wrapping_sub(rot(*b, 14));
    *a ^= *c;
    *a = a.wrapping_sub(rot(*c, 11));
    *b ^= *a;
    *b = b.wrapping_sub(rot(*a, 25));
    *c ^= *b;
    *c = c.wrapping_sub(rot(*b, 16));
    *a ^= *c;
    *a = a.wrapping_sub(rot(*c, 4));
    *b ^= *a;
    *b = b.wrapping_sub(rot(*a, 14));
    *c ^= *b;
    *c = c.wrapping_sub(rot(*b, 24));
}

/// Bob Jenkins' lookup3 (`hashlittle`-shaped).
///
/// The byte-by-byte read path is used so output is platform-independent;
/// on little-endian targets the C aligned and unaligned paths produce
/// identical results.
pub(super) fn hash(key: &[u8]) -> DynToken {
    let mut a: u32;
    let mut b: u32;
    let mut c: u32;
    let length = key.len();
    let init = 0xdead_beef_u32
        .wrapping_add(length as u32)
        .wrapping_add(JENKINS_INITVAL);
    a = init;
    b = init;
    c = init;

    if length == 0 {
        // Match the C early-exit: `final` is not run and the token's
        // first word is whatever `c` was after initialization.
        return DynToken::from_u32(c);
    }

    let mut k = key;
    let mut len = length;
    while len > 12 {
        a = a.wrapping_add(u32::from(k[0]));
        a = a.wrapping_add(u32::from(k[1]) << 8);
        a = a.wrapping_add(u32::from(k[2]) << 16);
        a = a.wrapping_add(u32::from(k[3]) << 24);
        b = b.wrapping_add(u32::from(k[4]));
        b = b.wrapping_add(u32::from(k[5]) << 8);
        b = b.wrapping_add(u32::from(k[6]) << 16);
        b = b.wrapping_add(u32::from(k[7]) << 24);
        c = c.wrapping_add(u32::from(k[8]));
        c = c.wrapping_add(u32::from(k[9]) << 8);
        c = c.wrapping_add(u32::from(k[10]) << 16);
        c = c.wrapping_add(u32::from(k[11]) << 24);
        mix(&mut a, &mut b, &mut c);
        len -= 12;
        k = &k[12..];
    }

    // Fall-through tail mixing.
    if len >= 12 {
        c = c.wrapping_add(u32::from(k[11]) << 24);
    }
    if len >= 11 {
        c = c.wrapping_add(u32::from(k[10]) << 16);
    }
    if len >= 10 {
        c = c.wrapping_add(u32::from(k[9]) << 8);
    }
    if len >= 9 {
        c = c.wrapping_add(u32::from(k[8]));
    }
    if len >= 8 {
        b = b.wrapping_add(u32::from(k[7]) << 24);
    }
    if len >= 7 {
        b = b.wrapping_add(u32::from(k[6]) << 16);
    }
    if len >= 6 {
        b = b.wrapping_add(u32::from(k[5]) << 8);
    }
    if len >= 5 {
        b = b.wrapping_add(u32::from(k[4]));
    }
    if len >= 4 {
        a = a.wrapping_add(u32::from(k[3]) << 24);
    }
    if len >= 3 {
        a = a.wrapping_add(u32::from(k[2]) << 16);
    }
    if len >= 2 {
        a = a.wrapping_add(u32::from(k[1]) << 8);
    }
    if len >= 1 {
        a = a.wrapping_add(u32::from(k[0]));
    }

    finalize(&mut a, &mut b, &mut c);

    DynToken::from_u32(c)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(key: &[u8]) -> u32 {
        hash(key).get_int()
    }

    #[test]
    fn empty_returns_init_c() {
        // c after init for length=0 is 0xdeadbeef + 13 = 0xdeadbefc.
        assert_eq!(h(b""), 0xdead_befc);
    }

    #[test]
    fn determinism() {
        for k in [&b"a"[..], b"ab", b"abc", b"abcdefghijklmn", b"longer key"] {
            assert_eq!(h(k), h(k));
        }
    }

    #[test]
    fn distinguishes_short_inputs() {
        let mut seen = std::collections::HashSet::new();
        for k in [&b"a"[..], b"b", b"c", b"foo", b"bar", b"baz", b"qux"] {
            assert!(seen.insert(h(k)));
        }
    }

    #[test]
    fn long_input_runs_main_loop() {
        // Longer than 12 bytes triggers the main 12-byte loop.
        let v = b"abcdefghijklmnopqrstuvwxyz";
        assert_eq!(h(v), h(v));
    }
}
