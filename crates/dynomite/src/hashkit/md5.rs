// RFC 1321 names the round registers a, b, c, d; preserving the
// short single-letter identifiers makes the round table directly
// verifiable against the spec.
#![allow(clippy::many_single_char_names, clippy::similar_names)]

use crate::hashkit::token::DynToken;

/// MD5 (RFC 1321). The implementation is a straight transliteration of
/// the openwall public-domain reference code.
#[derive(Clone)]
struct Ctx {
    a: u32,
    b: u32,
    c: u32,
    d: u32,
    lo: u32,
    hi: u32,
    buffer: [u8; 64],
}

impl Ctx {
    fn new() -> Self {
        Self {
            a: 0x6745_2301,
            b: 0xefcd_ab89,
            c: 0x98ba_dcfe,
            d: 0x1032_5476,
            lo: 0,
            hi: 0,
            buffer: [0u8; 64],
        }
    }

    /// Process whole 64-byte blocks. `data.len()` must be a multiple of 64.
    fn body(&mut self, data: &[u8]) {
        debug_assert_eq!(data.len() % 64, 0);
        let mut a = self.a;
        let mut b = self.b;
        let mut c = self.c;
        let mut d = self.d;

        let mut chunk = data;
        while !chunk.is_empty() {
            let block = &chunk[..64];
            chunk = &chunk[64..];

            let saved_a = a;
            let saved_b = b;
            let saved_c = c;
            let saved_d = d;

            let mut x = [0u32; 16];
            for (i, slot) in x.iter_mut().enumerate() {
                let off = i * 4;
                *slot = u32::from_le_bytes([
                    block[off],
                    block[off + 1],
                    block[off + 2],
                    block[off + 3],
                ]);
            }

            // Round 1
            step(f1, &mut a, b, c, d, x[0], 0xd76a_a478, 7);
            step(f1, &mut d, a, b, c, x[1], 0xe8c7_b756, 12);
            step(f1, &mut c, d, a, b, x[2], 0x2420_70db, 17);
            step(f1, &mut b, c, d, a, x[3], 0xc1bd_ceee, 22);
            step(f1, &mut a, b, c, d, x[4], 0xf57c_0faf, 7);
            step(f1, &mut d, a, b, c, x[5], 0x4787_c62a, 12);
            step(f1, &mut c, d, a, b, x[6], 0xa830_4613, 17);
            step(f1, &mut b, c, d, a, x[7], 0xfd46_9501, 22);
            step(f1, &mut a, b, c, d, x[8], 0x6980_98d8, 7);
            step(f1, &mut d, a, b, c, x[9], 0x8b44_f7af, 12);
            step(f1, &mut c, d, a, b, x[10], 0xffff_5bb1, 17);
            step(f1, &mut b, c, d, a, x[11], 0x895c_d7be, 22);
            step(f1, &mut a, b, c, d, x[12], 0x6b90_1122, 7);
            step(f1, &mut d, a, b, c, x[13], 0xfd98_7193, 12);
            step(f1, &mut c, d, a, b, x[14], 0xa679_438e, 17);
            step(f1, &mut b, c, d, a, x[15], 0x49b4_0821, 22);

            // Round 2
            step(g1, &mut a, b, c, d, x[1], 0xf61e_2562, 5);
            step(g1, &mut d, a, b, c, x[6], 0xc040_b340, 9);
            step(g1, &mut c, d, a, b, x[11], 0x265e_5a51, 14);
            step(g1, &mut b, c, d, a, x[0], 0xe9b6_c7aa, 20);
            step(g1, &mut a, b, c, d, x[5], 0xd62f_105d, 5);
            step(g1, &mut d, a, b, c, x[10], 0x0244_1453, 9);
            step(g1, &mut c, d, a, b, x[15], 0xd8a1_e681, 14);
            step(g1, &mut b, c, d, a, x[4], 0xe7d3_fbc8, 20);
            step(g1, &mut a, b, c, d, x[9], 0x21e1_cde6, 5);
            step(g1, &mut d, a, b, c, x[14], 0xc337_07d6, 9);
            step(g1, &mut c, d, a, b, x[3], 0xf4d5_0d87, 14);
            step(g1, &mut b, c, d, a, x[8], 0x455a_14ed, 20);
            step(g1, &mut a, b, c, d, x[13], 0xa9e3_e905, 5);
            step(g1, &mut d, a, b, c, x[2], 0xfcef_a3f8, 9);
            step(g1, &mut c, d, a, b, x[7], 0x676f_02d9, 14);
            step(g1, &mut b, c, d, a, x[12], 0x8d2a_4c8a, 20);

            // Round 3
            step(h1, &mut a, b, c, d, x[5], 0xfffa_3942, 4);
            step(h1, &mut d, a, b, c, x[8], 0x8771_f681, 11);
            step(h1, &mut c, d, a, b, x[11], 0x6d9d_6122, 16);
            step(h1, &mut b, c, d, a, x[14], 0xfde5_380c, 23);
            step(h1, &mut a, b, c, d, x[1], 0xa4be_ea44, 4);
            step(h1, &mut d, a, b, c, x[4], 0x4bde_cfa9, 11);
            step(h1, &mut c, d, a, b, x[7], 0xf6bb_4b60, 16);
            step(h1, &mut b, c, d, a, x[10], 0xbebf_bc70, 23);
            step(h1, &mut a, b, c, d, x[13], 0x289b_7ec6, 4);
            step(h1, &mut d, a, b, c, x[0], 0xeaa1_27fa, 11);
            step(h1, &mut c, d, a, b, x[3], 0xd4ef_3085, 16);
            step(h1, &mut b, c, d, a, x[6], 0x0488_1d05, 23);
            step(h1, &mut a, b, c, d, x[9], 0xd9d4_d039, 4);
            step(h1, &mut d, a, b, c, x[12], 0xe6db_99e5, 11);
            step(h1, &mut c, d, a, b, x[15], 0x1fa2_7cf8, 16);
            step(h1, &mut b, c, d, a, x[2], 0xc4ac_5665, 23);

            // Round 4
            step(i1, &mut a, b, c, d, x[0], 0xf429_2244, 6);
            step(i1, &mut d, a, b, c, x[7], 0x432a_ff97, 10);
            step(i1, &mut c, d, a, b, x[14], 0xab94_23a7, 15);
            step(i1, &mut b, c, d, a, x[5], 0xfc93_a039, 21);
            step(i1, &mut a, b, c, d, x[12], 0x655b_59c3, 6);
            step(i1, &mut d, a, b, c, x[3], 0x8f0c_cc92, 10);
            step(i1, &mut c, d, a, b, x[10], 0xffef_f47d, 15);
            step(i1, &mut b, c, d, a, x[1], 0x8584_5dd1, 21);
            step(i1, &mut a, b, c, d, x[8], 0x6fa8_7e4f, 6);
            step(i1, &mut d, a, b, c, x[15], 0xfe2c_e6e0, 10);
            step(i1, &mut c, d, a, b, x[6], 0xa301_4314, 15);
            step(i1, &mut b, c, d, a, x[13], 0x4e08_11a1, 21);
            step(i1, &mut a, b, c, d, x[4], 0xf753_7e82, 6);
            step(i1, &mut d, a, b, c, x[11], 0xbd3a_f235, 10);
            step(i1, &mut c, d, a, b, x[2], 0x2ad7_d2bb, 15);
            step(i1, &mut b, c, d, a, x[9], 0xeb86_d391, 21);

            a = a.wrapping_add(saved_a);
            b = b.wrapping_add(saved_b);
            c = c.wrapping_add(saved_c);
            d = d.wrapping_add(saved_d);
        }

        self.a = a;
        self.b = b;
        self.c = c;
        self.d = d;
    }

    fn update(&mut self, mut data: &[u8]) {
        let saved_lo = self.lo;
        let added = (saved_lo.wrapping_add(data.len() as u32)) & 0x1fff_ffff;
        if added < saved_lo {
            self.hi = self.hi.wrapping_add(1);
        }
        self.lo = added;
        // Match C: ctx->hi += size >> 29 where size is unsigned long.
        // On 32-bit ulong this is always 0 for sane lengths; on 64-bit
        // it could be nonzero only for >= 512 MiB inputs. We cap at the
        // usize precision available to us.
        self.hi = self.hi.wrapping_add((data.len() >> 29) as u32);

        let used = (saved_lo & 0x3f) as usize;
        if used != 0 {
            let free = 64 - used;
            if data.len() < free {
                self.buffer[used..used + data.len()].copy_from_slice(data);
                return;
            }
            self.buffer[used..64].copy_from_slice(&data[..free]);
            data = &data[free..];
            let block = self.buffer;
            self.body(&block);
        }

        if data.len() >= 64 {
            let take = data.len() & !0x3f;
            self.body(&data[..take]);
            data = &data[take..];
        }

        self.buffer[..data.len()].copy_from_slice(data);
    }

    fn finalize(mut self) -> [u8; 16] {
        let mut used = (self.lo & 0x3f) as usize;
        self.buffer[used] = 0x80;
        used += 1;
        let free = 64 - used;
        if free < 8 {
            for slot in &mut self.buffer[used..] {
                *slot = 0;
            }
            let block = self.buffer;
            self.body(&block);
            used = 0;
        }
        for slot in &mut self.buffer[used..56] {
            *slot = 0;
        }

        let lo_bits = self.lo << 3;
        self.buffer[56] = lo_bits as u8;
        self.buffer[57] = (lo_bits >> 8) as u8;
        self.buffer[58] = (lo_bits >> 16) as u8;
        self.buffer[59] = (lo_bits >> 24) as u8;
        self.buffer[60] = self.hi as u8;
        self.buffer[61] = (self.hi >> 8) as u8;
        self.buffer[62] = (self.hi >> 16) as u8;
        self.buffer[63] = (self.hi >> 24) as u8;

        let block = self.buffer;
        self.body(&block);

        let mut out = [0u8; 16];
        for (i, word) in [self.a, self.b, self.c, self.d].iter().enumerate() {
            out[i * 4] = *word as u8;
            out[i * 4 + 1] = (word >> 8) as u8;
            out[i * 4 + 2] = (word >> 16) as u8;
            out[i * 4 + 3] = (word >> 24) as u8;
        }
        out
    }
}

fn f1(x: u32, y: u32, z: u32) -> u32 {
    z ^ (x & (y ^ z))
}

fn g1(x: u32, y: u32, z: u32) -> u32 {
    y ^ (z & (x ^ y))
}

fn h1(x: u32, y: u32, z: u32) -> u32 {
    x ^ y ^ z
}

fn i1(x: u32, y: u32, z: u32) -> u32 {
    y ^ (x | !z)
}

#[allow(clippy::too_many_arguments)]
fn step(f: fn(u32, u32, u32) -> u32, a: &mut u32, b: u32, c: u32, d: u32, x: u32, t: u32, s: u32) {
    let aa = a.wrapping_add(f(b, c, d)).wrapping_add(x).wrapping_add(t);
    *a = aa.rotate_left(s).wrapping_add(b);
}

/// Compute the raw 16-byte MD5 digest of `key`.
pub(super) fn digest(key: &[u8]) -> [u8; 16] {
    let mut ctx = Ctx::new();
    ctx.update(key);
    ctx.finalize()
}

/// Hashkit MD5 -> 32-bit token. Uses the first 4 result bytes interpreted
/// little-endian.
pub(super) fn hash(key: &[u8]) -> DynToken {
    let r = digest(key);
    let val = u32::from(r[0])
        | (u32::from(r[1]) << 8)
        | (u32::from(r[2]) << 16)
        | (u32::from(r[3]) << 24);
    DynToken::from_u32(val)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(d: &[u8; 16]) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        for b in d {
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    // RFC 1321 test suite vectors.
    #[test]
    fn rfc_empty() {
        assert_eq!(hex(&digest(b"")), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn rfc_a() {
        assert_eq!(hex(&digest(b"a")), "0cc175b9c0f1b6a831c399e269772661");
    }

    #[test]
    fn rfc_abc() {
        assert_eq!(hex(&digest(b"abc")), "900150983cd24fb0d6963f7d28e17f72");
    }

    #[test]
    fn rfc_message_digest() {
        assert_eq!(
            hex(&digest(b"message digest")),
            "f96b697d7cb7938d525a2f31aaf161d0"
        );
    }

    #[test]
    fn rfc_alphabet() {
        assert_eq!(
            hex(&digest(b"abcdefghijklmnopqrstuvwxyz")),
            "c3fcd3d76192e4007dfb496cca67e13b"
        );
    }

    #[test]
    fn rfc_long() {
        assert_eq!(
            hex(&digest(
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
            )),
            "d174ab98d277d9f5a5611c2c9f419d9f"
        );
    }

    #[test]
    fn token_uses_first_four_bytes_le() {
        let t = hash(b"abc");
        // First 4 bytes of "900150983cd24fb0..." are 0x90, 0x01, 0x50, 0x98.
        // Little-endian decode: 0x98500190.
        assert_eq!(t.get_int(), 0x9850_0190);
    }
}
