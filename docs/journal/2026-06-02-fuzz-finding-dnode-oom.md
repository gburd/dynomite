# Fuzz finding: dnode_parse OOM via oversized length field

Date: 2026-06-02
Source: 1h fuzz soak (DoD #4) launched by the v0.1.0 audit.

## Summary

The DNODE header parser triggered libfuzzer's OOM detector
with a `malloc(2521176519)` (2.5 GiB) allocation after 29
seconds of fuzzing on a fresh corpus. Six other fuzz targets
ran the full 1h budget with zero findings.

## Reproduction

The artifact is `crates/fuzz/seeds/dnode_parse/regression-oom-2026-06-02`
(112 bytes; mirrors the libfuzzer artifact saved at
`crates/fuzz/artifacts/dnode_parse/oom-aa96a050570f091fc5e6da047222902989872120`).

Re-run with:

```
cd crates/fuzz
cargo +nightly fuzz run dnode_parse --fuzz-dir . -- \
    -runs=1 ./seeds/dnode_parse/regression-oom-2026-06-02
```

## Root cause (preliminary)

The artifact starts with an 11-byte `1` run inside one of
the numeric header fields. The parser accumulates digits
into a `u64` (`self.num`); 11 ones becomes `11_111_111_111`.
That value flows into a length-bounded buffer reservation
inside `step()` somewhere downstream of `MsgId` /
`PayloadLen` / `BitField`. The 2.5 GiB malloc shape is
consistent with `Vec::with_capacity(self.num as usize)`
or equivalent.

## Fix plan

Cap every numeric field at a sane upper bound:

  MSG_ID: u32 (the C reference uses uint32_t)
  PAYLOAD_LEN: 256 MiB (well above any legitimate frame)
  TYPE / VERSION / FLAGS: already u8

Reject the input with `ParseStep::Error` when any field
exceeds its declared bound. Add a hegeltest property that
asserts no input bytes can drive `step()` to allocate
more than 256 MiB of working memory.

## Status

NOT YET FIXED. This entry is filed against the v0.1.0
DoD audit so the fuzz finding has a tracked artifact + a
fix plan; the actual code change should be a follow-up
commit before v0.1.0.

## Other fuzz targets (clean)

| target | runs | duration |
|---|---:|---:|
| conf_parse | 16,534,625 | 3601s |
| crypto_aes_decrypt | 726,208,944 | 3601s |
| proto_memcache_parse | 752,122,061 | 3601s |
| proto_memcache_parse_rsp | 929,788,592 | 3601s |
| proto_redis_parse | 626,305,560 | 3601s |
| proto_redis_parse_rsp | 648,623,029 | 3601s |
