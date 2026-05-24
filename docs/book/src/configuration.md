# Configuration

This page covers configuration knobs that go beyond the basic
pool stanza. Refer to the inline rustdoc on
[`dynomite::conf::ConfPool`] for the exhaustive field list; the
pages here describe the operator-facing surface in more detail.

## Bucket types

A *bucket type* is a named bundle of routing properties that
applies to every key whose on-the-wire form starts with the
bucket prefix. Operators use bucket types to give different key
classes different SLAs from inside a single pool: cache-style
keys can sit on `DC_ONE` while transactional keys ride
`DC_EACH_SAFE_QUORUM`, and the dispatcher swaps in the right
settings on a per-request basis without needing more pools, more
listeners, or client-side routing.

The wire convention is intentionally simple. The bucket name is
the byte sequence before the first `/` in the key; everything
else (including the slash itself) is the user-visible key body.
A key with no `/` has no bucket, and the dispatcher falls back
to the pool defaults (or to `default_bucket_type`, when one is
named).

```yaml
dyn_o_mite:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '101134286'
  servers:
  - 127.0.0.1:22122:1
  data_store: 0
  read_consistency: DC_ONE
  write_consistency: DC_ONE
  bucket_types:
  - name: sessions
    read_consistency: DC_QUORUM
    write_consistency: DC_EACH_SAFE_QUORUM
    n_val: 3
  - name: cache
    read_consistency: DC_ONE
    write_consistency: DC_ONE
    n_val: 1
  default_bucket_type: cache
```

With this stanza:

* `GET sessions/abc123` is planned with `DC_QUORUM`; writes use
  `DC_EACH_SAFE_QUORUM` and may fan out across DCs.
* `GET cache/u/9000` uses `DC_ONE` reads and writes.
* `GET plain-key` (no slash) falls through to
  `default_bucket_type: cache` and inherits its `DC_ONE`
  routing.
* Removing `default_bucket_type` makes the slashless and
  unknown-prefix cases inherit the pool-level defaults.

`n_val` caps the number of replicas a single request fans out
to. `0` means "no cap" (the default; the consistency level
alone decides fan-out). A positive `n_val` truncates the plan
to its first `n_val` targets, where rack-local replicas are
already ordered first.

Validation enforces unique bucket-type names, valid consistency
strings, and that `default_bucket_type` (when set) names an
entry in `bucket_types`. Invalid stanzas are caught by
`dynomited --test-conf` before the daemon starts.
