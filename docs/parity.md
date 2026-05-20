# C-to-Rust Parity Matrix

This document maps every C symbol in `_/dynomite/` to its Rust home and is
the source of truth for "is the port complete?". Update this file in
every PR that adds Rust code or finishes a port.

Format:

* Sections follow the C source tree.
* Each row: `C symbol` -> `Rust path` (or `omitted: <reason>`).
* `# Ambiguities` collects places where the C code is unclear and the
  chosen Rust interpretation, with rationale and a regression test
  reference.
* `# Deviations` collects intentional differences from C and why.

## src/

| C symbol | Rust home | Notes |
|---|---|---|
| `rstatus_t` | `dynomite::core::types::Status` (alias for `Result<(), DynError>`) | done (Stage 0) |
| `DN_OK` / `DN_ERROR` / `DN_EAGAIN` / `DN_ENOMEM` | `dynomite::core::types::DynError::{Generic, Again, OutOfMemory}` | done (Stage 0) |

Stages 1 onward populate the rest of this table. Until a row exists, the
symbol is considered un-ported.

## src/event/

(Stage 2 - replaced wholesale by tokio-based reactor.)

## src/hashkit/

(Stage 3.)

## src/proto/

(Stage 8.)

## src/seedsprovider/

(Stage 10.)

## src/entropy/

(Stage 11.)

## src/tools/

(Stage 3 ships `crates/dyn-hash-tool/`.)

## contrib/

| Path | Disposition |
|---|---|
| `contrib/yaml-0.1.4/` | omitted: replaced by `serde_yaml`. |
| `contrib/fmemopen.{c,h}` | omitted: not needed in Rust. |
| `contrib/murmur3/` | open-coded in `dynomite::hashkit::murmur3` (Stage 3). |

## Ambiguities

(none yet)

## Deviations

(none yet)
