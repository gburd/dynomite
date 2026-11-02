---
name: transliterate-c
description: Transliterate a C Dynomite function or file into the Rust engine, verified against the C reference. Use before porting, re-porting, or reviewing any engine code that has a C counterpart in ~/src/dynomite.
---

The C reference is the specification. Your job is to reproduce its logic, not to improve it.

Plan and gates: `docs/transliteration/PLAN.md`. Deviations: `docs/transliteration/deviations.md`. Ledger: `docs/transliteration/ledger.tsv`.

## Steps

1. **Claim the rows.** `scripts/check_parity.sh --file <c_file>` lists every ledger row for that C file. Work a whole C file at a time. Done when you have the row list.
2. **Read the C in full**: the `.c`, its `.h`, and every caller of each function (`grep -rn <fn> $DYNOMITE_C_REF/src`). Note every branch, early return, error code, constant, log line, and side effect on globals. Done when you can name every branch of every function in the file.
3. **Check the deviation ledger.** If the file is covered by an `AUTHORIZED` substrate entry (DEV-S1..S3), port its observable effect onto tokio / Rust collections and record `replaced`. If you want to change any behaviour, write a `PROPOSED` entry with evidence and ask the owner. Never mark anything `AUTHORIZED` yourself.
4. **Transliterate.** One Rust function per C function, in the module that mirrors the C file, keeping the C name in snake_case. Same control flow, same branch order, same constants, same error outcomes. Replace C idioms (out-params, `rstatus_t`, manual free) with Rust equivalents that keep the behaviour. Comments explain why, never "ported from C".
5. **Oracle-test it.** Pure logic: a `hegeltest` property in `crates/c-oracle` that feeds identical random inputs to the C function (FFI) and the Rust function and asserts identical output. System behaviour: a differential case (`crates/dynomited/tests/differential.rs`) against the C binary. Substrate replacements: a behavioural test of the C-observable effect. Done when the test runs against the real C code and passes.
6. **Port the C tests** that touch these functions (`src/dyn_test.c`, `test/*.py`, `scripts/dynomite/*.py`), same inputs, same assertions.
7. **Wire it in.** Every ported function has a production caller matching the C call graph. A function with no caller is a bug unless the C function was itself dead (cite the C grep).
8. **Update the ledger rows** (`status`, `rust_path`, `oracle_test`, `c_test`) and run `scripts/check_parity.sh` and `scripts/check.sh`. Done when both are green and the file shows zero `todo` rows.

## Reference

* Scope rulings (`deviations.md` section 0): **P1** Rust nodes never talk to C nodes, so encodings (wire, file) are chosen on merit while the C logic is still transliterated; **P2** configuration is Rust-native, every C capability preserved; **P3** no Lua: where the C builds a Lua script, transliterate the script's logic into Rust.
* C pin: `~/src/dynomite` at `2046b05`, `DYNOMITE_C_REF=~/src/dynomite`. Build the oracle with `scripts/build_cref.sh`.
* Ledger statuses: `todo`, `ported`, `replaced` (authorised substrate), `deviation:<DEV-id>` (authorised only), `dead` (C function unreachable in C, with evidence).
* A C test that cannot run against the C binary on this host runs on the `chef` EC2 burner (Debian). Tear down every instance you create.
* Performance claims follow `pg-benchmark-rigor`: interleaved A/B against the C build, n>=5, STABLE/NOISY verdict.
