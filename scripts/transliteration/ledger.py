#!/usr/bin/env python3
"""Ledger tooling for the C -> Rust transliteration.

The ledger (docs/transliteration/ledger.tsv) has one row per C symbol.
Inventory columns come from scripts/transliteration/c_inventory.sh; the
status columns are maintained by whoever ports the symbol.

Columns:
  kind  c_symbol  c_file  c_line  status  rust_path  oracle_test  c_test  notes

status is one of:
  todo                 not yet transliterated
  ported               Rust function exists, oracle-tested
  replaced             authorised substrate replacement (DEV-S*)
  deviation:<DEV-id>   authorised behavioural deviation
  pending:<DEV-id>     C and Rust differ; held for an owner decision on a
                       PROPOSED deviations.md entry (counts as not done)
  dead                 unreachable in C itself (notes cite the evidence)

Subcommands:
  sync     merge a fresh inventory into the ledger (keeps status columns,
           adds new symbols as todo, marks vanished symbols in notes)
  check    validate the ledger; exit 1 on any violation
  summary  per-file progress table
  file F   list the rows for one C file

Run through scripts/check_parity.sh, which supplies the inventory.
"""
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LEDGER = os.environ.get("LEDGER_PATH") or os.path.join(ROOT, "docs", "transliteration", "ledger.tsv")
DEVIATIONS = os.path.join(ROOT, "docs", "transliteration", "deviations.md")
COLS = ["kind", "c_symbol", "c_file", "c_line", "status", "rust_path",
        "oracle_test", "c_test", "notes"]
STATUSES = ("todo", "ported", "replaced", "dead")
# Symbols ctags names from an anonymous enum/struct are not stable across
# ctags versions, so they are not tracked as rows; their enumerators are.
ANON = re.compile(r"__anon")


def key(row):
    return (row["kind"], row["c_symbol"], row["c_file"])


def read_inventory(path):
    rows = []
    with open(path) as f:
        for line in f:
            kind, sym, cfile, cline = line.rstrip("\n").split("\t")
            if ANON.search(sym):
                continue
            rows.append({"kind": kind, "c_symbol": sym, "c_file": cfile,
                         "c_line": cline})
    return rows


def read_ledger():
    if not os.path.exists(LEDGER):
        return []
    rows = []
    with open(LEDGER) as f:
        header = f.readline().rstrip("\n").split("\t")
        if header != COLS:
            sys.exit(f"ledger: unexpected header {header}")
        for line in f:
            vals = line.rstrip("\n").split("\t")
            vals += [""] * (len(COLS) - len(vals))
            rows.append(dict(zip(COLS, vals)))
    return rows


def write_ledger(rows):
    rows.sort(key=lambda r: (r["c_file"], int(r["c_line"] or 0), r["kind"], r["c_symbol"]))
    with open(LEDGER, "w") as f:
        f.write("\t".join(COLS) + "\n")
        for r in rows:
            f.write("\t".join(r.get(c, "") for c in COLS) + "\n")


def proposed_ids():
    """IDs whose heading is followed by an `* Status: PROPOSED` line."""
    ids = set()
    if not os.path.exists(DEVIATIONS):
        return ids
    current = None
    for line in open(DEVIATIONS).read().splitlines():
        m = re.match(r"^#{2,4} (DEV-[A-Za-z0-9]+)", line)
        if m:
            current = m.group(1)
        elif current and line.startswith("* Status: PROPOSED"):
            ids.add(current)
    return ids


def authorized_ids():
    """IDs with an `* Status: AUTHORIZED` line under their heading, plus
    the DEV-A* additions listed in section B when section B states they
    are AUTHORIZED."""
    ids = set()
    if not os.path.exists(DEVIATIONS):
        return ids
    text = open(DEVIATIONS).read()
    current = None
    for line in text.splitlines():
        m = re.match(r"^#{2,4} (DEV-[A-Za-z0-9]+)", line)
        if m:
            current = m.group(1)
        elif current and line.startswith("* Status: AUTHORIZED"):
            ids.add(current)
    section_b = text.split("## B.", 1)[-1].split("## C.", 1)[0]
    if "AUTHORIZED" in section_b:
        ids.update(re.findall(r"\* (DEV-A\d+):", section_b))
    return ids


def cmd_sync(inventory_path):
    inv = read_inventory(inventory_path)
    old = {key(r): r for r in read_ledger()}
    out = []
    seen = set()
    for r in inv:
        k = key(r)
        seen.add(k)
        if k in old:
            row = old[k]
            row["c_line"] = r["c_line"]
            row["notes"] = row["notes"].replace("VANISHED-FROM-C ", "")
            out.append(row)
        else:
            out.append({**r, "status": "todo", "rust_path": "", "oracle_test": "",
                        "c_test": "", "notes": ""})
    for k, row in old.items():
        if k not in seen:
            if "VANISHED-FROM-C" not in row["notes"]:
                row["notes"] = ("VANISHED-FROM-C " + row["notes"]).strip()
            out.append(row)
    write_ledger(out)
    print(f"ledger: {len(out)} rows ({len(inv)} in C inventory)")


def resolve_rust_path(p):
    """rust_path is `crate-dir/path/to/file.rs::item`. Check the file
    exists and the item is defined in it."""
    if "::" not in p:
        return False
    rel, item = p.split("::", 1)
    f = os.path.join(ROOT, rel)
    if not os.path.isfile(f):
        return False
    leaf = item.split("::")[-1]
    pat = re.compile(r"\b(fn|struct|enum|const|static|type|trait|mod|macro_rules!)\s+" + re.escape(leaf) + r"\b"
                     r"|^\s*" + re.escape(leaf) + r"\s*[,({=]", re.M)
    return bool(pat.search(open(f).read()))


def cmd_check(closed_files):
    rows = read_ledger()
    if not rows:
        sys.exit("ledger: missing or empty; run scripts/check_parity.sh --sync")
    auth = authorized_ids()
    proposed = proposed_ids()
    errors = []
    for r in rows:
        where = f"{r['c_file']}:{r['c_line']} {r['kind']} {r['c_symbol']}"
        st = r["status"]
        if "VANISHED-FROM-C" in r["notes"]:
            errors.append(f"{where}: symbol no longer in the C inventory")
        if st.startswith("deviation:"):
            dev = st.split(":", 1)[1]
            if dev not in auth:
                errors.append(f"{where}: {dev} is not AUTHORIZED in deviations.md")
        elif st.startswith("pending:"):
            dev = st.split(":", 1)[1]
            if dev not in proposed:
                errors.append(f"{where}: {dev} is not a PROPOSED entry in deviations.md")
            if r["c_file"] in closed_files:
                errors.append(f"{where}: pending decision in a closed file")
        elif st not in STATUSES:
            errors.append(f"{where}: unknown status '{st}'")
        if st in ("ported", "replaced") or st.startswith(("deviation:", "pending:")):
            if not r["rust_path"] or not resolve_rust_path(r["rust_path"]):
                errors.append(f"{where}: rust_path '{r['rust_path']}' does not resolve")
        if st == "ported" and r["kind"] == "fn" and not r["oracle_test"]:
            errors.append(f"{where}: ported fn has no oracle_test")
        if st == "dead" and not r["notes"]:
            errors.append(f"{where}: dead row needs evidence in notes")
        if st == "todo" and r["c_file"] in closed_files:
            errors.append(f"{where}: todo in a closed file")
    for e in errors:
        print("parity:", e)
    print(f"parity: {len(errors)} violation(s) across {len(rows)} rows")
    return 1 if errors else 0


def cmd_summary():
    rows = read_ledger()
    per = {}
    for r in rows:
        st = r["status"].split(":")[0]
        per.setdefault(r["c_file"], {}).setdefault(st, 0)
        per[r["c_file"]][st] += 1
    cols = ["todo", "pending", "ported", "replaced", "deviation", "dead"]
    tot = {c: 0 for c in cols}
    print(f"{'c_file':42s} " + " ".join(f"{c:>9s}" for c in cols))
    for f in sorted(per):
        line = f"{f:42s} "
        for c in cols:
            n = per[f].get(c, 0)
            tot[c] += n
            line += f"{n:9d} "
        print(line)
    print(f"{'TOTAL':42s} " + " ".join(f"{tot[c]:9d}" for c in cols))
    done = sum(tot.values()) - tot["todo"] - tot["pending"]
    print(f"progress: {done}/{sum(tot.values())} rows done "
          f"({tot['pending']} pending an owner decision)")


def cmd_file(cfile):
    for r in read_ledger():
        if r["c_file"] == cfile:
            print("\t".join(r[c] for c in COLS))


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    sub = sys.argv[1]
    if sub == "sync":
        cmd_sync(sys.argv[2])
    elif sub == "check":
        closed = set(sys.argv[2:])
        sys.exit(cmd_check(closed))
    elif sub == "summary":
        cmd_summary()
    elif sub == "file":
        cmd_file(sys.argv[2])
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
