#!/usr/bin/env python3
"""Reply-comparison allowlist for the differential chaos rig.

Phase 4 of the differential rig (P3-3.9) compares every workload
reply between the Rust dynomited proxy and the C ``dynomite``
reference. The two clusters share the same redis backend, so
every K/V command is expected to produce identical bytes. A
small set of commands has known semantic divergences that are
not bugs (timing fields, connection bookkeeping, unordered
arrays, error wording). This module captures those rules and
provides the comparator used by ``workload-driver.py``.

The allowlist is intentionally additive: every entry says
"this command is permitted to differ in the way named by the
rule". Anything not on the allowlist must match byte-for-byte.

Stdlib only; no third-party deps.
"""

from __future__ import annotations

import unittest


# The allowlist rules. Tuples of ``(redis-command, rule-name)``.
# When a workload op-class matches the command, the comparator
# applies the named rule before declaring agreement / divergence.
#
# Rules:
#   ignore_timing_fields    -- INFO-style ``key:value\r\n``
#                              line block; drop volatile lines
#                              before comparing.
#   ignore_connection_ids   -- CLIENT-LIST style line block;
#                              strip per-client identifiers
#                              before comparing.
#   sort_array_response     -- top-level array reply; sort
#                              both sides before comparing.
ALLOWLIST = [
    ("INFO", "ignore_timing_fields"),
    ("CLIENT", "ignore_connection_ids"),
    ("KEYS", "sort_array_response"),
    ("SCAN", "sort_array_response"),
    ("SMEMBERS", "sort_array_response"),
    ("HKEYS", "sort_array_response"),
    ("HVALS", "sort_array_response"),
    ("HGETALL", "sort_array_response"),
    ("TIME", "ignore_timing_fields"),
]


# INFO fields that are permitted to differ. Keys are lower-case
# for case-insensitive match. The list is conservative: anything
# version-, pid-, uptime-, byte-count-, or rate-related is here.
# Keys not in this set must match byte-for-byte after the strip.
_INFO_VOLATILE_KEYS = frozenset(
    [
        b"redis_version",
        b"redis_git_sha1",
        b"redis_git_dirty",
        b"redis_build_id",
        b"redis_mode",
        b"os",
        b"arch_bits",
        b"multiplexing_api",
        b"process_id",
        b"run_id",
        b"tcp_port",
        b"uptime_in_seconds",
        b"uptime_in_days",
        b"hz",
        b"lru_clock",
        b"executable",
        b"config_file",
        b"connected_clients",
        b"client_recent_max_input_buffer",
        b"client_recent_max_output_buffer",
        b"blocked_clients",
        b"used_memory",
        b"used_memory_human",
        b"used_memory_rss",
        b"used_memory_rss_human",
        b"used_memory_peak",
        b"used_memory_peak_human",
        b"used_memory_lua",
        b"used_memory_scripts",
        b"used_memory_scripts_human",
        b"maxmemory",
        b"maxmemory_human",
        b"latest_fork_usec",
        b"loading",
        b"rdb_changes_since_last_save",
        b"rdb_bgsave_in_progress",
        b"rdb_last_save_time",
        b"rdb_last_bgsave_status",
        b"rdb_last_bgsave_time_sec",
        b"rdb_current_bgsave_time_sec",
        b"aof_enabled",
        b"aof_rewrite_in_progress",
        b"aof_rewrite_scheduled",
        b"aof_last_rewrite_time_sec",
        b"aof_current_rewrite_time_sec",
        b"aof_last_bgrewrite_status",
        b"aof_last_write_status",
        b"total_connections_received",
        b"total_commands_processed",
        b"instantaneous_ops_per_sec",
        b"total_net_input_bytes",
        b"total_net_output_bytes",
        b"instantaneous_input_kbps",
        b"instantaneous_output_kbps",
        b"rejected_connections",
        b"sync_full",
        b"sync_partial_ok",
        b"sync_partial_err",
        b"expired_keys",
        b"evicted_keys",
        b"keyspace_hits",
        b"keyspace_misses",
        b"pubsub_channels",
        b"pubsub_patterns",
        b"latest_fork_usec",
        b"used_cpu_sys",
        b"used_cpu_user",
        b"used_cpu_sys_children",
        b"used_cpu_user_children",
        # Dynomite-specific fields that differ between Rust and
        # C builds: build identification, node identity (each
        # cluster has its own), peer-list summaries.
        b"node_token",
        b"node_dc",
        b"node_rack",
        b"server_version",
        b"node",
        b"build_id",
    ]
)


def lookup_rule(op_class):
    """Return the allowlist rule for ``op_class`` or ``None``.

    Matching is case-insensitive against the first entry in
    ``ALLOWLIST`` whose command equals ``op_class``.
    """
    if op_class is None:
        return None
    head = op_class.upper() if isinstance(op_class, str) else None
    if head is None:
        return None
    for cmd, rule in ALLOWLIST:
        if head == cmd:
            return rule
    return None


def _to_bytes(reply):
    """Coerce a reply chunk to bytes for comparison.

    ``None`` -> b"". ``str`` -> utf-8 bytes. ``int`` -> ascii. Any
    other shape (list / tuple) is left unchanged for the caller
    to handle.
    """
    if reply is None:
        return b""
    if isinstance(reply, bytes):
        return reply
    if isinstance(reply, str):
        return reply.encode("utf-8", "replace")
    if isinstance(reply, int):
        return str(reply).encode()
    return reply


def _normalize(reply):
    """Recursively convert a parsed RESP reply to a comparable form.

    ``str`` and ``int`` become bytes / int; lists become tuples
    so they hash; nested lists recurse. ``None`` stays ``None``.
    """
    if reply is None:
        return None
    if isinstance(reply, list):
        return tuple(_normalize(x) for x in reply)
    if isinstance(reply, tuple):
        return tuple(_normalize(x) for x in reply)
    if isinstance(reply, str):
        return reply.encode("utf-8", "replace")
    return reply


def _sortable_key(item):
    """Return a key that lets heterogeneous list elements sort.

    bytes pass through; everything else is repr-encoded so a
    mixed list (rare, but Redis does return them for some
    commands) does not raise during sorted()."""
    if isinstance(item, bytes):
        return (0, item)
    if isinstance(item, int):
        return (1, item)
    if isinstance(item, tuple):
        # Recursively sort nested tuples too.
        return (2, tuple(_sortable_key(x) for x in item))
    return (3, repr(item).encode("utf-8", "replace"))


def _sort_top_level(reply):
    """Sort a top-level list reply. Non-lists are returned as-is.

    Nested lists are recursively normalised first so each inner
    element compares as a tuple.
    """
    norm = _normalize(reply)
    if isinstance(norm, tuple):
        return tuple(sorted(norm, key=_sortable_key))
    return norm


def _strip_info_blob(reply):
    """Filter an INFO bulk reply to its non-volatile lines.

    The reply may be ``bytes`` / ``str`` / ``None``. Section
    headers (``# Server``) and blank lines are dropped. Any
    ``key:value`` line whose lower-cased key is in
    ``_INFO_VOLATILE_KEYS`` is dropped. The remaining lines are
    returned as a sorted, deduped, newline-joined ``bytes`` so
    the caller can compare two strips with ``==``.
    """
    blob = _to_bytes(reply)
    out = []
    for raw in blob.replace(b"\r\n", b"\n").split(b"\n"):
        line = raw.strip()
        if not line:
            continue
        if line.startswith(b"#"):
            # Section header; drop entirely. The set of section
            # names differs between dynomite versions and is
            # not load-bearing.
            continue
        sep = line.find(b":")
        if sep < 0:
            out.append(line)
            continue
        key = line[:sep].lower()
        if key in _INFO_VOLATILE_KEYS:
            continue
        out.append(key + b":" + line[sep + 1 :])
    out.sort()
    return b"\n".join(out)


def _strip_client_list(reply):
    """Filter a CLIENT-LIST reply by dropping per-connection IDs.

    Each client occupies one line of ``key=value`` pairs. We drop
    fields whose key is volatile (``id``, ``addr``, ``laddr``,
    ``fd``, ``age``, ``idle``, ``qbuf``, ``qbuf-free``, ``obl``,
    ``oll``, ``omem``, ``events``, ``cmd``, ``user``,
    ``redir``, ``multi``, ``argv-mem``, ``tot-mem``,
    ``psub``, ``ssub``, ``resp``) and sort the remaining pairs
    on each line so reordering is invisible.
    """
    blob = _to_bytes(reply)
    volatile = frozenset(
        [
            b"id",
            b"addr",
            b"laddr",
            b"fd",
            b"age",
            b"idle",
            b"qbuf",
            b"qbuf-free",
            b"obl",
            b"oll",
            b"omem",
            b"events",
            b"cmd",
            b"user",
            b"redir",
            b"multi",
            b"argv-mem",
            b"tot-mem",
            b"psub",
            b"ssub",
            b"resp",
            b"lib-name",
            b"lib-ver",
            b"watch",
        ]
    )
    out_lines = []
    for raw in blob.replace(b"\r\n", b"\n").split(b"\n"):
        line = raw.strip()
        if not line:
            continue
        kept = []
        for pair in line.split(b" "):
            sep = pair.find(b"=")
            if sep < 0:
                kept.append(pair)
                continue
            key = pair[:sep].lower()
            if key in volatile:
                continue
            kept.append(pair)
        kept.sort()
        out_lines.append(b" ".join(kept))
    out_lines.sort()
    return b"\n".join(out_lines)


def classify_error(exc):
    """Map an exception to a coarse class for cross-cluster compare.

    Returns ``None`` when ``exc`` is ``None``. Otherwise returns
    a tuple ``(type-name, head)`` where ``head`` is the
    leading token of the message (RESP error prefix, memcache
    keyword, or empty for opaque transport failures). Matching
    by head rather than full text is what the brief calls "by
    class not text" -- ``-ERR no such key`` and
    ``-ERR key not found`` both classify to
    ``("RespError", "ERR")``.
    """
    if exc is None:
        return None
    name = type(exc).__name__
    msg_obj = exc.args[0] if exc.args else ""
    if isinstance(msg_obj, bytes):
        msg = msg_obj.decode("utf-8", "replace")
    else:
        msg = str(msg_obj)
    head = msg.strip()
    # Split on whichever boundary appears first.
    sep_indices = [i for i in (head.find(":"), head.find(" ")) if i >= 0]
    if sep_indices:
        head = head[: min(sep_indices)]
    head = head.strip().upper()
    return (name, head)


def _snippet(reply, n=80):
    """Render a short bytes-form snippet of a reply for ndjson logs."""
    if reply is None:
        return ""
    if isinstance(reply, bytes):
        body = reply
    elif isinstance(reply, str):
        body = reply.encode("utf-8", "replace")
    else:
        body = repr(reply).encode("utf-8", "replace")
    if len(body) > n:
        body = body[:n] + b"..."
    return body.decode("utf-8", "replace")


def compare_replies(rust_reply, c_reply, rust_exc, c_exc, op_class):
    """Compare a Rust + C reply pair and return the bucket name + detail.

    Returns one of:

    * ``("agreed", detail)`` -- bytes-equal, semantically equal
      under the allowlist, OR both sides raised the same error
      class. ``detail`` carries the rule that fired.
    * ``("divergent", detail)`` -- replies differ in a way the
      allowlist does not permit. ``detail`` carries
      ``reason`` plus short snippets of both sides.
    * ``("one_side_failed", detail)`` -- exactly one side
      raised. ``detail`` carries ``which`` and the error class
      classified by :func:`classify_error`.
    * ``("both_failed", detail)`` -- both sides raised but with
      different error classes. Counts as a divergence outside
      the three documented buckets; the workload driver folds
      it into ``divergent`` for accounting since neither side
      produced a reply we can vouch for.

    The function is total: it never raises on a malformed
    reply. Anything it does not understand is reported as
    ``divergent`` so the operator sees it.
    """
    if rust_exc is not None and c_exc is None:
        return (
            "one_side_failed",
            {
                "which": "rust",
                "op": op_class,
                "error_class": classify_error(rust_exc),
            },
        )
    if c_exc is not None and rust_exc is None:
        return (
            "one_side_failed",
            {
                "which": "c",
                "op": op_class,
                "error_class": classify_error(c_exc),
            },
        )
    if rust_exc is not None and c_exc is not None:
        rcls = classify_error(rust_exc)
        ccls = classify_error(c_exc)
        if rcls == ccls:
            return ("agreed", {"reason": "matching_error", "error_class": rcls})
        return (
            "both_failed",
            {
                "op": op_class,
                "rust_error_class": rcls,
                "c_error_class": ccls,
            },
        )
    rule = lookup_rule(op_class)
    if rule == "sort_array_response":
        rs = _sort_top_level(rust_reply)
        cs = _sort_top_level(c_reply)
        if rs == cs:
            return ("agreed", {"reason": "sorted_match", "rule": rule})
        return (
            "divergent",
            {
                "reason": "sorted_diff",
                "rule": rule,
                "op": op_class,
                "snippet_rust": _snippet(rust_reply),
                "snippet_c": _snippet(c_reply),
            },
        )
    if rule == "ignore_timing_fields":
        rs = _strip_info_blob(rust_reply)
        cs = _strip_info_blob(c_reply)
        if rs == cs:
            return ("agreed", {"reason": "timing_stripped_match", "rule": rule})
        return (
            "divergent",
            {
                "reason": "timing_after_strip_diff",
                "rule": rule,
                "op": op_class,
                "snippet_rust": _snippet(rs),
                "snippet_c": _snippet(cs),
            },
        )
    if rule == "ignore_connection_ids":
        rs = _strip_client_list(rust_reply)
        cs = _strip_client_list(c_reply)
        if rs == cs:
            return ("agreed", {"reason": "client_stripped_match", "rule": rule})
        return (
            "divergent",
            {
                "reason": "client_after_strip_diff",
                "rule": rule,
                "op": op_class,
                "snippet_rust": _snippet(rs),
                "snippet_c": _snippet(cs),
            },
        )
    # No allowlist entry: byte-exact.
    rn = _normalize(rust_reply)
    cn = _normalize(c_reply)
    if rn == cn:
        return ("agreed", {"reason": "byte_equal"})
    return (
        "divergent",
        {
            "reason": "byte_diff",
            "op": op_class,
            "snippet_rust": _snippet(rust_reply),
            "snippet_c": _snippet(c_reply),
        },
    )


# --- self-tests (for `python3 differential_allowlist.py --self-test`) ---


class _AllowlistShapeTests(unittest.TestCase):
    def test_lookup_known_rules(self):
        self.assertEqual(lookup_rule("INFO"), "ignore_timing_fields")
        self.assertEqual(lookup_rule("info"), "ignore_timing_fields")
        self.assertEqual(lookup_rule("KEYS"), "sort_array_response")
        self.assertEqual(lookup_rule("SMEMBERS"), "sort_array_response")
        self.assertEqual(lookup_rule("CLIENT"), "ignore_connection_ids")

    def test_lookup_unknown_returns_none(self):
        self.assertIsNone(lookup_rule("SET"))
        self.assertIsNone(lookup_rule("GET"))
        self.assertIsNone(lookup_rule(None))


if __name__ == "__main__":
    import sys

    if "--self-test" in sys.argv:
        sys.argv.remove("--self-test")
        unittest.main(verbosity=2)
    else:
        print("This module is loaded by workload-driver.py; nothing to do.")
