#!/usr/bin/env python3
"""Unit tests for the differential allowlist module.

Run directly:

    python3 scripts/chaos-multi-host/test_differential_allowlist.py

The allowlist module's filename matches a valid Python module
name, so a plain import works once the script's directory is on
``sys.path``.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

import differential_allowlist as dal  # noqa: E402


class InfoCommandIgnoresTimingFields(unittest.TestCase):
    """``ignore_timing_fields`` strips known volatile lines."""

    RUST_INFO = (
        b"# Server\r\n"
        b"redis_version:6.2.6\r\n"
        b"process_id:12345\r\n"
        b"uptime_in_seconds:42\r\n"
        b"server_version:rust-0.1.0\r\n"
        b"node:floki-rust\r\n"
        b"\r\n"
        b"# Stats\r\n"
        b"keyspace_hits:1000\r\n"
        b"total_commands_processed:5000\r\n"
        b"some_stable_field:always-the-same\r\n"
    )
    C_INFO = (
        b"# Server\r\n"
        b"redis_version:6.2.7\r\n"
        b"process_id:99887\r\n"
        b"uptime_in_seconds:888\r\n"
        b"server_version:c-0.5.10\r\n"
        b"node:floki-c\r\n"
        b"\r\n"
        b"# Stats\r\n"
        b"keyspace_hits:7\r\n"
        b"total_commands_processed:3\r\n"
        b"some_stable_field:always-the-same\r\n"
    )

    def test_strip_drops_volatile_keeps_stable(self) -> None:
        rs = dal._strip_info_blob(self.RUST_INFO)
        cs = dal._strip_info_blob(self.C_INFO)
        self.assertEqual(rs, cs)
        # The stable field survives.
        self.assertIn(b"some_stable_field:always-the-same", rs)
        # The volatile fields do not.
        self.assertNotIn(b"redis_version", rs)
        self.assertNotIn(b"process_id", rs)
        self.assertNotIn(b"uptime_in_seconds", rs)
        self.assertNotIn(b"server_version", rs)
        self.assertNotIn(b"node:", rs)

    def test_compare_replies_info_agreed(self) -> None:
        bucket, detail = dal.compare_replies(
            self.RUST_INFO, self.C_INFO, None, None, "INFO"
        )
        self.assertEqual(bucket, "agreed")
        self.assertEqual(detail.get("rule"), "ignore_timing_fields")

    def test_compare_replies_info_divergent_when_stable_differs(self) -> None:
        c_changed = self.C_INFO.replace(
            b"some_stable_field:always-the-same",
            b"some_stable_field:NOT-the-same",
        )
        bucket, detail = dal.compare_replies(
            self.RUST_INFO, c_changed, None, None, "INFO"
        )
        self.assertEqual(bucket, "divergent")
        self.assertEqual(detail.get("reason"), "timing_after_strip_diff")
        self.assertIn("snippet_rust", detail)
        self.assertIn("snippet_c", detail)


class UnsortedArrayResponsesGetSortedBeforeCompare(unittest.TestCase):
    """``sort_array_response`` makes element order irrelevant."""

    def test_keys_command_two_orderings_agree(self) -> None:
        rust = [b"alpha", b"beta", b"gamma"]
        c = [b"gamma", b"alpha", b"beta"]
        bucket, detail = dal.compare_replies(rust, c, None, None, "KEYS")
        self.assertEqual(bucket, "agreed")
        self.assertEqual(detail.get("rule"), "sort_array_response")

    def test_smembers_two_orderings_agree(self) -> None:
        rust = [b"x", b"y", b"z"]
        c = [b"z", b"y", b"x"]
        bucket, _ = dal.compare_replies(rust, c, None, None, "SMEMBERS")
        self.assertEqual(bucket, "agreed")

    def test_keys_command_actual_diff_is_divergent(self) -> None:
        rust = [b"alpha", b"beta", b"gamma"]
        c = [b"alpha", b"beta", b"DELTA"]  # gamma vs DELTA
        bucket, detail = dal.compare_replies(rust, c, None, None, "KEYS")
        self.assertEqual(bucket, "divergent")
        self.assertEqual(detail.get("reason"), "sorted_diff")
        self.assertIn(b"gamma", detail["snippet_rust"].encode("utf-8", "replace"))

    def test_unsorted_does_not_apply_to_unallowlisted_command(self) -> None:
        # MGET is order-sensitive (per-position lookup), so a
        # reorder must surface as a real divergence.
        rust = [b"a", b"b"]
        c = [b"b", b"a"]
        bucket, _ = dal.compare_replies(rust, c, None, None, "MGET")
        self.assertEqual(bucket, "divergent")

    def test_hkeys_with_str_normalisation(self) -> None:
        # Some replies are str (older test fixtures) -- the
        # comparator coerces to bytes before sorting.
        rust = ["a", "b", "c"]
        c = ["c", "b", "a"]
        bucket, _ = dal.compare_replies(rust, c, None, None, "HKEYS")
        self.assertEqual(bucket, "agreed")


class ErrorMessagesMatchByClassNotText(unittest.TestCase):
    """Error wording differences within the same class are tolerated."""

    def test_resp_err_with_different_text_agrees(self) -> None:
        # Both sides surface a -ERR with different message text.
        # classify_error keys off the prefix only.
        class FakeRespError(Exception):
            pass

        FakeRespError.__name__ = "RespError"
        rust = FakeRespError("ERR no such key")
        c = FakeRespError("ERR key not found")
        bucket, detail = dal.compare_replies(None, None, rust, c, "GET")
        self.assertEqual(bucket, "agreed")
        self.assertEqual(detail.get("reason"), "matching_error")
        self.assertEqual(detail["error_class"], ("RespError", "ERR"))

    def test_resp_dynomite_prefix_matches_when_text_differs(self) -> None:
        class FakeRespError(Exception):
            pass

        FakeRespError.__name__ = "RespError"
        rust = FakeRespError("DYNOMITE: no quorum reached")
        c = FakeRespError("DYNOMITE: dispatcher refused request")
        bucket, _ = dal.compare_replies(None, None, rust, c, "SET")
        self.assertEqual(bucket, "agreed")

    def test_different_error_prefix_is_both_failed(self) -> None:
        class FakeRespError(Exception):
            pass

        FakeRespError.__name__ = "RespError"
        rust = FakeRespError("ERR wrong type")
        c = FakeRespError("WRONGTYPE Operation against wrong kind")
        bucket, detail = dal.compare_replies(None, None, rust, c, "SET")
        self.assertEqual(bucket, "both_failed")
        self.assertEqual(detail["rust_error_class"], ("RespError", "ERR"))
        self.assertEqual(detail["c_error_class"], ("RespError", "WRONGTYPE"))

    def test_classify_error_strips_message_body(self) -> None:
        class FakeRespError(Exception):
            pass

        FakeRespError.__name__ = "RespError"
        cls1 = dal.classify_error(FakeRespError("ERR bad thing happened"))
        cls2 = dal.classify_error(FakeRespError("ERR a totally other thing"))
        self.assertEqual(cls1, cls2)
        self.assertEqual(cls1, ("RespError", "ERR"))


class CompareRepliesByteExactPath(unittest.TestCase):
    """Commands not on the allowlist demand byte-exact replies."""

    def test_set_byte_equal_agrees(self) -> None:
        bucket, detail = dal.compare_replies("OK", "OK", None, None, "SET")
        self.assertEqual(bucket, "agreed")
        self.assertEqual(detail.get("reason"), "byte_equal")

    def test_get_bytes_differ_diverge(self) -> None:
        bucket, detail = dal.compare_replies(b"alpha", b"beta", None, None, "GET")
        self.assertEqual(bucket, "divergent")
        self.assertEqual(detail.get("reason"), "byte_diff")
        self.assertEqual(detail.get("op"), "GET")
        self.assertIn("alpha", detail["snippet_rust"])
        self.assertIn("beta", detail["snippet_c"])

    def test_int_replies_agree(self) -> None:
        bucket, _ = dal.compare_replies(7, 7, None, None, "INCR")
        self.assertEqual(bucket, "agreed")

    def test_int_vs_bytes_diverge(self) -> None:
        # The normaliser leaves ints as ints and bytes as bytes;
        # 7 != b"7". This is intentional: a well-behaved proxy
        # never returns mixed types for the same command.
        bucket, _ = dal.compare_replies(7, b"7", None, None, "STRLEN")
        self.assertEqual(bucket, "divergent")

    def test_array_with_nested_bytes_agree(self) -> None:
        bucket, _ = dal.compare_replies(
            [b"a", b"b"], [b"a", b"b"], None, None, "MGET"
        )
        self.assertEqual(bucket, "agreed")


class CompareRepliesOneSideFailed(unittest.TestCase):
    """One side raised, the other succeeded."""

    def test_rust_failed_c_succeeded(self) -> None:
        class FakeRespError(Exception):
            pass

        FakeRespError.__name__ = "RespError"
        bucket, detail = dal.compare_replies(
            None, "OK", FakeRespError("ERR boom"), None, "SET"
        )
        self.assertEqual(bucket, "one_side_failed")
        self.assertEqual(detail["which"], "rust")
        self.assertEqual(detail["error_class"], ("RespError", "ERR"))
        self.assertEqual(detail["op"], "SET")

    def test_c_failed_rust_succeeded(self) -> None:
        bucket, detail = dal.compare_replies(
            "OK", None, None, ConnectionError("peer closed"), "SET"
        )
        self.assertEqual(bucket, "one_side_failed")
        self.assertEqual(detail["which"], "c")
        self.assertEqual(detail["error_class"][0], "ConnectionError")


class ClientCommandStripsConnectionBookkeeping(unittest.TestCase):
    """``ignore_connection_ids`` strips per-client identifiers."""

    def test_client_list_with_different_ids_agrees(self) -> None:
        rust = (
            b"id=1 addr=127.0.0.1:55001 fd=8 name= age=5 idle=0 cmd=client "
            b"qbuf=0 db=0 sub=0\n"
            b"id=2 addr=127.0.0.1:55002 fd=9 name= age=2 idle=1 cmd=ping "
            b"qbuf=0 db=0 sub=0\n"
        )
        c = (
            b"id=42 addr=127.0.0.1:55999 fd=11 name= age=99 idle=5 cmd=client "
            b"qbuf=0 db=0 sub=0\n"
            b"id=43 addr=127.0.0.1:56000 fd=12 name= age=88 idle=4 cmd=ping "
            b"qbuf=0 db=0 sub=0\n"
        )
        bucket, _ = dal.compare_replies(rust, c, None, None, "CLIENT")
        self.assertEqual(bucket, "agreed")

    def test_client_list_with_real_diff_diverges(self) -> None:
        rust = b"id=1 addr=127.0.0.1:55001 db=0 sub=0\n"
        c = b"id=1 addr=127.0.0.1:55001 db=99 sub=0\n"  # db differs
        bucket, detail = dal.compare_replies(rust, c, None, None, "CLIENT")
        self.assertEqual(bucket, "divergent")
        self.assertEqual(detail.get("reason"), "client_after_strip_diff")


if __name__ == "__main__":
    unittest.main(verbosity=2)
