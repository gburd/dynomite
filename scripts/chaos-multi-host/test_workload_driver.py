#!/usr/bin/env python3
"""Unit tests for the chaos workload driver's retry layer.

Run directly:

    python3 scripts/chaos-multi-host/test_workload_driver.py

The driver's filename uses a hyphen, which Python's import
machinery cannot handle directly, so the tests load it via
``importlib`` from a fixed relative path. This mirrors the
loader used by ``test_generate_report.py``.

Most of the retry-layer test surface lives inline in
``workload-driver.py`` itself (under ``--self-test``) and is
preserved for backwards compatibility. This file adds the
post-pass-4 cases that exercise the ``Closed`` recoverable
class -- which became the dominant failure mode under chaos --
and pins the coordinator's new default retry policy.
"""

from __future__ import annotations

import importlib.util
import socket
import sys
import unittest
from pathlib import Path


def _load_driver():
    """Load ``workload-driver.py`` as a module despite the hyphen."""
    here = Path(__file__).resolve().parent
    target = here / "workload-driver.py"
    if not target.exists():
        raise RuntimeError(f"workload-driver.py not found next to {__file__}")
    spec = importlib.util.spec_from_file_location("workload_driver", target)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not build importlib spec for workload-driver.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["workload_driver"] = module
    spec.loader.exec_module(module)
    return module


_driver = _load_driver()


class _FakeConn:
    """Minimal stand-in for a network connection.

    The retry loop only calls ``close()`` on the conn between
    attempts, so this is all the surface area we need for the
    tests below.
    """

    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


def _scripted_workload(script):
    """Build a workload_fn that walks the given script.

    Each script entry is either an exception instance (raised
    on that attempt) or a string (returned as the op name on a
    successful attempt). Exhausting the script raises
    ``AssertionError`` so a buggy retry loop cannot quietly
    consume more attempts than the test expects.
    """
    state = {"i": 0}

    def fn(_conn):
        i = state["i"]
        state["i"] = i + 1
        if i >= len(script):
            raise AssertionError(
                "workload script exhausted at attempt %d" % i
            )
        item = script[i]
        if isinstance(item, BaseException):
            raise item
        return item

    fn.calls = state
    return fn


class ClosedRetryTests(unittest.TestCase):
    """Pin the post-pass-4 default retry policy.

    Pass-4 redis-mode (2026-05-25) saw >99.9% of failures land
    in the ``Closed`` class because the cluster's chaos cycle
    drops connections aggressively (SIGKILL of dynomited mid-
    stream). The default retry policy at that point was
    ``NoTargets:1,Timeout:0`` and recorded zero retries across
    the entire 2h run. The coordinator now ships
    ``NoTargets:1,Timeout:0,Closed:2`` so a transient TCP reset
    is reabsorbed before counting against the success rate.
    """

    DEFAULT_POLICY = "NoTargets:1,Timeout:0,Closed:2"

    def test_default_policy_parses_to_three_classes(self) -> None:
        got = _driver.parse_retry_policy(self.DEFAULT_POLICY)
        self.assertEqual(
            got,
            {"NoTargets": 1, "Timeout": 0, "Closed": 2},
        )

    def test_closed_retried_then_success(self) -> None:
        """One ConnectionError then a success returns op=SET, retry=1."""
        retries: dict = {}
        fn = _scripted_workload([
            ConnectionError("peer closed mid-reply"),
            "SET",
        ])
        conn = _FakeConn()
        op, err = _driver.run_with_retry(
            fn,
            conn,
            "redis",
            _driver.parse_retry_policy(self.DEFAULT_POLICY),
            retries,
            "strings",
        )
        self.assertEqual(op, "SET")
        self.assertIsNone(err)
        self.assertEqual(retries, {"strings/Closed": 1})
        self.assertEqual(conn.closed, 1)

    def test_closed_two_retries_consumed_then_success(self) -> None:
        """Closed budget=2 absorbs two resets before the third call wins."""
        retries: dict = {}
        fn = _scripted_workload([
            ConnectionError("peer closed mid-reply"),
            ConnectionError("peer closed mid-reply"),
            "GET",
        ])
        conn = _FakeConn()
        op, err = _driver.run_with_retry(
            fn,
            conn,
            "redis",
            _driver.parse_retry_policy(self.DEFAULT_POLICY),
            retries,
            "strings",
        )
        self.assertEqual(op, "GET")
        self.assertIsNone(err)
        self.assertEqual(retries, {"strings/Closed": 2})
        self.assertEqual(conn.closed, 2)

    def test_closed_three_resets_with_budget_two_fails(self) -> None:
        """The third Closed exhausts the budget and counts as a failure."""
        retries: dict = {}
        fn = _scripted_workload([
            ConnectionError("peer closed mid-reply"),
            ConnectionError("peer closed mid-reply"),
            ConnectionError("peer closed mid-reply"),
        ])
        op, err = _driver.run_with_retry(
            fn,
            _FakeConn(),
            "redis",
            _driver.parse_retry_policy(self.DEFAULT_POLICY),
            retries,
            "strings",
        )
        self.assertIsNone(op)
        self.assertEqual(err, "Closed")
        self.assertEqual(retries, {"strings/Closed": 2})

    def test_oserror_is_classified_as_closed_and_retried(self) -> None:
        """ECONNRESET / EPIPE etc. travel the same recovery path."""
        retries: dict = {}
        fn = _scripted_workload([
            OSError(104, "Connection reset by peer"),
            "DEL",
        ])
        op, err = _driver.run_with_retry(
            fn,
            _FakeConn(),
            "redis",
            _driver.parse_retry_policy(self.DEFAULT_POLICY),
            retries,
            "keyspace",
        )
        self.assertEqual(op, "DEL")
        self.assertIsNone(err)
        self.assertEqual(retries, {"keyspace/Closed": 1})

    def test_default_policy_handles_mixed_classes(self) -> None:
        """A NoTargets followed by a Closed exhausts independent budgets."""
        retries: dict = {}
        fn = _scripted_workload([
            _driver.RespError("DYNOMITE: no quorum"),
            ConnectionError("peer closed mid-reply"),
            "PING",
        ])
        op, err = _driver.run_with_retry(
            fn,
            _FakeConn(),
            "redis",
            _driver.parse_retry_policy(self.DEFAULT_POLICY),
            retries,
            "scripting",
        )
        self.assertEqual(op, "PING")
        self.assertIsNone(err)
        self.assertEqual(
            retries,
            {"scripting/NoTargets": 1, "scripting/Closed": 1},
        )

    def test_timeout_still_zero_in_default_policy(self) -> None:
        """Timeouts remain non-retried; the default policy did not change there."""
        retries: dict = {}
        fn = _scripted_workload([socket.timeout("read")])
        op, err = _driver.run_with_retry(
            fn,
            _FakeConn(),
            "redis",
            _driver.parse_retry_policy(self.DEFAULT_POLICY),
            retries,
            "hash",
        )
        self.assertIsNone(op)
        self.assertEqual(err, "Timeout")
        self.assertEqual(retries, {})


class ClassifyClosedTests(unittest.TestCase):
    """Confirm classify_error returns 'Closed' for the documented sources."""

    def test_connection_error_redis(self) -> None:
        self.assertEqual(
            _driver.classify_error(ConnectionError("peer closed"), "redis"),
            "Closed",
        )

    def test_connection_error_memcache(self) -> None:
        self.assertEqual(
            _driver.classify_error(ConnectionError("peer closed"), "memcache"),
            "Closed",
        )

    def test_connection_error_riak(self) -> None:
        self.assertEqual(
            _driver.classify_error(ConnectionError("peer closed"), "riak"),
            "Closed",
        )

    def test_oserror_redis(self) -> None:
        self.assertEqual(
            _driver.classify_error(OSError("EPIPE"), "redis"),
            "Closed",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
