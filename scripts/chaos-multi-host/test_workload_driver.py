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
and pins the coordinator's new default retry policy. It also
covers the post-pass-5 backoff-with-jitter layer (per-class
exponential backoff between attempts and a wallclock deadline
that caps total time-in-retry per op).
"""

from __future__ import annotations

import importlib.util
import socket
import sys
import unittest
from pathlib import Path
from unittest import mock


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
            {
                "NoTargets": (1, 50, 200),
                "Timeout": (0, 50, 200),
                "Closed": (2, 50, 200),
            },
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


class BackoffParseTests(unittest.TestCase):
    """Backoff-with-jitter parser cases.

    The ``--retry-on`` syntax extends from ``<class>:<count>``
    to ``<class>:<count>[:<base_ms>[:<max_ms>]]`` so an operator
    can dial in per-class backoff. Pass-5 (2026-05-26) showed
    that a freshly-restarted dynomited can be re-saturated by
    instantaneous retries from N parallel drivers; backoff with
    jitter is the mitigation. These tests pin both the new
    syntax and the documented defaults that the short form
    (``Closed:2``) expands to.
    """

    def test_parse_retry_policy_accepts_backoff_suffixes(self) -> None:
        got = _driver.parse_retry_policy("Closed:2:100:1000")
        self.assertEqual(got, {"Closed": (2, 100, 1000)})

    def test_parse_retry_policy_uses_default_backoff_when_suffixes_omitted(
        self,
    ) -> None:
        got = _driver.parse_retry_policy("Closed:2")
        self.assertEqual(got, {"Closed": (2, 50, 200)})

    def test_parse_retry_policy_full_default_policy(self) -> None:
        # The coordinator's default RETRY_POLICY shipped on
        # 2026-05-26 with class-specific backoff windows.
        got = _driver.parse_retry_policy(
            "NoTargets:1:50:200,Timeout:0,Closed:2:100:1000"
        )
        self.assertEqual(
            got,
            {
                "NoTargets": (1, 50, 200),
                "Timeout": (0, 50, 200),
                "Closed": (2, 100, 1000),
            },
        )

    def test_parse_retry_policy_rejects_max_below_base(self) -> None:
        with self.assertRaises(ValueError):
            _driver.parse_retry_policy("Closed:2:1000:100")

    def test_parse_retry_policy_rejects_too_many_segments(self) -> None:
        with self.assertRaises(ValueError):
            _driver.parse_retry_policy("Closed:2:100:1000:9999")


class BackoffSleepTests(unittest.TestCase):
    """Backoff sleep + deadline behaviour in run_with_retry.

    The retry loop now sleeps an exponentially-growing window
    (capped at ``max_ms``) with a uniform jitter factor in
    ``[0.5, 1.5]`` before re-attempting a recoverable error,
    and gives up early if the cumulative sleep exceeds
    ``retry_deadline_ms``. We patch ``time.sleep`` and
    ``random.random`` on the driver module so the tests are
    deterministic and fast.
    """

    def test_run_with_retry_sleeps_with_jitter_between_attempts(self) -> None:
        retries: dict = {}
        retry_sleep_ms: dict = {}
        # Two recoverable errors, then a success. Budget=2 so
        # both retries fire and we observe TWO backoff windows.
        fn = _scripted_workload([
            ConnectionError("peer closed mid-reply"),
            ConnectionError("peer closed mid-reply"),
            "GET",
        ])
        # Closed:2:100:1000 means attempt 0 -> 100ms window,
        # attempt 1 -> 200ms window (still under the 1000ms
        # cap). Pin random() to 0.5 so jitter resolves to 1.0
        # exactly: sleep_ms == window_ms.
        with mock.patch.object(_driver.time, "sleep") as fake_sleep, \
                mock.patch.object(_driver.random, "random", return_value=0.5):
            op, err = _driver.run_with_retry(
                fn,
                _FakeConn(),
                "redis",
                _driver.parse_retry_policy("Closed:2:100:1000"),
                retries,
                "strings",
                retry_sleep_ms=retry_sleep_ms,
            )
        self.assertEqual(op, "GET")
        self.assertIsNone(err)
        # Two sleeps; the first matches the base window, the
        # second is doubled but still under max.
        self.assertEqual(fake_sleep.call_count, 2)
        sleeps = [c.args[0] for c in fake_sleep.call_args_list]
        # jitter factor 0.5+0.5 == 1.0, so windows are 100ms
        # and 200ms exactly.
        self.assertAlmostEqual(sleeps[0], 0.100, places=4)
        self.assertAlmostEqual(sleeps[1], 0.200, places=4)
        self.assertEqual(retries, {"strings/Closed": 2})
        # retry_sleep_ms accumulates the wallclock cost, in ms.
        self.assertEqual(retry_sleep_ms, {"strings/Closed": 300})

    def test_run_with_retry_sleeps_within_jitter_band(self) -> None:
        # With random() pinned to its extremes we stay inside
        # the documented [0.5, 1.5] band. Confirm both bounds.
        for r_val, expected_factor in [(0.0, 0.5), (0.999, 1.499)]:
            retries: dict = {}
            retry_sleep_ms: dict = {}
            fn = _scripted_workload([
                ConnectionError("peer closed mid-reply"),
                "SET",
            ])
            with mock.patch.object(_driver.time, "sleep") as fake_sleep, \
                    mock.patch.object(
                        _driver.random, "random", return_value=r_val
                    ):
                op, _ = _driver.run_with_retry(
                    fn,
                    _FakeConn(),
                    "redis",
                    _driver.parse_retry_policy("Closed:1:100:1000"),
                    retries,
                    "strings",
                    retry_sleep_ms=retry_sleep_ms,
                )
            self.assertEqual(op, "SET")
            self.assertEqual(fake_sleep.call_count, 1)
            slept = fake_sleep.call_args.args[0]
            self.assertAlmostEqual(slept, 0.100 * expected_factor, places=4)

    def test_run_with_retry_respects_retry_deadline_ms(self) -> None:
        retries: dict = {}
        retry_sleep_ms: dict = {}
        # Budget=100 so the policy alone would never give up,
        # but retry_deadline_ms=10 should chop the loop after
        # the first sleep that would push past 10ms. With
        # base_ms=100 and jitter pinned to 1.0, the very first
        # window (100ms) already exceeds the 10ms deadline, so
        # we should give up before consuming any budget.
        scripted = [ConnectionError("peer closed mid-reply")] * 10
        scripted.append("GET")
        fn = _scripted_workload(scripted)
        with mock.patch.object(_driver.time, "sleep") as fake_sleep, \
                mock.patch.object(_driver.random, "random", return_value=0.5):
            op, err = _driver.run_with_retry(
                fn,
                _FakeConn(),
                "redis",
                _driver.parse_retry_policy("Closed:100:100:1000"),
                retries,
                "strings",
                retry_sleep_ms=retry_sleep_ms,
                retry_deadline_ms=10,
            )
        self.assertIsNone(op)
        self.assertEqual(err, "Closed")
        # No sleep ever happened: the very first backoff would
        # have overrun the deadline.
        self.assertEqual(fake_sleep.call_count, 0)
        # And no budget was consumed (we surfaced the failure
        # rather than burning retries we could not afford).
        self.assertEqual(retries, {})
        self.assertEqual(retry_sleep_ms, {})

    def test_run_with_retry_deadline_allows_partial_progress(self) -> None:
        # A deadline that lets ONE backoff through but not two
        # should retry exactly once, then surface the failure
        # with budget still nominally remaining. Confirms that
        # the deadline is wallclock-based, not budget-based.
        retries: dict = {}
        retry_sleep_ms: dict = {}
        fn = _scripted_workload([
            ConnectionError("peer closed mid-reply"),
            ConnectionError("peer closed mid-reply"),
            "GET",
        ])
        # base=100ms, jitter pinned to 1.0. First retry sleeps
        # 100ms, second would sleep 200ms (attempt=1) for a
        # cumulative 300ms. Set the deadline at 150ms: the
        # first sleep fits (100 <= 150), the second does not
        # (100 + 200 > 150).
        with mock.patch.object(_driver.time, "sleep") as fake_sleep, \
                mock.patch.object(_driver.random, "random", return_value=0.5):
            op, err = _driver.run_with_retry(
                fn,
                _FakeConn(),
                "redis",
                _driver.parse_retry_policy("Closed:5:100:1000"),
                retries,
                "strings",
                retry_sleep_ms=retry_sleep_ms,
                retry_deadline_ms=150,
            )
        self.assertIsNone(op)
        self.assertEqual(err, "Closed")
        self.assertEqual(fake_sleep.call_count, 1)
        self.assertEqual(retries, {"strings/Closed": 1})
        self.assertEqual(retry_sleep_ms, {"strings/Closed": 100})

    def test_run_with_retry_without_sleep_dict_still_works(self) -> None:
        # retry_sleep_ms is optional; legacy callers should not
        # need to thread it through.
        retries: dict = {}
        fn = _scripted_workload([
            ConnectionError("peer closed mid-reply"),
            "SET",
        ])
        with mock.patch.object(_driver.time, "sleep"), \
                mock.patch.object(_driver.random, "random", return_value=0.5):
            op, err = _driver.run_with_retry(
                fn,
                _FakeConn(),
                "redis",
                _driver.parse_retry_policy("Closed:1:100:1000"),
                retries,
                "strings",
            )
        self.assertEqual(op, "SET")
        self.assertIsNone(err)
        self.assertEqual(retries, {"strings/Closed": 1})


if __name__ == "__main__":
    unittest.main(verbosity=2)
