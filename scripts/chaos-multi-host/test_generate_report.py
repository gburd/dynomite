#!/usr/bin/env python3
"""Unit tests for the chaos report generator.

Run directly:

    python3 scripts/chaos-multi-host/test_generate_report.py

The generator's filename uses a hyphen, which Python's import
machinery cannot handle directly, so the tests load it via
``importlib`` from a fixed relative path.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path


def _load_generator():
    """Load ``generate-report.py`` as a module despite the hyphen."""
    here = Path(__file__).resolve().parent
    target = here / "generate-report.py"
    if not target.exists():
        raise RuntimeError(f"generate-report.py not found next to {__file__}")
    spec = importlib.util.spec_from_file_location("generate_report", target)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not build importlib spec for generate-report.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["generate_report"] = module
    spec.loader.exec_module(module)
    return module


GR = _load_generator()


# ---------- helpers for synthesising a fake run-dir ----------


def _write_ndjson(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def _make_workload_rows(label, mode, ops_per_window, fails_per_window, n_windows,
                        start_ts=None):
    """Synthesise n workload-driver snapshot rows.

    Each window logs ``ops_per_window`` successes split across two
    op classes (``strings/SET`` and ``strings/GET``) and
    ``fails_per_window`` failures bucketed under
    ``strings/ConnectionRefusedError``.
    """
    if start_ts is None:
        start_ts = 1_700_000_000.0
    rows = []
    for i in range(n_windows):
        ts = start_ts + i * 10.0
        rows.append({
            "ts": ts,
            "label": label,
            "mode": mode,
            "elapsed": (i + 1) * 10.0,
            "counts": {
                "strings/SET": ops_per_window // 2,
                "strings/GET": ops_per_window - ops_per_window // 2,
            },
            "failures": (
                {"strings/ConnectionRefusedError": fails_per_window}
                if fails_per_window > 0 else {}
            ),
        })
    return rows


def _make_chaos_rows(label, kinds_with_counts, base_ts="2026-05-25T03:42:30Z"):
    """Synthesise chaos events ndjson rows.

    ``kinds_with_counts`` is a dict like
    ``{"pause_start": 5, "kill": 2}``.
    """
    rows = []
    for kind, count in kinds_with_counts.items():
        for i in range(count):
            rows.append({
                "ts": base_ts,
                "host": label,
                "kind": kind,
                "detail": {},
            })
    return rows


def _make_run_dir(root, run_id, hosts, mode="redis", coord_lines=None,
                   workload_per_host=None, chaos_per_host=None):
    """Build a synthetic run-dir under ``root/run_id``.

    ``workload_per_host`` and ``chaos_per_host`` map label ->
    list of pre-built ndjson rows. Hosts not present produce
    empty files (zero rows).
    """
    run_dir = root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    coord_path = run_dir / "coordinator.log"
    if coord_lines is None:
        coord_lines = [
            "[03:41:49] ================================================================",
            "[03:41:49] multi-host chaos coordinator starting",
            f"[03:41:49]   run id:   {run_id}",
            "[03:41:49]   duration: 7200 s",
            f"[03:41:49]   mode:     {mode}",
            f"[03:41:49]   hosts:    {' '.join(h.replace('dc-', '') for h in hosts)}",
            "[03:42:20] ==> all components up; sleeping for 7200 seconds",
            "[05:42:20] ==> duration elapsed",
            "[05:42:24] ==> coordinator done",
        ]
    coord_path.write_text("\n".join(coord_lines) + "\n")

    workload_per_host = workload_per_host or {}
    chaos_per_host = chaos_per_host or {}

    for label in hosts:
        host_root = run_dir / (label.replace("dc-", "") + "-logs")
        host_root.mkdir(parents=True, exist_ok=True)
        wpath = host_root / f"workload-{label}.ndjson"
        cpath = host_root / f"chaos-events-{label}.ndjson"
        _write_ndjson(wpath, workload_per_host.get(label, []))
        _write_ndjson(cpath, chaos_per_host.get(label, []))
    return run_dir


# ---------- tests ----------


class RunIdParsingTests(unittest.TestCase):
    def test_pass_with_mode(self):
        info = GR.parse_run_id("pass3-redis-20260525-034149Z")
        self.assertEqual(info["pass_num"], 3)
        self.assertEqual(info["mode"], "redis")

    def test_pass_without_mode(self):
        info = GR.parse_run_id("pass2-20260522-032705Z")
        self.assertEqual(info["pass_num"], 2)
        self.assertIsNone(info["mode"])

    def test_prod_run_id(self):
        info = GR.parse_run_id("prod-20260522-010136Z")
        self.assertIsNone(info["pass_num"])
        self.assertIsNone(info["mode"])

    def test_default_out_path_pass_with_mode(self):
        out = GR.default_out_path("pass3-redis-20260525-034149Z")
        self.assertTrue(str(out).endswith("multi-host-pass-3-redis.md"))

    def test_default_out_path_pass_without_mode(self):
        out = GR.default_out_path("pass2-20260522-032705Z")
        self.assertTrue(str(out).endswith("multi-host-pass-2.md"))

    def test_default_out_path_prod(self):
        out = GR.default_out_path("prod-20260522-010136Z")
        self.assertTrue(str(out).endswith("multi-host-prod-20260522-010136Z.md"))


class FullRunSynthesisTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="chaos-report-test-"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_three_hosts_thousand_ops_each(self):
        """3 hosts, ~1000 ops each, 5% failure, 50 chaos events."""
        hosts = ["dc-floki", "dc-arnold", "dc-nuc"]
        # 100 windows x 10 ops = 1000 ok; 5% = 50 fails per host
        # spread across the windows. We synthesise as 5 fails per
        # 10 windows -> 1 fail every 2 windows.
        wrows = {}
        crows = {}
        for h in hosts:
            wrows[h] = _make_workload_rows(h, "redis", 10, 0, 100)
            # Sprinkle 50 failures: bump failures field on every
            # other window.
            for idx in range(0, 100, 2):
                wrows[h][idx]["failures"] = {
                    "strings/ConnectionRefusedError": 1,
                }
            crows[h] = _make_chaos_rows(h, {
                "pause_start": 8,
                "pause_end": 8,
                "kill": 2,
                "restart": 2,
            })
        run_dir = _make_run_dir(
            self.tmp, "pass9-redis-20260525-000000Z", hosts,
            mode="redis", workload_per_host=wrows, chaos_per_host=crows,
        )
        md = GR.render_report(run_dir)
        # Per-host ok = 1000, fail = 50; aggregate ok = 3000, fail = 150
        self.assertIn("3,000", md)
        self.assertIn("150", md)
        self.assertIn("`dc-floki`", md)
        self.assertIn("`dc-arnold`", md)
        self.assertIn("`dc-nuc`", md)
        # Aggregate success rate: 3000/3150 = 95.24%
        self.assertIn("95.24%", md)
        # Chaos table is a per-kind histogram; verify the
        # aggregate cells for the four kinds we synthesised:
        # 8+8+2+2=20 events per host x 3 hosts = 60 total,
        # split as pause_start=24, pause_end=24, kill=6,
        # restart=6.
        self.assertIn("`pause_start`", md)
        self.assertIn("`kill`", md)
        self.assertIn("**24**", md)
        self.assertIn("**6**", md)
        # Provenance section present, top failure reasons too
        self.assertIn("Provenance", md)
        self.assertIn("Top failure reasons", md)
        self.assertIn("strings/ConnectionRefusedError", md)

    def test_empty_workload_does_not_divide_by_zero(self):
        """Host produced 0 ops; report must not raise."""
        hosts = ["dc-floki"]
        wrows = {"dc-floki": []}  # empty file
        crows = {"dc-floki": []}
        run_dir = _make_run_dir(
            self.tmp, "pass9-redis-20260101-000000Z", hosts,
            mode="redis", workload_per_host=wrows, chaos_per_host=crows,
        )
        md = GR.render_report(run_dir)
        # Aggregate row should show 0/0 with "n/a" rate; per-host
        # row likewise.
        self.assertIn("`dc-floki`", md)
        self.assertIn("**0**", md)
        self.assertIn("n/a", md)
        # No exception was raised; that is the primary assertion.

    def test_no_chaos_events_table_shows_zeros(self):
        """Run has hosts and workload, but zero chaos events."""
        hosts = ["dc-floki", "dc-arnold"]
        wrows = {h: _make_workload_rows(h, "redis", 5, 0, 4) for h in hosts}
        crows = {h: [] for h in hosts}
        run_dir = _make_run_dir(
            self.tmp, "pass9-redis-20260202-000000Z", hosts,
            mode="redis", workload_per_host=wrows, chaos_per_host=crows,
        )
        md = GR.render_report(run_dir)
        # The chaos-events section must render gracefully. We
        # accept either the "no events" placeholder row or a
        # header with zero data rows; both leave the report
        # readable.
        self.assertIn("Chaos events by kind", md)
        self.assertIn("_(no events)_", md)
        # Stability indicator row should also be all zeros.
        # Find the dc-floki row in the stability section.
        stab_match = re.search(
            r"\| `dc-floki` \| (\d+) \| (\d+) \| (\d+) \| (\d+) \| (\d+) \|",
            md,
        )
        self.assertIsNotNone(stab_match)
        if stab_match:
            self.assertEqual(stab_match.groups(), ("0", "0", "0", "0", "0"))

    def test_metrics_snapshot_section(self):
        """When metrics-*.json is present it shows up in the report."""
        hosts = ["dc-floki"]
        wrows = {h: _make_workload_rows(h, "redis", 10, 0, 4) for h in hosts}
        run_dir = _make_run_dir(
            self.tmp, "pass9-redis-20260303-000000Z", hosts,
            mode="redis", workload_per_host=wrows,
        )
        # P3-2.5 cause counters (synthetic).
        snapshot = {
            "dispatch_no_targets_total": 12345,
            "dispatch_response_timeout_total": 6,
            "peer_state_transitions_total": {
                "peer=1,from=NORMAL,to=DOWN": 4,
            },
        }
        (run_dir / "metrics-001.json").write_text(json.dumps(snapshot))
        md = GR.render_report(run_dir)
        self.assertIn("Failure-cause metrics snapshot", md)
        self.assertIn("dispatch_no_targets_total", md)
        self.assertIn("12,345", md)

    def test_metrics_snapshot_silent_when_missing(self):
        """No metrics file -> the section is omitted entirely."""
        hosts = ["dc-floki"]
        wrows = {h: _make_workload_rows(h, "redis", 10, 0, 4) for h in hosts}
        run_dir = _make_run_dir(
            self.tmp, "pass9-redis-20260404-000000Z", hosts,
            mode="redis", workload_per_host=wrows,
        )
        md = GR.render_report(run_dir)
        self.assertNotIn("Failure-cause metrics snapshot", md)

    def test_restart_failed_with_tail_renders(self):
        """A restart_failed event with a tail field is surfaced."""
        hosts = ["dc-floki"]
        wrows = {h: _make_workload_rows(h, "redis", 10, 0, 2) for h in hosts}
        crows = {
            "dc-floki": [
                {
                    "ts": "2026-05-25T03:55:00Z",
                    "host": "dc-floki",
                    "kind": "restart_failed",
                    "detail": {
                        "reason": "start-host.sh-nonzero",
                        "rc": 1,
                        "tail": "could not bind: address in use\\nfatal exit",
                    },
                },
            ],
        }
        run_dir = _make_run_dir(
            self.tmp, "pass9-redis-20260505-000000Z", hosts,
            mode="redis", workload_per_host=wrows, chaos_per_host=crows,
        )
        md = GR.render_report(run_dir)
        self.assertIn("Notable timeline events", md)
        self.assertIn("First three `restart_failed`", md)
        self.assertIn("could not bind", md)
        self.assertIn("fatal exit", md)
        self.assertIn("2026-05-25T03:55:00Z", md)

    def test_git_sha_recorded_in_run_dir(self):
        hosts = ["dc-floki"]
        wrows = {h: _make_workload_rows(h, "redis", 10, 0, 2) for h in hosts}
        run_dir = _make_run_dir(
            self.tmp, "pass9-redis-20260606-000000Z", hosts,
            mode="redis", workload_per_host=wrows,
        )
        (run_dir / "git-sha").write_text("abcdef1234567\n")
        md = GR.render_report(run_dir)
        self.assertIn("`abcdef1234567`", md)


class RestartFailedClassifierTests(unittest.TestCase):
    """P3-1.3: classifier + extractor unit tests."""

    def test_port_collision(self):
        stderr_tail = (
            "==> failure mode: dynomited (pid=12345) CRASHED mid-startup\n"
            "some prior line\n"
            "thread 'main' panicked at: Address already in use (os error 98)\n"
        )
        # Two patterns hit; port-collision wins because it is
        # earlier in the matcher list.
        self.assertEqual(
            GR.chaos_restart_failed_class(stderr_tail, ""),
            "port-collision",
        )

    def test_backend_down(self):
        stderr = (
            "==> ERROR: backend on 17100 did not respond to redis "
            "protocol probe within 6 seconds\n"
            "==> the port may be bound by a stale process\n"
        )
        self.assertEqual(
            GR.chaos_restart_failed_class(stderr, ""),
            "backend-down",
        )

    def test_crash_mid_startup(self):
        log_tail = (
            "INFO  starting dynomited\n"
            "thread 'tokio-runtime-worker' panicked at "
            "'invariant: ring cover', src/cluster.rs:42\n"
            "stack backtrace:\n"
            "   0: rust_begin_unwind\n"
        )
        self.assertEqual(
            GR.chaos_restart_failed_class("", log_tail),
            "crash-mid-startup",
        )

    def test_unknown_when_blob_is_empty(self):
        self.assertEqual(
            GR.chaos_restart_failed_class("", ""),
            "unknown",
        )

    def test_unknown_when_no_pattern_matches(self):
        self.assertEqual(
            GR.chaos_restart_failed_class(
                "some completely unrelated noise\nmore noise",
                "and more noise here too",
            ),
            "unknown",
        )

    def test_extract_skips_non_detail_events(self):
        events = [
            {"kind": "restart", "detail": {}},
            {"kind": "restart_failed", "detail": {"tail": "address already in use"}},
            {"kind": "pause_start", "detail": {}},
        ]
        ctr = GR.extract_restart_failed_classes(events)
        # No restart_failed_detail events present.
        self.assertEqual(sum(ctr.values()), 0)

    def test_extract_decodes_base64_and_classifies(self):
        import base64 as b64
        events = [
            {
                "event": "restart_failed_detail",
                "kind": "restart_failed_detail",
                "host": "dc-floki",
                "rc": 1,
                "stderr_tail": b64.b64encode(
                    b"bind: Address already in use\n"
                ).decode(),
                "log_tail": b64.b64encode(b"").decode(),
                "timestamp": "2026-06-01T00:00:00Z",
            },
            {
                "event": "restart_failed_detail",
                "kind": "restart_failed_detail",
                "host": "dc-floki",
                "rc": 1,
                "stderr_tail": b64.b64encode(
                    b"backend on 17100 did not respond\n"
                ).decode(),
                "log_tail": b64.b64encode(b"").decode(),
                "timestamp": "2026-06-01T00:00:01Z",
            },
            {
                "event": "restart_failed_detail",
                "kind": "restart_failed_detail",
                "host": "dc-floki",
                "rc": 1,
                "stderr_tail": b64.b64encode(b"").decode(),
                "log_tail": b64.b64encode(
                    b"thread 'main' panicked at 'oops'\nstack backtrace:\n"
                ).decode(),
                "timestamp": "2026-06-01T00:00:02Z",
            },
            {
                "event": "restart_failed_detail",
                "kind": "restart_failed_detail",
                "host": "dc-floki",
                "rc": 1,
                "stderr_tail": b64.b64encode(b"random unparseable noise\n").decode(),
                "log_tail": b64.b64encode(b"").decode(),
                "timestamp": "2026-06-01T00:00:03Z",
            },
        ]
        ctr = GR.extract_restart_failed_classes(events)
        self.assertEqual(ctr["port-collision"], 1)
        self.assertEqual(ctr["backend-down"], 1)
        self.assertEqual(ctr["crash-mid-startup"], 1)
        self.assertEqual(ctr["unknown"], 1)

    def test_extract_ignores_corrupt_base64(self):
        events = [
            {
                "event": "restart_failed_detail",
                "kind": "restart_failed_detail",
                "host": "dc-floki",
                "rc": 1,
                "stderr_tail": "!!!not-valid-base64!!!",
                "log_tail": None,
                "timestamp": "2026-06-01T00:00:00Z",
            },
        ]
        ctr = GR.extract_restart_failed_classes(events)
        # Corrupt blob decodes to empty -> unknown bucket.
        self.assertEqual(ctr["unknown"], 1)
        self.assertEqual(sum(ctr.values()), 1)


class RestartFailedClassReportTests(unittest.TestCase):
    """P3-1.3: end-to-end render_report includes the new section."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="chaos-class-report-"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _b64(self, s: str) -> str:
        import base64 as b64
        return b64.b64encode(s.encode()).decode()

    def test_classes_table_renders_when_events_present(self):
        hosts = ["dc-floki", "dc-arnold"]
        wrows = {h: _make_workload_rows(h, "redis", 10, 0, 4) for h in hosts}
        crows = {
            "dc-floki": [
                {
                    "event": "restart_failed_detail",
                    "kind": "restart_failed_detail",
                    "host": "dc-floki",
                    "rc": 1,
                    "stderr_tail": self._b64("bind: Address already in use\n"),
                    "log_tail": self._b64(""),
                    "timestamp": "2026-06-01T00:00:00Z",
                    "ts": "2026-06-01T00:00:00Z",
                },
                {
                    "event": "restart_failed_detail",
                    "kind": "restart_failed_detail",
                    "host": "dc-floki",
                    "rc": 1,
                    "stderr_tail": self._b64("thread 'main' panicked at\n"),
                    "log_tail": self._b64(""),
                    "timestamp": "2026-06-01T00:00:01Z",
                    "ts": "2026-06-01T00:00:01Z",
                },
            ],
            "dc-arnold": [
                {
                    "event": "restart_failed_detail",
                    "kind": "restart_failed_detail",
                    "host": "dc-arnold",
                    "rc": 1,
                    "stderr_tail": self._b64(
                        "backend on 17100 did not respond\n"
                    ),
                    "log_tail": self._b64(""),
                    "timestamp": "2026-06-01T00:00:02Z",
                    "ts": "2026-06-01T00:00:02Z",
                },
            ],
        }
        run_dir = _make_run_dir(
            self.tmp, "pass9-redis-20260909-000000Z", hosts,
            mode="redis", workload_per_host=wrows, chaos_per_host=crows,
        )
        md = GR.render_report(run_dir)
        self.assertIn("Restart-failed class breakdown", md)
        # Per-host counts present.
        self.assertIn("`dc-floki`", md)
        self.assertIn("`dc-arnold`", md)
        # Aggregate row reflects 2+1 = 3 classified events.
        self.assertIn("**aggregate**", md)
        # The per-class column headers are visible.
        self.assertIn("port-collision", md)
        self.assertIn("backend-down", md)
        self.assertIn("crash-mid-startup", md)

    def test_classes_section_omitted_when_no_detail_events(self):
        hosts = ["dc-floki"]
        wrows = {h: _make_workload_rows(h, "redis", 10, 0, 4) for h in hosts}
        # Standard events but NO restart_failed_detail rows.
        crows = {
            "dc-floki": [
                {"ts": "x", "host": "dc-floki", "kind": "restart",
                 "detail": {}},
            ],
        }
        run_dir = _make_run_dir(
            self.tmp, "pass9-redis-20261010-000000Z", hosts,
            mode="redis", workload_per_host=wrows, chaos_per_host=crows,
        )
        md = GR.render_report(run_dir)
        self.assertNotIn("Restart-failed class breakdown", md)


class Pass1RegressionTest(unittest.TestCase):
    """Regression: re-derive the hand-curated pass-1 numbers."""

    PASS1_PATH = (
        Path("/home/gburd/ws/dynomite") /
        "target" / "chaos-multi-host" / "prod-20260522-010136Z"
    )

    def test_pass1_numbers_match(self):
        if not self.PASS1_PATH.is_dir():
            self.skipTest(f"pass-1 run-dir not present at {self.PASS1_PATH}")
        md = GR.render_report(self.PASS1_PATH)
        # Hand-curated pass-1 totals: 3,344,844 ok / 182,339 fail
        # / 94.83% success.
        self.assertIn("3,344,844", md)
        self.assertIn("182,339", md)
        self.assertIn("94.83%", md)
        # Per-host hand-curated numbers.
        for needle in ("1,051,024", "118,458", "1,237,228", "45,598",
                       "1,056,592", "18,283"):
            self.assertIn(needle, md)


class CliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="chaos-report-cli-"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_explicit_run_id_and_out(self):
        hosts = ["dc-floki"]
        wrows = {h: _make_workload_rows(h, "redis", 10, 0, 4) for h in hosts}
        run_id = "pass9-redis-20260707-000000Z"
        runs_dir = self.tmp / "runs"
        runs_dir.mkdir(parents=True)
        _make_run_dir(runs_dir, run_id, hosts, mode="redis",
                      workload_per_host=wrows)
        out = self.tmp / "out.md"
        rc = GR.main([
            "--runs-dir", str(runs_dir),
            "--run-id", run_id,
            "--out", str(out),
        ])
        self.assertEqual(rc, 0)
        self.assertTrue(out.exists())
        body = out.read_text()
        self.assertIn("Multi-host chaos report", body)

    def test_latest_run_id_default(self):
        hosts = ["dc-floki"]
        wrows = {h: _make_workload_rows(h, "redis", 10, 0, 4) for h in hosts}
        runs_dir = self.tmp / "runs"
        runs_dir.mkdir(parents=True)
        _make_run_dir(runs_dir, "pass1-redis-20260101-000000Z", hosts,
                      mode="redis", workload_per_host=wrows)
        # Force a newer mtime on the second run-dir.
        time.sleep(0.05)
        new_run = _make_run_dir(
            runs_dir, "pass2-redis-20260102-000000Z", hosts,
            mode="redis", workload_per_host=wrows,
        )
        os.utime(new_run, None)
        latest = GR.latest_run_dir(runs_dir)
        self.assertEqual(latest.name, "pass2-redis-20260102-000000Z")


if __name__ == "__main__":
    unittest.main(verbosity=2)
