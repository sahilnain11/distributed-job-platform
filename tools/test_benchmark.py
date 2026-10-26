"""No Docker required: test reporting arithmetic and bounded-generator behavior."""
import argparse
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import benchmark as b


class BenchmarkTests(unittest.TestCase):
    def test_percentiles_are_nearest_rank_and_empty_is_unknown(self):
        self.assertIsNone(b.percentile([], .99))
        self.assertEqual(b.percentile([4, 1, 3, 2], .5), 2)
        self.assertEqual(b.percentile(range(1, 101), .99), 99)

    def test_postgres_trimmed_timestamp_fractions_work_on_python39(self):
        for value, micros in [(".3", 300000), (".33352", 333520), (".333521", 333521), ("", 0)]:
            self.assertEqual(b.timestamp("2026-10-25T08:39:41" + value + "+00:00").microsecond, micros)
        self.assertEqual(b.timestamp("2026-10-25T08:39:41.1Z").microsecond, 100000)

    def test_fixed_requests_are_identical_and_simulation_has_no_planned_failures(self):
        self.assertEqual(len(json.loads(b.request_body("ECHO"))["payload"]["message"]), 128)
        self.assertEqual(json.loads(b.request_body("SIMULATED_WORK"))["payload"],
                         {"durationMillis": 1000, "failFirstAttempts": 0})
        self.assertEqual(b.request_body("ECHO"), b.request_body("ECHO"))

    def test_invalid_bounds_are_rejected_before_any_container_start(self):
        for overrides in [{"seconds": 0}, {"max_pending": 1000}, {"workers": 0}, {"warmup_seconds": 0}]:
            args = argparse.Namespace(seconds=60, warmup_seconds=10, workers=16, max_pending=32, drain_seconds=120)
            for key, value in overrides.items():
                setattr(args, key, value)
            with self.assertRaises(ValueError):
                b.validate_args(args)
        with self.assertRaises(ValueError):
            b.validate_args(argparse.Namespace(seconds=60, warmup_seconds=10, workers=16, max_pending=32,
                                              drain_seconds=120, cases=["echo", "echo"]))

    def test_measurement_prefix_excludes_warmup_rows(self):
        instance = b.IsolatedPlatform(Path("/tmp"), "bench-0000")
        queries = []
        instance.sql = lambda query: queries.append(query) or {}
        instance.sample("bench-0000-echo-")
        self.assertIn("NOT LIKE 'bench-0000-echo-warmup-%'", queries[0])
        with self.assertRaises(AssertionError):
            instance.sample("unsafe' OR true --")

    def test_sample_math_counts_errors_and_excludes_drain_from_completion_rate(self):
        raw = {"requests": [{"job_id": "one", "client_ms": 20, "scheduled_to_response_ms": 25, "dispatch_lag_ms": 5},
                            {"error": "TimeoutError", "commit_unknown": True}, {"error": "generator_capacity_drop"}],
               "start": {"db_now": "2026-10-25T00:00:00+00:00", "completed": 0},
               "at_end": {"db_now": "2026-10-25T00:01:00+00:00", "completed": 1, "failed": 0, "stored": 2, "outbox_pending": 1}}
        final = {"stored": 2, "completed": 2, "failed": 0, "jobs": [
            {"status": "COMPLETED", "created_at": "2026-10-25T00:00:00+00:00", "result": b.PAYLOAD,
             "payload_bytes": 143, "result_bytes": 143, "attempts": [
                 {"started_at": "2026-10-25T00:00:01+00:00", "ended_at": "2026-10-25T00:00:02+00:00"}]}]}
        summary = b.summarize(raw, final, "ECHO", 1, 60, 30, False)
        self.assertEqual(summary["completed_during_window"], 1)
        self.assertEqual(summary["final_completed"], 2)
        self.assertAlmostEqual(summary["completion_rate_during_window"], 1 / 60)
        self.assertEqual(summary["unknown_commits"], 1)
        self.assertEqual(summary["unfinished_at_window_end"], 1)
        self.assertEqual(summary["creation_to_first_start_ms_APPROX"]["p99"], 1000)
        self.assertEqual(summary["wrong_results"], 0)
        self.assertFalse(summary["measurement_valid_without_missing_arrivals"])
        self.assertEqual(summary["approx_started_within_5s_fraction"], .5)

    def test_queued_dispatch_past_window_does_not_send_an_http_request(self):
        with patch.object(b.http.client, "HTTPConnection") as http:
            record = b.submit(12345, "bench-0000-echo-", 0, b"{}", time.monotonic() - 2,
                              time.monotonic() - 2, time.monotonic() - 1)
        self.assertEqual(record["error"], "generator_dispatch_deadline_drop")
        http.assert_not_called()

    def test_slow_generator_keeps_account_of_every_arrival_with_bounded_outstanding_work(self):
        class Platform:
            http_port = 12345
            def sample(self, prefix):
                return {"db_now": "2026-10-25T00:00:00+00:00", "stored": 0, "completed": 0, "failed": 0}

        released = threading.Event()
        def slow(*args):
            released.wait(timeout=2)
            return {"index": args[2], "error": "controlled_timeout"}
        original_sample = Platform.sample
        def boundary_sample(self, prefix):
            # End-of-window DB snapshot happens only after every arrival was
            # offered; release worker deterministically at that boundary.
            if getattr(self, "sampled", False):
                released.set()
            self.sampled = True
            return original_sample(self, prefix)
        with patch.object(b, "submit", side_effect=slow), patch.object(Platform, "sample", boundary_sample):
            raw = b.load(Platform(), "bench-0000-echo-", "ECHO", 100, .04, 1, 1, observe=False)
        self.assertEqual(len(raw["requests"]), 4)
        self.assertTrue(any(r["error"] == "generator_capacity_drop" for r in raw["requests"]))
        self.assertEqual(len({r["index"] for r in raw["requests"]}), 4)

    def test_empty_report_is_valid_without_faking_performance_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            b.write_reports(folder, {"metadata": {"run_id": "bench-test"}, "cases": {}})
            self.assertEqual(json.loads((folder / "report.json").read_text())["cases"], {})
            self.assertIn("<svg", (folder / "backlog.svg").read_text())

    def test_cleanup_deduplicates_owned_ids_and_never_removes_unowned_containers(self):
        with tempfile.TemporaryDirectory() as directory:
            instance = b.IsolatedPlatform(Path(directory), "bench-0000")
            instance.ids = ["owned", "foreign"]
            calls = []
            def fake(*args, **kwargs):
                calls.append(args)
                if args[1] == "ps":
                    return "owned"
                if args[1] == "inspect":
                    return json.dumps([{"Config": {"Labels": {b.LABEL: "bench-0000" if args[2] == "owned" else "someone-else"}}}])
                return ""
            with patch.object(b, "command", side_effect=fake):
                with self.assertRaises(RuntimeError):
                    instance.close()
            removed = [call[-1] for call in calls if call[1] == "rm"]
            self.assertEqual(removed, ["owned"])
            self.assertTrue((Path(directory) / "cleanup-errors.json").exists())


if __name__ == "__main__":
    unittest.main()