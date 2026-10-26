"""Docker-free scaling contracts; regressions fail rather than bless bad evidence.

Only temporary reports/logs are written. Process, HTTP, Docker and load-generator
entry points are denied unless a test explicitly replaces them with a mock.
"""
import copy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, call, patch
import uuid
import xml.etree.ElementTree as ET

import benchmark as b
import scaling as s


def cohort(workers=2, count=3, kind="SIMULATED_WORK"):
    """A complete measured cohort, with two completions occurring during drain."""
    requests, jobs = [], []
    result = (b.PAYLOAD if kind == "ECHO" else
              {"durationMillis": 1000, "cycleAttempt": 1,
               "message": "Simulated work completed"})
    for index in range(count):
        ident = str(uuid.UUID(int=index + 1))
        requests.append({"index": index, "job_id": ident, "status": 202,
                         "client_ms": 10 + index, "scheduled_to_response_ms": 12 + index,
                         "dispatch_lag_ms": 2})
        jobs.append({
            "job_id": ident, "status": "COMPLETED", "attempt_count": 1,
            "created_at": "2026-10-25T00:00:00+00:00", "result": copy.deepcopy(result),
            "payload_bytes": 143 if kind == "ECHO" else 48,
            "result_bytes": 143 if kind == "ECHO" else 82,
            "attempts": [{"number": 1, "status": "COMPLETED",
                          "worker_id": "worker-" + str(index % workers + 1),
                          "started_at": f"2026-10-25T00:00:0{index + 1}+00:00",
                          "ended_at": f"2026-10-25T00:00:0{index + 2}+00:00",
                          "failure_type": None}],
        })
    at_end = {"db_now": "2026-10-25T00:00:03+00:00", "elapsed_s": 3,
              "stored": count, "completed": min(1, count), "failed": 0,
              "outbox_pending": max(0, count - 1), "connections": {"total": 9}}
    raw = {"requests": requests,
           "start": {"db_now": "2026-10-25T00:00:00+00:00", "completed": 0},
           "snapshots": [{"elapsed_s": 1, "stored": count, "completed": 0,
                          "failed": 0, "connections": {"total": 4}}],
           "at_end": at_end}
    final = {"stored": count, "completed": count, "failed": 0,
             "outbox_pending": 0, "jobs": jobs}
    return raw, final


def summarize(raw, final, kind="SIMULATED_WORK", rate=1, seconds=3, timed_out=False):
    return b.summarize(raw, final, kind, rate, seconds, 7, timed_out)


def comparison_row(name="two-workers"):
    config = copy.deepcopy(s.SCENARIOS[name])
    raw, final = cohort(workers=config["workers"], kind=config["kind"])
    summary = summarize(raw, final, config["kind"], config["rate"], 3 / config["rate"])
    return {"config": config,
            "metadata": {"run_id": "bench-test", "jar_sha256": "a" * 64,
                         "scaling_runner_sha256": "b" * 64,
                         "role_layout": "API/publisher plus separate workers",
                         "process_resource_cost": {"jvms": config["workers"] + 1}},
            "initial_database": {"jobs": 5, "database_bytes": 1000},
            "final_database": {"jobs": 8, "database_bytes": 2000},
            "summary": summary,
            "correctness": s.correctness(raw, final, summary, config["workers"]),
            "snapshots": raw["snapshots"], "at_end": raw["at_end"]}


class OfflineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.folder = Path(temporary.name)

        def forbidden(*args, **kwargs):
            raise AssertionError("Unmocked external operation in Docker-free scaling tests")

        for target, attribute in [(b, "command"), (s.subprocess, "Popen"),
                                  (s.subprocess, "run"), (s.urllib.request, "urlopen"),
                                  (b.IsolatedPlatform, "start"),
                                  (b.IsolatedPlatform, "close"), (b, "load"), (b, "drain")]:
            guard = patch.object(target, attribute, new=forbidden)
            guard.start()
            self.addCleanup(guard.stop)

    def platform(self, name="two-workers"):
        return s.ScalingPlatform(self.folder, "bench-test", copy.deepcopy(s.SCENARIOS[name]))


class RoleAndScenarioTests(OfflineTests):
    def test_api_disables_only_worker_and_sets_publisher_delay(self):
        for name, config in s.SCENARIOS.items():
            with self.subTest(scenario=name):
                self.assertEqual(self.platform(name).application_arguments(), [
                    "--platform.messaging.worker-enabled=false",
                    f"--platform.messaging.publisher-delay-ms={config['publisher_ms']}"])

    def test_workers_disable_publisher_and_pollers_and_set_local_slots(self):
        for name, config in s.SCENARIOS.items():
            with self.subTest(scenario=name):
                self.assertEqual(self.platform(name).worker_arguments(), [
                    "--platform.messaging.publisher-enabled=false",
                    "--platform.reliability.pollers-enabled=false",
                    f"--platform.messaging.worker-concurrency={config['slots']}"])

    def test_one_vs_two_workers_changes_only_worker_count(self):
        one, two = s.SCENARIOS["one-worker"], s.SCENARIOS["two-workers"]
        self.assertEqual(one["workers"], 1)
        self.assertEqual(two["workers"], 2)
        self.assertEqual(one["slots"], 10)
        self.assertEqual({**one, "workers": 2}, two)
        self.assertEqual((one["kind"], one["rate"], one["publisher_ms"]),
                         ("SIMULATED_WORK", 15, 1000))

    def test_equal_slot_control_changes_process_layout_not_workload(self):
        two = s.SCENARIOS["two-workers"]
        control = s.SCENARIOS["one-worker-20-slots"]
        self.assertEqual({**two, "workers": 1, "slots": 20}, control)
        self.assertEqual(two["workers"] * two["slots"],
                         control["workers"] * control["slots"])
        self.assertEqual(control["workers"] * control["slots"], 20)

    def test_overload_changes_only_rate_and_echo_changes_only_publisher_delay(self):
        self.assertEqual({**s.SCENARIOS["two-workers"], "rate": 25},
                         s.SCENARIOS["two-workers-overload"])
        default = s.SCENARIOS["echo-publisher-default"]
        self.assertEqual((default["kind"], default["rate"], default["publisher_ms"]),
                         ("ECHO", 15, 1000))
        self.assertEqual({**default, "publisher_ms": 100},
                         s.SCENARIOS["echo-publisher-faster"])
        self.assertEqual(json.loads(b.request_body("SIMULATED_WORK"))["payload"],
                         {"durationMillis": 1000, "failFirstAttempts": 0})

    def test_start_uses_separate_loopback_workers_and_accounts_for_extra_jvms(self):
        for name in ("one-worker", "two-workers", "one-worker-20-slots"):
            with self.subTest(scenario=name):
                instance = self.platform(name)

                def base_start(platform):
                    platform.java, platform.jar = "/mock/java", Path("/mock/app.jar")
                    platform.environment = {"MESSAGING_ENABLED": "true", "WORKER_ENABLED": "true",
                                            "PUBLISHER_ENABLED": "true", "DB_POOL_SIZE": "10"}
                    platform.http_port = 18080
                    platform.metadata = {"settings": {"prefetch_per_consumer": 1},
                                         "jar_sha256": "a" * 64, "runner_sha256": "b" * 64}
                    return platform

                children = []

                def spawn(*args, **kwargs):
                    child = Mock()
                    child.poll.return_value = None
                    children.append(child)
                    kwargs["stdout"].write(f"Tomcat started on port {18080 + len(children)}\n")
                    kwargs["stdout"].flush()
                    return child

                consumers = instance.config["workers"] * instance.config["slots"]
                with patch.object(b.IsolatedPlatform, "start", autospec=True,
                                  side_effect=base_start) as base, \
                        patch.object(s.subprocess, "Popen", side_effect=spawn) as popen, \
                        patch.object(instance, "wait_ready", side_effect=lambda operation: operation()), \
                        patch.object(instance, "broker_sample", return_value={"consumers": consumers}), \
                        patch.object(s.urllib.request, "urlopen", side_effect=lambda *a, **k:
                                     io.BytesIO(b'{"status":"UP"}')):
                    self.assertIs(instance.start(), instance)
                for log in instance.child_logs:
                    self.addCleanup(log.close)
                base.assert_called_once_with(instance)
                self.assertEqual(instance.children, children)
                self.assertEqual(popen.call_count, instance.config["workers"])
                self.assertEqual(instance.process_ports, [("api-publisher", 18080)] + [
                    (f"worker-{index + 1}", 18081 + index) for index in range(len(children))])
                for invocation in popen.call_args_list:
                    args = invocation.args[0]
                    self.assertEqual(args[:6], ["/mock/java", "-Xms256m", "-Xmx512m",
                                               "-XX:ActiveProcessorCount=2", "-jar", "/mock/app.jar"])
                    self.assertIn("--server.port=0", args)
                    self.assertIn("--server.address=127.0.0.1", args)
                    self.assertIn("--spring.config.location=classpath:/application.yml", args)
                    self.assertEqual(args[-3:], instance.worker_arguments())
                    self.assertIs(invocation.kwargs["env"], instance.environment)
                    self.assertEqual(invocation.kwargs["cwd"], b.ROOT)
                    self.assertEqual(invocation.kwargs["stderr"], subprocess.STDOUT)
                metadata = instance.metadata
                self.assertEqual(metadata["scaling"], instance.config)
                self.assertEqual(metadata["jar_sha256"], "a" * 64)
                self.assertEqual(metadata["runner_sha256"], "b" * 64)
                self.assertEqual(metadata["scaling_runner_sha256"],
                                 s.hashlib.sha256(Path(s.__file__).read_bytes()).hexdigest())
                self.assertEqual(metadata["settings"]["slots"], consumers)
                self.assertEqual(metadata["settings"]["workers"], len(children))
                self.assertEqual(metadata["settings"]["prefetch_per_consumer"], 1)
                self.assertEqual(metadata["settings"]["publisher_delay_ms"], instance.config["publisher_ms"])
                resources = metadata["process_resource_cost"]
                for key, per_jvm in (("jvms", 1), ("heap_max_mib", 512),
                                     ("primary_connection_max", 10), ("reserved_connection_max", 4)):
                    self.assertEqual(resources[key], per_jvm * (len(children) + 1))
                self.assertIn("not OS limit", resources["application_cpu_quota"])


class MetricAndObservationTests(OfflineTests):
    def test_metric_sums_labels_without_matching_prefixes_or_suffixes(self):
        text = ('# HELP jobs_worker_active description\n'
                'jobs_worker_active_extra 900\n'
                'other_jobs_worker_active 800\n'
                'jobs_worker_active_total 700\n'
                'jobs_worker_active{worker="one"} 2\n'
                'jobs_worker_active{worker="two with spaces"} 3.5\n')
        self.assertEqual(s.metric(text, "jobs_worker_active"), 5.5)
        self.assertEqual(s.metric("process_cpu_usage 2e-1\n", "process_cpu_usage"), .2)
        self.assertEqual(s.metric('a.b{x="1"} 4\naXb 50\n', "a.b"), 4)

    def test_absent_is_unknown_and_explicit_zero_is_zero(self):
        for text in ("", "# jobs_worker_active 8\n", "jobs_worker_active_total 9\n"):
            with self.subTest(text=text):
                self.assertIsNone(s.metric(text, "jobs_worker_active"))
        self.assertEqual(s.metric("jobs_worker_active 0\n", "jobs_worker_active"), 0)

    def test_malformed_matching_metric_is_not_manufactured_zero(self):
        with self.assertRaises(ValueError):
            s.metric("jobs_worker_active not-a-number\n", "jobs_worker_active")
        self.assertIsNone(s.metric("irrelevant not-a-number\n", "jobs_worker_active"))

    def test_optional_prometheus_timestamp_is_not_the_metric_value(self):
        self.assertEqual(s.metric('jobs_worker_active{worker="one"} 2 1792886400000\n',
                                  "jobs_worker_active"), 2)

    def test_sample_preserves_database_evidence_and_sums_process_metrics(self):
        instance = self.platform()
        instance.process_ports = [("api-publisher", 18080), ("worker-1", 18081)]
        instance.children = [Mock()]
        instance.children[0].poll.return_value = None
        row = {"stored": 3, "completed": 1}
        text = (b'jobs_worker_active 2\nhikaricp_connections_active{pool="primary"} 3\n'
                b'hikaricp_connections_active{pool="control"} 1\n'
                b'process_cpu_usage 0.25\njvm_memory_used_bytes{area="heap"} 100\n'
                b'jvm_memory_used_bytes{area="nonheap"} 40\n')
        with patch.object(b.IsolatedPlatform, "sample", return_value=row) as sample, \
                patch.object(instance, "sql", return_value={"total": 9, "active": 2, "waiting": 1}) as sql, \
                patch.object(s.urllib.request, "urlopen", side_effect=lambda *a, **k: io.BytesIO(text)) as http:
            observed = instance.sample("bench-0000-simulation-", details=True)
        sample.assert_called_once_with("bench-0000-simulation-", True)
        self.assertEqual(observed["stored"], 3)
        self.assertEqual(observed["connections"], {"total": 9, "active": 2, "waiting": 1})
        self.assertIn("pg_stat_activity", sql.call_args.args[0])
        self.assertIn("pid<>pg_backend_pid()", sql.call_args.args[0])
        self.assertEqual(http.call_args_list, [
            call("http://127.0.0.1:18080/actuator/prometheus", timeout=3),
            call("http://127.0.0.1:18081/actuator/prometheus", timeout=3)])
        for process, role in zip(observed["processes"], ("api-publisher", "worker-1")):
            self.assertEqual(process, {"role": role, "active_handlers": 2, "pool_active": 4,
                                       "pool_pending": None, "process_cpu": .25,
                                       "jvm_memory_used_bytes": 140})

    def test_http_observer_failure_is_explicit_and_other_process_is_still_sampled(self):
        instance = self.platform()
        instance.process_ports = [("api-publisher", 18080), ("worker-1", 18081)]
        with patch.object(b.IsolatedPlatform, "sample", return_value={}), \
                patch.object(instance, "sql", return_value={"total": 2}), \
                patch.object(s.urllib.request, "urlopen", side_effect=[OSError("offline"), io.BytesIO(b"")]):
            row = instance.sample("bench-0000-simulation-")
        self.assertEqual(row["processes"][0], {"role": "api-publisher", "observer_error": "OSError"})
        self.assertEqual(row["processes"][1]["role"], "worker-1")
        self.assertIsNone(row["processes"][1]["active_handlers"])

    def test_dead_worker_invalidates_run_before_observing_database_or_http(self):
        instance = self.platform()
        instance.children = [Mock()]
        instance.children[0].poll.return_value = 1
        with patch.object(b.IsolatedPlatform, "sample") as sample, patch.object(instance, "sql") as sql:
            with self.assertRaisesRegex(RuntimeError, "Worker exited"):
                instance.sample("bench-0000-simulation-")
        sample.assert_not_called()
        sql.assert_not_called()


class CorrectnessTests(OfflineTests):
    def verify(self, raw, final, workers=2, **options):
        return s.correctness(raw, final, summarize(raw, final, **options), workers)

    def test_complete_cohort_counts_each_worker_and_is_valid(self):
        raw, final = cohort()
        result = self.verify(raw, final)
        self.assertTrue(result["valid"])
        self.assertTrue(all(result["checks"].values()))
        self.assertEqual(result["attempts_by_worker"], {"worker-1": 2, "worker-2": 1})

    def test_duplicate_stored_ids_are_rejected(self):
        raw, final = cohort()
        final["jobs"].append(copy.deepcopy(final["jobs"][0]))
        result = self.verify(raw, final)
        self.assertFalse(result["checks"]["every_response_maps_to_exactly_one_stored_job"])
        self.assertFalse(result["valid"])

    def test_duplicate_response_ids_violate_one_to_one_mapping(self):
        raw, final = cohort()
        raw["requests"].append({**raw["requests"][0], "index": 3})
        result = self.verify(raw, final, seconds=4)
        self.assertFalse(result["checks"]["every_response_maps_to_exactly_one_stored_job"],
                         "Converting response IDs to a set must not hide duplicate acknowledgements")
        self.assertFalse(result["valid"])

    def test_missing_or_unexpected_stored_job_is_rejected(self):
        for unexpected in (False, True):
            with self.subTest(unexpected=unexpected):
                raw, final = cohort()
                if unexpected:
                    extra = copy.deepcopy(final["jobs"][0])
                    extra["job_id"] = str(uuid.UUID(int=99))
                    final["jobs"].append(extra)
                else:
                    final["jobs"].pop()
                result = self.verify(raw, final)
                self.assertFalse(result["checks"]["every_response_maps_to_exactly_one_stored_job"])
                self.assertFalse(result["valid"])

    def test_missing_worker_participation_is_rejected(self):
        raw, final = cohort(workers=1)
        result = self.verify(raw, final, workers=2)
        self.assertFalse(result["checks"]["all_workers_participated"])
        self.assertFalse(result["valid"])

    def test_retry_history_or_counter_cannot_pass_completed_once(self):
        for change in ("counter", "history", "both"):
            with self.subTest(change=change):
                raw, final = cohort()
                job = final["jobs"][0]
                if change != "history":
                    job["attempt_count"] = 2
                if change != "counter":
                    job["attempts"].append({**job["attempts"][0], "number": 2})
                result = self.verify(raw, final)
                self.assertFalse(result["checks"]["all_completed_once"])
                self.assertFalse(result["valid"])

    def test_incomplete_or_failed_jobs_and_missing_history_are_rejected(self):
        for state in ("QUEUED", "RUNNING", "RETRYING", "FAILED", "missing-history"):
            with self.subTest(state=state):
                raw, final = cohort()
                if state == "missing-history":
                    final["jobs"][0]["attempts"] = None
                else:
                    final["jobs"][0]["status"] = state
                result = self.verify(raw, final)
                self.assertFalse(result["checks"]["all_completed_once"])
                self.assertFalse(result["valid"])

    def test_wrong_echo_and_simulation_results_are_rejected(self):
        for kind in ("ECHO", "SIMULATED_WORK"):
            with self.subTest(kind=kind):
                raw, final = cohort(kind=kind)
                final["jobs"][0]["result"] = {"message": "wrong"}
                result = self.verify(raw, final, kind=kind)
                self.assertFalse(result["checks"]["all_results_correct"])
                self.assertFalse(result["valid"])

    def test_errors_unknown_commits_and_generator_drops_invalidate_measurement(self):
        for record in ({"error": "TimeoutError", "commit_unknown": True},
                       {"error": "http_error", "status": 503},
                       {"error": "generator_capacity_drop"}):
            with self.subTest(record=record):
                raw, final = cohort()
                raw["requests"].append({"index": 3, **record})
                result = self.verify(raw, final, seconds=4)
                self.assertFalse(result["checks"]["clean_measurement"])
                self.assertFalse(result["valid"])

    def test_drain_timeout_cannot_be_valid_even_if_last_snapshot_completed(self):
        raw, final = cohort()
        self.assertFalse(self.verify(raw, final, timed_out=True)["valid"])

    def test_missing_planned_arrival_cannot_be_a_clean_complete_cohort(self):
        raw, final = cohort()
        result = self.verify(raw, final, seconds=4)
        self.assertFalse(result["checks"]["clean_measurement"],
                         "Three successful responses are not evidence for four planned arrivals")
        self.assertFalse(result["valid"])

    def test_empty_cohort_does_not_claim_worker_success(self):
        raw, final = cohort(count=0)
        result = self.verify(raw, final)
        self.assertFalse(result["valid"])
        self.assertEqual(result["attempts_by_worker"], {})
        self.assertFalse(result["checks"]["all_workers_participated"])


class ReportTests(OfflineTests):
    def test_summary_excludes_drain_and_subtracts_initial_completions(self):
        raw, final = cohort()
        raw["start"]["completed"] = 1
        raw["at_end"]["completed"] = 2
        result = summarize(raw, final)
        self.assertEqual(result["planned_requests"], 3)
        self.assertEqual(result["acknowledged_202"], 3)
        self.assertEqual(result["database_completion_window_seconds"], 3)
        self.assertEqual(result["completed_during_window"], 1)
        self.assertAlmostEqual(result["completion_rate_during_window"], 1 / 3)
        self.assertEqual(result["acknowledged_jobs_per_offered_second"], 1)
        self.assertEqual(result["final_completed"], 3)
        self.assertEqual(result["drain_seconds"], 7)
        self.assertEqual(result["unfinished_at_window_end"], 1)
        self.assertEqual(result["creation_to_first_start_ms_APPROX"]["p95"], 3000)
        self.assertEqual(result["recorded_attempt_ms"]["p95"], 1000)

    def test_comparison_retains_metadata_and_uses_total_slots_window_rates_and_sampled_peak(self):
        results = {"started_utc": "2026-10-25T00:00:00+00:00",
                   "scenarios": {"two-workers": comparison_row()}}
        before = copy.deepcopy(results)
        s.write_comparison(self.folder, results)
        self.assertEqual(json.loads((self.folder / "comparison.json").read_text()), results)
        self.assertEqual(results, before)
        markdown = (self.folder / "comparison.md").read_text()
        self.assertIn("| two-workers | 20 | 15 | 0.33 | 3.00 | 2 | 9 |", markdown)
        self.assertIn("adding JVMs adds resources", markdown)
        self.assertIn("includes precommit", markdown)
        self.assertIn("no exact commit-to-start/HA/1,000/s claim", markdown)
        self.assertIn("scaling-backlog.svg", markdown)

    def test_svg_plots_stored_minus_completed_minus_failed_not_drained_totals(self):
        row = comparison_row()
        row["snapshots"][0]["failed"] = 1
        s.write_comparison(self.folder, {"scenarios": {"two-workers": row}})
        root = ET.parse(self.folder / "scaling-backlog.svg").getroot()
        polylines = root.findall("{http://www.w3.org/2000/svg}polyline")
        self.assertEqual(len(polylines), 1)
        self.assertEqual(polylines[0].attrib["points"], "343.3,60.0 910.0,60.0")
        self.assertEqual(row["summary"]["final_completed"], 3)
        self.assertEqual(row["at_end"]["completed"], 1)

    def test_empty_comparison_does_not_fabricate_samples(self):
        results = {"scenarios": {}}
        s.write_comparison(self.folder, results)
        self.assertEqual(json.loads((self.folder / "comparison.json").read_text()), results)
        root = ET.parse(self.folder / "scaling-backlog.svg").getroot()
        self.assertEqual(root.findall("{http://www.w3.org/2000/svg}polyline"), [])

    def test_invalid_correctness_is_visible_in_markdown_not_only_raw_json(self):
        row = comparison_row()
        row["correctness"]["checks"]["all_results_correct"] = False
        row["correctness"]["valid"] = False
        s.write_comparison(self.folder, {"scenarios": {"two-workers": row}})
        stored = json.loads((self.folder / "comparison.json").read_text())
        self.assertFalse(stored["scenarios"]["two-workers"]["correctness"]["valid"])
        self.assertRegex((self.folder / "comparison.md").read_text().lower(),
                         r"invalid|failed correctness|correctness[^\n]*false",
                         "A failed scenario must not look like an ordinary valid comparison")

    def test_no_first_start_samples_are_reportable_without_inventing_zero_latency(self):
        row = comparison_row()
        row["summary"]["creation_to_first_start_ms_APPROX"] = b.distribution([])
        row["correctness"]["valid"] = False
        s.write_comparison(self.folder, {"scenarios": {"two-workers": row}})
        stored = json.loads((self.folder / "comparison.json").read_text())
        self.assertIsNone(stored["scenarios"]["two-workers"]["summary"]
                          ["creation_to_first_start_ms_APPROX"]["p95"])
        self.assertTrue((self.folder / "comparison.md").is_file())
        ET.parse(self.folder / "scaling-backlog.svg")


class MainFlowTests(OfflineTests):
    def arguments(self, *scenarios):
        return ["scaling.py", "--output", str(self.folder), "--seconds", "0.2",
                "--warmup-seconds", "0.1", "--drain-seconds", "5",
                "--workers", "2", "--max-pending", "3", "--scenarios", *scenarios]

    def run_mocked(self, raw, final, scenarios=("one-worker",), warm_timeout=False):
        instance = Mock()
        instance.metadata = {"run_id": "bench-test", "settings": {"workers": 1, "slots": 10}}
        instance.inventory.side_effect = [{"jobs": 5}, {"jobs": 8}]
        output = io.StringIO()
        with patch.object(s, "ScalingPlatform", return_value=instance) as factory, \
                patch.object(b, "load", side_effect=[{"requests": []}, raw]) as load, \
                patch.object(b, "drain", side_effect=[({"failed": 0}, 0, warm_timeout),
                                                     (final, 7, False)]), \
                patch("sys.argv", self.arguments(*scenarios)), redirect_stdout(output):
            try:
                s.main()
            finally:
                self.instance, self.factory, self.load, self.output = instance, factory, load, output
        return instance

    def test_duplicate_scenarios_are_rejected_before_creating_suite_or_platform(self):
        with patch.object(s, "ScalingPlatform") as factory, \
                patch("sys.argv", self.arguments("one-worker", "one-worker")):
            with self.assertRaisesRegex(ValueError, "unique"):
                s.main()
        factory.assert_not_called()
        self.assertEqual(list(self.folder.iterdir()), [])

    def test_invalid_bounds_are_rejected_before_creating_suite_or_platform(self):
        for option, value in (("--seconds", "0"), ("--warmup-seconds", "61"),
                              ("--drain-seconds", "301"), ("--workers", "0"),
                              ("--max-pending", "129")):
            with self.subTest(option=option):
                args = self.arguments("one-worker")
                args[args.index(option) + 1] = value
                with patch.object(s, "ScalingPlatform") as factory, patch("sys.argv", args):
                    with self.assertRaises(ValueError):
                        s.main()
                factory.assert_not_called()
                self.assertEqual(list(self.folder.iterdir()), [])

    def test_success_retains_evidence_and_routes_generator_options_not_worker_count(self):
        raw, final = cohort(workers=1)
        instance = self.run_mocked(raw, final)
        instance.start.assert_called_once_with()
        instance.close.assert_called_once_with()
        self.assertEqual(instance.inventory.call_count, 2)
        self.assertEqual(self.load.call_count, 2)
        warm, measured = self.load.call_args_list
        self.assertTrue(warm.args[1].endswith("simulation-warmup-"))
        self.assertEqual(warm.args[2:], ("SIMULATED_WORK", 5, .1, 2, 3))
        self.assertEqual(warm.kwargs, {"observe": False})
        self.assertTrue(measured.args[1].endswith("simulation-"))
        self.assertEqual(measured.args[2:], ("SIMULATED_WORK", 15, .2, 2, 3))
        reports = list(self.folder.glob("scaling-*/comparison.json"))
        self.assertEqual(len(reports), 1)
        row = json.loads(reports[0].read_text())["scenarios"]["one-worker"]
        self.assertTrue(row["correctness"]["valid"])
        self.assertEqual(row["metadata"], instance.metadata)
        self.assertEqual(row["initial_database"], {"jobs": 5})
        self.assertEqual(row["final_database"], {"jobs": 8})
        case = reports[0].parent / "one-worker"
        self.assertEqual(json.loads((case / "requests.json").read_text()), raw)
        self.assertEqual(json.loads((case / "final.json").read_text()), final)
        self.assertEqual(json.loads((case / "metadata.json").read_text()), instance.metadata)
        self.assertIn("Comparison:", self.output.getvalue())

    def test_bad_result_retains_failed_evidence_closes_and_stops_before_next_scenario(self):
        raw, final = cohort(workers=1)
        final["jobs"][0]["result"] = {"wrong": True}
        with self.assertRaisesRegex(RuntimeError, "correctness/arrival"):
            self.run_mocked(raw, final, ("one-worker", "two-workers"))
        self.instance.close.assert_called_once_with()
        self.factory.assert_called_once()
        self.assertEqual(self.load.call_count, 2)
        report = next(self.folder.glob("scaling-*/comparison.json"))
        rows = json.loads(report.read_text())["scenarios"]
        self.assertEqual(set(rows), {"one-worker"})
        self.assertFalse(rows["one-worker"]["correctness"]["valid"])
        self.assertNotIn("Comparison:", self.output.getvalue())

    def test_warmup_timeout_closes_and_never_offers_measured_load(self):
        raw, final = cohort(workers=1)
        with self.assertRaisesRegex(RuntimeError, "warm-up"):
            self.run_mocked(raw, final, warm_timeout=True)
        self.instance.close.assert_called_once_with()
        self.assertEqual(self.load.call_count, 1)
        self.assertEqual(list(self.folder.glob("scaling-*/comparison.json")), [])

    def test_incomplete_offered_cohort_does_not_print_success(self):
        raw, final = cohort(workers=1, count=2)
        # The CLI schedules ceil(15 * .2) == 3, but only two arrivals are recorded.
        with self.assertRaisesRegex(RuntimeError, "correctness/arrival"):
            self.run_mocked(raw, final)
        self.instance.close.assert_called_once_with()
        self.assertNotIn("Comparison:", self.output.getvalue())


class CleanupTests(OfflineTests):
    def test_all_children_are_terminated_and_reaped_before_logs_and_base_cleanup(self):
        instance = self.platform()
        events = Mock()
        first, second, exited = Mock(), Mock(), Mock()
        first.poll.return_value = second.poll.return_value = None
        exited.poll.return_value = 0
        first.wait.side_effect = [subprocess.TimeoutExpired("first", 20), 0]
        logs = [Mock(), Mock()]
        instance.children, instance.child_logs = [first, second, exited], logs
        for name, child in (("first", first), ("second", second), ("exited", exited),
                            ("log1", logs[0]), ("log2", logs[1])):
            events.attach_mock(child, name)
        with patch.object(b.IsolatedPlatform, "close", autospec=True) as base:
            events.attach_mock(base, "base")
            instance.close()
        self.assertEqual(events.mock_calls, [
            call.first.poll(), call.first.terminate(), call.second.poll(), call.second.terminate(),
            call.exited.poll(), call.first.wait(timeout=20), call.first.kill(), call.first.wait(timeout=5),
            call.second.wait(timeout=20), call.exited.wait(timeout=20),
            call.log1.close(), call.log2.close(), call.base(instance)])
        exited.terminate.assert_not_called()
        second.kill.assert_not_called()

    def test_partial_start_with_no_children_still_runs_base_cleanup(self):
        instance = self.platform()
        instance.child_logs = [Mock()]
        with patch.object(b.IsolatedPlatform, "close", autospec=True) as base:
            instance.close()
        instance.child_logs[0].close.assert_called_once_with()
        base.assert_called_once_with(instance)

    def test_one_unreapable_child_does_not_skip_waiting_for_remaining_children(self):
        instance = self.platform()
        first, second, log = Mock(), Mock(), Mock()
        first.poll.return_value = second.poll.return_value = None
        first.wait.side_effect = [subprocess.TimeoutExpired("first", 20),
                                  subprocess.TimeoutExpired("first", 5)]
        instance.children, instance.child_logs = [first, second], [log]
        reaped_before_base = []

        def base_close(platform):
            reaped_before_base.append(second.wait.called)

        with patch.object(b.IsolatedPlatform, "close", autospec=True, side_effect=base_close):
            with self.assertRaises(subprocess.TimeoutExpired):
                instance.close()
        second.terminate.assert_called_once_with()
        log.close.assert_called_once_with()
        self.assertEqual(reaped_before_base, [True],
                         "Base cleanup must not tear dependencies down before attempting every child wait")
        second.wait.assert_called_once_with(timeout=20)


if __name__ == "__main__":
    unittest.main()