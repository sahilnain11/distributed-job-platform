#!/usr/bin/env python3
"""Role-separated local scaling experiments; no development-service changes.

Reuses Phase 7's bounded arrivals, clock boundaries, SQL evidence and owned cleanup.
Each scenario gets NEW dependencies and one API/publisher JVM plus worker JVMs.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
import urllib.request
import uuid

import benchmark as b

SCENARIOS = {
    "one-worker": dict(workers=1, slots=10, kind="SIMULATED_WORK", rate=15, publisher_ms=1000),
    "two-workers": dict(workers=2, slots=10, kind="SIMULATED_WORK", rate=15, publisher_ms=1000),
    "one-worker-20-slots": dict(workers=1, slots=20, kind="SIMULATED_WORK", rate=15, publisher_ms=1000),
    "two-workers-overload": dict(workers=2, slots=10, kind="SIMULATED_WORK", rate=25, publisher_ms=1000),
    "echo-publisher-default": dict(workers=2, slots=10, kind="ECHO", rate=15, publisher_ms=1000),
    "echo-publisher-faster": dict(workers=2, slots=10, kind="ECHO", rate=15, publisher_ms=100),
}


def metric(text, name):
    values = []
    for line in text.splitlines():
        match = re.fullmatch(re.escape(name) + r'(?:\{.*\})?\s+(\S+)(?:\s+\S+)?', line)
        if match:
            values.append(float(match.group(1)))
    return sum(values) if values else None


class ScalingPlatform(b.IsolatedPlatform):
    def __init__(self, folder, run_id, config):
        super().__init__(folder, run_id)
        self.config = config
        self.children = []
        self.child_logs = []
        self.process_ports = []

    def application_arguments(self):
        return ["--platform.messaging.worker-enabled=false",
                f"--platform.messaging.publisher-delay-ms={self.config['publisher_ms']}"]

    def worker_arguments(self):
        return ["--platform.messaging.publisher-enabled=false", "--platform.reliability.pollers-enabled=false",
                f"--platform.messaging.worker-concurrency={self.config['slots']}"]

    def start(self):
        super().start()
        self.process_ports.append(("api-publisher", self.http_port))
        for number in range(self.config["workers"]):
            name = f"worker-{number+1}"
            path = self.folder / f"{name}.log"
            log = path.open("w")
            self.child_logs.append(log)
            child = subprocess.Popen([self.java, "-Xms256m", "-Xmx512m", "-XX:ActiveProcessorCount=2",
                                      "-jar", str(self.jar), "--server.port=0", "--server.address=127.0.0.1",
                                      "--spring.config.location=classpath:/application.yml", *self.worker_arguments()],
                                     cwd=b.ROOT, env=self.environment, stdout=log, stderr=subprocess.STDOUT)
            self.children.append(child)

            def worker_port():
                if child.poll() is not None:
                    raise RuntimeError(f"Scaling {name} exited; inspect {path}")
                found = re.search(r"Tomcat started on port (\d+)", path.read_text())
                return int(found.group(1)) if found else None

            port = self.wait_ready(worker_port)
            self.wait_ready(lambda: json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/actuator/health", timeout=5))["status"] == "UP")
            self.process_ports.append((name, port))
        self.wait_ready(lambda: self.broker_sample().get("consumers") == self.config["workers"] * self.config["slots"])
        self.metadata.update(
            scaling=self.config,
            scaling_runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            role_layout="One API/publisher/retry-recovery JVM, worker JVMs with publishing/pollers disabled",
            process_resource_cost={"jvms": len(self.process_ports), "heap_max_mib": 512 * len(self.process_ports),
                                   "primary_connection_max": 10 * len(self.process_ports),
                                   "reserved_connection_max": 4 * len(self.process_ports),
                                   "application_cpu_quota": "none, ActiveProcessorCount=2 per JVM is not OS limit"})
        self.metadata["settings"].update(workers=self.config["workers"], slots=self.config["slots"] * self.config["workers"],
                                         publisher_delay_ms=self.config["publisher_ms"])
        return self

    def capture_failure(self, failure):
        (self.folder / "failure.json").write_text(json.dumps({"type": type(failure).__name__,
            "message": str(failure), "valid_measurement": False}, indent=2) + "\n")
        for label, ident in [("postgres", self.postgres), ("rabbit", self.rabbit)]:
            try:
                info = json.loads(b.command("docker", "inspect", ident))[0]
                (self.folder / f"{label}-state.json").write_text(json.dumps(info["State"], indent=2) + "\n")
                logs = subprocess.run(["docker", "logs", "--tail", "300", ident], text=True, capture_output=True, timeout=10)
                (self.folder / f"{label}.log").write_text(logs.stdout + logs.stderr)
            except (OSError, subprocess.SubprocessError):
                pass

    def sample(self, prefix, details=False):
        for child in self.children:
            if child.poll() is not None:
                raise RuntimeError("Worker exited during scaling run; measurement is invalid")
        row = super().sample(prefix, details)
        row["connections"] = self.sql("""SELECT json_build_object('total',count(*),
            'active',count(*) FILTER (WHERE state='active'),
            'waiting',count(*) FILTER (WHERE wait_event_type='Lock'))
            FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()""")
        row["processes"] = []
        for name, port in self.process_ports:
            try:
                text = urllib.request.urlopen(f"http://127.0.0.1:{port}/actuator/prometheus", timeout=3).read().decode()
                row["processes"].append({"role": name, "active_handlers": metric(text, "jobs_worker_active"),
                                         "pool_active": metric(text, "hikaricp_connections_active"),
                                         "pool_pending": metric(text, "hikaricp_connections_pending"),
                                         "process_cpu": metric(text, "process_cpu_usage"),
                                         "jvm_memory_used_bytes": metric(text, "jvm_memory_used_bytes")})
            except OSError as failure:
                row["processes"].append({"role": name, "observer_error": type(failure).__name__})
        return row

    def close(self):
        failures = []
        try:
            for child in self.children:
                try:
                    if child.poll() is None:
                        child.terminate()
                except OSError as error:
                    failures.append(error)
            for child in self.children:
                try:
                    child.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    try:
                        child.kill()
                        child.wait(timeout=5)
                    except (OSError, subprocess.SubprocessError) as error:
                        failures.append(error)
                except (OSError, subprocess.SubprocessError) as error:
                    failures.append(error)
        finally:
            for log in self.child_logs:
                log.close()
            super().close()
        if failures:
            raise failures[0]


def correctness(raw, final, summary, expected_workers):
    response_ids = [r["job_id"] for r in raw["requests"] if "job_id" in r]
    expected_ids = set(response_ids)
    stored_ids = [j["job_id"] for j in final["jobs"]]
    attempt_workers = collections.Counter(a["worker_id"] for job in final["jobs"] for a in job["attempts"] or [])
    checks = {"clean_measurement": summary["measurement_valid_without_missing_arrivals"],
              "every_response_maps_to_exactly_one_stored_job": len(response_ids) == len(expected_ids)
                  and len(stored_ids) == len(set(stored_ids)) and set(stored_ids) == expected_ids,
              "all_completed_once": all(job["status"] == "COMPLETED" and job["attempt_count"] == 1
                                         and len(job["attempts"] or []) == 1 for job in final["jobs"]),
              "all_results_correct": summary["wrong_results"] == 0,
              "all_workers_participated": len(attempt_workers) == expected_workers}
    return {"checks": checks, "attempts_by_worker": dict(attempt_workers), "valid": all(checks.values())}


def write_comparison(folder, results):
    (folder / "comparison.json").write_text(json.dumps(results, indent=2) + "\n")
    lines = ["# Local scaling comparison", "", "Same dependency limits; adding JVMs adds resources, not free speed.", "",
             "| Scenario | Worker slots | Offered/s | Completed/s | Start p95 APPROX s | Unfinished | DB connections max sampled |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for name, row in results["scenarios"].items():
        s, c = row["summary"], row["config"]
        samples = row["snapshots"] + [row["at_end"]]
        peak = max(sample["connections"]["total"] for sample in samples)
        first_start = s['creation_to_first_start_ms_APPROX']['p95']
        formatted_start = f"{first_start/1000:.2f}" if first_start is not None else "unknown"
        lines.append(f"| {name} | {c['workers'] * c['slots']} | {c['rate']} | {s['completion_rate_during_window']:.2f} | "
                     f"{formatted_start} | {s['unfinished_at_window_end']} | {peak} |")
        if not row["correctness"]["valid"]:
            lines.append(f"\n**INVALID — {name}: failed correctness/arrival checks; do not interpret as capacity evidence.**\n")
    lines += ["", "Creation-to-start includes precommit time; no exact commit-to-start/HA/1,000/s claim.", "",
              "![Scaling backlog](scaling-backlog.svg)", ""]
    (folder / "comparison.md").write_text("\n".join(lines))
    svg = ['<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="450" viewBox="0 0 1000 450">',
           '<rect width="1000" height="450" fill="#0f172a"/><text x="35" y="30" fill="white" font-size="20">Scaling: unfinished stored jobs during offered load</text>']
    colors = ["#fb923c", "#4ade80", "#38bdf8", "#e879f9", "#facc15", "#f8fafc"]
    rows = [(name, r["snapshots"] + [r["at_end"]]) for name, r in results["scenarios"].items()]
    maximum = max([1] + [s["stored"] - s["completed"] - s["failed"] for _, samples in rows for s in samples])
    xmax = max([1] + [s["elapsed_s"] for _, samples in rows for s in samples])
    for index, (name, samples) in enumerate(rows):
        points = " ".join(f"{60+s['elapsed_s']/xmax*850:.1f},{300-(s['stored']-s['completed']-s['failed'])/maximum*240:.1f}" for s in samples)
        svg += [f'<polyline points="{points}" fill="none" stroke="{colors[index]}" stroke-width="3"/>',
                f'<text x="{60+(index%2)*460}" y="{355+(index//2)*32}" fill="{colors[index]}" font-size="15">{name}</text>']
    svg += ['<path d="M60 55V300H925" fill="none" stroke="#94a3b8"/>',
            f'<text x="5" y="65" fill="white">{maximum}</text>',
            f'<text x="835" y="325" fill="white">{xmax:.0f} seconds</text>', '</svg>']
    (folder / "scaling-backlog.svg").write_text("\n".join(svg))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--warmup-seconds", type=float, default=10)
    parser.add_argument("--drain-seconds", type=float, default=180)
    parser.add_argument("--workers", type=int, default=16, help="HTTP request threads, NOT worker JVM count")
    parser.add_argument("--max-pending", type=int, default=32)
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS, default=list(SCENARIOS))
    parser.add_argument("--output", type=Path, default=b.ROOT / "target/scaling")
    args = parser.parse_args()
    b.validate_args(args)
    if len(set(args.scenarios)) != len(args.scenarios):
        raise ValueError("Each scenario must be unique")
    suite = args.output.resolve() / ("scaling-" + str(uuid.uuid4()))
    suite.mkdir(parents=True, exist_ok=False)
    results = {"started_utc": dt.datetime.now(dt.timezone.utc).isoformat(), "scenarios": {}}
    for name in args.scenarios:
        config = SCENARIOS[name]
        folder = suite / name
        folder.mkdir()
        run_id = "bench-" + str(uuid.uuid4())
        instance = ScalingPlatform(folder, run_id, config)
        try:
            instance.start()
            (folder / "metadata.json").write_text(json.dumps(instance.metadata, indent=2) + "\n")
            case = "echo" if config["kind"] == "ECHO" else "simulation"
            prefix = f"{run_id}-{case}-"
            warm = b.load(instance, prefix + "warmup-", config["kind"], 5, args.warmup_seconds, args.workers, args.max_pending, observe=False)
            warm_final, _, warm_timeout = b.drain(instance, prefix + "warmup-", args.drain_seconds)
            if warm_timeout or warm_final["failed"] or any("error" in r for r in warm["requests"]):
                raise RuntimeError("Scaling warm-up did not complete cleanly")
            initial = instance.inventory()
            print(f"{name}: {config}; warm-up drained, offering load for {args.seconds:g}s", flush=True)
            raw = b.load(instance, prefix, config["kind"], config["rate"], args.seconds, args.workers, args.max_pending)
            (folder / "requests.json").write_text(json.dumps(raw, indent=2) + "\n")
            final, drained, timed_out = b.drain(instance, prefix, args.drain_seconds)
            (folder / "final.json").write_text(json.dumps(final, indent=2) + "\n")
            summary = b.summarize(raw, final, config["kind"], config["rate"], args.seconds, drained, timed_out)
            verification = correctness(raw, final, summary, config["workers"])
            row = dict(config=config, metadata=instance.metadata, initial_database=initial,
                       final_database=instance.inventory(), summary=summary, correctness=verification,
                       snapshots=raw["snapshots"], at_end=raw["at_end"])
            results["scenarios"][name] = row
            write_comparison(suite, results)
            print(json.dumps({"scenario": name, "summary": summary, "correctness": verification}), flush=True)
            if not verification["valid"]:
                raise RuntimeError("Scaling experiment failed correctness/arrival checks; evidence retained")
        except Exception as failure:
            if hasattr(instance, "postgres") and hasattr(instance, "rabbit"):
                instance.capture_failure(failure)
            raise
        finally:
            instance.close()
            print(f"Scenario evidence: {folder}", flush=True)
    print(f"Comparison: {suite}", flush=True)


if __name__ == "__main__":
    main()