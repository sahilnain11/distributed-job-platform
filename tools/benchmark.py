#!/usr/bin/env python3
"""Bounded, open-loop learning experiments against NEW disposable infrastructure.

No dependency faults, schema changes, or requests target development services.
The only deletion is this invocation's label-verified disposable containers/volumes.
Python 3.9+, Docker, Java 21 and an already-built executable JAR are required.
"""
from __future__ import annotations

import argparse
import base64
import concurrent.futures
import datetime as dt
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
LABEL = "job-platform.benchmark"
PAYLOAD = {"message": "x" * 128}
CASES = {"echo": ("ECHO", 50), "simulation": ("SIMULATED_WORK", 5),
         "overload": ("SIMULATED_WORK", 15)}


def command(*args, input_text=None, timeout=30):
    return subprocess.run(args, input=input_text, text=True, capture_output=True,
                          timeout=timeout, check=True).stdout.strip()


def percentile(values, fraction):
    """Nearest-rank sample percentile. Empty is unknown, never a manufactured zero."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)] if ordered else None


def distribution(values):
    return {"samples": len(values), "p50": percentile(values, .5),
            "p95": percentile(values, .95), "p99": percentile(values, .99),
            "max": max(values) if values else None}


def timestamp(value):
    # PostgreSQL JSON trims fractional trailing zeroes. Python 3.9's ISO parser
    # accepts only 3/6 fractional digits, unlike newer Python versions.
    normalized = value.replace("Z", "+00:00")
    normalized = re.sub(r"\.(\d+)(?=[+-]\d{2}:\d{2}$)",
                        lambda match: "." + match.group(1).ljust(6, "0")[:6], normalized)
    return dt.datetime.fromisoformat(normalized)


def request_body(kind):
    payload = PAYLOAD if kind == "ECHO" else {"durationMillis": 1000, "failFirstAttempts": 0}
    return json.dumps({"type": kind, "payload": payload}, separators=(",", ":")).encode()


def offered_count(rate, seconds):
    return math.ceil(rate * seconds)


def validate_args(args):
    if not 0 < args.seconds <= 600 or not 0 < args.warmup_seconds <= 60:
        raise ValueError("Measured duration must be (0,600] seconds, warm-up (0,60]")
    if not 1 <= args.workers <= 64 or not args.workers <= args.max_pending <= 128:
        raise ValueError("Require 1..64 request workers and workers <= max-pending <= 128")
    if not 0 < args.drain_seconds <= 300:
        raise ValueError("Drain timeout must be (0,300] seconds")
    if hasattr(args, "cases") and len(set(args.cases)) != len(args.cases):
        raise ValueError("Duplicate cases would replay request keys; run each case only once per invocation")


class IsolatedPlatform:
    def __init__(self, folder, run_id):
        self.folder, self.run_id = folder, run_id
        self.ids = []
        self.app = None
        self.log = None

    def container(self, service, image, ports, environment, mounts=()):
        args = ["docker", "run", "-d", "--name", f"{self.run_id}-{service}",
                "--label", f"{LABEL}={self.run_id}", "--cpus", "1", "--memory", "768m"]
        for port in ports:
            args += ["-p", f"127.0.0.1::{port}"]
        for key, value in environment.items():
            args += ["-e", f"{key}={value}"]
        for source, dest in mounts:
            args += ["-v", f"{source}:{dest}:ro"]
        ident = command(*args, image, timeout=120)
        self.ids.append(ident)
        return ident

    def port(self, ident, internal):
        info = json.loads(command("docker", "inspect", ident))[0]
        return int(info["NetworkSettings"]["Ports"][f"{internal}/tcp"][0]["HostPort"])

    def sql(self, query):
        text = command("docker", "exec", "-i", self.postgres, "psql", "-XAt", "-v", "ON_ERROR_STOP=1",
                       "-U", "job_platform", "-d", "job_platform", input_text=query, timeout=15)
        return json.loads(text)

    def wait_ready(self, operation, timeout=90):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            if self.app and self.app.poll() is not None:
                raise RuntimeError(f"Benchmark application exited; inspect {self.folder / 'application.log'}")
            try:
                result = operation()
                if result:
                    return result
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                last = type(error).__name__
            threading.Event().wait(.2)  # Bounded readiness check, not a fixed startup assumption.
        raise TimeoutError(f"Disposable service readiness deadline exceeded ({last})")

    def start(self):
        jar = ROOT / "target/distributed-job-platform-0.0.1-SNAPSHOT.jar"
        if not jar.is_file():
            raise ValueError("Build the executable JAR with the existing Verify project task first")
        java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if os.environ.get("JAVA_HOME") else shutil.which("java")
        version = subprocess.run([java, "-version"], capture_output=True, text=True, check=True).stderr
        if not re.search(r'version "21[.\"]', version):
            raise ValueError("Select Java 21 before running benchmarks")
        self.postgres = self.container("postgres", "postgres:17.6", [5432], {
            "POSTGRES_DB": "job_platform", "POSTGRES_USER": "job_platform",
            "POSTGRES_PASSWORD": "disposable-benchmark-only"})
        self.rabbit = self.container("rabbit", "rabbitmq:4.1.8-management", [5672, 15672], {
            "RABBITMQ_DEFAULT_USER": "job_platform", "RABBITMQ_DEFAULT_PASS": "disposable-benchmark-only"},
            [(ROOT / "docker/rabbitmq.conf", "/etc/rabbitmq/rabbitmq.conf")])
        self.wait_ready(lambda: self.sql("SELECT 1") == 1)
        self.wait_ready(lambda: command("docker", "exec", self.rabbit, "rabbitmq-diagnostics", "-q", "check_running", timeout=10) is not None)
        env = {key: value for key, value in os.environ.items()
               if key in {"PATH", "HOME", "JAVA_HOME", "LANG", "LC_ALL", "TZ", "TMPDIR"}}
        # Do not inherit tuning flags or accidentally connect to development services.
        env.update(DB_URL=f"jdbc:postgresql://127.0.0.1:{self.port(self.postgres,5432)}/job_platform",
                   DB_USERNAME="job_platform", DB_PASSWORD="disposable-benchmark-only", DB_POOL_SIZE="10",
                   RABBITMQ_HOST="127.0.0.1", RABBITMQ_PORT=str(self.port(self.rabbit,5672)),
                   RABBITMQ_USERNAME="job_platform", RABBITMQ_PASSWORD="disposable-benchmark-only",
                   MESSAGING_ENABLED="true", PUBLISHER_ENABLED="true", WORKER_ENABLED="true")
        self.java, self.jar, self.environment = java, jar, env
        self.log = (self.folder / "application.log").open("w")
        self.app = subprocess.Popen([java, "-Xms256m", "-Xmx512m", "-XX:ActiveProcessorCount=2", "-jar", str(jar),
                                     "--server.port=0", "--server.address=127.0.0.1",
                                     "--spring.config.location=classpath:/application.yml", *self.application_arguments()],
                                    cwd=ROOT, env=env, stdout=self.log, stderr=subprocess.STDOUT)

        def app_port():
            content = (self.folder / "application.log").read_text()
            found = re.search(r"Tomcat started on port (\d+)", content)
            return int(found.group(1)) if found else None
        self.http_port = self.wait_ready(app_port)
        self.base = f"http://127.0.0.1:{self.http_port}"
        self.wait_ready(lambda: json.load(urllib.request.urlopen(self.base + "/actuator/health", timeout=5))["status"] == "UP")
        info = json.loads(command("docker", "info", "--format", "{{json .}}"))
        self.metadata = {
            "run_id": self.run_id, "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "host": {"os": platform.platform(), "architecture": platform.machine(), "logical_cpus": os.cpu_count(),
                     "memory_bytes": command("sysctl", "-n", "hw.memsize") if platform.system() == "Darwin" else "not collected"},
            "docker": {k: info.get(k) for k in ["ServerVersion", "Architecture", "NCPU", "MemTotal", "OperatingSystem"]},
            "java": version.splitlines()[0], "jar_sha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
            "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "resources": {"postgres": "1 CPU / 768 MiB", "rabbitmq": "1 CPU / 768 MiB",
                          "application": "host JVM, 256..512 MiB heap, ActiveProcessorCount=2 (NOT OS CPU limit)",
                          "generator": "same host, Python stdlib threads, bounded outstanding requests"},
            "settings": {"workers": 1, "slots": 10, "prefetch_per_consumer": 1, "primary_pool": 10,
                         "control_pool": 4, "publisher_batch": 100, "publisher_delay_ms": 1000,
                         "lease_seconds": 30, "heartbeat_ms": 10000, "max_runtime_ms": 300000,
                         "db_snapshot_ms": 10000, "pollers": "retry 1s / recovery 10s", "logs": "ECS INFO to file"},
            "versions": {"spring_boot": "3.5.16", "postgres": "17.6", "rabbitmq": "4.1.8-management"},
            "isolation": "New label-owned containers/random localhost ports/empty DB; development services untouched",
            "interference": "Existing development app, PostgreSQL, RabbitMQ, Prometheus and Grafana share this machine",
        }
        self.metadata["containers"] = [{"image": row["Config"]["Image"], "id": row["Id"],
                                        "memory_bytes": row["HostConfig"]["Memory"], "nano_cpus": row["HostConfig"]["NanoCpus"]}
                                       for row in json.loads(command("docker", "inspect", *self.ids))]
        return self

    def application_arguments(self):
        return []

    def inventory(self):
        return self.sql("""SELECT json_build_object('jobs', (SELECT count(*) FROM job_platform.jobs),
            'attempts', (SELECT count(*) FROM job_platform.job_attempts),
            'outbox', (SELECT count(*) FROM job_platform.outbox),
            'database_bytes', pg_database_size(current_database()))""")

    def sample(self, prefix, details=False):
        assert re.fullmatch(r"bench-[a-f0-9-]+-(?:echo|simulation|overload)(?:-warmup)?-", prefix), prefix
        selection = f"""WITH selected AS (SELECT j.*, i.idempotency_key AS request_key FROM job_platform.jobs j
            JOIN job_platform.idempotency_keys i ON i.response_body->>'jobId'=j.job_id::text
                        WHERE i.operation_scope='SUBMIT_JOB' AND i.idempotency_key LIKE '{prefix}%'
                            AND i.idempotency_key NOT LIKE '{prefix}warmup-%') """
        snapshot = self.sql(selection + """SELECT json_build_object('db_now', clock_timestamp(), 'stored', count(*),
            'completed', count(*) FILTER (WHERE status='COMPLETED'), 'failed', count(*) FILTER (WHERE status='FAILED'),
            'queued', count(*) FILTER (WHERE status='QUEUED'), 'running', count(*) FILTER (WHERE status='RUNNING'),
            'retrying', count(*) FILTER (WHERE status='RETRYING'),
            'outbox_pending', (SELECT count(*) FROM job_platform.outbox WHERE NOT published AND job_id IN (SELECT job_id FROM selected))) FROM selected""")
        if details:
            snapshot["jobs"] = self.sql(selection + """SELECT coalesce(json_agg(json_build_object(
                'job_id',j.job_id, 'request_key',j.request_key, 'status',j.status, 'created_at',j.created_at, 'completed_at',j.completed_at,
                'attempt_count',j.attempt_count, 'result',r.result,
                'payload_bytes',octet_length(p.payload::text), 'result_bytes',octet_length(r.result::text),
                'attempts',(SELECT json_agg(json_build_object('number',a.attempt_number,'status',a.status,'worker_id',a.worker_id,
                    'started_at',a.started_at,'ended_at',a.ended_at,'failure_type',a.failure_type) ORDER BY a.attempt_number)
                    FROM job_platform.job_attempts a WHERE a.job_id=j.job_id))), '[]'::json)
                FROM selected j JOIN job_platform.job_payloads p USING(job_id)
                LEFT JOIN job_platform.job_results r USING(job_id)""")
        return snapshot

    def broker_sample(self):
        url = f"http://127.0.0.1:{self.port(self.rabbit,15672)}/api/queues/%2F/jobs.queue"
        auth = base64.b64encode(b"job_platform:disposable-benchmark-only").decode()
        try:
            row = json.load(urllib.request.urlopen(urllib.request.Request(url, headers={"Authorization": "Basic " + auth}), timeout=3))
            return {key: row.get(key) for key in ["messages_ready", "messages_unacknowledged", "consumers"]}
        except OSError as error:
            return {"observer_error": type(error).__name__}

    def close(self):
        errors = []
        if self.app and self.app.poll() is None:
            self.app.terminate()
            try:
                self.app.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.app.kill()
                self.app.wait(timeout=5)
        if self.log:
            self.log.close()
        # Also discover creation that succeeded on Docker but whose CLI response
        # timed out. Always restrict to this unguessable run's exact label.
        try:
            owned = command("docker", "ps", "-aq", "--no-trunc", "--filter", f"label={LABEL}={self.run_id}").split()
        except (OSError, subprocess.SubprocessError) as error:
            owned = []
            errors.append(type(error).__name__)
        for ident in dict.fromkeys(owned + self.ids):
            try:
                record = json.loads(command("docker", "inspect", ident))[0]
                if record["Config"]["Labels"].get(LABEL) != self.run_id:
                    raise RuntimeError("Refusing cleanup of a container not owned by this experiment")
                command("docker", "rm", "-f", "-v", ident)
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                errors.append(f"{ident}: {type(error).__name__}")
        if errors:
            (self.folder / "cleanup-errors.json").write_text(json.dumps(errors, indent=2) + "\n")
            if sys.exc_info()[0] is None:
                raise RuntimeError(f"Incomplete disposable cleanup: {errors}")
            print(f"Additional cleanup errors (original failure preserved): {errors}", flush=True)


def submit(port, prefix, index, body, scheduled, zero, window_end):
    started = time.monotonic()
    record = {"index": index, "scheduled_s": scheduled - zero,
              "dispatch_lag_ms": (started - scheduled) * 1000}
    if started >= window_end or started - scheduled > 1:
        record["error"] = "generator_dispatch_deadline_drop"
        return record
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.request("POST", "/jobs", body=body, headers={"Content-Type": "application/json", "Idempotency-Key": prefix + str(index)})
        response = connection.getresponse()
        raw = response.read()
        record.update(status=response.status, response_bytes=len(raw))
        if response.status == 202:
            parsed = json.loads(raw)
            if not isinstance(parsed, dict) or not isinstance(parsed.get("jobId"), str):
                raise ValueError("Invalid accepted-response envelope")
            record["job_id"] = str(uuid.UUID(parsed["jobId"]))
        else:
            record["error"] = "http_error"
    except (OSError, ValueError, KeyError, http.client.HTTPException) as error:
        record["error"] = type(error).__name__
        record["commit_unknown"] = True  # Never count a network exception as a proven rejection.
    finally:
        connection.close()
    record["client_ms"] = (time.monotonic() - started) * 1000
    record["scheduled_to_response_ms"] = (time.monotonic() - scheduled) * 1000
    return record


def load(platform, prefix, kind, rate, seconds, workers, max_pending, observe=True):
    """Fixed arrival schedule; bounded in-flight work, no unbounded executor queue."""
    body = request_body(kind)
    gate = threading.BoundedSemaphore(max_pending)
    records, futures, samples = [], [], []
    db_start = platform.sample(prefix)
    zero = time.monotonic()
    next_sample = zero + 5
    observation = None

    def observe_state():
        sample = platform.sample(prefix)
        sample.update(elapsed_s=time.monotonic() - zero, broker=platform.broker_sample())
        return sample

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor, \
            concurrent.futures.ThreadPoolExecutor(max_workers=1) as observers:
        for index in range(offered_count(rate, seconds)):
            due = zero + index / rate
            delay = due - time.monotonic()
            if delay > 0:
                threading.Event().wait(delay)  # Actual arrival pacing, not a throughput-dependent closed loop.
            if time.monotonic() - due > 1:
                records.append({"index": index, "error": "generator_late_drop"})
            elif not gate.acquire(blocking=False):
                records.append({"index": index, "error": "generator_capacity_drop"})
            else:
                future = executor.submit(submit, platform.http_port, prefix, index, body, due, zero, zero + seconds)
                future.add_done_callback(lambda done: gate.release())
                futures.append(future)
            if observe and time.monotonic() >= next_sample:
                if observation is None or observation.done():
                    if observation is not None:
                        samples.append(observation.result())
                    observation = observers.submit(observe_state)
                next_sample += 5
        remaining = zero + seconds - time.monotonic()
        if remaining > 0:
            threading.Event().wait(remaining)
        at_end = platform.sample(prefix)
        at_end.update(elapsed_s=time.monotonic() - zero)
        # Snapshot BEFORE waiting for late request responses. Do not turn drain
        # completions into measured-window completions.
        records.extend(future.result() for future in futures)
        if observation is not None:
            samples.append(observation.result())
    return {"requests": sorted(records, key=lambda r: r["index"]), "snapshots": samples,
            "start": db_start, "at_end": at_end, "submission_tail_s": max(0, time.monotonic() - zero - seconds)}


def drain(platform, prefix, timeout):
    zero = time.monotonic()
    while True:
        row = platform.sample(prefix)
        if row["stored"] == row["completed"] + row["failed"] and row["outbox_pending"] == 0:
            return platform.sample(prefix, details=True), time.monotonic() - zero, False
        if time.monotonic() - zero >= timeout:
            return platform.sample(prefix, details=True), time.monotonic() - zero, True
        threading.Event().wait(2)  # Bounded state-based drain check.


def summarize(raw, final, kind, rate, seconds, drain_seconds, timed_out):
    accepted = [r for r in raw["requests"] if "job_id" in r]
    jobs = final["jobs"]
    delays, durations = [], []
    for job in jobs:
        history = job["attempts"] or []
        if history:
            delays.append((timestamp(history[0]["started_at"]) - timestamp(job["created_at"])).total_seconds() * 1000)
        for attempt in history:
            if attempt["ended_at"]:
                durations.append((timestamp(attempt["ended_at"]) - timestamp(attempt["started_at"])).total_seconds() * 1000)
    errors = {}
    for record in raw["requests"]:
        if "error" in record:
            key = str(record.get("status", record["error"]))
            errors[key] = errors.get(key, 0) + 1
    window = (timestamp(raw["at_end"]["db_now"]) - timestamp(raw["start"]["db_now"])).total_seconds()
    wrong_results = sum(job["status"] == "COMPLETED" and job["result"] != (PAYLOAD if kind == "ECHO" else
                        {"durationMillis": 1000, "cycleAttempt": 1, "message": "Simulated work completed"}) for job in jobs)
    return {
        "kind": kind, "offered_rate": rate, "offered_seconds": seconds, "planned_requests": offered_count(rate, seconds),
        "acknowledged_202": len(accepted), "stored_jobs": final["stored"], "errors": errors,
        "unknown_commits": sum(r.get("commit_unknown", False) for r in raw["requests"]),
        "acknowledged_jobs_per_offered_second": len(accepted) / seconds,
        "client_acceptance_ms": distribution([r["client_ms"] for r in accepted]),
        "scheduled_to_response_ms": distribution([r["scheduled_to_response_ms"] for r in accepted]),
        "dispatch_lag_ms": distribution([r["dispatch_lag_ms"] for r in raw["requests"] if "dispatch_lag_ms" in r]),
        "creation_to_first_start_ms_APPROX": distribution(delays),
        "approx_started_within_5s_fraction": sum(d <= 5000 for d in delays) / final["stored"] if final["stored"] else None,
        "never_started_at_final_observation": final["stored"] - len(delays),
        "recorded_attempt_ms": distribution(durations), "database_completion_window_seconds": window,
        "completed_during_window": raw["at_end"]["completed"] - raw["start"]["completed"],
        "completion_rate_during_window": (raw["at_end"]["completed"] - raw["start"]["completed"]) / window,
        "unfinished_at_window_end": raw["at_end"]["stored"] - raw["at_end"]["completed"] - raw["at_end"]["failed"],
        "outbox_pending_at_window_end": raw["at_end"]["outbox_pending"],
        "final_completed": final["completed"], "final_failed": final["failed"],
        "wrong_results": wrong_results, "drain_seconds": drain_seconds, "drain_timed_out": timed_out,
        "payload_utf8_bytes_jsonb": sorted({j["payload_bytes"] for j in jobs}),
        "result_utf8_bytes_jsonb": sorted({j["result_bytes"] for j in jobs if j["result_bytes"] is not None}),
        "request_utf8_bytes": len(request_body(kind)),
        "acceptance_p99_below_200ms_observed": percentile([r["client_ms"] for r in accepted], .99) < 200 if accepted else None,
        "scope": "Single fixed-duration local run, includes cold samples after warmup; not a capacity limit, exact commit delay, or availability estimate",
        "measurement_valid_without_missing_arrivals": not errors and not timed_out and final["stored"] == len(accepted)
            and len(raw["requests"]) == offered_count(rate, seconds)
            and {r["index"] for r in raw["requests"]} == set(range(offered_count(rate, seconds))),
    }


def write_reports(folder, report):
    (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    lines = ["# Isolated learning benchmark", "", f"Run: {report['metadata']['run_id']}", "",
             "Raw request/history samples are in each case JSON. No production data was modified.", "",
             "| Case | Offered/s | 202 jobs | Client p99 ms | Completed/s in window | Unfinished at end | Approx first-start p95 ms |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for case, item in report["cases"].items():
        s = item["summary"]
        lines.append(f"| {case} | {s['offered_rate']} | {s['acknowledged_202']} | {s['client_acceptance_ms']['p99']} | "
                     f"{s['completion_rate_during_window']:.2f} | {s['unfinished_at_window_end']} | {s['creation_to_first_start_ms_APPROX']['p95']} |")
    lines += ["", "First-start is creation-to-claim, including acceptance precommit time, NOT exact commit-to-start.",
              "Completion window is independently bracketed by database samples; drain is excluded.",
              "Unknown commit responses and dropped/late generator arrivals are retained, never silently retried.",
              "No 1,000/s capacity or 99.9% availability claim. Shared-host resource interference applies.", "",
              "![Measured backlog](backlog.svg)", ""]
    (folder / "report.md").write_text("\n".join(lines))
    svg = ['<svg xmlns="http://www.w3.org/2000/svg" width="960" height="350" viewBox="0 0 960 350">',
           '<rect width="960" height="350" fill="#0f172a"/><text x="35" y="32" fill="#f8fafc" font-size="20">Unfinished accepted jobs during the offered-load window</text>']
    colors = ["#38bdf8", "#4ade80", "#fb923c"]
    all_samples = [(name, data["snapshots"] + [data["at_end"]]) for name, data in report["cases"].items()]
    ymax = max([1] + [r["stored"] - r["completed"] - r["failed"] for _, rows in all_samples for r in rows])
    xmax = max([1] + [r["elapsed_s"] for _, rows in all_samples for r in rows])
    for index, (name, rows) in enumerate(all_samples):
        color = colors[index % len(colors)]
        points = " ".join(f"{55+r['elapsed_s']/xmax*850:.1f},{280-(r['stored']-r['completed']-r['failed'])/ymax*220:.1f}" for r in rows)
        svg += [f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="3"/>',
                f'<text x="{70+index*280}" y="330" fill="{color}" font-size="16">{name}</text>']
    svg += ['<path d="M55 55V280H915" stroke="#94a3b8" fill="none"/>',
            f'<text x="8" y="70" fill="#cbd5e1">{ymax}</text>',
            f'<text x="840" y="305" fill="#cbd5e1">{xmax:.0f} seconds</text>', '</svg>']
    (folder / "backlog.svg").write_text("\n".join(svg))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--warmup-seconds", type=float, default=10)
    parser.add_argument("--drain-seconds", type=float, default=120)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--max-pending", type=int, default=32)
    parser.add_argument("--cases", nargs="+", choices=CASES, default=list(CASES))
    parser.add_argument("--output", type=Path, default=ROOT / "target/benchmarks")
    args = parser.parse_args()
    validate_args(args)
    run_id = "bench-" + str(uuid.uuid4())
    folder = args.output.resolve() / run_id
    folder.mkdir(parents=True, exist_ok=False)
    instance = IsolatedPlatform(folder, run_id)
    report = {"cases": {}}
    try:
        instance.start()
        report["metadata"] = instance.metadata
        report["metadata"]["generator_options"] = vars(args) | {"output": str(args.output)}
        write_reports(folder, report)
        for name in args.cases:
            kind, rate = CASES[name]
            prefix = f"{run_id}-{name}-"
            before_warmup = instance.inventory()
            warm = load(instance, prefix + "warmup-", kind, min(rate, 10 if kind == "ECHO" else 5),
                        args.warmup_seconds, args.workers, args.max_pending, observe=False)
            warm_final, _, warm_timeout = drain(instance, prefix + "warmup-", args.drain_seconds)
            if warm_timeout or warm_final["failed"] or any("error" in r for r in warm["requests"]):
                raise RuntimeError("Warm-up did not complete cleanly; refusing a misleading baseline")
            initial = instance.inventory()
            print(f"{name}: warm-up drained; offering {rate}/s for {args.seconds:g}s", flush=True)
            raw = load(instance, prefix, kind, rate, args.seconds, args.workers, args.max_pending)
            (folder / f"{name}-requests.json").write_text(json.dumps(raw, indent=2) + "\n")
            final, drain_seconds, timed_out = drain(instance, prefix, args.drain_seconds)
            # Preserve raw evidence even if later report generation fails.
            (folder / f"{name}-raw.json").write_text(json.dumps({"raw": raw, "final": final}, indent=2) + "\n")
            raw["initial_database"] = initial
            raw["database_before_warmup"] = before_warmup
            raw["warmup"] = {"requests": len(warm["requests"]), "seconds": args.warmup_seconds,
                             "final": {k: v for k, v in warm_final.items() if k != "jobs"}}
            raw["summary"] = summarize(raw, final, kind, rate, args.seconds, drain_seconds, timed_out)
            raw["final"] = final
            raw["final_database"] = instance.inventory()
            report["cases"][name] = raw
            (folder / f"{name}.json").write_text(json.dumps(raw, indent=2) + "\n")
            write_reports(folder, report)
            print(json.dumps(raw["summary"]), flush=True)
            if timed_out:
                raise RuntimeError("Drain deadline exceeded; unresolved jobs are reported, no subsequent case started")
            if raw["summary"]["unknown_commits"]:
                raise RuntimeError("Unknown request outcomes remain observations, not proven rejection; refusing subsequent cases")
    finally:
        instance.close()
        print(f"Reports/logs: {folder}", flush=True)


if __name__ == "__main__":
    main()