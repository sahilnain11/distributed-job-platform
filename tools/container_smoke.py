#!/usr/bin/env python3
"""Validate container packaging in a unique, disposable Compose project only."""
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import urllib.request
import uuid

import benchmark as b


def main():
    project = "job-platform-smoke-" + str(uuid.uuid4())
    folder = b.ROOT / "target/container-smoke" / project
    folder.mkdir(parents=True)
    env = {key: value for key, value in os.environ.items() if key in {"PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT"}}
    env["WORKERS_API_PORT"] = "0"
    compose = ["docker", "compose", "--env-file", "/dev/null", "-p", project,
               "-f", str(b.ROOT / "compose.workers.yaml")]

    def run(*args, input_text=None, timeout=60):
        return subprocess.run(args, input=input_text, text=True, capture_output=True,
                              timeout=timeout, check=True, env=env, cwd=b.ROOT).stdout.strip()

    def await_condition(operation, seconds=90):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                value = operation()
                if value:
                    return value
            except (OSError, ValueError, subprocess.SubprocessError):
                pass
            threading.Event().wait(.3)
        raise TimeoutError("Disposable container smoke condition failed")

    try:
        run(*compose, "up", "-d", "--build", "--scale", "worker=2", "--wait", timeout=360)
        api = run(*compose, "ps", "-q", "api")
        record = json.loads(run("docker", "inspect", api))[0]
        port = record["NetworkSettings"]["Ports"]["8080/tcp"][0]["HostPort"]
        base = f"http://127.0.0.1:{port}"
        await_condition(lambda: json.load(urllib.request.urlopen(base + "/actuator/health", timeout=3))["status"] == "UP")

        def sql(query):
            return json.loads(run(*compose, "exec", "-T", "postgres", "psql", "-XAt", "-v", "ON_ERROR_STOP=1",
                                  "-U", "job_platform", "-d", "job_platform", input_text=query))

        # Wait for actual broker consumer registration, not merely a running container.
        await_condition(lambda: int(run(*compose, "exec", "-T", "rabbitmq", "rabbitmqctl", "list_queues", "-q", "consumers").splitlines()[-1]) == 20)
        items = [{"type": "SIMULATED_WORK", "payload": {"durationMillis": 1000, "failFirstAttempts": 0}} for _ in range(20)]
        request = urllib.request.Request(base + "/jobs/batch", data=json.dumps({"items": items}).encode(),
                                         headers={"Content-Type": "application/json", "Idempotency-Key": project})
        with urllib.request.urlopen(request, timeout=10) as response:
            assert response.status == 202
            accepted = json.load(response)
        assert accepted["acceptedCount"] == 20
        await_condition(lambda: sql("SELECT count(*) FROM job_platform.jobs WHERE status='COMPLETED'") == 20)
        evidence = sql("""SELECT json_build_object('jobs',(SELECT count(*) FROM job_platform.jobs),
            'attempts',(SELECT count(*) FROM job_platform.job_attempts),
            'worker_ids',(SELECT count(DISTINCT worker_id) FROM job_platform.job_attempts),
            'results',(SELECT count(*) FROM job_platform.job_results),
            'bad_results',(SELECT count(*) FROM job_platform.job_results WHERE result IS DISTINCT FROM
                '{"durationMillis":1000,"cycleAttempt":1,"message":"Simulated work completed"}'::jsonb),
            'max_attempt_count',(SELECT max(attempt_count) FROM job_platform.jobs))""")
        assert evidence == {"jobs": 20, "attempts": 20, "worker_ids": 2, "results": 20, "bad_results": 0, "max_attempt_count": 1}, evidence
        ids = run(*compose, "ps", "-q", "api", "worker").split()
        containers = json.loads(run("docker", "inspect", *ids))
        assert len(containers) == 3
        for row in containers:
            assert row["Config"]["User"] == "10001:10001"
            assert row["HostConfig"]["ReadonlyRootfs"]
            assert row["HostConfig"]["Memory"] == 512 * 1024 * 1024
            assert row["HostConfig"]["NanoCpus"] == 1_000_000_000
        report = {"project": project, "evidence": evidence, "application_containers": len(containers),
                  "nonroot_readonly_resource_limits_verified": True,
                  "runtime_java": run(*compose, "exec", "-T", "api", "java", "--version"),
                  "image_id": record["Image"], "api_host_port": int(port)}
        (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
    finally:
        # Only this invocation's project can be removed. Never down the default
        # development project or the separately persistent worker deployment.
        assert project.startswith("job-platform-smoke-") and str(uuid.UUID(project.removeprefix("job-platform-smoke-")))
        owned = run("docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={project}").split()
        if owned:
            for row in json.loads(run("docker", "inspect", *owned)):
                assert row["Config"]["Labels"]["com.docker.compose.project"] == project
            try:
                (folder / "compose.log").write_text(run(*compose, "logs", "--no-color", timeout=30))
            except subprocess.SubprocessError as failure:
                (folder / "log-capture-error.txt").write_text(type(failure).__name__ + "\n")
        run(*compose, "down", "-v", "--remove-orphans", timeout=90)
        print(f"Removed only disposable project {project}; evidence: {folder}")


if __name__ == "__main__":
    main()