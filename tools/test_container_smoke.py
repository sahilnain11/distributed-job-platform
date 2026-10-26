"""Docker-free smoke-runner contracts; only temporary evidence files are written."""
import copy
from contextlib import redirect_stdout
import io
import itertools
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, call, patch
import uuid

import container_smoke as smoke


class JsonResponse(io.BytesIO):
    def __init__(self, value, status=200):
        super().__init__(json.dumps(value).encode())
        self.status = status


class ContainerSmokeTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.environment = {
            "PATH": "/mock/bin", "HOME": "/mock/home",
            "DOCKER_HOST": "unix:///mock/docker.sock", "DOCKER_CONTEXT": "mock-context",
            "WORKERS_API_PORT": "8080", "COMPOSE_PROJECT_NAME": "development",
            "COMPOSE_FILE": "development.yaml", "DB_PASSWORD": "must-not-inherit",
            "JAVA_TOOL_OPTIONS": "must-not-inherit", "WORKER_CONCURRENCY": "999",
        }
        self.expected_evidence = {
            "jobs": 20, "attempts": 20, "worker_ids": 2,
            "results": 20, "bad_results": 0, "max_attempt_count": 1,
        }
        self.evidence = dict(self.expected_evidence)
        self.ids = ("api-id", "worker-one-id", "worker-two-id")
        self.owned = self.ids
        self.containers = [{
            "Config": {"User": "10001:10001"},
            "HostConfig": {"ReadonlyRootfs": True, "Memory": 512 * 1024 * 1024,
                           "NanoCpus": 1_000_000_000},
        } for _ in self.ids]
        self.record = {
            "Image": "sha256:mock-image",
            "NetworkSettings": {"Ports": {"8080/tcp": [{"HostPort": "49173"}]}},
        }
        self.health = ["UP"]
        self.consumers = ["20"]
        self.completed = [20]
        self.batch_status = 202
        self.accepted_count = 20
        self.start_error = None
        self.evidence_error = None
        self.owner_override = None
        self.events = Mock()

        def install(target, attribute, **kwargs):
            replacement = patch.object(target, attribute, **kwargs)
            result = replacement.start()
            self.addCleanup(replacement.stop)
            return result

        install(smoke.b, "ROOT", new=self.root)
        environment = patch.dict(smoke.os.environ, self.environment, clear=True)
        environment.start()
        self.addCleanup(environment.stop)
        self.run = install(smoke.subprocess, "run", side_effect=self.fake_run)
        self.http = install(smoke.urllib.request, "urlopen", side_effect=self.fake_http)
        self.events.attach_mock(self.run, "run")
        self.events.attach_mock(self.http, "http")
        # Unexpected process launches fail closed; readiness never sleeps or uses real time.
        install(smoke.subprocess, "Popen", side_effect=AssertionError("Unmocked process launch"))
        install(smoke.time, "monotonic", side_effect=itertools.count())
        self.event = install(smoke.threading, "Event")

    @staticmethod
    def next_value(values):
        value = values.pop(0) if len(values) > 1 else values[0]
        if isinstance(value, BaseException):
            raise value
        return value

    def fake_run(self, args, **kwargs):
        if args[:2] == ("docker", "compose"):
            project = args[args.index("-p") + 1]
            command = args[args.index("-f") + 2:]
            if command[:1] == ("up",):
                self.project = project
                if self.start_error:
                    raise self.start_error
                output = "started"
            elif command == ("ps", "-q", "api"):
                output = self.ids[0]
            elif command == ("ps", "-q", "api", "worker"):
                output = "\n".join(self.ids)
            elif command[:4] == ("exec", "-T", "postgres", "psql"):
                query = kwargs["input"]
                if query == "SELECT count(*) FROM job_platform.jobs WHERE status='COMPLETED'":
                    output = json.dumps(self.next_value(self.completed))
                elif query.startswith("SELECT json_build_object("):
                    if self.evidence_error:
                        raise self.evidence_error
                    output = json.dumps(self.evidence)
                else:
                    raise RuntimeError(f"Unexpected SQL: {query}")
            elif command == ("exec", "-T", "rabbitmq", "rabbitmqctl",
                              "list_queues", "-q", "consumers"):
                output = self.next_value(self.consumers)
            elif command == ("exec", "-T", "api", "java", "--version"):
                output = "openjdk 21 mock runtime"
            elif command == ("logs", "--no-color"):
                output = "mock project logs"
            elif command == ("down", "-v", "--remove-orphans"):
                output = "removed"
            else:
                raise RuntimeError(f"Unexpected Compose operation: {args}")
        elif args[:3] == ("docker", "ps", "-aq"):
            output = "\n".join(self.owned)
        elif args[:2] == ("docker", "inspect"):
            if args[2:] == (self.ids[0],):
                output = json.dumps([self.record])
            elif args[2:] in (self.ids, self.owned):
                rows = copy.deepcopy(self.containers)
                for row in rows:
                    row["Config"]["Labels"] = {
                        "com.docker.compose.project": self.owner_override or self.project,
                    }
                output = json.dumps(rows)
            else:
                raise RuntimeError(f"Unexpected inspected IDs: {args}")
        else:
            raise RuntimeError(f"Unexpected external command: {args}")
        return subprocess.CompletedProcess(args, 0, stdout=output + "\n", stderr="")

    def fake_http(self, request, **kwargs):
        if isinstance(request, str):
            if request != "http://127.0.0.1:49173/actuator/health":
                raise RuntimeError(f"Unexpected health URL: {request}")
            return JsonResponse({"status": self.next_value(self.health)})
        if request.full_url != "http://127.0.0.1:49173/jobs/batch":
            raise RuntimeError(f"Unexpected batch URL: {request.full_url}")
        return JsonResponse({"acceptedCount": self.accepted_count}, self.batch_status)

    def invoke(self):
        self.first_call = self.run.call_count
        self.output = io.StringIO()
        with redirect_stdout(self.output):
            smoke.main()

    @property
    def invocations(self):
        return self.run.call_args_list[self.first_call:]

    @property
    def compose(self):
        return ("docker", "compose", "--env-file", "/dev/null", "-p", self.project,
                "-f", str(self.root / "compose.workers.yaml"))

    @property
    def folder(self):
        return self.root / "target/container-smoke" / self.project

    def assert_cleanup(self):
        commands = [invocation.args[0] for invocation in self.invocations]
        self.assertTrue(self.project.startswith("job-platform-smoke-"))
        identifier = uuid.UUID(self.project.removeprefix("job-platform-smoke-"))
        self.assertEqual(identifier.version, 4)
        self.assertEqual(str(identifier), self.project.removeprefix("job-platform-smoke-"))
        for command in commands:
            if command[:2] == ("docker", "compose"):
                self.assertEqual(command[:len(self.compose)], self.compose)
        down = self.compose + ("down", "-v", "--remove-orphans")
        self.assertEqual(commands.count(down), 1)
        self.assertEqual(commands[-1], down)
        self.assertEqual(self.invocations[-1].kwargs["timeout"], 90)
        discovery = ("docker", "ps", "-aq", "--filter",
                     f"label=com.docker.compose.project={self.project}")
        self.assertEqual(commands.count(discovery), 1)
        cleanup = commands[commands.index(discovery):]
        if self.owned:
            self.assertEqual(cleanup, [discovery, ("docker", "inspect", *self.owned),
                                      self.compose + ("logs", "--no-color"), down])
            self.assertEqual((self.folder / "compose.log").read_text(), "mock project logs")
            self.assertEqual(self.invocations[-2].kwargs["timeout"], 30)
        else:
            self.assertEqual(cleanup, [discovery, down])
            self.assertFalse((self.folder / "compose.log").exists())

    def assert_no_success_report(self):
        self.assertFalse((self.folder / "report.json").exists())
        self.assertNotIn('"nonroot_readonly_resource_limits_verified"', self.output.getvalue())

    def test_each_invocation_uses_a_fresh_uuid_project_and_evidence_directory(self):
        projects = []
        for _ in range(2):
            self.invoke()
            self.assert_cleanup()
            projects.append(self.project)
            self.assertTrue((self.folder / "report.json").is_file())
        self.assertNotEqual(*projects)
        self.assertEqual({item.name for item in (self.root / "target/container-smoke").iterdir()},
                         set(projects))

    def test_all_commands_use_dev_null_env_file_and_sanitized_port_zero_environment(self):
        self.invoke()
        self.assert_cleanup()
        expected = {key: self.environment[key] for key in
                    ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT")}
        expected["WORKERS_API_PORT"] = "0"
        for invocation in self.invocations:
            with self.subTest(command=invocation.args[0]):
                self.assertEqual(invocation.kwargs["env"], expected)
                self.assertEqual(invocation.kwargs["cwd"], self.root)
                self.assertTrue(invocation.kwargs["check"])
                self.assertTrue(invocation.kwargs["text"])
                self.assertTrue(invocation.kwargs["capture_output"])
                self.assertIn("input", invocation.kwargs)

    def test_startup_builds_only_the_isolated_project_with_two_workers_and_wait(self):
        self.invoke()
        startup = self.invocations[0]
        self.assertEqual(startup.args[0], self.compose +
                         ("up", "-d", "--build", "--scale", "worker=2", "--wait"))
        self.assertEqual(startup.kwargs["timeout"], 360)
        self.assert_cleanup()

    def test_discovered_loopback_health_and_twenty_consumers_precede_submission(self):
        self.invoke()
        self.assertEqual(self.http.call_args_list[0],
                         call("http://127.0.0.1:49173/actuator/health", timeout=3))
        consumer = call.run(self.compose + ("exec", "-T", "rabbitmq", "rabbitmqctl",
                                            "list_queues", "-q", "consumers"),
                            **self.run.call_args_list[3].kwargs)
        events = self.events.mock_calls
        health_index = next(i for i, item in enumerate(events) if item[0] == "http")
        consumer_index = events.index(consumer)
        batch_index = next(i for i, item in enumerate(events)
                           if item[0] == "http" and not isinstance(item.args[0], str))
        self.assertLess(health_index, consumer_index)
        self.assertLess(consumer_index, batch_index)
        self.assertEqual(self.invocations[1].args[0], self.compose + ("ps", "-q", "api"))
        self.assertEqual(self.invocations[2].args[0], ("docker", "inspect", "api-id"))

    def test_batch_contains_twenty_one_second_jobs_and_project_idempotency_key(self):
        self.invoke()
        request = self.http.call_args_list[1].args[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Content-type"), "application/json")
        self.assertEqual(request.get_header("Idempotency-key"), self.project)
        self.assertEqual(json.loads(request.data), {"items": [
            {"type": "SIMULATED_WORK", "payload": {"durationMillis": 1000, "failFirstAttempts": 0}}
            for _ in range(20)]})
        self.assertEqual(self.http.call_args_list[1].kwargs, {"timeout": 10})

    def test_sql_checks_twenty_completions_one_attempt_two_workers_and_correct_results(self):
        self.invoke()
        sql = [item for item in self.invocations if item.kwargs["input"] is not None]
        self.assertEqual(len(sql), 2)
        for item in sql:
            self.assertEqual(item.args[0], self.compose + (
                "exec", "-T", "postgres", "psql", "-XAt", "-v", "ON_ERROR_STOP=1",
                "-U", "job_platform", "-d", "job_platform"))
        self.assertEqual(sql[0].kwargs["input"],
                         "SELECT count(*) FROM job_platform.jobs WHERE status='COMPLETED'")
        query = " ".join(sql[1].kwargs["input"].split())
        for fragment in (
                "'jobs',(SELECT count(*) FROM job_platform.jobs)",
                "'attempts',(SELECT count(*) FROM job_platform.job_attempts)",
                "'worker_ids',(SELECT count(DISTINCT worker_id) FROM job_platform.job_attempts)",
                "'results',(SELECT count(*) FROM job_platform.job_results)",
                "'bad_results',(SELECT count(*) FROM job_platform.job_results WHERE "
                "result IS DISTINCT FROM",
                "'max_attempt_count',(SELECT max(attempt_count) FROM job_platform.jobs)"):
            self.assertIn(fragment, query)
        report = json.loads((self.folder / "report.json").read_text())
        self.assertEqual(report["evidence"], self.expected_evidence)

    def test_success_report_records_inspected_image_port_java_and_security_checks(self):
        self.invoke()
        report = json.loads((self.folder / "report.json").read_text())
        self.assertEqual(report, {
            "project": self.project, "evidence": self.expected_evidence,
            "application_containers": 3, "nonroot_readonly_resource_limits_verified": True,
            "runtime_java": "openjdk 21 mock runtime", "image_id": "sha256:mock-image",
            "api_host_port": 49173,
        })
        commands = [item.args[0] for item in self.invocations]
        application_ps = self.compose + ("ps", "-q", "api", "worker")
        index = commands.index(application_ps)
        self.assertEqual(commands[index + 1], ("docker", "inspect", *self.ids))
        self.assertIn(json.dumps(report, indent=2), self.output.getvalue())
        self.assert_cleanup()

    def assert_resource_rejected(self, section, field, bad_value):
        original = copy.deepcopy(self.containers)
        for index in range(3):
            with self.subTest(container=index, field=field):
                self.containers = copy.deepcopy(original)
                self.containers[index][section][field] = bad_value
                with self.assertRaises(AssertionError):
                    self.invoke()
                self.assert_cleanup()
                self.assert_no_success_report()

    def test_every_application_container_must_run_as_nonroot_10001(self):
        self.assert_resource_rejected("Config", "User", "0:0")

    def test_every_application_container_must_have_readonly_rootfs(self):
        self.assert_resource_rejected("HostConfig", "ReadonlyRootfs", False)

    def test_every_application_container_must_have_512_mib_memory_limit(self):
        self.assert_resource_rejected("HostConfig", "Memory", 0)

    def test_every_application_container_must_have_one_cpu_limit(self):
        self.assert_resource_rejected("HostConfig", "NanoCpus", 0)

    def test_application_container_count_must_be_exactly_three(self):
        original = copy.deepcopy(self.containers)
        for count in (0, 2, 4):
            with self.subTest(count=count):
                self.containers = (copy.deepcopy(original) * 2)[:count]
                with self.assertRaises(AssertionError):
                    self.invoke()
                self.assert_cleanup()
                self.assert_no_success_report()

    def test_each_incorrect_sql_evidence_field_fails_and_still_cleans_up(self):
        for key, value in {"jobs": 19, "attempts": 21, "worker_ids": 1,
                           "results": 19, "bad_results": 1, "max_attempt_count": 2}.items():
            with self.subTest(field=key):
                self.evidence = {**self.expected_evidence, key: value}
                with self.assertRaises(AssertionError) as raised:
                    self.invoke()
                self.assertEqual(raised.exception.args, (self.evidence,))
                self.assert_cleanup()
                self.assert_no_success_report()

    def test_batch_must_return_202_before_sql_or_runtime_checks(self):
        for status in (200, 400, 500):
            with self.subTest(status=status):
                self.batch_status = status
                with self.assertRaises(AssertionError):
                    self.invoke()
                self.assertTrue(all(item.kwargs["input"] is None for item in self.invocations))
                self.assert_cleanup()
                self.assert_no_success_report()

    def test_batch_must_accept_exactly_twenty_jobs(self):
        for count in (0, 19, 21):
            with self.subTest(accepted_count=count):
                self.accepted_count = count
                with self.assertRaises(AssertionError):
                    self.invoke()
                self.assert_cleanup()
                self.assert_no_success_report()

    def test_readiness_retries_transient_errors_and_incomplete_conditions_without_sleeping(self):
        self.health = [OSError("not listening"), ValueError("invalid JSON"), "DOWN", "UP"]
        self.consumers = ["19", subprocess.CalledProcessError(1, "rabbitmqctl"), "20"]
        self.completed = [0, 20]
        self.invoke()
        self.assertEqual(self.event.return_value.wait.call_args_list, [call(.3)] * 6)
        self.assertEqual(self.http.call_count, 5)
        self.assert_cleanup()

    def test_each_readiness_timeout_cleans_up_and_does_not_write_success(self):
        for stage, completed_waits in (("health", 0), ("consumers", 1), ("completed", 2)):
            with self.subTest(stage=stage):
                self.health, self.consumers, self.completed = ["UP"], ["20"], [20]
                setattr(self, stage, {"health": ["DOWN"], "consumers": ["19"],
                                      "completed": [19]}[stage])
                clock = [0, 0] * completed_waits + [0, 0, 91]
                with patch.object(smoke.time, "monotonic", side_effect=clock):
                    with self.assertRaisesRegex(TimeoutError, "Disposable container smoke condition failed"):
                        self.invoke()
                self.assert_cleanup()
                self.assert_no_success_report()

    def test_failed_startup_preserves_error_and_finally_removes_only_its_project(self):
        self.start_error = subprocess.CalledProcessError(1, "mock compose up")
        with self.assertRaises(subprocess.CalledProcessError) as raised:
            self.invoke()
        self.assertIs(raised.exception, self.start_error)
        self.http.assert_not_called()
        self.assert_cleanup()
        self.assert_no_success_report()

    def test_failed_startup_without_containers_still_runs_same_project_down_v(self):
        self.owned = ()
        self.start_error = subprocess.TimeoutExpired("mock compose up", 360)
        with self.assertRaises(subprocess.TimeoutExpired) as raised:
            self.invoke()
        self.assertIs(raised.exception, self.start_error)
        self.http.assert_not_called()
        self.assert_cleanup()
        self.assert_no_success_report()

    def test_sql_subprocess_failure_preserves_error_and_finally_cleans_up(self):
        self.evidence_error = subprocess.CalledProcessError(1, "mock psql")
        with self.assertRaises(subprocess.CalledProcessError) as raised:
            self.invoke()
        self.assertIs(raised.exception, self.evidence_error)
        self.assert_cleanup()
        self.assert_no_success_report()

    def test_cleanup_refuses_down_when_an_inspected_container_has_another_project_label(self):
        self.owner_override = "development-project"
        with self.assertRaises(AssertionError):
            self.invoke()
        commands = [item.args[0] for item in self.invocations]
        discovery = ("docker", "ps", "-aq", "--filter",
                     f"label=com.docker.compose.project={self.project}")
        self.assertEqual(commands[-2:], [discovery, ("docker", "inspect", *self.owned)])
        self.assertFalse(any("down" in command for command in commands))
        self.assertFalse((self.folder / "compose.log").exists())


if __name__ == "__main__":
    unittest.main()