"""End-to-end HTTP contracts, isolated process and journal per test."""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request


class HttpContracts(unittest.TestCase):
    executable = ""
    token = "queue-http-integration-token"

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="leasequeue-test-")
        self.journal = Path(self.directory.name) / "queue.wal"
        self.log = Path(self.directory.name) / "server.log"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.process = None
        self.start()

    def start(self):
        self.output = self.log.open("a")
        environment = {**os.environ, "HOST": "127.0.0.1", "PORT": str(self.port),
                       "API_TOKEN": self.token, "JOURNAL_PATH": str(self.journal)}
        self.process = subprocess.Popen([self.executable], env=environment, stdout=self.output, stderr=self.output)
        for _ in range(100):
            if self.process.poll() is not None:
                raise RuntimeError(self.log.read_text())
            try:
                if self.request("GET", "/health", auth=False)[0] == 200:
                    return
            except (urllib.error.URLError, TimeoutError):
                pass
            time.sleep(0.05)
        raise RuntimeError("Readiness timed out: " + self.log.read_text())

    def stop(self, force=False):
        if self.process is not None:
            self.process.kill() if force else self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.output.close()
            self.process = None
            # Only our own test lock, after wait confirmed process exit.
            lock = Path(str(self.journal) + ".lock")
            if lock.exists():
                lock.rmdir()

    def tearDown(self):
        self.stop()
        self.directory.cleanup()

    def request(self, method, path, data=None, auth=True):
        headers = {"Content-Type": "application/json"}
        if auth:
            headers["Authorization"] = "Bearer " + self.token
        req = urllib.request.Request(self.base + path, method=method, headers=headers,
                                     data=None if data is None else json.dumps(data).encode())
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, None if response.status == 204 else json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)

    def submit(self, key="sample", payload="hello", attempts=3):
        status, result = self.request("POST", "/v1/jobs", {"key": key, "payload": payload, "max_attempts": attempts})
        self.assertEqual(status, 200, result)
        return result["id"]

    def claim(self, lease=60000):
        return self.request("POST", "/v1/claims", {"lease_ms": lease})

    def test_validation_auth_and_size(self):
        self.assertEqual(self.request("GET", "/v1/stats", auth=False)[0], 401)
        for duration in [0, -1, True, 1.2, 60001, 18446744073709551616]:
            self.assertEqual(self.claim(duration)[0], 400)
        self.assertEqual(self.request("POST", "/v1/jobs", {"key": "x", "payload": "a"*9000, "max_attempts": 3})[0], 413)
        self.assertEqual(self.request("POST", "/v1/jobs", {"key": "x", "payload": "a"*4097, "max_attempts": 3})[0], 400)
        self.assertEqual(self.request("GET", "/v1/jobs/18446744073709551616")[0], 400)

    def test_idempotency(self):
        identifier = self.submit()
        self.assertEqual(self.submit(), identifier)
        self.assertEqual(self.request("POST", "/v1/jobs", {"key": "sample", "payload": "different", "max_attempts": 3})[0], 409)
        self.assertEqual(self.request("GET", "/v1/stats")[1]["ready"], 1)

    def test_concurrent_claim_has_one_winner(self):
        identifier = self.submit()
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            responses = list(executor.map(lambda _: self.claim(), range(12)))
        winners = [job for status, job in responses if status == 201]
        self.assertEqual(len(winners), 1, responses)
        self.assertEqual(sum(status == 204 for status, _ in responses), 11)
        token = winners[0]["token"]
        self.assertEqual(self.request("POST", f"/v1/jobs/{identifier}/ack", {"token": token})[0], 200)
        self.assertEqual(self.request("POST", f"/v1/jobs/{identifier}/ack", {"token": token})[0], 409)

    def test_expiration_rejects_stale_worker(self):
        identifier = self.submit()
        status, first = self.claim(60)
        self.assertEqual(status, 201)
        time.sleep(0.12)
        self.assertEqual(self.request("POST", f"/v1/jobs/{identifier}/ack", {"token": first["token"]})[0], 409)
        status, second = self.claim()
        self.assertEqual(status, 201)
        self.assertNotEqual(first["token"], second["token"])
        self.assertEqual(self.request("POST", f"/v1/jobs/{identifier}/ack", {"token": first["token"]})[0], 409)
        self.assertEqual(self.request("POST", f"/v1/jobs/{identifier}/ack", {"token": second["token"]})[0], 200)

    def test_retry_budget(self):
        identifier = self.submit(attempts=2)
        for attempt in range(2):
            status, job = self.claim()
            self.assertEqual(status, 201)
            self.assertEqual(job["attempts"], attempt + 1)
            self.assertEqual(self.request("POST", f"/v1/jobs/{identifier}/nack", {"token": job["token"], "backoff_ms": 0})[0], 200)
        self.assertEqual(self.claim()[0], 204)
        self.assertEqual(self.request("GET", f"/v1/jobs/{identifier}")[1]["status"], "dead")

    def test_restart_and_forced_process_exit(self):
        identifier = self.submit(payload="payload\u0000with binary")
        _, job = self.claim()
        self.assertEqual(self.request("POST", f"/v1/jobs/{identifier}/ack", {"token": job["token"]})[0], 200)
        self.stop(force=True)
        self.start()
        self.assertEqual(self.submit(payload="payload\u0000with binary"), identifier)
        self.assertEqual(self.request("GET", f"/v1/jobs/{identifier}")[1]["status"], "succeeded")
        self.assertEqual(self.claim()[0], 204)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True)
    HttpContracts.executable = str(Path(parser.parse_args().server).resolve())
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(HttpContracts))
    raise SystemExit(0 if result.wasSuccessful() else 1)
