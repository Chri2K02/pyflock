"""Job executors for the three built-in job types."""

from __future__ import annotations

import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from pyflock.worker.executor import UnknownJobType, execute


# --------------------------------------------------------------------------- #
# shell
# --------------------------------------------------------------------------- #
def test_shell_exec_form_success():
    result = execute("shell", {"command": [sys.executable, "-c", "print('hello')"]}, timeout=10)
    assert result.ok
    assert result.exit_code == 0
    assert "hello" in result.stdout


def test_shell_string_form_success():
    result = execute("shell", {"command": "echo hi"}, timeout=10)
    assert result.ok
    assert "hi" in result.stdout


def test_shell_nonzero_exit_is_failure():
    cmd = [sys.executable, "-c", "import sys; sys.exit(3)"]
    result = execute("shell", {"command": cmd}, timeout=10)
    assert not result.ok
    assert result.exit_code == 3
    assert "code 3" in result.error


def test_shell_missing_command():
    result = execute("shell", {}, timeout=10)
    assert not result.ok
    assert "missing 'command'" in result.error


def test_shell_timeout():
    result = execute(
        "shell", {"command": [sys.executable, "-c", "import time; time.sleep(5)"]}, timeout=1
    )
    assert not result.ok
    assert result.exit_code == 124


# --------------------------------------------------------------------------- #
# sleep
# --------------------------------------------------------------------------- #
def test_sleep_success():
    result = execute("sleep", {"seconds": 0.01}, timeout=10)
    assert result.ok
    assert "slept" in result.stdout


def test_sleep_invalid_seconds():
    result = execute("sleep", {"seconds": "not-a-number"}, timeout=10)
    assert not result.ok


def test_sleep_exceeding_timeout_is_rejected():
    result = execute("sleep", {"seconds": 100}, timeout=1)
    assert not result.ok
    assert result.exit_code == 124


# --------------------------------------------------------------------------- #
# fetch_url
# --------------------------------------------------------------------------- #
class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 (http.server API)
        if self.path == "/ok":
            body = b"hello world"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):  # silence test server logging
        pass


@pytest.fixture
def http_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    yield f"http://{host}:{port}"
    server.shutdown()


def test_fetch_url_success(http_server):
    result = execute("fetch_url", {"url": f"{http_server}/ok"}, timeout=10)
    assert result.ok
    assert "200" in result.stdout
    assert "11 bytes" in result.stdout


def test_fetch_url_http_error(http_server):
    result = execute("fetch_url", {"url": f"{http_server}/missing"}, timeout=10)
    assert not result.ok
    assert "404" in result.error


def test_fetch_url_missing_url():
    result = execute("fetch_url", {}, timeout=10)
    assert not result.ok
    assert "missing 'url'" in result.error


def test_fetch_url_bad_scheme():
    result = execute("fetch_url", {"url": "ftp://example.com"}, timeout=10)
    assert not result.ok
    assert "scheme" in result.error


# --------------------------------------------------------------------------- #
# dispatch
# --------------------------------------------------------------------------- #
def test_unknown_job_type_raises():
    with pytest.raises(UnknownJobType):
        execute("does-not-exist", {}, timeout=10)
