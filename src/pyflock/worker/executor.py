"""Job execution: turns a job spec into a real side effect and a result.

Three built-in job types ship with pyflock so the cluster does useful work out
of the box:

* ``shell``     — run an arbitrary command. ``spec = {"command": [...] | "..."}``
* ``sleep``     — sleep for N seconds (a stand-in for a long compute job).
                  ``spec = {"seconds": N}``
* ``fetch_url`` — HTTP GET a URL and report status + body length.
                  ``spec = {"url": "https://..."}``

Executors never raise for *job* failures; they return an :class:`ExecutionResult`
with a non-zero exit code and/or an ``error`` string. They only raise for
programmer errors (unknown type, malformed spec) so those surface loudly.
"""

from __future__ import annotations

import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

from pyflock.core.enums import JobType


@dataclass
class ExecutionResult:
    """Outcome of running a job."""

    exit_code: int
    stdout: str = ""
    stderr: str = ""
    error: str | None = None

    @property
    def ok(self) -> bool:
        """True when the job completed successfully."""
        return self.exit_code == 0 and self.error is None


class UnknownJobType(ValueError):
    """Raised when a job references a type with no registered executor."""


def execute(job_type: str, spec: dict, *, timeout: int) -> ExecutionResult:
    """Dispatch to the executor for ``job_type`` and return its result."""
    handler = _HANDLERS.get(job_type)
    if handler is None:
        raise UnknownJobType(f"no executor registered for job type {job_type!r}")
    return handler(spec, timeout)


def _run_shell(spec: dict, timeout: int) -> ExecutionResult:
    command = spec.get("command")
    if command is None:
        return ExecutionResult(exit_code=1, error="shell job missing 'command'")

    # Accept either a list (exec form, no shell) or a string (shell form).
    use_shell = isinstance(command, str)
    try:
        proc = subprocess.run(
            command,
            shell=use_shell,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        return ExecutionResult(
            exit_code=124,
            stdout=_as_text(exc.stdout),
            stderr=_as_text(exc.stderr),
            error=f"timed out after {timeout}s",
        )
    except (OSError, ValueError) as exc:
        return ExecutionResult(exit_code=127, error=f"failed to launch command: {exc}")

    return ExecutionResult(
        exit_code=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
        error=None if proc.returncode == 0 else f"exited with code {proc.returncode}",
    )


def _run_sleep(spec: dict, timeout: int) -> ExecutionResult:
    seconds = spec.get("seconds", 1)
    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        return ExecutionResult(exit_code=1, error=f"invalid 'seconds': {seconds!r}")

    if seconds > timeout:
        return ExecutionResult(
            exit_code=124, error=f"sleep {seconds}s exceeds timeout {timeout}s"
        )
    time.sleep(seconds)
    return ExecutionResult(exit_code=0, stdout=f"slept {seconds}s")


def _run_fetch_url(spec: dict, timeout: int) -> ExecutionResult:
    url = spec.get("url")
    if not url:
        return ExecutionResult(exit_code=1, error="fetch_url job missing 'url'")
    if not (url.startswith("http://") or url.startswith("https://")):
        return ExecutionResult(exit_code=1, error=f"unsupported URL scheme: {url!r}")

    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 (scheme checked)
            body = resp.read()
            status = getattr(resp, "status", resp.getcode())
    except urllib.error.HTTPError as exc:
        return ExecutionResult(
            exit_code=1, error=f"HTTP {exc.code} for {url}", stderr=str(exc)
        )
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return ExecutionResult(exit_code=1, error=f"request failed: {exc}")

    return ExecutionResult(
        exit_code=0,
        stdout=f"GET {url} -> {status}, {len(body)} bytes",
    )


def _as_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


_HANDLERS = {
    JobType.SHELL.value: _run_shell,
    JobType.SLEEP.value: _run_sleep,
    JobType.FETCH_URL.value: _run_fetch_url,
}
