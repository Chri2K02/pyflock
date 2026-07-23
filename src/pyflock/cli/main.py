"""``pyflock`` command-line client.

A thin, dependency-light wrapper over the REST API. Point it at a control plane
with ``--api-url`` or the ``PYFLOCK_API_URL`` environment variable.

Examples
--------
    pyflock health
    pyflock shell "echo hello from the cluster"
    pyflock sleep 10
    pyflock fetch https://example.com
    pyflock jobs --state running
    pyflock status <job-id>
    pyflock logs <job-id>
    pyflock nodes
"""

from __future__ import annotations

import json
from typing import Annotated

import httpx
import typer

from pyflock.config import get_settings

app = typer.Typer(help="Client for the pyflock distributed job scheduler.", no_args_is_help=True)

ApiUrl = Annotated[
    str,
    typer.Option(
        envvar="PYFLOCK_API_URL",
        help="Base URL of the control plane.",
    ),
]


def _client(api_url: str) -> httpx.Client:
    return httpx.Client(base_url=api_url.rstrip("/"), timeout=10.0)


def _request(api_url: str, method: str, path: str, **kwargs) -> httpx.Response:
    try:
        with _client(api_url) as client:
            resp = client.request(method, path, **kwargs)
    except httpx.HTTPError as exc:
        typer.secho(f"error: could not reach control plane at {api_url}: {exc}", fg="red", err=True)
        raise typer.Exit(code=1) from exc
    if resp.status_code >= 400:
        detail = _safe_detail(resp)
        typer.secho(f"error: {resp.status_code} {detail}", fg="red", err=True)
        raise typer.Exit(code=1)
    return resp


def _safe_detail(resp: httpx.Response) -> str:
    try:
        return str(resp.json().get("detail", resp.text))
    except Exception:
        return resp.text


def _submit(
    api_url: str,
    job_type: str,
    spec: dict,
    max_attempts: int | None,
    timeout: int | None,
) -> None:
    body = {"type": job_type, "spec": spec}
    if max_attempts is not None:
        body["max_attempts"] = max_attempts
    if timeout is not None:
        body["timeout"] = timeout
    resp = _request(api_url, "POST", "/jobs", json=body)
    job = resp.json()
    typer.secho(f"submitted {job['type']} job {job['id']}", fg="green")


# --------------------------------------------------------------------------- #
# Submit commands
# --------------------------------------------------------------------------- #
@app.command()
def shell(
    command: Annotated[str, typer.Argument(help="Shell command to run on a worker.")],
    api_url: ApiUrl = None,  # type: ignore[assignment]
    max_attempts: Annotated[int | None, typer.Option(help="Override retry budget.")] = None,
    timeout: Annotated[int | None, typer.Option(help="Per-job timeout (seconds).")] = None,
) -> None:
    """Submit a shell-command job."""
    _submit(_resolve(api_url), "shell", {"command": command}, max_attempts, timeout)


@app.command()
def sleep(
    seconds: Annotated[float, typer.Argument(help="How long the job should sleep.")],
    api_url: ApiUrl = None,  # type: ignore[assignment]
    max_attempts: Annotated[int | None, typer.Option()] = None,
    timeout: Annotated[int | None, typer.Option()] = None,
) -> None:
    """Submit a sleep job (a stand-in for long compute)."""
    _submit(_resolve(api_url), "sleep", {"seconds": seconds}, max_attempts, timeout)


@app.command()
def fetch(
    url: Annotated[str, typer.Argument(help="URL to GET.")],
    api_url: ApiUrl = None,  # type: ignore[assignment]
    max_attempts: Annotated[int | None, typer.Option()] = None,
    timeout: Annotated[int | None, typer.Option()] = None,
) -> None:
    """Submit a fetch-url job."""
    _submit(_resolve(api_url), "fetch_url", {"url": url}, max_attempts, timeout)


@app.command()
def submit(
    type: Annotated[str, typer.Option(help="Job type: shell|sleep|fetch_url.")],
    spec: Annotated[str, typer.Option(help="JSON spec payload.")] = "{}",
    api_url: ApiUrl = None,  # type: ignore[assignment]
    max_attempts: Annotated[int | None, typer.Option()] = None,
    timeout: Annotated[int | None, typer.Option()] = None,
) -> None:
    """Submit a raw job by type + JSON spec."""
    try:
        parsed = json.loads(spec)
    except json.JSONDecodeError as exc:
        typer.secho(f"error: --spec is not valid JSON: {exc}", fg="red", err=True)
        raise typer.Exit(code=1) from exc
    _submit(_resolve(api_url), type, parsed, max_attempts, timeout)


# --------------------------------------------------------------------------- #
# Inspection commands
# --------------------------------------------------------------------------- #
@app.command()
def jobs(
    state: Annotated[str | None, typer.Option(help="Filter by state.")] = None,
    limit: Annotated[int, typer.Option()] = 50,
    api_url: ApiUrl = None,  # type: ignore[assignment]
) -> None:
    """List recent jobs."""
    params = {"limit": limit}
    if state:
        params["state"] = state
    rows = _request(_resolve(api_url), "GET", "/jobs", params=params).json()
    if not rows:
        typer.echo("(no jobs)")
        return
    typer.echo(f"{'ID':<34}{'TYPE':<12}{'STATE':<12}{'ATT':<5}{'NODE'}")
    for j in rows:
        typer.echo(
            f"{j['id']:<34}{j['type']:<12}{j['state']:<12}"
            f"{str(j['attempts']) + '/' + str(j['max_attempts']):<5}{j['assigned_node'] or '-'}"
        )


@app.command()
def status(
    job_id: Annotated[str, typer.Argument()],
    api_url: ApiUrl = None,  # type: ignore[assignment]
) -> None:
    """Show the full record for one job."""
    job = _request(_resolve(api_url), "GET", f"/jobs/{job_id}").json()
    for key in (
        "id", "type", "spec", "state", "attempts", "max_attempts", "timeout",
        "assigned_node", "exit_code", "error", "created_at", "started_at", "finished_at",
    ):
        typer.echo(f"{key:<15} {job.get(key)}")


@app.command()
def logs(
    job_id: Annotated[str, typer.Argument()],
    api_url: ApiUrl = None,  # type: ignore[assignment]
) -> None:
    """Print a job's stdout and stderr."""
    job = _request(_resolve(api_url), "GET", f"/jobs/{job_id}").json()
    typer.secho("--- stdout ---", fg="cyan")
    typer.echo(job.get("stdout") or "")
    typer.secho("--- stderr ---", fg="cyan")
    typer.echo(job.get("stderr") or "")


@app.command()
def nodes(api_url: ApiUrl = None) -> None:  # type: ignore[assignment]
    """List worker nodes and their liveness."""
    rows = _request(_resolve(api_url), "GET", "/nodes").json()
    if not rows:
        typer.echo("(no nodes registered)")
        return
    typer.echo(f"{'ID':<28}{'STATE':<8}{'CONC':<6}{'LAST HEARTBEAT'}")
    for n in rows:
        typer.echo(f"{n['id']:<28}{n['state']:<8}{n['concurrency']:<6}{n['last_heartbeat']}")


@app.command()
def health(api_url: ApiUrl = None) -> None:  # type: ignore[assignment]
    """Show cluster health."""
    data = _request(_resolve(api_url), "GET", "/health").json()
    typer.secho(f"status: {data['status']}", fg="green")
    typer.echo(f"nodes: {data['nodes_alive']}/{data['nodes_total']} alive")
    typer.echo(f"queue depth: {data['queue_depth']}  delayed: {data['delayed']}  "
               f"dead-letter: {data['dead_letter']}")
    typer.echo("jobs by state:")
    for state, count in data["jobs"].items():
        typer.echo(f"  {state:<12} {count}")


def _resolve(api_url: str | None) -> str:
    """Fall back to the configured API URL when the option is omitted."""
    return api_url or get_settings().api_url


if __name__ == "__main__":
    app()
