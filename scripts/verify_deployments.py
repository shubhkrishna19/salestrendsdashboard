from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Iterable

import requests


ROOT = Path(__file__).resolve().parents[1]
VERCEL_PORT = 4110
APPSAIL_PORT = 4111
CATALYST_APPSAIL_PORT = 4112
APPSAIL_BUNDLE_DIR = ROOT / "dist" / "appsail"
CATALYST_PROXY_PORT = 3001


class CommandFailure(RuntimeError):
    pass


def run_command(command: list[str], cwd: Path, extra_env: dict[str, str] | None = None) -> None:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    print(f"\n$ {' '.join(command)}")
    result = subprocess.run(
        command,
        cwd=str(cwd),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise CommandFailure(f"Command failed with exit code {result.returncode}: {' '.join(command)}")


def _python_from_launcher(version: str) -> Path | None:
    result = subprocess.run(
        ["py", f"-{version}", "-c", "import sys; print(sys.executable)"],
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        return None
    executable = Path(result.stdout.strip())
    return executable if executable.exists() else None


def vercel_cli_env() -> dict[str, str]:
    for version in ("3.12", "3.13", "3.14", "3.11", "3.10", "3.9"):
        executable = _python_from_launcher(version)
        if executable is None:
            continue
        script_dir = executable.parent / "Scripts"
        path_prefix = [str(executable.parent)]
        if script_dir.exists():
            path_prefix.append(str(script_dir))
        env = {
            "PATH": os.pathsep.join(path_prefix + [os.environ.get("PATH", "")]),
        }
        print(f"Using local Python {version} for Vercel CLI: {executable}")
        return env
    raise CommandFailure("No supported local Python runtime found for Vercel CLI smoke tests.")


def cli_executable(name: str) -> str:
    if platform.system() == "Windows":
        appdata = Path(os.environ["APPDATA"])
        candidate = appdata / "npm" / f"{name}.cmd"
        if candidate.exists():
            return str(candidate)
    return name


def docker_executable() -> str | None:
    return shutil.which("docker")


def wait_for_http(url: str, timeout_seconds: int = 120) -> requests.Response:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=5)
            if response.ok:
                return response
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(1)
    raise CommandFailure(f"Timed out waiting for {url}: {last_error}")


def _stream_output(process: subprocess.Popen[str], sink: deque[str]) -> None:
    assert process.stdout is not None
    for line in process.stdout:
        sink.append(line.rstrip())


def run_server_smoke(
    command: list[str],
    cwd: Path,
    port: int,
    extra_env: dict[str, str] | None = None,
    startup_timeout: int = 120,
    health_url: str | None = None,
    dashboard_url: str | None = None,
) -> None:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    print(f"\n$ {' '.join(command)}")
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    logs: deque[str] = deque(maxlen=80)
    reader = threading.Thread(target=_stream_output, args=(process, logs), daemon=True)
    reader.start()

    try:
        health_target = health_url or f"http://127.0.0.1:{port}/api/health"
        dashboard_target = dashboard_url or f"http://127.0.0.1:{port}/api/dashboard"
        health = wait_for_http(health_target, timeout_seconds=startup_timeout)
        dashboard = requests.get(dashboard_target, timeout=10)
        if dashboard.status_code != 200:
            raise CommandFailure(f"Dashboard endpoint failed on port {port}: {dashboard.status_code}")
        print(health.text)
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=20)

    if process.returncode not in (0, 1, -15):
        tail = "\n".join(logs)
        raise CommandFailure(f"Server command exited unexpectedly ({process.returncode}). Logs:\n{tail}")


def ensure_file(path: Path) -> None:
    if not path.exists():
        raise CommandFailure(f"Expected file missing: {path}")


def restore_local_snapshot() -> None:
    if not (ROOT / "data.xlsx").exists():
        return

    sys.path.insert(0, str(ROOT / "functions" / "salestrends"))
    import app_api  # noqa: PLC0415

    raw_df, summary = app_api._dm._load_local("data.xlsx")
    processed = app_api._dm._process_dataframe(raw_df)
    app_api._dm._set_loaded_state(processed, "data.xlsx", "local", summary)
    app_api._dm._write_snapshot()
    print("Restored local snapshot baseline.")


def cleanup_windows_appsail_processes() -> None:
    if platform.system() != "Windows":
        return

    command = (
        "$targets = Get-CimInstance Win32_Process | "
        "Where-Object { $_.CommandLine -like '*appsail_main.py*' -or $_.CommandLine -like '*catalyst serve*' "
        "-or $_.CommandLine -like '*serve\\\\server\\\\lib\\\\appsail*' }; "
        "foreach ($target in $targets) { try { Stop-Process -Id $target.ProcessId -Force -ErrorAction Stop } catch {} }"
    )
    subprocess.run(
        ["powershell", "-NoProfile", "-Command", command],
        cwd=str(ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def cleanup_appsail_build_dir() -> None:
    if APPSAIL_BUNDLE_DIR.exists():
        shutil.rmtree(APPSAIL_BUNDLE_DIR)


def cleanup_catalyst_runtime_build_dir() -> None:
    build_dir = ROOT / ".build"
    if build_dir.exists():
        shutil.rmtree(build_dir)


def main() -> None:
    try:
        cleanup_windows_appsail_processes()
        cleanup_appsail_build_dir()
        cleanup_catalyst_runtime_build_dir()
        run_command([sys.executable, "build_snapshot.py"], ROOT)
        run_command([sys.executable, "scripts/package_appsail.py"], ROOT)
        if platform.system() == "Windows":
            print(
                "Skipping local Vercel CLI runtime smoke on Windows. "
                "Use preview deployment or WSL/Linux for full Vercel emulation."
            )
        else:
            vercel_env = vercel_cli_env()
            run_command([cli_executable("vercel"), "build", "--prod", "--yes"], ROOT, extra_env=vercel_env)
            ensure_file(ROOT / ".vercel" / "output" / "config.json")
            run_server_smoke(
                [cli_executable("vercel"), "dev", "--listen", f"127.0.0.1:{VERCEL_PORT}", "--yes"],
                ROOT,
                VERCEL_PORT,
                extra_env=vercel_env,
            )

        ensure_file(APPSAIL_BUNDLE_DIR / "appsail_main.py")
        ensure_file(APPSAIL_BUNDLE_DIR / "functions" / "salestrends" / "data_snapshot.csv.gz")
        ensure_file(ROOT / "Dockerfile.appsail")
        ensure_file(ROOT / "catalyst.custom-runtime.example.json")

        docker = docker_executable()
        if docker:
            run_command([sys.executable, "scripts/build_appsail_image.py", "--tag", "salestrends-dashboard:verify"], ROOT)
            ensure_file(ROOT / "dist" / "appsail-image.tar")
        else:
            print(
                "Skipping custom AppSail image build because Docker is not installed. "
                "The repo still validated the image build script and custom runtime config."
            )

        cleanup_windows_appsail_processes()
        cleanup_catalyst_runtime_build_dir()
        run_server_smoke(
            [cli_executable("catalyst"), "serve", "--only", "appsail:salestrends-dashboard", "--no-open", "--no-watch"],
            ROOT,
            CATALYST_APPSAIL_PORT,
            health_url=f"http://127.0.0.1:{CATALYST_PROXY_PORT}/api/health",
            dashboard_url=f"http://127.0.0.1:{CATALYST_PROXY_PORT}/api/dashboard",
        )
        run_server_smoke(
            [sys.executable, "appsail_main.py"],
            APPSAIL_BUNDLE_DIR,
            APPSAIL_PORT,
            extra_env={"X_ZOHO_CATALYST_LISTEN_PORT": str(APPSAIL_PORT)},
        )

        print("\nDeployment verification completed successfully.")
    finally:
        cleanup_windows_appsail_processes()
        try:
            cleanup_catalyst_runtime_build_dir()
        except Exception:
            pass
        try:
            restore_local_snapshot()
        except Exception as exc:  # noqa: BLE001
            print(f"Warning: could not restore the local snapshot baseline: {exc}")


if __name__ == "__main__":
    main()
