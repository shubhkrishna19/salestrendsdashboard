from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app as root_app
import appsail_main
from scripts import build_appsail_image
from scripts import package_appsail


def test_vercel_static_root_shell_exists() -> None:
    public_index = (ROOT / "public" / "index.html").read_text(encoding="utf-8")

    assert "Bluewud Sales Intelligence" in public_index
    assert 'id="overviewKpis"' in public_index


def test_vercel_static_root_matches_dashboard_shell() -> None:
    dashboard_shell = (ROOT / "functions" / "salestrends" / "dashboard.html").read_text(encoding="utf-8")
    public_index = (ROOT / "public" / "index.html").read_text(encoding="utf-8")

    assert public_index == dashboard_shell


def test_root_fastapi_entrypoint_exposes_dashboard_and_api_routes() -> None:
    route_paths = {route.path for route in root_app.app.routes}

    assert "/" in route_paths
    assert "/api/health" in route_paths
    assert "/api/dashboard" in route_paths
    assert "/api/export" in route_paths


def test_vercel_python_runtime_is_declared() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    python_version = (ROOT / ".python-version").read_text(encoding="utf-8").strip()

    assert 'requires-python = ">=3.12,<3.15"' in pyproject
    assert 'build = "python build_snapshot.py"' in pyproject
    assert python_version == "3.12"


def test_catalyst_config_declares_appsail_target() -> None:
    catalyst_config = json.loads((ROOT / "catalyst.json").read_text(encoding="utf-8"))
    app_config = json.loads((ROOT / "app-config.json").read_text(encoding="utf-8"))

    assert catalyst_config["functions"]["targets"] == ["salestrends"]
    assert catalyst_config["appsail"] == [{"name": "salestrends-dashboard", "source": "."}]
    assert app_config["command"] == "python appsail_main.py"
    assert app_config["build_path"] == "dist/appsail"
    assert app_config["stack"] == "python_3_9"


def test_appsail_bundle_does_not_use_catalyst_cli_build_dir() -> None:
    app_config = json.loads((ROOT / "app-config.json").read_text(encoding="utf-8"))

    assert not app_config["build_path"].startswith(".build")


def test_custom_runtime_example_targets_docker_archive() -> None:
    custom_runtime_config = json.loads((ROOT / "catalyst.custom-runtime.example.json").read_text(encoding="utf-8"))
    appsail_target = custom_runtime_config["appsail"][0]

    assert appsail_target["source"] == "docker-archive://dist/appsail-image.tar"
    assert appsail_target["command"] == "python appsail_main.py"
    assert appsail_target["memory"] == 1024
    assert appsail_target["port"] == {"http": 9000}


def test_appsail_main_reads_zoho_port(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(app, host: str, port: int, log_level: str) -> None:
        captured["app"] = app
        captured["host"] = host
        captured["port"] = port
        captured["log_level"] = log_level

    monkeypatch.setattr(appsail_main.uvicorn, "run", fake_run)
    monkeypatch.setenv("X_ZOHO_CATALYST_LISTEN_PORT", "9123")

    appsail_main.main()

    assert captured == {
        "app": root_app.app,
        "host": "0.0.0.0",
        "port": 9123,
        "log_level": "info",
    }


def test_appsail_packager_creates_runtime_bundle(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(package_appsail, "BUILD_DIR", tmp_path / "appsail")

    build_dir = package_appsail.package_appsail_bundle(refresh_snapshot=False)

    assert build_dir.exists()
    assert (build_dir / "app.py").exists()
    assert (build_dir / "appsail_main.py").exists()
    assert (build_dir / "requirements.txt").exists()
    assert (build_dir / "functions" / "salestrends" / "app_api.py").exists()
    assert (build_dir / "functions" / "salestrends" / "dashboard.html").exists()
    assert (build_dir / "functions" / "salestrends" / "data_snapshot.csv.gz").exists()
    assert not (build_dir / "data.xlsx").exists()


def test_custom_appsail_docker_context_is_prepared(tmp_path, monkeypatch) -> None:
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "app.py").write_text("print('ok')\n", encoding="utf-8")
    (bundle_dir / "appsail_main.py").write_text("print('appsail')\n", encoding="utf-8")
    (bundle_dir / "requirements.txt").write_text("fastapi==0.111.0\n", encoding="utf-8")
    (bundle_dir / "functions").mkdir()
    (bundle_dir / "functions" / "salestrends").mkdir(parents=True, exist_ok=True)
    (bundle_dir / "functions" / "salestrends" / "dashboard.html").write_text("<html></html>\n", encoding="utf-8")

    dockerfile_template = tmp_path / "Dockerfile.appsail"
    dockerfile_template.write_text("FROM python:3.12-slim\n", encoding="utf-8")

    monkeypatch.setattr(package_appsail, "package_appsail_bundle", lambda refresh_snapshot=True: bundle_dir)
    monkeypatch.setattr(build_appsail_image, "get_package_appsail_module", lambda: package_appsail)
    monkeypatch.setattr(build_appsail_image, "DOCKER_CONTEXT_DIR", tmp_path / "context")
    monkeypatch.setattr(build_appsail_image, "DOCKERFILE_TEMPLATE", dockerfile_template)

    context_dir = build_appsail_image.prepare_docker_context(refresh_snapshot=False)

    assert (context_dir / "Dockerfile").exists()
    assert (context_dir / "app.py").exists()
    assert (context_dir / "appsail_main.py").exists()
    assert (context_dir / "requirements.txt").exists()
    assert (context_dir / "functions" / "salestrends" / "dashboard.html").exists()


def test_custom_appsail_image_builder_uses_linux_amd64_archive(monkeypatch, tmp_path) -> None:
    commands: list[list[str]] = []

    monkeypatch.setattr(build_appsail_image, "docker_executable", lambda: "docker")
    monkeypatch.setattr(build_appsail_image, "prepare_docker_context", lambda refresh_snapshot=True: tmp_path / "context")
    monkeypatch.setattr(build_appsail_image, "run_command", lambda command: commands.append(command))

    archive_path = build_appsail_image.build_appsail_image(
        image_tag="salestrends:test",
        archive_path=tmp_path / "appsail-image.tar",
        refresh_snapshot=False,
        platform="linux/amd64",
    )

    assert archive_path == tmp_path / "appsail-image.tar"
    assert commands == [
        ["docker", "build", "--platform", "linux/amd64", "-t", "salestrends:test", str(tmp_path / "context")],
        ["docker", "save", "-o", str(tmp_path / "appsail-image.tar"), "salestrends:test"],
    ]


def test_appsail_dockerfile_targets_stable_python() -> None:
    dockerfile = (ROOT / "Dockerfile.appsail").read_text(encoding="utf-8")

    assert "FROM python:3.12-slim" in dockerfile
    assert 'CMD ["python", "appsail_main.py"]' in dockerfile
