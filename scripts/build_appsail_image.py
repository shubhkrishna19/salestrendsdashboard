from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DIST_DIR = ROOT / "dist"
DOCKER_CONTEXT_DIR = DIST_DIR / "appsail-image-context"
DOCKERFILE_TEMPLATE = ROOT / "Dockerfile.appsail"
DEFAULT_IMAGE_TAG = "salestrends-dashboard:latest"
DEFAULT_IMAGE_ARCHIVE = DIST_DIR / "appsail-image.tar"


class DockerBuildError(RuntimeError):
    pass


def get_package_appsail_module():
    from scripts import package_appsail

    return package_appsail


def docker_executable() -> str:
    executable = shutil.which("docker")
    if not executable:
        raise DockerBuildError(
            "Docker is required to build the custom AppSail image. "
            "Install Docker Desktop or run this in a Linux CI runner with Docker."
        )
    return executable


def prepare_docker_context(refresh_snapshot: bool = True) -> Path:
    package_appsail = get_package_appsail_module()
    bundle_dir = package_appsail.package_appsail_bundle(refresh_snapshot=refresh_snapshot)

    if DOCKER_CONTEXT_DIR.exists():
        shutil.rmtree(DOCKER_CONTEXT_DIR)
    DOCKER_CONTEXT_DIR.mkdir(parents=True, exist_ok=True)

    shutil.copy2(DOCKERFILE_TEMPLATE, DOCKER_CONTEXT_DIR / "Dockerfile")

    for source in bundle_dir.iterdir():
        destination = DOCKER_CONTEXT_DIR / source.name
        if source.is_dir():
            shutil.copytree(source, destination)
        else:
            shutil.copy2(source, destination)

    return DOCKER_CONTEXT_DIR


def run_command(command: list[str]) -> None:
    print(f"\n$ {' '.join(command)}")
    result = subprocess.run(
        command,
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise DockerBuildError(f"Command failed with exit code {result.returncode}: {' '.join(command)}")


def build_appsail_image(
    image_tag: str = DEFAULT_IMAGE_TAG,
    archive_path: Path = DEFAULT_IMAGE_ARCHIVE,
    refresh_snapshot: bool = True,
    platform: str = "linux/amd64",
) -> Path:
    docker = docker_executable()
    context_dir = prepare_docker_context(refresh_snapshot=refresh_snapshot)
    archive_path.parent.mkdir(parents=True, exist_ok=True)

    run_command([docker, "build", "--platform", platform, "-t", image_tag, str(context_dir)])
    run_command([docker, "save", "-o", str(archive_path), image_tag])

    return archive_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a custom Zoho AppSail image archive.")
    parser.add_argument("--tag", default=DEFAULT_IMAGE_TAG, help="Docker image tag to build.")
    parser.add_argument(
        "--archive",
        default=str(DEFAULT_IMAGE_ARCHIVE),
        help="Path to the output docker archive tarball.",
    )
    parser.add_argument(
        "--skip-snapshot",
        action="store_true",
        help="Reuse the existing packaged snapshot instead of rebuilding it first.",
    )
    parser.add_argument(
        "--platform",
        default="linux/amd64",
        help="Docker build platform. Catalyst AppSail custom runtimes require linux/amd64.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    archive_path = Path(args.archive)
    built_archive = build_appsail_image(
        image_tag=args.tag,
        archive_path=archive_path,
        refresh_snapshot=not args.skip_snapshot,
        platform=args.platform,
    )
    print(f"Custom AppSail image archive ready at {built_archive}")


if __name__ == "__main__":
    main()
