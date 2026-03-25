from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


BUILD_DIR = ROOT / "dist" / "appsail"
VENDOR_DIR = BUILD_DIR / "vendor"
PIP_PLATFORM_ARGS = [
    "--platform",
    "manylinux2014_x86_64",
    "--implementation",
    "cp",
    "--python-version",
    "39",
    "--only-binary=:all:",
]

FILES_TO_COPY = [
    "app.py",
    "appsail_main.py",
    "requirements.txt",
]

TREE_TO_COPY = [
    "functions/salestrends",
]

EXCLUDED_NAMES = {
    "__pycache__",
    "data.xlsx",
}


def _ignore(_: str, names: list[str]) -> set[str]:
    ignored = {name for name in names if name in EXCLUDED_NAMES}
    ignored.update(name for name in names if name.endswith(".pyc"))
    return ignored


def package_appsail_bundle(refresh_snapshot: bool = True) -> Path:
    if refresh_snapshot:
        import build_snapshot

        build_snapshot.main()

    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir(parents=True, exist_ok=True)

    for relative_path in FILES_TO_COPY:
        source = ROOT / relative_path
        destination = BUILD_DIR / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)

    for relative_path in TREE_TO_COPY:
        source = ROOT / relative_path
        destination = BUILD_DIR / relative_path
        shutil.copytree(source, destination, ignore=_ignore)

    _install_vendor_dependencies()

    return BUILD_DIR


def _install_vendor_dependencies() -> None:
    VENDOR_DIR.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--upgrade",
        "--target",
        str(VENDOR_DIR),
        *PIP_PLATFORM_ARGS,
        "-r",
        str(ROOT / "requirements.txt"),
    ]
    subprocess.run(command, check=True)


def main() -> None:
    build_dir = package_appsail_bundle()
    print(f"AppSail bundle ready at {build_dir}")


if __name__ == "__main__":
    main()
