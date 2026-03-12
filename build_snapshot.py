import json
import os
import sys
import shutil
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent / "functions" / "salestrends"
ROOT_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import app_api  # noqa: E402


def sync_public_dashboard_shell() -> None:
    source = APP_DIR / "dashboard.html"
    destination = ROOT_DIR / "public" / "index.html"
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def local_workbook_exists() -> bool:
    candidates = [
        Path(app_api.DATA_FILE),
        APP_DIR / app_api.DATA_FILE,
        ROOT_DIR / app_api.DATA_FILE,
    ]
    return any(candidate.exists() for candidate in candidates)


def main() -> None:
    sync_public_dashboard_shell()
    url = os.environ.get("DATA_URL", "").strip()
    if app_api._dm.ready:
        print(json.dumps(app_api._dm.health(), indent=2, default=str))
        return
    if url:
        app_api._dm.load_from_url(url)
    elif local_workbook_exists():
        app_api._dm.refresh_current_source()
    else:
        print(
            json.dumps(
                {
                    "status": "skipped",
                    "reason": "No DATA_URL, snapshot, or local workbook available during build.",
                    "health": app_api._dm.health(),
                },
                indent=2,
                default=str,
            )
        )
        return
    print(json.dumps(app_api._dm.health(), indent=2, default=str))


if __name__ == "__main__":
    main()
