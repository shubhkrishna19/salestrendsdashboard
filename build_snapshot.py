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


def main() -> None:
    sync_public_dashboard_shell()
    url = os.environ.get("DATA_URL", "").strip()
    if url:
        app_api._dm.load_from_url(url)
    else:
        app_api._dm.refresh_current_source()
    print(json.dumps(app_api._dm.health(), indent=2, default=str))


if __name__ == "__main__":
    main()
