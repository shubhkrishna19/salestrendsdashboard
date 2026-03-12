from pathlib import Path
import sys

APP_DIR = Path(__file__).resolve().parent / "functions" / "salestrends"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from app_api import app  # noqa: E402
