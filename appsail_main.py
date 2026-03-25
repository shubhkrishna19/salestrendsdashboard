import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
VENDOR_DIR = ROOT_DIR / "vendor"
if os.name != "nt" and VENDOR_DIR.exists():
    sys.path.insert(0, str(VENDOR_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
import uvicorn

from app import app


def main() -> None:
    port = int(os.environ.get("X_ZOHO_CATALYST_LISTEN_PORT", "9000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    main()
