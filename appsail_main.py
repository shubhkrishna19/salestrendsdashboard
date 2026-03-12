import os

import uvicorn

from app import app


def main() -> None:
    port = int(os.environ.get("X_ZOHO_CATALYST_LISTEN_PORT", "9000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    main()
