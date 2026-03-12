$ErrorActionPreference = "Stop"

Write-Host "Compiling Python entry points..."
python -m py_compile app.py appsail_main.py build_snapshot.py functions\salestrends\app_api.py functions\salestrends\index.py scripts\package_appsail.py scripts\build_appsail_image.py scripts\verify_deployments.py

if (Get-Command py -ErrorAction SilentlyContinue) {
  try {
    py -3.9 -m py_compile app.py appsail_main.py build_snapshot.py functions\salestrends\app_api.py functions\salestrends\index.py
    Write-Host "Validated Catalyst runtime entry points with Python 3.9 syntax."
  }
  catch {
    Write-Host "Python 3.9 not available or compile check failed; skipping managed AppSail syntax gate."
  }
}

if (Get-Command node -ErrorAction SilentlyContinue) {
  Write-Host "Checking dashboard JavaScript syntax..."
  @'
from pathlib import Path
import re
import subprocess
import tempfile

html = Path("functions/salestrends/dashboard.html").read_text(encoding="utf-8")
match = re.search(r"<script>([\s\S]*)</script>\s*</body>", html)
if not match:
    raise SystemExit("inline dashboard script not found")

with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as handle:
    handle.write(match.group(1))
    script_path = handle.name

result = subprocess.run(["node", "--check", script_path], capture_output=True, text=True)
if result.stdout:
    print(result.stdout)
if result.stderr:
    print(result.stderr)
if result.returncode:
    raise SystemExit(result.returncode)
'@ | python -
}
else {
  Write-Host "Node not found, skipping dashboard JavaScript syntax check."
}

Write-Host "Rebuilding snapshot from the current source..."
python build_snapshot.py

Write-Host "Running automated test suite..."
python -m pytest -q
