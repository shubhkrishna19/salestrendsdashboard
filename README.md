# SalesTrendsDashboard

Internal sales analytics dashboard for Bluewud. The app reads the `Final Sale Data` sheet from a workbook, normalizes the data into a stable analytics model, and serves a FastAPI dashboard with snapshot-backed runtime performance.

## What is fixed

- Return math now uses signed return rows correctly.
- Unique-order metrics no longer count blank order IDs.
- Broken dashboard sections now have working API payloads behind them.
- Workbook ingestion supports local files, public Google Drive links, and SharePoint or OneDrive download links.
- Snapshot generation avoids re-parsing the workbook on every cold start.
- The repo now includes automated regression tests for data math, endpoint contracts, and reload behavior.

## Current baseline

Verified against the current local workbook:

- Rows: `151770`
- Unique orders: `107748`
- Gross sales: `397286879`
- Return value: `59496957`
- Net revenue: `337789922`
- Value return rate: `14.98%`
- Quantity return rate: `3.86%`

## Project structure

- `functions/salestrends/app_api.py`: FastAPI backend, loaders, snapshot handling, analytics
- `functions/salestrends/dashboard.html`: dashboard shell and client-side rendering
- `app.py`: Vercel entry point
- `build_snapshot.py`: build-time snapshot refresh
- `scripts/verify.ps1`: local production-readiness verification
- `scripts/verify_deployments.py`: deployment smoke verification for Vercel config and Catalyst AppSail boot
- `scripts/build_appsail_image.py`: build a custom AppSail Docker archive for production deploys
- `tests/test_app_api.py`: regression suite

## Data flow

1. Load the workbook from an explicit URL, configured `DATA_URL`, GitHub fallback, or local workbook.
2. Normalize the raw sheet into stable analytics columns.
3. Write a compressed snapshot to `functions/salestrends/data_snapshot.csv.gz`.
4. Serve dashboard and API requests from the snapshot-backed dataframe.

Snapshot artifacts are generated runtime files and are ignored by git.

## Local setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
Copy-Item .env.example .env
```

Edit `.env` and set `DATA_URL` if you want to load from a public workbook link instead of the local `data.xlsx`.

## Local run

```powershell
python build_snapshot.py
python -m uvicorn app:app --reload --port 8000
```

Open `http://127.0.0.1:8000`.

## Verification workflow

Run the full local verification pass:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

This does four things:

1. Compiles the Python entry points.
2. Checks the inline dashboard JavaScript syntax when Node is available.
3. Rebuilds the snapshot from the current source.
4. Runs the automated test suite.

Run the deployment verification pass:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify_deployments.ps1
```

This verifies:

1. Snapshot rebuild and AppSail bundle packaging.
2. Vercel deployment config integrity.
3. Catalyst AppSail local proxy boot.
4. Direct AppSail entry-point boot against the packaged bundle.
5. Custom AppSail image assets and config presence.

## Environment variables

Primary runtime configuration:

- `DATA_URL`: public Google Drive, SharePoint, OneDrive, or direct workbook URL
- `SHEET_NAME`: defaults to `Final Sale Data`
- `SUMMARY_SHEET_NAME`: defaults to `Sales Analytics Dashboard`

Optional fallback configuration:

- `GITHUB_TOKEN`: read token for a private GitHub workbook source
- `GITHUB_REPO`: repo path like `owner/repo`
- `DATA_FILE`: workbook name, defaults to `data.xlsx`

Optional snapshot overrides:

- `SNAPSHOT_FILE`
- `SNAPSHOT_META_FILE`

## Recommended stack

For long-term use and future consolidation into a larger internal platform, the recommended stack is:

- FastAPI backend with snapshot-backed pandas analytics
- Vercel preview deployments on Python `3.12`
- Zoho AppSail production deploys via a custom Linux AMD64 container image on Python `3.12`

Why this is the better long-term choice:

- Vercel officially supports Python `3.12`, `3.13`, and `3.14`, and `3.12` is the safest stable target for scientific wheels.
- Zoho managed AppSail is still capped at `python_3_9`, which blocks the current analytics dependency stack.
- A custom AppSail container keeps the same runtime, dependency set, and app behavior between pre-production and Zoho production.

## Vercel deployment

Vercel is the first deployment target for verification.

1. Set `DATA_URL` in the Vercel project environment variables.
2. Keep `vercel.json` as-is so the build step runs `python build_snapshot.py`.
3. Deploy the repo.
4. Validate:
   - `/api/health`
   - `/api/dashboard`
   - filter changes
   - export flow

Why this works:

- `app.py` exposes the FastAPI app for Vercel's Python runtime.
- `build_snapshot.py` refreshes the snapshot during build so runtime requests do not need to parse the workbook.
- `vercel.json` sets `maxDuration` to `120` seconds for the app function.

Windows note:

- `vercel dev` and `vercel build` are not reliable local runtime checks on this machine because the Windows Vercel Python shim is failing before app startup.
- The repo now verifies the Vercel entry point and config in tests, but the final Vercel gate is still a real preview deployment.

## Zoho Catalyst and Zoho Creator path

This repo still contains the legacy Catalyst function entry point for compatibility, but raw workbook parsing is not the recommended production path on Catalyst.

Recommended path:

1. Verify on Vercel first.
2. Move the same FastAPI app to Catalyst AppSail.
3. Bind the server to `X_ZOHO_CATALYST_LISTEN_PORT`.
4. Keep the snapshot workflow so AppSail serves preprocessed data rather than parsing the workbook per request.
5. Embed the verified internal app into Zoho Creator or the company portal only after the AppSail build is stable.

Why AppSail is preferred:

- The workbook is heavy.
- Catalyst request limits are tighter than Vercel.
- Snapshot-backed startup is safer than request-time workbook parsing.

## Zoho AppSail custom runtime path

The production path should use a custom AppSail image, not the managed `python_3_9` runtime.

Preparation steps already in this repo:

1. `python scripts/package_appsail.py`
2. `python scripts/build_appsail_image.py`
3. Use `catalyst.custom-runtime.example.json` as the production Catalyst config shape.
4. Deploy the generated `docker-archive://dist/appsail-image.tar` target to AppSail.

Why this is the right Zoho path:

- The image ships with the prebuilt snapshot, so cold start does not depend on parsing the workbook.
- The runtime matches the Vercel validation path more closely.
- Future internal apps can share a container-first deployment standard instead of being forced onto Zoho's older managed runtime.

Current local limitation:

- This machine does not have Docker installed, so the custom AppSail image build cannot be smoke-tested here yet.
- The repo includes the container build scripts, Dockerfile, and config template, but the final custom-runtime verification requires Docker or a CI runner with Docker.

## Operational rules

- Do not commit workbook links with credentials.
- Do not commit `.env`.
- Do not commit generated snapshot artifacts unless explicitly approved.
- Use `python -m pytest -q` after backend edits.
- Use `python build_snapshot.py` after data-loader changes.
