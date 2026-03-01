# PROJECT IDENTITY — SalesTrendsDashboard
> ⚠️ THIS FILE IS LOCKED. No AI agent may modify it without explicit approval from Shubh Krishna.

## What This Project Is
A sales analytics dashboard for Bluewud Industries showing revenue, platform performance, product rankings, return analysis, and operations data — sourced from an Excel file in a private GitHub repo.

## Deployment Target
**Zoho Catalyst** — Advanced I/O Serverless Function (Python 3.10)
- Project name: `SalesTrendsDashboard`
- Function name: `salestrends`
- Live URL pattern: `https://<project-id>.catalystserverless.com/server/salestrends/`

## Approved Tech Stack
| Layer | Technology | Version |
|-------|-----------|---------|
| Backend | FastAPI | 0.111.x |
| ASGI adapter | Mangum | 0.17.x |
| Data | Pandas | 2.2.x |
| HTTP client | requests | 2.31.x |
| Excel | openpyxl | 3.1.x |
| Frontend | Vanilla HTML/CSS/JS + Chart.js 4.x | — |

**NOT ALLOWED:** Streamlit, Dash, Flask, Next.js, React, SQLAlchemy, PyGithub, or any other framework not in the list above. If you think a new package is needed, STOP and ask Shubh first.

## Folder Structure (DO NOT CHANGE)
```
SalesTrendsDashboard/
├── .env.example          ← template only, never real values
├── .gitignore
├── catalyst.json         ← LOCKED - Catalyst project config
├── PROJECT_IDENTITY.md   ← LOCKED - this file
├── CLAUDE.md             ← AI agent instructions
├── README.md             ← deployment guide
└── functions/
    └── salestrends/
        ├── index.py      ← LOCKED - Catalyst handler entry point
        ├── app_api.py    ← main application (can be modified carefully)
        ├── dashboard.html← frontend dashboard
        └── requirements.txt ← LOCKED unless adding approved package
```

## Data Source
- Excel file: `data.xlsx` — sheet: `Final Sale Data`
- Loaded from GitHub via raw URL download using `GITHUB_TOKEN` env var
- Falls back to local `data.xlsx` for development
- **Data file is gitignored** — never commit it

## Files That Are UNTOUCHABLE
- `index.py` — only imports handler, nothing else
- `catalyst.json` — Catalyst project config, do not restructure
- `.gitignore` — security-critical, do not reduce its coverage
- `.env.example` — only add new vars, never remove existing ones

## Environment Variables (set in Catalyst console, never in code)
- `GITHUB_TOKEN` — PAT with repo:read
- `GITHUB_REPO` — e.g. `shubhkrishna19/salestrendsdashboard`
- `DATA_FILE` — default: `data.xlsx`
- `SHEET_NAME` — default: `Final Sale Data`

## Owner
Shubh Krishna — shubhkrishna.19@gmail.com
GitHub: shubhkrishna19
