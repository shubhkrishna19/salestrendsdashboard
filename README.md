# SalesTrendsDashboard

Sales analytics dashboard for Bluewud Industries.
Deployed as a Zoho Catalyst Advanced I/O serverless function (Python 3.10).

---

## Architecture

```
Browser
  └── GET /server/salestrends/           → dashboard.html (served by FastAPI)
  └── GET /server/salestrends/api/kpis   → FastAPI endpoint → Pandas aggregation
  └── GET /server/salestrends/api/trend  → FastAPI endpoint → Pandas aggregation
  └── ...

Catalyst Runtime
  └── Invokes index.handler (Mangum)
  └── Mangum translates Catalyst event → ASGI → FastAPI
  └── DataManager loads Excel from GitHub once per warm instance
```

---

## Local Development

### Prerequisites
- Python 3.10+
- pip
- Access to the private GitHub repo containing `data.xlsx`

### Setup

```bash
# 1. Clone the repo
git clone https://github.com/shubhkrishna19/salestrendsdashboard
cd salestrendsdashboard

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r functions/salestrends/requirements.txt

# 4. Set environment variables
cp .env.example .env
# Edit .env — add your GITHUB_TOKEN and GITHUB_REPO

# 5. Run locally
cd functions/salestrends
uvicorn app_api:app --reload --port 8000

# 6. Open browser
# http://localhost:8000
```

### Verify it's working

```bash
curl http://localhost:8000/api/health
# Expected: {"status":"ok","data_loaded":true,"rows":XXXXX,...}
```

If `data_loaded` is `false`, check your `.env` values.

---

## Catalyst Deployment

### First-time setup (one per machine)

```bash
npm install -g @zohocloud/catalyst-cli
catalyst login
```

### Link to existing Catalyst project

```bash
# From the SalesTrendsDashboard root directory
catalyst init
# Select existing project: SalesTrendsDashboard
# This updates .catalystrc with your project ID
```

### Set environment variables in Catalyst console

Go to: **Catalyst Console → SalesTrendsDashboard → Functions → salestrends → Environment Variables**

Add:
| Key | Value |
|-----|-------|
| `GITHUB_TOKEN` | Your GitHub PAT (repo:read scope) |
| `GITHUB_REPO` | `shubhkrishna19/salestrendsdashboard` |
| `DATA_FILE` | `data.xlsx` |
| `SHEET_NAME` | `Final Sale Data` |

### Deploy

```bash
# From project root
catalyst deploy --only functions:salestrends

# Watch logs
catalyst logs --function salestrends --tail
```

### Verify deployment

```bash
# Replace with your actual Catalyst URL
curl https://YOUR-PROJECT-ID.catalystserverless.com/server/salestrends/api/health
```

---

## Updating the Data

The dashboard reads `data.xlsx` from the GitHub repo on every cold start.

To update the data:
1. Replace `data.xlsx` in the GitHub repo (same sheet name: `Final Sale Data`)
2. Catalyst functions will pick up the new data on their next cold start
3. To force refresh: redeploy the function or wait for the instance to cycle

---

## API Reference

| Endpoint | Description |
|----------|------------|
| `GET /` | Dashboard HTML |
| `GET /api/health` | Health check, data load status |
| `GET /api/filters` | Available platforms, categories, date range |
| `GET /api/kpis` | Revenue, volume, orders, AOV, return rate |
| `GET /api/trend` | Revenue trend over time |
| `GET /api/platforms` | Platform aggregation |
| `GET /api/categories` | Category aggregation |
| `GET /api/products?n=10` | Top N products by revenue |
| `GET /api/products/volume?n=10` | Top N products by volume |
| `GET /api/returns/trend` | Return rate trend |
| `GET /api/returns/by-platform` | Returns broken down by platform |
| `GET /api/returns/by-reason` | Returns by return reason |
| `GET /api/returns/validity` | Valid vs invalid returns |
| `GET /api/operations` | Monthly orders, tax summary |
| `GET /api/export` | Download filtered data as CSV |

All endpoints accept optional query params: `platform`, `category`, `start_date`, `end_date`.

---

## Troubleshooting

**Data not loading on Catalyst**
→ Check environment variables in Catalyst console
→ Run `catalyst logs --function salestrends` and look for "Loading from GitHub"

**500 error on cold start**
→ Usually a missing package — check `requirements.txt` has all dependencies
→ Check `catalyst logs` for the exact ImportError

**Charts blank after filter**
→ The filter combination returned zero rows — widen your date range or remove a filter

**Deployment fails**
→ Run `catalyst deploy --verbose` for detailed output
→ Ensure `.catalystrc` has the correct project ID

---

## Project Rules

See `CLAUDE.md` for AI agent rules and `PROJECT_IDENTITY.md` for the locked project spec.
Never commit credentials. Never hardcode API keys. Always test locally before deploying.
