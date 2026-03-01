# AGENTS.md — SalesTrendsDashboard
# Universal AI context file. Read this first, regardless of which AI tool you are.
# Works with: Claude Code, MiniMax, Antigravity, OpenClaw, Codex, Cursor, Copilot

---

## Project Identity

- **Name:** SalesTrendsDashboard
- **Owner:** Shubh (Bluewud)
- **Platform:** Zoho Catalyst (Advanced I/O — Python Flask/FastAPI)
- **Status:** Live / Production
- **Purpose:** Sales analytics dashboard — visualizes revenue trends, order volumes, product performance from Zoho CRM/Books data.

---

## Tech Stack

| Layer       | Tech                                   |
|-------------|----------------------------------------|
| Backend     | Python (FastAPI or Flask) + Mangum ASGI adapter |
| Hosting     | Zoho Catalyst Advanced I/O function    |
| Data source | Zoho CRM + Zoho Books API              |
| Auth        | Zoho OAuth2 (client_credentials flow)  |
| Frontend    | (Served by Catalyst, if any)           |

---

## Critical Rules — Any AI Must Follow

1. **Never hardcode credentials.** All secrets go in `os.environ` / Catalyst environment variables.
2. **Never call `catalyst deploy` directly** — Shubh deploys. Your job is code changes only.
3. **The Mangum adapter wraps the ASGI app** — do not remove it or change the handler signature.
4. **Zoho API rate limits** — add caching (in-memory or Catalyst Datastore) before heavy API usage.
5. **Do not change the function name** in `catalyst.json` — it affects routing.

---

## File Structure (important files)

```
functions/
  salestrends/
    index.py          ← entry point (Mangum-wrapped FastAPI/Flask app)
    requirements.txt  ← Python deps (must include mangum, fastapi/flask)
catalyst.json         ← function registration (do not change function names)
.env.example          ← shows required env var names
PROJECT_IDENTITY.md   ← locked project identity (do not modify)
```

---

## Deployment (Shubh only — for context)

```bash
catalyst deploy --only functions:salestrends
```

---

## When Working on This Project

- Understand the Zoho data model before modifying API calls (CRM modules, Books reports)
- Test API calls with Postman or a local Python script before embedding in function code
- Keep function cold-start time low — avoid heavy imports at module level if possible
- Any new Zoho API scope → add to PROJECT_IDENTITY.md → tell Shubh to update OAuth app

---

## Handoff Protocol

When you finish a task on this project:
1. Summarize what you changed and why
2. List any files modified
3. Flag any TODOs or follow-ups needed
4. Do NOT push/deploy — Shubh does that


## Session Start Checklist

Every session, before writing any code:
1. Read this AGENTS.md fully
2. Read TASKS.md — check what's IN PROGRESS (don't duplicate work)
3. Claim your task in TASKS.md before starting
4. Work on a branch: feat/[agent-tag]-T[id]-[slug]
5. Full protocol: BluewudOrchestrator/COORDINATION.md
