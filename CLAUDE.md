# CLAUDE.md — SalesTrendsDashboard (Claude Code Extension)
# This file extends AGENTS.md with Claude Code-specific context.
# READ AGENTS.md FIRST — all architecture, rules, and project identity live there.

---

## Claude Code Notes

- **Entry point pattern:** Mangum-wrapped FastAPI/Flask → `module_name.handler` in catalyst.json
- **Python imports:** always check `requirements.txt` — Catalyst's Python environment is not full PyPI
- **Env vars in Catalyst:** set via Catalyst console UI, not in code. Never use `python-dotenv` in production functions.
- **Testing locally:** use `catalyst serve` to emulate the Catalyst runtime before `catalyst deploy`

## Useful Claude Code Commands for This Project

```bash
# Test the function locally
catalyst serve

# Check what Python packages are available
cat functions/salestrends/requirements.txt

# Check function config
cat catalyst.json
```

## What to Read Before Touching Code

1. `AGENTS.md` — project context, critical rules, tech stack
2. `PROJECT_IDENTITY.md` — project identity (locked)
3. `functions/salestrends/index.py` — main function entry point
4. `catalyst.json` — function registration
