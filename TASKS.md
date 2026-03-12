# Tasks - SalesTrendsDashboard

## Protocol
Before claiming a task: read AGENTS.md + COORDINATION.md (in BluewudOrchestrator/).
Claim a task by moving it to IN PROGRESS with your agent tag [CLAUDE]/[CODEX-XX]/[MINIMAX]/[OPENCLAW].
Always work on a branch: feat/[agent]-T[id]-[slug]. Never commit directly to main.

## PENDING

## IN PROGRESS
- [ ] [T-005] Rebuild analytics data model, dashboard UX, and Vercel-first deployment path (Priority: HIGH) - claimed by [CODEX-01] on 2026-03-12
  - Branch: codex/feat/codex-01-T005-rebuild-dashboard-foundation
  - Scope: Fix broken metrics, add preprocessed summaries, add regression tests, integrate learnings from Data Analysis, and prepare Vercel deployment

## DONE
- [x] [T-001] Add date range filter to the sales trends API endpoint (Priority: MED) - Already implemented
- [x] [T-002] Add caching layer (Redis or in-memory) to avoid repeated Zoho API calls (Priority: MED) - Done: in-memory cache with 5-min TTL
- [x] [T-003] Write unit tests for the data aggregation logic (Priority: LOW) - Done by [CODEX-01] on 2026-03-12 with API and loader regression coverage
- [x] [T-004] Add error handling for Zoho API rate limit responses (429) (Priority: HIGH) - Done: exponential backoff retry (3 attempts)
