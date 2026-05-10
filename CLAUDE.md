# nico-tools

Repository: https://github.com/jeremypruitt/nico-tools

## Agent skills

### Issue tracker

Issues live in GitHub Issues. See `docs/agents/issue-tracker.md`.

### Triage labels

Default label vocabulary (needs-triage, needs-info, ready-for-agent, ready-for-human, wontfix). See `docs/agents/triage-labels.md`.

### Priority scoring

Every issue carries a 1-100 priority score in the project board's **Score** number field. Score is the source of truth; the **Priority** single-select field and the band label (`crit`/`top`/`high`/`med`/`low`) are derived. See `docs/agents/issue-tracker.md` §"Priority scoring".

**Two write paths keep them in sync:**

1. **Claude writes all three inline** at scoring time (Score + Priority field + label). Instant correctness; no waiting on cron.
2. **Cron reconciliation** every 15 min (workflow `priority-reconcile` job) scans every open issue, derives the expected band from Score, and fixes Priority/label if drifted. This catches manual board edits to Score that bypass Claude.

GitHub does not deliver `projects_v2_item` events to repo workflows for user-owned ProjectsV2, which is why the cron pattern (rather than event-driven) is the safety net.

**When filing any GitHub issue:** run `/priority-score` (autonomous mode), include the one-liner rationale in the body's `## Priority` section between Acceptance criteria and Blocked by, and write **all three** — Score project field + Priority single-select + band label — in the same scoring step. Override path: if you judge a band wrong despite the math, write Score per the math AND deliberately set Priority + label to the desired band; the next cron pass will revert the override unless the underlying Score is updated. Note overrides explicitly in chat so the user can re-score if the override should stick.

### Domain docs

Single-context layout — one `CONTEXT.md` + `docs/adrs/` at the repo root. See `docs/agents/domain.md`.

## Plan Mode

- Make the plan extremely concise. Sacrifice grammar for the sake of concision.
- At the end of each plan, give me a list of unresolved questions to answer, if any.
