# AGENTS.md

Guidance for Codex and other coding agents working in this repository.

## Coding Preferences

- Prefer small, behavior-preserving changes. If behavior is already correct but unclear, clarify the invariant locally with comments or focused tests rather than adding broader validation layers.
- Keep edits scoped to the module and behavior under discussion. Avoid unrelated refactors, formatting churn, or defensive code unless it directly solves the issue.
- When a review comment looks suspicious, verify the actual runtime path before changing behavior. Distinguish frontend/API behavior from public helper functions and engine internals.
- Preserve existing public behavior unless the user explicitly asks to change it.

## Project Notes

- Read `CLAUDE.md` and the docs under `docs/` before broad source exploration. They are the current project reference.
- For data-viz unknown ticker handling, the frontend route intentionally prechecks the ticker map and returns an empty chart for missing symbols.
