# OpenLiangHua Agent Guide

This repository uses an ECC-style, skills-first workflow by default.

## Default Workflow

For non-trivial software engineering tasks, prefer this sequence:

1. Read `README.md` and relevant `docs/` before editing.
2. Use `product-capability` or planning-oriented skills first when the task crosses multiple layers.
3. Use `tdd-workflow` for features, bug fixes, and refactors unless the change is genuinely tiny.
4. Always apply `coding-standards`.
5. Add `security-review` when touching auth, external input, secrets, databases, APIs, or write paths.
6. Finish with `verification-loop` or equivalent build/test/typecheck verification.

## Skill Priority

The project-local ECC subset lives in `.agents/skills/`. Prefer these skills proactively when they fit:

- `coding-standards`
- `tdd-workflow`
- `security-review`
- `verification-loop`
- `product-capability`
- `api-design`
- `backend-patterns`
- `frontend-patterns`
- `frontend-design`
- `documentation-lookup`
- `e2e-testing`
- `strategic-compact`
- `agent-sort`
- `agent-introspection-debugging`

## Project-Specific Guidance

- This repo is an A-share quant research and trading dashboard.
- Prefer Chinese for summaries, plans, and implementation notes unless the user asks otherwise.
- Respect existing docs and domain constraints before changing API shapes or UI workflows.
- Treat PostgreSQL-backed operational state and stable UI payload contracts as first-class constraints.

## Verification

Before considering a task complete:

1. Run the narrowest relevant tests first.
2. Run broader verification if the change crosses layers.
3. Call out anything still mock/demo/incomplete rather than implying production readiness.
