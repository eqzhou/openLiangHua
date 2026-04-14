# Project Docs

These files are the durable handoff layer for this workspace. When a chat is interrupted, start here instead of reconstructing context from memory.

- `PROJECT_ARCHITECTURE.md`: what the project does, how data flows, which modules own which responsibilities, and what the dashboard reads.
- `APP_ARCHITECTURE_PROPOSAL_2026-04-02.md`: frontend/application-layer diagnosis and the recommended refactor path for turning the dashboard into a layered app.
- `PROJECT_MEMORY_2026-04-02.md`: latest operating snapshot, pipeline refresh status, recovery checklist, and near-term backlog.
- `../src/app/`: the new application layer for repositories, services, and page modules extracted from `streamlit_app.py`.
- `../reports/weekly/000078_action_memo_2026-04-02.md`: the current holding note for `000078.SZ` and the next-day action scenarios.
- `../reports/weekly/000078_watch_plan_2026-04-02.md`: the auto-generated next-trade-day watch plan for the current holding.
- `../reports/weekly/myquant_overlay_inference_packet.json`: the latest unlabeled cross-section inference snapshot for the newest feature date.

Maintenance rule:

1. Update `PROJECT_MEMORY_*.md` after any major pipeline run, dashboard repair, or watchlist change.
2. Regenerate `*_watch_plan_*.md` with `python -m src.agents.watch_plan` after watchlist or mark-price changes.
3. Add or refresh a holding memo in `reports/weekly/` when a position needs an execution plan.
4. Keep `README.md` pointing at the latest durable docs so the next recovery starts from files instead of chat history.
5. After running `python -m src.agents.overlay_inference_report`, refresh the latest inference packet/brief references if the newest feature date changes.
6. When moving dashboard logic, prefer `src/app/repositories`, `src/app/services`, and `src/app/pages` over adding new orchestration to `streamlit_app.py`.

Encoding note:

- Chinese markdown files should be read as UTF-8 in PowerShell, for example:
  - `Get-Content -Encoding UTF8 reports\weekly\000078_action_memo_2026-04-02.md`
