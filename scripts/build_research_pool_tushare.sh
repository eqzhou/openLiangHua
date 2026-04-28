#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_PATH="$ROOT/.env"
USER_ID="${1:-bootstrap-admin}"
LIMIT="${2:-50}"
END_DATE="${3:-}"

if [[ ! -f "$ENV_PATH" ]]; then
  echo "Missing .env. Copy .env.example to .env and set TUSHARE_TOKEN first." >&2
  exit 1
fi

TUSHARE_TOKEN="$(grep -E '^[[:space:]]*TUSHARE_TOKEN=' "$ENV_PATH" | head -n 1 | cut -d '=' -f 2- | tr -d '\r')"
if [[ -z "${TUSHARE_TOKEN:-}" ]]; then
  echo "TUSHARE_TOKEN is missing or empty in .env." >&2
  exit 1
fi

if [[ -x "$ROOT/.venv-tushare/bin/python" ]]; then
  PYTHON_BIN="$ROOT/.venv-tushare/bin/python"
elif [[ -x "$ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

ARGS=(-m src.data.research_pool_selector --data-source tushare --user-id "$USER_ID" --limit "$LIMIT")
if [[ -n "$END_DATE" ]]; then
  ARGS+=(--end-date "$END_DATE")
fi

(
  cd "$ROOT"
  export TUSHARE_TOKEN
  exec "$PYTHON_BIN" "${ARGS[@]}"
)
