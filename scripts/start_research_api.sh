#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_PORT="${API_PORT:-8989}"

choose_python() {
  local candidates=(
    "$ROOT/.venv/bin/python"
    "$ROOT/.venv/bin/python3"
    "$ROOT/.venv-codex/bin/python"
    "$ROOT/.venv-codex/bin/python3"
    "python3"
    "python"
  )

  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ "$candidate" == */* ]]; then
      if [[ -x "$candidate" ]]; then
        echo "$candidate"
        return 0
      fi
      continue
    fi

    if command -v "$candidate" >/dev/null 2>&1; then
      echo "$candidate"
      return 0
    fi
  done

  return 1
}

PYTHON_BIN="$(choose_python)"
if [[ -z "${PYTHON_BIN:-}" ]]; then
  echo "No usable Python interpreter found for FastAPI startup." >&2
  exit 1
fi

cd "$ROOT"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

exec "$PYTHON_BIN" -m uvicorn src.web_api.app:app --host 0.0.0.0 --port "$API_PORT"
