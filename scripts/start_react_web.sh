#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEB_DIR="$ROOT/web"
VITE_HOST="${VITE_HOST:-0.0.0.0}"
REACT_WEB_PORT="${REACT_WEB_PORT:-5174}"

if [[ ! -f "$WEB_DIR/package.json" ]]; then
  echo "React frontend not found at $WEB_DIR" >&2
  exit 1
fi

cd "$WEB_DIR"
if [[ -x "$WEB_DIR/node_modules/.bin/vite" ]]; then
  exec "$WEB_DIR/node_modules/.bin/vite" --host "$VITE_HOST" --port "$REACT_WEB_PORT"
fi

exec npm run dev
