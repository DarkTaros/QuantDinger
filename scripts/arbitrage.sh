#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PYTHONPATH="$ROOT/backend_api_python:$ROOT/nautilus_runtime${PYTHONPATH:+:$PYTHONPATH}"
PYTHON="$ROOT/nautilus_runtime/.venv/bin/python"
cd "$ROOT"
case "${1:-help}" in
  install)
    uv venv --python 3.14 "$ROOT/nautilus_runtime/.venv"
    uv pip sync --python "$PYTHON" "$ROOT/nautilus_runtime/requirements.lock"
    ;;
  supervisor) exec "$PYTHON" -m qd_nautilus.supervisor ;;
  backtest-worker) exec "$PYTHON" -m celery -A qd_nautilus.worker:app worker -Q arbitrage_backtest --pool=solo --concurrency=1 --loglevel=INFO --hostname=arbitrage-backtest@localhost ;;
  import-worker) exec "$PYTHON" -m celery -A qd_nautilus.worker:app worker -Q arbitrage_import --pool=solo --concurrency=1 --loglevel=INFO --hostname=arbitrage-import@localhost ;;
  test) exec "$PYTHON" -m pytest nautilus_runtime/tests -q ;;
  *) echo 'Usage: scripts/arbitrage.sh install|supervisor|backtest-worker|import-worker|test' ;;
esac
