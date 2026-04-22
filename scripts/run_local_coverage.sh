#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

python_bin="${OPX_COVERAGE_PYTHON:-}"
if [[ -z "$python_bin" ]]; then
  if [[ -x "$repo_root/.venv/bin/python" ]]; then
    python_bin="$repo_root/.venv/bin/python"
  else
    python_bin="python3"
  fi
fi

if ! command -v "$python_bin" >/dev/null 2>&1; then
  echo "Unable to find Python interpreter for local coverage: $python_bin" >&2
  exit 1
fi

echo "Running local coverage with: $python_bin"
echo "1/5 coverage erase"
"$python_bin" -m coverage erase

echo "2/5 pytest under coverage"
"$python_bin" -m coverage run -m pytest

echo "3/5 terminal report"
"$python_bin" -m coverage report -m

echo "4/5 html/xml/json artifacts"
"$python_bin" -m coverage html
"$python_bin" -m coverage xml
"$python_bin" -m coverage json

echo "5/5 uncovered-files focus"
"$python_bin" -m coverage report -m --skip-covered

echo "Coverage artifacts:"
echo "  htmlcov/index.html"
echo "  coverage.xml"
echo "  coverage.json"
