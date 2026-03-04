#!/usr/bin/env bash
#
# PySpark Lint Runner
#
# Runs `cy lint --format json` on every cloned repo that contains PySpark code.
# Produces one JSON result file per repo with metadata wrapper.
#
# Usage:
#   ./run_lint.sh --repos-dir corpus-pyspark/repos --results-dir lint-results
#
# Output structure:
#   lint-results/<owner__repo>.json
#

set -euo pipefail

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

REPOS_DIR="${REPOS_DIR:-corpus-pyspark/repos}"
RESULTS_DIR="${RESULTS_DIR:-lint-results}"
TIMEOUT="${TIMEOUT:-120}"

# Parse command-line flags
while [[ $# -gt 0 ]]; do
  case $1 in
    --repos-dir)    REPOS_DIR="$2"; shift 2 ;;
    --results-dir)  RESULTS_DIR="$2"; shift 2 ;;
    --timeout)      TIMEOUT="$2"; shift 2 ;;
    --help|-h)
      echo "Usage: $0 [--repos-dir DIR] [--results-dir DIR] [--timeout SECS]"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

# ─────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────

if [[ ! -d "$REPOS_DIR" ]]; then
  echo "Error: Repos directory not found: $REPOS_DIR" >&2
  echo "Run search_pyspark.py first." >&2
  exit 1
fi

if ! command -v cy &>/dev/null; then
  echo "Error: 'cy' command not found on PATH." >&2
  exit 1
fi

mkdir -p "$RESULTS_DIR"

# Count repos (directories in REPOS_DIR)
TOTAL_REPOS=0
for repo_dir in "$REPOS_DIR"/*/; do
  [[ -d "$repo_dir" ]] && TOTAL_REPOS=$((TOTAL_REPOS + 1))
done

if [[ "$TOTAL_REPOS" -eq 0 ]]; then
  echo "No repo directories found in $REPOS_DIR" >&2
  exit 1
fi

echo "═══════════════════════════════════════════════════════"
echo "  PySpark Lint Runner"
echo "═══════════════════════════════════════════════════════"
echo "  Repos dir:   $REPOS_DIR"
echo "  Results dir: $RESULTS_DIR"
echo "  Total repos: $TOTAL_REPOS"
echo "  Timeout:     ${TIMEOUT}s per repo"
echo ""

# ─────────────────────────────────────────────────────────────
# Lint Functions
# ─────────────────────────────────────────────────────────────

has_pyspark_files() {
  local repo_dir="$1"
  # Check if any .py file contains pyspark imports (quick grep)
  grep -rlq --include='*.py' -e 'from pyspark' -e 'import pyspark' -e 'SparkSession' "$repo_dir" 2>/dev/null
}

list_pyspark_files() {
  local repo_dir="$1"
  grep -rl --include='*.py' -e 'from pyspark' -e 'import pyspark' -e 'SparkSession' "$repo_dir" 2>/dev/null \
    | grep -v '/test/' | grep -v '/tests/' | grep -v '/venv/' | grep -v '/.venv/' | grep -v '/site-packages/' \
    || true
}

lint_repo() {
  local repo_dir="$1"
  local result_file="$2"
  local repo_name="$3"
  local timestamp
  timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  # Collect PySpark file list
  local pyspark_files
  pyspark_files=$(list_pyspark_files "$repo_dir")

  if [[ -z "$pyspark_files" ]]; then
    return 1  # No PySpark files
  fi

  # Convert file list to JSON array
  local files_json
  files_json=$(echo "$pyspark_files" | while IFS= read -r f; do
    # Make path relative to repo dir
    echo "\"${f#${repo_dir}/}\""
  done | paste -sd ',' - | sed 's/^/[/;s/$/]/')

  # Run cy lint on the repo directory
  local cy_output
  if cy_output=$(timeout "$TIMEOUT" cy lint --format json "$repo_dir" 2>/dev/null); then
    :  # success
  else
    local exit_code=$?
    if [[ $exit_code -eq 124 ]]; then
      echo "TIMEOUT"
      return 2
    fi
    # cy lint returns non-zero when findings exist — that's expected
    # Only treat it as a real failure if there's no JSON output
    if [[ -z "$cy_output" ]]; then
      return 3
    fi
  fi

  # Validate that we got JSON output
  if ! echo "$cy_output" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    return 3
  fi

  # Write result with metadata wrapper
  python3 -c "
import json, sys

cy_output = json.loads(sys.argv[1])
wrapper = {
    'repo': sys.argv[2],
    'scanned_at': sys.argv[3],
    'pyspark_files': json.loads(sys.argv[4]),
    'cy_output': cy_output,
}

with open(sys.argv[5], 'w') as f:
    json.dump(wrapper, f, indent=2)
" "$cy_output" "$repo_name" "$timestamp" "$files_json" "$result_file"

  return 0
}

# ─────────────────────────────────────────────────────────────
# Main Loop
# ─────────────────────────────────────────────────────────────

SUCCESS=0
SKIPPED=0
FAILED=0
TIMED_OUT=0
CURRENT=0

for repo_dir in "$REPOS_DIR"/*/; do
  [[ -d "$repo_dir" ]] || continue
  CURRENT=$((CURRENT + 1))

  # Derive repo name from directory: owner__repo → owner/repo
  dir_name=$(basename "$repo_dir")
  repo_name="${dir_name/__/\/}"
  result_file="${RESULTS_DIR}/${dir_name}.json"

  printf "\r  [%d/%d] %s ... " "$CURRENT" "$TOTAL_REPOS" "$repo_name"

  # Skip if already processed
  if [[ -f "$result_file" ]]; then
    printf "cached\n"
    SUCCESS=$((SUCCESS + 1))
    continue
  fi

  lint_repo "$repo_dir" "$result_file" "$repo_name"
  exit_code=$?

  case $exit_code in
    0)
      printf "ok\n"
      SUCCESS=$((SUCCESS + 1))
      ;;
    1)
      printf "skipped (no PySpark files)\n"
      SKIPPED=$((SKIPPED + 1))
      ;;
    2)
      printf "TIMEOUT\n"
      TIMED_OUT=$((TIMED_OUT + 1))
      ;;
    *)
      printf "FAILED\n"
      FAILED=$((FAILED + 1))
      ;;
  esac
done

echo ""
echo ""
echo "── Results ─────────────────────────────────────────────"
echo "  Repos processed: $CURRENT"
echo "  Linted OK:       $SUCCESS"
echo "  Skipped:         $SKIPPED (no PySpark files)"
echo "  Failed:          $FAILED"
echo "  Timed out:       $TIMED_OUT"
echo "  Results saved to: $RESULTS_DIR/"
echo ""
echo "Next step:"
echo "  python3 scripts/pyspark-lint/aggregate_lint.py --results-dir $RESULTS_DIR"
