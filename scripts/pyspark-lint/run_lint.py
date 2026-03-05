#!/usr/bin/env python3
"""
PySpark Lint Runner — Runs `cy lint --format json` on every cloned repo.

Produces one JSON result file per repo with a metadata wrapper containing
the repo name, timestamp, PySpark file list, and raw cy lint output.

Usage:
  python3 run_lint.py --repos-dir corpus-pyspark/repos --results-dir lint-results
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


PYSPARK_INDICATORS = [
    "from pyspark",
    "import pyspark",
    "SparkSession",
    "SparkContext",
    "pyspark.sql",
]


_SKIP_DIRS = [
    "/test/", "/tests/", "/testing/",
    "/test_", "/_test",
    "/spec/", "/specs/",
    "/fixtures/", "/testdata/", "/test-data/", "/test_data/",
    "/mock/", "/mocks/",
    "/examples/", "/example/", "/samples/", "/sample/",
    "/demo/", "/demos/",
    "/benchmark/", "/benchmarks/",
    "/venv/", "/.venv/", "/site-packages/",
    "/node_modules/", "/.tox/", "/.nox/",
    "/docs/", "/doc/",
]

_SKIP_NAMES = {
    "setup.py", "conftest.py", "conf.py", "noxfile.py", "fabfile.py",
}

_SKIP_PREFIXES = ("test_", "tests_")
_SKIP_SUFFIXES = ("_test.py", "_tests.py", "_spec.py")


def find_pyspark_files(repo_dir: Path) -> list[str]:
    """Find .py files containing PySpark usage, excluding test/example code.
    Returns paths relative to repo_dir."""
    candidates = []
    for py_file in repo_dir.rglob("*.py"):
        rel = str(py_file.relative_to(repo_dir))
        # Skip test/example/venv directories
        if any(skip in f"/{rel}" for skip in _SKIP_DIRS):
            continue
        # Skip known non-production files by name
        if py_file.name in _SKIP_NAMES:
            continue
        # Skip test files by naming convention
        if py_file.name.startswith(_SKIP_PREFIXES) or py_file.name.endswith(_SKIP_SUFFIXES):
            continue
        try:
            content = py_file.read_text(encoding="utf-8", errors="replace")
            if any(ind in content for ind in PYSPARK_INDICATORS):
                candidates.append(rel)
        except (OSError, UnicodeDecodeError):
            continue
    return candidates


def lint_repo(repo_dir: Path, files: list[str], timeout: int) -> tuple[dict | None, str]:
    """
    Run cy lint on specific files within a repo directory.
    Returns (parsed_json, error_message). On success error_message is empty.
    """
    cmd = ["cy", "lint", "--format", "json"]
    cmd.extend(str(repo_dir / f) for f in files)
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return None, "timeout"
    except FileNotFoundError:
        return None, "cy not found on PATH"
    except OSError as e:
        return None, str(e)

    stdout = result.stdout.strip()
    if not stdout:
        # cy lint produced no output — might be an error
        stderr = result.stderr.strip()
        if stderr:
            return None, stderr[:200]
        return None, f"no output (exit code {result.returncode})"

    try:
        parsed = json.loads(stdout)
        return parsed, ""
    except json.JSONDecodeError as e:
        return None, f"invalid JSON: {e}"


def main():
    parser = argparse.ArgumentParser(
        description="Run cy lint on all cloned PySpark repos"
    )
    parser.add_argument(
        "--repos-dir", type=Path, default=Path("corpus-pyspark/repos"),
        help="Directory containing cloned repos (default: corpus-pyspark/repos)",
    )
    parser.add_argument(
        "--results-dir", type=Path, default=Path("lint-results"),
        help="Directory for lint result JSONs (default: lint-results/)",
    )
    parser.add_argument(
        "--timeout", type=int, default=120,
        help="Timeout in seconds per repo (default: 120)",
    )
    args = parser.parse_args()

    repos_dir = args.repos_dir.resolve()
    results_dir = args.results_dir.resolve()

    if not repos_dir.exists():
        print(f"Error: Repos directory not found: {repos_dir}", file=sys.stderr)
        print("Run search_pyspark.py first.", file=sys.stderr)
        sys.exit(1)

    if not shutil.which("cy"):
        print("Error: 'cy' command not found on PATH.", file=sys.stderr)
        sys.exit(1)

    results_dir.mkdir(parents=True, exist_ok=True)

    # Collect repo directories
    repo_dirs = sorted([
        d for d in repos_dir.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    ])

    if not repo_dirs:
        print(f"No repo directories found in {repos_dir}", file=sys.stderr)
        sys.exit(1)

    print("═══════════════════════════════════════════════════════")
    print("  PySpark Lint Runner")
    print("═══════════════════════════════════════════════════════")
    print(f"  Repos dir:   {repos_dir}")
    print(f"  Results dir: {results_dir}")
    print(f"  Total repos: {len(repo_dirs)}")
    print(f"  Timeout:     {args.timeout}s per repo")
    print()

    success = 0
    skipped = 0
    failed = 0
    timed_out = 0

    for i, repo_dir in enumerate(repo_dirs, 1):
        dir_name = repo_dir.name
        # owner__repo → owner/repo
        repo_name = dir_name.replace("__", "/", 1)
        result_file = results_dir / f"{dir_name}.json"

        # Skip if already processed
        if result_file.exists():
            print(f"  [{i}/{len(repo_dirs)}] {repo_name} ... cached")
            success += 1
            continue

        print(f"  [{i}/{len(repo_dirs)}] {repo_name} ... ", end="", flush=True)

        # Find PySpark files
        pyspark_files = find_pyspark_files(repo_dir)
        if not pyspark_files:
            print("skipped (no PySpark files)")
            skipped += 1
            continue

        # Run cy lint on filtered files only
        cy_output, error = lint_repo(repo_dir, pyspark_files, args.timeout)

        if error == "timeout":
            print(f"TIMEOUT ({args.timeout}s)")
            timed_out += 1
            continue

        if cy_output is None:
            print(f"FAILED: {error}")
            failed += 1
            continue

        # Write result with metadata wrapper
        wrapper = {
            "repo": repo_name,
            "scanned_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "pyspark_files": pyspark_files,
            "cy_output": cy_output,
        }
        with open(result_file, "w") as f:
            json.dump(wrapper, f, indent=2)

        finding_count = cy_output.get("total_findings", 0)
        print(f"ok ({finding_count} findings, {len(pyspark_files)} files)")
        success += 1

    print()
    print("── Results ─────────────────────────────────────────────")
    print(f"  Repos processed: {i}")
    print(f"  Linted OK:       {success}")
    print(f"  Skipped:         {skipped} (no PySpark files)")
    print(f"  Failed:          {failed}")
    print(f"  Timed out:       {timed_out}")
    print(f"  Results saved to: {results_dir}/")
    print()
    print("Next steps:")
    print(f"  python3 scripts/pyspark-lint/collect_metadata.py --results-dir {results_dir} --output-dir metadata --token $GITHUB_TOKEN")
    print(f"  python3 scripts/pyspark-lint/classify_tiers.py --metadata-dir metadata")
    print(f"  python3 scripts/pyspark-lint/aggregate_lint.py --results-dir {results_dir} --metadata-dir metadata --format both")


if __name__ == "__main__":
    main()
