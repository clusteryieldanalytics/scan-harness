#!/usr/bin/env python3
"""
PySpark GitHub Scraper — Searches for PySpark code in public repos and clones them.

Phases:
  1. Search GitHub for repos containing PySpark code
  2. Clone matching repos (shallow, to save bandwidth)
  3. Identify candidate Python files containing PySpark usage

Usage:
  python3 search_pyspark.py --output-dir ../../corpus-pyspark --max-repos 500
  python3 search_pyspark.py --output-dir ../../corpus-pyspark --max-repos 500 --token ghp_xxx

Requires:
  pip install requests
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

try:
    import requests
except ImportError:
    print("Error: 'requests' package required. Install with: pip install requests")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────
# GitHub API Search
# ─────────────────────────────────────────────────────────────

SEARCH_QUERIES = [
    # PySpark imports and session creation
    '"from pyspark" language:python',
    '"SparkSession" language:python',
    '"spark.sql" "pyspark" language:python',
    '"pyspark.sql.functions" language:python',
    # DataFrame chain patterns
    '".groupBy(" "pyspark" language:python',
    '".join(" "pyspark" language:python',
]

GITHUB_API_BASE = "https://api.github.com"
SEARCH_ENDPOINT = f"{GITHUB_API_BASE}/search/code"


def search_github(
    query: str,
    token: Optional[str] = None,
    max_results: int = 100,
) -> list[dict]:
    """
    Search GitHub code API for files matching the query.
    Returns a list of {repo_full_name, repo_clone_url, file_path, file_url}.
    """
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    results = []
    page = 1
    per_page = min(100, max_results)  # GitHub max is 100 per page

    while len(results) < max_results:
        params = {
            "q": query,
            "per_page": per_page,
            "page": page,
            "sort": "indexed",  # most recently indexed first
        }

        resp = requests.get(SEARCH_ENDPOINT, headers=headers, params=params)

        if resp.status_code == 403:
            # Rate limited — check when we can retry
            reset_time = int(resp.headers.get("X-RateLimit-Reset", 0))
            wait = max(reset_time - int(time.time()), 10)
            print(f"  Rate limited. Waiting {wait}s...")
            time.sleep(wait)
            continue

        if resp.status_code != 200:
            print(f"  GitHub API error {resp.status_code}: {resp.text[:200]}")
            break

        data = resp.json()
        items = data.get("items", [])
        if not items:
            break

        for item in items:
            repo = item.get("repository", {})
            results.append({
                "repo_full_name": repo.get("full_name", ""),
                "repo_clone_url": f"https://github.com/{repo.get('full_name', '')}.git",
                "file_path": item.get("path", ""),
                "file_html_url": item.get("html_url", ""),
            })

        page += 1

        # Respect rate limits — GitHub code search allows 10 requests/minute
        # for unauthenticated, 30/minute for authenticated
        time.sleep(6 if not token else 2)

        # GitHub code search returns max 1000 results
        if page > 10:
            break

    return results[:max_results]


def deduplicate_repos(results: list[dict]) -> dict[str, dict]:
    """
    Deduplicate by repo, collecting all matching file paths per repo.
    Returns {repo_full_name: {clone_url, files: [path, ...]}}.
    """
    repos = {}
    for r in results:
        name = r["repo_full_name"]
        if name not in repos:
            repos[name] = {
                "clone_url": r["repo_clone_url"],
                "files": [],
            }
        repos[name]["files"].append(r["file_path"])
    return repos


# ─────────────────────────────────────────────────────────────
# Repo Cloning
# ─────────────────────────────────────────────────────────────

def clone_repo(clone_url: str, target_dir: Path, timeout: int = 60) -> tuple[bool, str]:
    """Shallow clone a repo. Returns (success, error_message)."""
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", "--single-branch", clone_url, str(target_dir)],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=True,
        )
        return True, ""
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        for line in stderr.splitlines():
            if "fatal:" in line or "error:" in line:
                return False, line.strip()
        return False, stderr[:200] if stderr else "unknown error"
    except subprocess.TimeoutExpired:
        import shutil
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        return False, f"timed out after {timeout}s"


def find_pyspark_files(repo_dir: Path) -> list[Path]:
    """
    Find Python files that contain PySpark usage.
    These are candidates for linting with cy lint.
    """
    pyspark_indicators = [
        "from pyspark",
        "import pyspark",
        "SparkSession",
        "SparkContext",
        "pyspark.sql",
    ]

    candidates = []
    for py_file in repo_dir.rglob("*.py"):
        # Skip test files
        if "/test/" in str(py_file) or "/tests/" in str(py_file):
            continue
        # Skip setup/config files
        if py_file.name in ("setup.py", "conftest.py", "conf.py"):
            continue
        # Skip virtual environments
        if "/venv/" in str(py_file) or "/.venv/" in str(py_file) or "/site-packages/" in str(py_file):
            continue

        try:
            content = py_file.read_text(encoding="utf-8", errors="replace")
            if any(ind in content for ind in pyspark_indicators):
                candidates.append(py_file)
        except (OSError, UnicodeDecodeError):
            continue

    return candidates


# ─────────────────────────────────────────────────────────────
# Main Pipeline
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Search GitHub for PySpark code and prepare for cy lint analysis"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("../../corpus-pyspark"),
        help="Directory for cloned repos and metadata (default: ../../corpus-pyspark)"
    )
    parser.add_argument(
        "--max-repos", type=int, default=500,
        help="Maximum number of repos to process (default: 500)"
    )
    parser.add_argument(
        "--token", type=str, default=os.environ.get("GITHUB_TOKEN"),
        help="GitHub API token (or set GITHUB_TOKEN env var)"
    )
    parser.add_argument(
        "--clone-dir", type=Path, default=None,
        help="Directory for cloned repos (default: output-dir/repos)"
    )
    parser.add_argument(
        "--skip-clone", action="store_true",
        help="Skip cloning, only search and report"
    )
    parser.add_argument(
        "--skip-search", action="store_true",
        help="Skip search, reuse existing search-results.json"
    )
    parser.add_argument(
        "--max-clone-failures", type=int, default=3,
        help="Abort after this many consecutive clone failures (default: 3)"
    )
    args = parser.parse_args()

    output_dir = args.output_dir.resolve()
    clone_dir = (args.clone_dir or output_dir / "repos").resolve()

    output_dir.mkdir(parents=True, exist_ok=True)
    clone_dir.mkdir(parents=True, exist_ok=True)

    search_results_path = output_dir / "search-results.json"

    # Step 1: Search GitHub (or load cached results)
    if args.skip_search or (search_results_path.exists() and not args.token):
        if search_results_path.exists():
            print(f"  Loading cached search results from {search_results_path}")
            with open(search_results_path) as f:
                repos = json.load(f)
            print(f"  Loaded {len(repos)} repos")
        else:
            print("Error: --skip-search specified but no search-results.json found")
            sys.exit(1)
    else:
        print("═══════════════════════════════════════════════════════")
        print("  PySpark GitHub Scraper — Searching for PySpark code")
        print("═══════════════════════════════════════════════════════")
        print(f"  Token: {'provided' if args.token else 'none (rate limits will be tight)'}")
        print()

        all_results = []
        for query in SEARCH_QUERIES:
            print(f"  Searching: {query[:60]}...")
            results = search_github(query, token=args.token, max_results=200)
            print(f"    Found {len(results)} results")
            all_results.extend(results)

        repos = deduplicate_repos(all_results)
        print(f"\n  Unique repos: {len(repos)}")

        # Save search results
        with open(search_results_path, "w") as f:
            json.dump(repos, f, indent=2)
        print(f"  Saved search results to {search_results_path}")

    if args.skip_clone:
        print("\n  --skip-clone specified, stopping here.")
        return

    # Step 2: Clone repos and find PySpark candidates
    print(f"\n── Cloning repos (max {args.max_repos}) ──")

    total_pyspark_files = 0
    repos_with_pyspark = 0
    processed = 0
    clone_failures = 0
    consecutive_failures = 0
    max_consecutive = args.max_clone_failures

    # Track per-repo PySpark file counts for summary
    repo_summary = {}

    for repo_name, repo_info in list(repos.items())[:args.max_repos]:
        processed += 1
        repo_dir = clone_dir / repo_name.replace("/", "__")
        label = f"[{processed}/{min(len(repos), args.max_repos)}]"

        if repo_dir.exists():
            print(f"  {label} {repo_name} (cached)")
            consecutive_failures = 0
        else:
            print(f"  {label} {repo_name} ... ", end="", flush=True)
            success, error = clone_repo(repo_info["clone_url"], repo_dir)
            if success:
                print("ok")
                consecutive_failures = 0
            else:
                clone_failures += 1
                consecutive_failures += 1
                print(f"FAILED: {error}")

                if consecutive_failures >= max_consecutive:
                    print(f"\n  Aborting: {consecutive_failures} consecutive clone failures.")
                    print(f"    Last error: {error}")
                    print(f"    Use --max-clone-failures to adjust threshold.")
                    sys.exit(1)
                continue

        # Find PySpark files
        pyspark_files = find_pyspark_files(repo_dir)
        if pyspark_files:
            repos_with_pyspark += 1
            total_pyspark_files += len(pyspark_files)
            repo_summary[repo_name] = {
                "repo_dir": str(repo_dir),
                "pyspark_files": [str(f.relative_to(repo_dir)) for f in pyspark_files],
            }

    # Save repo summary (used by run_lint.sh to know which repos to lint)
    summary_path = output_dir / "repo-summary.json"
    with open(summary_path, "w") as f:
        json.dump(repo_summary, f, indent=2)

    print(f"\n  Repos processed: {processed}")
    print(f"  Repos with PySpark code: {repos_with_pyspark}")
    print(f"  Total PySpark files: {total_pyspark_files}")
    print(f"  Clone failures: {clone_failures}")
    print(f"  Repo summary saved to: {summary_path}")
    print(f"\nNext step:")
    print(f"  ./scripts/pyspark-lint/run_lint.sh --repos-dir {clone_dir} --results-dir lint-results")


if __name__ == "__main__":
    main()
