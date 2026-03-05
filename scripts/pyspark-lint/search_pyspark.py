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

# GitHub Code Search returns max 1000 results per query, so we use
# multiple search terms to maximize corpus coverage.

_BASE_QUERIES = [
    # PySpark imports and session creation
    '"from pyspark" language:python',
    '"SparkSession" language:python',
    '"spark.sql" "pyspark" language:python',
    '"pyspark.sql.functions" language:python',
    # DataFrame chain patterns
    '".groupBy(" "pyspark" language:python',
    '".join(" "pyspark" language:python',
    # Additional patterns for broader coverage
    '"pyspark.sql.types" language:python',
    '"spark.read" "pyspark" language:python',
    '".toPandas()" "pyspark" language:python',
    '"spark.createDataFrame" language:python',
]

SEARCH_QUERIES = _BASE_QUERIES

GITHUB_API_BASE = "https://api.github.com"
SEARCH_ENDPOINT = f"{GITHUB_API_BASE}/search/code"
REPO_SEARCH_ENDPOINT = f"{GITHUB_API_BASE}/search/repositories"

# Queries for repository search (used with --min-stars to target mature repos).
# These use GitHub's repository search which supports stars:/forks: qualifiers.
_REPO_QUERIES = [
    "pyspark language:python",
    "spark etl language:python",
    "spark pipeline language:python",
    "spark data language:python",
    "pyspark topic:pyspark",
    "pyspark topic:spark",
    "pyspark topic:etl",
    "pyspark topic:data-engineering",
]


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


def search_github_repos(
    query: str,
    token: Optional[str] = None,
    min_stars: int = 50,
    max_results: int = 1000,
) -> list[dict]:
    """
    Search GitHub repository API for repos matching the query with star filter.
    Returns a list of {repo_full_name, repo_clone_url, file_path, file_url}.
    """
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    full_query = f"{query} stars:>={min_stars}"
    results = []
    page = 1
    per_page = 100

    while len(results) < max_results:
        params = {
            "q": full_query,
            "per_page": per_page,
            "page": page,
            "sort": "stars",
            "order": "desc",
        }

        resp = requests.get(REPO_SEARCH_ENDPOINT, headers=headers, params=params)

        if resp.status_code == 403:
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
            full_name = item.get("full_name", "")
            results.append({
                "repo_full_name": full_name,
                "repo_clone_url": f"https://github.com/{full_name}.git",
                "file_path": "",
                "file_html_url": item.get("html_url", ""),
            })

        page += 1
        time.sleep(2 if token else 6)

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

    skip_dirs = [
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
    skip_names = {"setup.py", "conftest.py", "conf.py", "noxfile.py", "fabfile.py"}

    candidates = []
    for py_file in repo_dir.rglob("*.py"):
        rel = str(py_file.relative_to(repo_dir))
        if any(skip in f"/{rel}" for skip in skip_dirs):
            continue
        if py_file.name in skip_names:
            continue
        if py_file.name.startswith(("test_", "tests_")) or py_file.name.endswith(("_test.py", "_tests.py", "_spec.py")):
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
    parser.add_argument(
        "--min-stars", type=int, default=0,
        help="Use repository search (not code search) to find repos with at least this many stars. "
             "Targets Tier 3/4 repos. (default: 0 = use code search)"
    )
    parser.add_argument(
        "--lint-results-dir", type=Path, default=None,
        help="Skip cloning repos that already have a lint result file in this directory"
    )
    parser.add_argument(
        "--cleanup", action="store_true",
        help="Delete cloned repos that already have lint results (requires --lint-results-dir). "
             "Frees disk space while preserving lint data."
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
        search_mode = "repo" if args.min_stars > 0 else "code"
        print("═══════════════════════════════════════════════════════")
        print("  PySpark GitHub Scraper — Searching for PySpark code")
        print("═══════════════════════════════════════════════════════")
        print(f"  Token: {'provided' if args.token else 'none (rate limits will be tight)'}")
        print(f"  Search mode: {search_mode}"
              + (f" (stars >= {args.min_stars})" if search_mode == "repo" else ""))
        print()

        # Load existing search results to merge with (accumulate across runs)
        existing_repos = {}
        if search_results_path.exists():
            try:
                with open(search_results_path) as f:
                    existing_repos = json.load(f)
                print(f"  Loaded {len(existing_repos)} existing repos to merge with")
            except (json.JSONDecodeError, OSError):
                pass

        all_results = []
        if search_mode == "repo":
            for query in _REPO_QUERIES:
                print(f"  Searching repos: {query} stars:>={args.min_stars} ...")
                results = search_github_repos(
                    query, token=args.token, min_stars=args.min_stars, max_results=1000
                )
                print(f"    Found {len(results)} repos")
                all_results.extend(results)
        else:
            for query in SEARCH_QUERIES:
                print(f"  Searching: {query[:60]}...")
                results = search_github(query, token=args.token, max_results=1000)
                print(f"    Found {len(results)} results")
                all_results.extend(results)

        new_repos = deduplicate_repos(all_results)

        # Merge: existing repos keep their data, new repos are added
        for name, info in new_repos.items():
            if name not in existing_repos:
                existing_repos[name] = info
            else:
                # Merge file lists (deduplicate)
                existing_files = set(existing_repos[name].get("files", []))
                existing_files.update(info.get("files", []))
                existing_repos[name]["files"] = list(existing_files)

        repos = existing_repos
        print(f"\n  Unique repos (after merge): {len(repos)}")

        # Save merged search results
        with open(search_results_path, "w") as f:
            json.dump(repos, f, indent=2)
        print(f"  Saved search results to {search_results_path}")

    if args.skip_clone:
        print("\n  --skip-clone specified, stopping here.")
        return

    # Build set of already-linted repos (to skip cloning)
    linted_repos: set[str] = set()
    if args.lint_results_dir:
        lint_dir = args.lint_results_dir.resolve()
        if lint_dir.exists():
            for f in lint_dir.glob("*.json"):
                # owner__repo.json -> owner/repo
                linted_repos.add(f.stem.replace("__", "/", 1))
            print(f"  Found {len(linted_repos)} already-linted repos")

    # Cleanup mode: delete cloned repos that have lint results
    if args.cleanup:
        if not linted_repos:
            print("Error: --cleanup requires --lint-results-dir with existing results")
            sys.exit(1)

        import shutil
        cleaned = 0
        freed = 0
        for repo_name in linted_repos:
            repo_dir = clone_dir / repo_name.replace("/", "__")
            if repo_dir.exists():
                # Estimate size before deleting
                size = sum(f.stat().st_size for f in repo_dir.rglob("*") if f.is_file())
                shutil.rmtree(repo_dir)
                cleaned += 1
                freed += size
        print(f"\n  Cleanup: deleted {cleaned} repos, freed {freed / (1024**2):.0f} MB")
        if not args.skip_search:
            print("  Continuing with clone step...")
        else:
            return

    # Step 2: Clone repos and find PySpark candidates
    # --max-repos limits NEW clones only; already-cached repos are always processed
    print(f"\n── Cloning repos (max {args.max_repos} new clones, {len(repos)} total) ──")

    total_pyspark_files = 0
    repos_with_pyspark = 0
    processed = 0
    new_clones = 0
    clone_failures = 0
    consecutive_failures = 0
    max_consecutive = args.max_clone_failures
    skipped_linted = 0

    # Track per-repo PySpark file counts for summary
    repo_summary = {}

    for repo_name, repo_info in repos.items():
        processed += 1
        repo_dir = clone_dir / repo_name.replace("/", "__")
        label = f"[{processed}/{len(repos)}]"

        if repo_dir.exists():
            print(f"  {label} {repo_name} (cached)")
            consecutive_failures = 0
        elif repo_name in linted_repos:
            # Already linted, clone was deleted — skip
            skipped_linted += 1
            continue
        else:
            # Check if we've hit the new-clone budget
            if args.max_repos > 0 and new_clones >= args.max_repos:
                continue

            print(f"  {label} {repo_name} ... ", end="", flush=True)
            success, error = clone_repo(repo_info["clone_url"], repo_dir)
            if success:
                print("ok")
                new_clones += 1
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

    # Merge with existing repo summary (accumulate across runs)
    summary_path = output_dir / "repo-summary.json"
    if summary_path.exists():
        try:
            with open(summary_path) as f:
                existing_summary = json.load(f)
            # Keep existing entries, add/update new ones
            existing_summary.update(repo_summary)
            repo_summary = existing_summary
        except (json.JSONDecodeError, OSError):
            pass

    with open(summary_path, "w") as f:
        json.dump(repo_summary, f, indent=2)

    print(f"\n  Repos in corpus: {processed}")
    print(f"  New clones this run: {new_clones}")
    print(f"  Skipped (already linted): {skipped_linted}")
    print(f"  Repos with PySpark code: {repos_with_pyspark}")
    print(f"  Total PySpark files: {total_pyspark_files}")
    print(f"  Clone failures: {clone_failures}")
    print(f"  Repo summary saved to: {summary_path}")
    print(f"\nNext step:")
    print(f"  python3 scripts/pyspark-lint/run_lint.py --repos-dir {clone_dir} --results-dir lint-results")


if __name__ == "__main__":
    main()
