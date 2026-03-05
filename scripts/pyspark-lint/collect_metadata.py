#!/usr/bin/env python3
"""
Metadata Collector — Enriches each scraped repo with GitHub metadata for tier classification.

For each repo in the corpus, fetches:
  - Repo metadata (stars, forks, is_org, description, topics)
  - Contributor count (via Link header pagination trick)
  - Commit count (via Link header pagination trick)
  - Recursive file tree (for CI, tests, packaging, deployment detection)
  - README content (first 2KB, for keyword matching)

Usage:
  python3 collect_metadata.py --results-dir lint-results --output-dir metadata --token $GITHUB_TOKEN

Requires:
  pip install requests
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Optional

try:
    import requests
except ImportError:
    print("Error: 'requests' package required. Install with: pip install requests")
    sys.exit(1)


GITHUB_API_BASE = "https://api.github.com"

# Keywords that signal learning/hobby repos
README_KEYWORDS = {
    "homework", "assignment", "coursework", "class project",
    "university", "course", "tutorial", "learning exercise",
}


# ─────────────────────────────────────────────────────────────
# GitHub API Helpers
# ─────────────────────────────────────────────────────────────

def _headers(token: Optional[str]) -> dict:
    h = {"Accept": "application/vnd.github.v3+json"}
    if token:
        h["Authorization"] = f"token {token}"
    return h


def _rate_limit_wait(resp: requests.Response, token: Optional[str]):
    """Sleep until rate limit resets if we hit 403."""
    remaining = int(resp.headers.get("X-RateLimit-Remaining", 1))
    if remaining == 0 or resp.status_code == 403:
        reset_time = int(resp.headers.get("X-RateLimit-Reset", 0))
        wait = max(reset_time - int(time.time()), 10)
        print(f"    Rate limited. Waiting {wait}s...", file=sys.stderr)
        time.sleep(wait)
        return True
    return False


def _count_from_link_header(resp: requests.Response) -> int:
    """
    Extract total count from the Link header's last page number.
    GitHub returns: Link: <...?page=47>; rel="last"
    With per_page=1, page number == total count.
    """
    link = resp.headers.get("Link", "")
    match = re.search(r'[?&]page=(\d+)>;\s*rel="last"', link)
    if match:
        return int(match.group(1))
    # If no Link header, the response itself is the only page
    # (0 or 1 items)
    if resp.status_code == 200:
        data = resp.json()
        if isinstance(data, list):
            return len(data)
    return 0


def _api_get(url: str, token: Optional[str], params: Optional[dict] = None,
             max_retries: int = 3) -> Optional[requests.Response]:
    """Make a GitHub API GET request with retry on rate limit."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=_headers(token), params=params, timeout=30)
        except requests.RequestException as e:
            print(f"    Request error: {e}", file=sys.stderr)
            time.sleep(5)
            continue

        if resp.status_code == 403 and _rate_limit_wait(resp, token):
            continue
        return resp
    return None


# ─────────────────────────────────────────────────────────────
# Metadata Fetchers
# ─────────────────────────────────────────────────────────────

def fetch_repo_metadata(repo_name: str, token: Optional[str]) -> Optional[dict]:
    """Fetch basic repo metadata from GET /repos/{owner}/{repo}."""
    resp = _api_get(f"{GITHUB_API_BASE}/repos/{repo_name}", token)
    if resp is None or resp.status_code != 200:
        return None
    data = resp.json()
    return {
        "repo": data.get("full_name", repo_name),
        "stars": data.get("stargazers_count", 0),
        "forks": data.get("forks_count", 0),
        "is_org": data.get("owner", {}).get("type") == "Organization",
        "is_fork": data.get("fork", False),
        "description": data.get("description"),
        "topics": data.get("topics", []),
        "default_branch": data.get("default_branch", "main"),
    }


def fetch_contributor_count(repo_name: str, token: Optional[str]) -> int:
    """Get contributor count via Link header pagination trick."""
    resp = _api_get(
        f"{GITHUB_API_BASE}/repos/{repo_name}/contributors",
        token,
        params={"per_page": "1", "anon": "true"},
    )
    if resp is None or resp.status_code != 200:
        return 0
    return _count_from_link_header(resp)


def fetch_commit_count(repo_name: str, token: Optional[str]) -> int:
    """Get commit count via Link header pagination trick."""
    resp = _api_get(
        f"{GITHUB_API_BASE}/repos/{repo_name}/commits",
        token,
        params={"per_page": "1"},
    )
    if resp is None or resp.status_code != 200:
        return 0
    return _count_from_link_header(resp)


def fetch_tree(repo_name: str, default_branch: str, token: Optional[str]) -> list[str]:
    """Fetch the recursive file tree. Returns list of file paths."""
    resp = _api_get(
        f"{GITHUB_API_BASE}/repos/{repo_name}/git/trees/{default_branch}",
        token,
        params={"recursive": "1"},
    )
    if resp is None or resp.status_code != 200:
        return []
    data = resp.json()
    return [item.get("path", "") for item in data.get("tree", [])]


def fetch_readme_text(repo_name: str, token: Optional[str]) -> str:
    """Fetch first 2KB of README content."""
    resp = _api_get(
        f"{GITHUB_API_BASE}/repos/{repo_name}/readme",
        token,
    )
    if resp is None or resp.status_code != 200:
        return ""
    data = resp.json()
    download_url = data.get("download_url")
    if not download_url:
        return ""
    try:
        raw_resp = requests.get(download_url, timeout=15)
        if raw_resp.status_code == 200:
            return raw_resp.text[:2048]
    except requests.RequestException:
        pass
    return ""


# ─────────────────────────────────────────────────────────────
# Tree Analysis
# ─────────────────────────────────────────────────────────────

def detect_ci(tree: list[str]) -> bool:
    """Check if tree contains CI configuration files."""
    ci_patterns = {
        ".travis.yml",
        "Jenkinsfile",
        ".circleci/config.yml",
        ".gitlab-ci.yml",
        "azure-pipelines.yml",
    }
    for path in tree:
        if path in ci_patterns:
            return True
        # GitHub Actions workflows
        if path.startswith(".github/workflows/") and (
            path.endswith(".yml") or path.endswith(".yaml")
        ):
            return True
    return False


def detect_tests(tree: list[str]) -> bool:
    """Check if tree contains test directories or test files."""
    for path in tree:
        parts = path.split("/")
        if "tests" in parts or "test" in parts:
            return True
        basename = parts[-1] if parts else ""
        if basename.endswith("_test.py") or basename.startswith("test_"):
            return True
    return False


def detect_packaging(tree: list[str]) -> bool:
    """Check if tree contains Python packaging files."""
    packaging_files = {"setup.py", "pyproject.toml", "setup.cfg", "poetry.lock"}
    basenames = {path.split("/")[-1] for path in tree}
    return bool(basenames & packaging_files)


def detect_deployment(tree: list[str]) -> bool:
    """Check if tree contains deployment-related files."""
    deployment_files = {
        "Dockerfile", "docker-compose.yml", "docker-compose.yaml",
    }
    for path in tree:
        basename = path.split("/")[-1]
        parts = path.split("/")
        if basename in deployment_files:
            return True
        # Directory-based checks
        if "airflow" in parts or "dags" in parts:
            return True
        if "kubernetes" in parts or "k8s" in parts:
            return True
        # Terraform files
        if basename.endswith(".tf"):
            return True
        # Databricks job configs
        if basename.endswith("_job.json"):
            return True
    return False


def detect_readme(tree: list[str]) -> bool:
    """Check if tree contains a README."""
    basenames = {path.split("/")[-1].lower() for path in tree}
    return "readme.md" in basenames or "readme.rst" in basenames


def match_readme_keywords(readme_text: str) -> list[str]:
    """Find learning/hobby keywords in README text."""
    text_lower = readme_text.lower()
    return [kw for kw in README_KEYWORDS if kw in text_lower]


# ─────────────────────────────────────────────────────────────
# Main Pipeline
# ─────────────────────────────────────────────────────────────

def collect_for_repo(
    repo_name: str,
    token: Optional[str],
    pyspark_file_count: int = 0,
) -> Optional[dict[str, Any]]:
    """Collect all metadata for a single repo. Returns metadata dict or None on failure."""

    # 1. Basic repo metadata
    meta = fetch_repo_metadata(repo_name, token)
    if meta is None:
        return None

    # 2. Contributor count
    meta["contributor_count"] = fetch_contributor_count(repo_name, token)

    # 3. Commit count
    meta["commit_count"] = fetch_commit_count(repo_name, token)

    # 4. File tree analysis
    tree = fetch_tree(repo_name, meta["default_branch"], token)
    meta["has_ci"] = detect_ci(tree)
    meta["has_tests"] = detect_tests(tree)
    meta["has_packaging"] = detect_packaging(tree)
    meta["has_deployment"] = detect_deployment(tree)
    meta["has_readme"] = detect_readme(tree)

    # 5. README keyword matching (conditional)
    if meta["has_readme"]:
        readme_text = fetch_readme_text(repo_name, token)
        meta["readme_keywords_matched"] = match_readme_keywords(readme_text)
    else:
        meta["readme_keywords_matched"] = []

    # 6. PySpark file count (from lint results)
    meta["pyspark_file_count"] = pyspark_file_count

    return meta


def main():
    parser = argparse.ArgumentParser(
        description="Collect GitHub metadata for tier classification"
    )
    parser.add_argument(
        "--results-dir", type=Path, default=Path("lint-results"),
        help="Directory containing per-repo lint result JSONs (default: lint-results/)",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("metadata"),
        help="Directory for metadata output files (default: metadata/)",
    )
    parser.add_argument(
        "--token", type=str, default=os.environ.get("GITHUB_TOKEN"),
        help="GitHub API token (or set GITHUB_TOKEN env var)",
    )
    parser.add_argument(
        "--max-repos", type=int, default=0,
        help="Limit number of repos to process (0 = all, default: 0)",
    )
    args = parser.parse_args()

    results_dir = args.results_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if not results_dir.exists():
        print(f"Error: Results directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)

    # Build repo list from lint results
    repos: dict[str, int] = {}  # repo_name -> pyspark_file_count
    for result_file in sorted(results_dir.glob("*.json")):
        try:
            data = json.loads(result_file.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        repo_name = data.get("repo", "")
        pyspark_files = data.get("pyspark_files", [])
        if repo_name:
            repos[repo_name] = len(pyspark_files)

    if not repos:
        print("Error: No repos found in lint results", file=sys.stderr)
        sys.exit(1)

    repo_list = list(repos.items())
    if args.max_repos > 0:
        repo_list = repo_list[:args.max_repos]

    print("═══════════════════════════════════════════════════════", file=sys.stderr)
    print("  Metadata Collector", file=sys.stderr)
    print("═══════════════════════════════════════════════════════", file=sys.stderr)
    print(f"  Results dir:  {results_dir}", file=sys.stderr)
    print(f"  Output dir:   {output_dir}", file=sys.stderr)
    print(f"  Repos to process: {len(repo_list)}", file=sys.stderr)
    print(f"  Token: {'provided' if args.token else 'none'}", file=sys.stderr)
    print("", file=sys.stderr)

    if not args.token:
        print("  Warning: No token provided. Rate limit is 60 req/hr (vs 5,000 with token).",
              file=sys.stderr)

    success = 0
    skipped = 0
    failed = 0

    for i, (repo_name, pyspark_count) in enumerate(repo_list, 1):
        safe_name = repo_name.replace("/", "__")
        meta_file = output_dir / f"{safe_name}.json"

        # Skip if already collected
        if meta_file.exists():
            try:
                existing = json.loads(meta_file.read_text())
                # Check if metadata is complete (has all required fields)
                if all(k in existing for k in ("stars", "contributor_count", "commit_count", "has_ci")):
                    print(f"  [{i}/{len(repo_list)}] {repo_name} (cached)", file=sys.stderr)
                    skipped += 1
                    continue
            except (json.JSONDecodeError, OSError):
                pass  # Re-fetch if file is corrupt

        print(f"  [{i}/{len(repo_list)}] {repo_name} ... ", end="", flush=True, file=sys.stderr)

        meta = collect_for_repo(repo_name, args.token, pyspark_count)
        if meta is None:
            print("FAILED", file=sys.stderr)
            failed += 1
            continue

        with open(meta_file, "w") as f:
            json.dump(meta, f, indent=2)

        print(f"ok (stars={meta['stars']}, contribs={meta['contributor_count']})",
              file=sys.stderr)
        success += 1

        # Brief pause between repos to be a good API citizen
        time.sleep(0.5)

    print("", file=sys.stderr)
    print("── Results ─────────────────────────────────────────────", file=sys.stderr)
    print(f"  Collected: {success}", file=sys.stderr)
    print(f"  Cached:    {skipped}", file=sys.stderr)
    print(f"  Failed:    {failed}", file=sys.stderr)
    print(f"  Output:    {output_dir}/", file=sys.stderr)
    print("", file=sys.stderr)
    print("Next step:", file=sys.stderr)
    print(f"  python3 scripts/pyspark-lint/classify_tiers.py --metadata-dir {output_dir}",
          file=sys.stderr)


if __name__ == "__main__":
    main()
