#!/usr/bin/env python3
"""
PySpark Lint Aggregator — Produces statistics from cy lint results.

Reads per-repo JSON files produced by run_lint.sh and computes:
  - Issue counts by rule and severity
  - File hit rate (% of PySpark files with at least one finding)
  - Project hit rate (% of repos with at least one finding)
  - Per-repo breakdown

Usage:
  python3 aggregate_lint.py --results-dir lint-results/
  python3 aggregate_lint.py --results-dir lint-results/ --format both
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


# ─────────────────────────────────────────────────────────────
# Core Aggregation
# ─────────────────────────────────────────────────────────────

def aggregate_lint_results(results_dir: Path) -> dict[str, Any]:
    """
    Read all per-repo lint result JSONs and compute aggregate statistics.
    """
    # Global counters
    repos_scanned = 0
    repos_with_findings = 0
    total_files_scanned = 0
    files_with_findings: set[str] = set()
    all_files: set[str] = set()
    total_findings = 0

    # By severity
    severity_counts = Counter()
    severity_files: dict[str, set[str]] = defaultdict(set)
    severity_repos: dict[str, set[str]] = defaultdict(set)

    # By rule
    rule_counts = Counter()
    rule_severity: dict[str, str] = {}
    rule_files: dict[str, set[str]] = defaultdict(set)
    rule_repos: dict[str, set[str]] = defaultdict(set)
    rule_sample_message: dict[str, str] = {}

    # Per-repo details
    per_repo = []

    for result_file in sorted(results_dir.glob("*.json")):
        try:
            data = json.loads(result_file.read_text())
        except (json.JSONDecodeError, OSError) as e:
            print(f"  Warning: Could not read {result_file}: {e}", file=sys.stderr)
            continue

        repo_name = data.get("repo", result_file.stem.replace("__", "/"))
        cy_output = data.get("cy_output", {})
        pyspark_files = data.get("pyspark_files", [])
        findings = cy_output.get("findings", [])

        files_scanned = cy_output.get("files_scanned", 0)
        repos_scanned += 1
        total_files_scanned += files_scanned

        # Track all files in this repo (using repo-qualified paths)
        for pf in pyspark_files:
            all_files.add(f"{repo_name}/{pf}")

        # Per-repo tracking
        repo_findings_count = len(findings)
        repo_severity = Counter()
        repo_rules = Counter()
        repo_files_with_findings: set[str] = set()

        if repo_findings_count > 0:
            repos_with_findings += 1

        total_findings += repo_findings_count

        for finding in findings:
            rule_id = finding.get("rule_id", "unknown")
            severity = finding.get("severity", "unknown")
            filepath = finding.get("filepath", "")
            message = finding.get("message", "")

            # Global severity
            severity_counts[severity] += 1
            file_key = f"{repo_name}/{filepath}"
            severity_files[severity].add(file_key)
            severity_repos[severity].add(repo_name)

            # Global rule
            rule_counts[rule_id] += 1
            rule_severity[rule_id] = severity
            rule_files[rule_id].add(file_key)
            rule_repos[rule_id].add(repo_name)
            if rule_id not in rule_sample_message:
                rule_sample_message[rule_id] = message

            # Per-repo
            repo_severity[severity] += 1
            repo_rules[rule_id] += 1
            files_with_findings.add(file_key)
            repo_files_with_findings.add(file_key)

        per_repo.append({
            "repo": repo_name,
            "files_scanned": files_scanned,
            "files_with_findings": len(repo_files_with_findings),
            "total_findings": repo_findings_count,
            "findings_per_file": round(repo_findings_count / max(files_scanned, 1), 2),
            "by_severity": dict(repo_severity),
            "by_rule": dict(repo_rules),
        })

    # Use the larger of: pyspark_files count or files_scanned across all repos
    total_files = max(len(all_files), total_files_scanned)

    # Build output
    return {
        "summary": {
            "repos_scanned": repos_scanned,
            "repos_with_findings": repos_with_findings,
            "project_hit_rate": round(
                repos_with_findings / max(repos_scanned, 1) * 100, 1
            ),
            "total_files_scanned": total_files,
            "files_with_findings": len(files_with_findings),
            "file_hit_rate": round(
                len(files_with_findings) / max(total_files, 1) * 100, 1
            ),
            "total_findings": total_findings,
            "avg_findings_per_repo": round(
                total_findings / max(repos_scanned, 1), 2
            ),
            "avg_findings_per_file": round(
                total_findings / max(total_files, 1), 2
            ),
        },
        "by_severity": {
            sev: {
                "count": severity_counts[sev],
                "files": len(severity_files[sev]),
                "repos": len(severity_repos[sev]),
                "file_hit_rate": round(
                    len(severity_files[sev]) / max(total_files, 1) * 100, 1
                ),
                "project_hit_rate": round(
                    len(severity_repos[sev]) / max(repos_scanned, 1) * 100, 1
                ),
            }
            for sev in ["critical", "warning", "info"]
            if sev in severity_counts
        },
        "by_rule": {
            rule_id: {
                "count": count,
                "severity": rule_severity.get(rule_id, "unknown"),
                "files_affected": len(rule_files[rule_id]),
                "repos_affected": len(rule_repos[rule_id]),
                "file_hit_rate": round(
                    len(rule_files[rule_id]) / max(total_files, 1) * 100, 1
                ),
                "project_hit_rate": round(
                    len(rule_repos[rule_id]) / max(repos_scanned, 1) * 100, 1
                ),
                "sample_message": rule_sample_message.get(rule_id, ""),
            }
            for rule_id, count in rule_counts.most_common()
        },
        "per_repo": sorted(per_repo, key=lambda r: r["total_findings"], reverse=True),
    }


# ─────────────────────────────────────────────────────────────
# Output Formatting
# ─────────────────────────────────────────────────────────────

def format_markdown(stats: dict) -> str:
    """Format aggregate statistics as Markdown tables."""
    lines = []
    s = stats["summary"]

    # Header
    lines.append("# PySpark Lint — Aggregate Statistics")
    lines.append("")
    lines.append(f"Repos scanned: **{s['repos_scanned']}**")
    lines.append(f"Total PySpark files: **{s['total_files_scanned']}**")
    lines.append(f"Total findings: **{s['total_findings']}**")
    lines.append("")

    # Hit rates
    lines.append("## Hit Rates")
    lines.append("")
    lines.append(f"- **Project hit rate**: {s['project_hit_rate']}% "
                 f"({s['repos_with_findings']}/{s['repos_scanned']} repos have at least one finding)")
    lines.append(f"- **File hit rate**: {s['file_hit_rate']}% "
                 f"({s['files_with_findings']}/{s['total_files_scanned']} files have at least one finding)")
    lines.append(f"- **Avg findings per repo**: {s['avg_findings_per_repo']}")
    lines.append(f"- **Avg findings per file**: {s['avg_findings_per_file']}")
    lines.append("")

    # Table 1: By severity
    lines.append("## Findings by Severity")
    lines.append("")
    lines.append("| Severity | Count | Files | Repos | File Hit Rate | Project Hit Rate |")
    lines.append("|----------|-------|-------|-------|---------------|------------------|")
    for sev in ["critical", "warning", "info"]:
        if sev in stats["by_severity"]:
            d = stats["by_severity"][sev]
            lines.append(
                f"| {sev} | {d['count']} | {d['files']} | {d['repos']} "
                f"| {d['file_hit_rate']}% | {d['project_hit_rate']}% |"
            )
    lines.append("")

    # Table 2: By rule
    lines.append("## Findings by Rule")
    lines.append("")
    lines.append("| Rule | Severity | Count | Files | Repos | File % | Project % | Description |")
    lines.append("|------|----------|-------|-------|-------|--------|-----------|-------------|")
    for rule_id, d in stats["by_rule"].items():
        # Truncate message for table readability
        msg = d["sample_message"][:80] + "..." if len(d["sample_message"]) > 80 else d["sample_message"]
        lines.append(
            f"| {rule_id} | {d['severity']} | {d['count']} | {d['files_affected']} "
            f"| {d['repos_affected']} | {d['file_hit_rate']}% | {d['project_hit_rate']}% "
            f"| {msg} |"
        )
    lines.append("")

    # Table 3: Top repos by findings
    lines.append("## Top 20 Repos by Findings")
    lines.append("")
    lines.append("| Repo | Files Scanned | Files w/ Findings | Total Findings | Findings/File |")
    lines.append("|------|---------------|-------------------|----------------|---------------|")
    for repo in stats["per_repo"][:20]:
        lines.append(
            f"| {repo['repo']} | {repo['files_scanned']} | {repo['files_with_findings']} "
            f"| {repo['total_findings']} | {repo['findings_per_file']} |"
        )
    lines.append("")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Aggregate cy lint results into statistics and hit rates"
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("lint-results"),
        help="Directory containing per-repo lint result JSONs (default: lint-results/)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("lint-report"),
        help="Output directory for reports (default: lint-report/)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "markdown", "both"],
        default="both",
        help="Output format (default: both)",
    )
    args = parser.parse_args()

    results_dir = args.results_dir.resolve()
    if not results_dir.exists():
        print(f"Error: Results directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)

    result_count = len(list(results_dir.glob("*.json")))
    if result_count == 0:
        print(f"Error: No JSON result files found in {results_dir}", file=sys.stderr)
        sys.exit(1)

    print("═══════════════════════════════════════════════════════", file=sys.stderr)
    print("  PySpark Lint Aggregator", file=sys.stderr)
    print("═══════════════════════════════════════════════════════", file=sys.stderr)
    print(f"  Results dir: {results_dir}", file=sys.stderr)
    print(f"  Result files: {result_count}", file=sys.stderr)
    print("", file=sys.stderr)

    # Run aggregation
    print("  Aggregating lint results...", file=sys.stderr)
    stats = aggregate_lint_results(results_dir)

    s = stats["summary"]
    print(f"    {s['repos_scanned']} repos, {s['total_files_scanned']} files, "
          f"{s['total_findings']} findings", file=sys.stderr)
    print(f"    Project hit rate: {s['project_hit_rate']}%", file=sys.stderr)
    print(f"    File hit rate: {s['file_hit_rate']}%", file=sys.stderr)
    print("", file=sys.stderr)

    # Output
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.format in ("json", "both"):
        json_path = output_dir / "stats.json"
        with open(json_path, "w") as f:
            json.dump(stats, f, indent=2)
        print(f"  JSON saved to: {json_path}", file=sys.stderr)

    if args.format in ("markdown", "both"):
        md = format_markdown(stats)
        md_path = output_dir / "stats.md"
        with open(md_path, "w") as f:
            f.write(md)
        print(f"  Markdown saved to: {md_path}", file=sys.stderr)

        if args.format == "markdown":
            print(md)

    print("", file=sys.stderr)
    print("  Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
