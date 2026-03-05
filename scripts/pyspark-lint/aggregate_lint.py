#!/usr/bin/env python3
"""
PySpark Lint Aggregator — Produces statistics from cy lint results.

Reads per-repo JSON files produced by run_lint.sh and computes:
  - Issue counts by rule and severity
  - File hit rate (% of PySpark files with at least one finding)
  - Project hit rate (% of repos with at least one finding)
  - Per-repo breakdown

When --metadata-dir is provided, also produces tiered aggregation:
  - Table A: Corpus distribution by tier
  - Table B: Findings by rule and tier (prevalence, density)
  - Table C: Pattern persistence classification (beginner/declining/persistent)

Usage:
  python3 aggregate_lint.py --results-dir lint-results/
  python3 aggregate_lint.py --results-dir lint-results/ --metadata-dir metadata/ --format both
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


TIER_LABELS = {
    1: "Learning/Hobby",
    2: "Personal",
    3: "Serious",
    4: "Prod-adjacent",
}


# ─────────────────────────────────────────────────────────────
# Metadata Loading
# ─────────────────────────────────────────────────────────────

def load_metadata_index(metadata_dir: Path) -> dict[str, dict]:
    """
    Load metadata for all repos into a lookup dict.
    Returns {repo_name: {tier, excluded, pyspark_file_count, ...}}.
    """
    index = {}
    for meta_file in metadata_dir.glob("*.json"):
        if meta_file.name == "tier-summary.json":
            continue
        try:
            meta = json.loads(meta_file.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        repo_name = meta.get("repo", "")
        if repo_name:
            index[repo_name] = meta
    return index


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
# Tiered Aggregation
# ─────────────────────────────────────────────────────────────

def aggregate_tiered(
    results_dir: Path,
    metadata_index: dict[str, dict],
) -> dict[str, Any]:
    """
    Produce per-tier aggregation: corpus distribution, findings by rule×tier,
    and pattern persistence classification.
    """
    # Per-tier counters
    tier_repo_count: dict[int, int] = Counter()
    tier_file_count: dict[int, int] = Counter()
    tier_finding_count: dict[int, int] = Counter()
    excluded_count = 0
    excluded_reasons: Counter = Counter()

    # Rule × tier tracking
    # rule_id -> tier -> set of repo names with that finding
    rule_tier_repos: dict[str, dict[int, set]] = defaultdict(lambda: defaultdict(set))
    # rule_id -> tier -> total finding count
    rule_tier_findings: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    # rule_id -> severity (for reference)
    rule_severity: dict[str, str] = {}
    rule_sample_message: dict[str, str] = {}

    for result_file in sorted(results_dir.glob("*.json")):
        try:
            data = json.loads(result_file.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        repo_name = data.get("repo", result_file.stem.replace("__", "/"))
        meta = metadata_index.get(repo_name)
        if meta is None:
            continue  # No metadata — skip

        # Handle exclusions
        if meta.get("excluded", False):
            excluded_count += 1
            excluded_reasons[meta.get("exclusion_reason", "unknown")] += 1
            continue

        tier = meta.get("tier", 1)
        cy_output = data.get("cy_output", {})
        findings = cy_output.get("findings", [])
        pyspark_file_count = meta.get("pyspark_file_count", 0)

        tier_repo_count[tier] += 1
        tier_file_count[tier] += pyspark_file_count
        tier_finding_count[tier] += len(findings)

        # Track findings per rule per tier
        repo_rules_seen: set[str] = set()
        for finding in findings:
            rule_id = finding.get("rule_id", "unknown")
            severity = finding.get("severity", "unknown")

            rule_tier_findings[rule_id][tier] += 1
            repo_rules_seen.add(rule_id)
            rule_severity[rule_id] = severity
            if rule_id not in rule_sample_message:
                rule_sample_message[rule_id] = finding.get("message", "")

        for rule_id in repo_rules_seen:
            rule_tier_repos[rule_id][tier].add(repo_name)

    # Table A — Corpus distribution
    corpus = {
        "total": sum(tier_repo_count.values()),
        "excluded": excluded_count,
        "excluded_reasons": dict(excluded_reasons),
        "by_tier": {
            str(t): {
                "count": tier_repo_count.get(t, 0),
                "pyspark_files": tier_file_count.get(t, 0),
                "total_findings": tier_finding_count.get(t, 0),
                "avg_per_file_density": round(
                    tier_finding_count.get(t, 0) / max(tier_file_count.get(t, 0), 1), 2
                ),
            }
            for t in [1, 2, 3, 4]
        },
    }

    # Table B — Findings by rule and tier
    all_rules = sorted(
        set(rule_tier_findings.keys()),
        key=lambda r: sum(rule_tier_findings[r].values()),
        reverse=True,
    )
    by_rule_and_tier = {}
    for rule_id in all_rules:
        by_rule_and_tier[rule_id] = {}
        for tier in [1, 2, 3, 4]:
            repos_with = len(rule_tier_repos[rule_id].get(tier, set()))
            tier_repos = tier_repo_count.get(tier, 0)
            tier_files = tier_file_count.get(tier, 0)
            total = rule_tier_findings[rule_id].get(tier, 0)

            by_rule_and_tier[rule_id][str(tier)] = {
                "repos_with_finding": repos_with,
                "prevalence_pct": round(
                    repos_with / max(tier_repos, 1) * 100, 1
                ),
                "total_findings": total,
                "per_file_density": round(
                    total / max(tier_files, 1), 2
                ),
            }

    # Table C — Pattern persistence classification
    pattern_classification = {}
    for rule_id in all_rules:
        t1_data = by_rule_and_tier[rule_id].get("1", {})
        t4_data = by_rule_and_tier[rule_id].get("4", {})
        t1_prev = t1_data.get("prevalence_pct", 0)
        t4_prev = t4_data.get("prevalence_pct", 0)

        if t1_prev > 0:
            ratio = round(t4_prev / t1_prev, 2)
        else:
            # If Tier 1 prevalence is 0, ratio is N/A
            ratio = float("inf") if t4_prev > 0 else 0

        if ratio >= 0.7:
            classification = "persistent"
        elif ratio >= 0.3:
            classification = "declining"
        else:
            classification = "beginner"

        pattern_classification[rule_id] = {
            "severity": rule_severity.get(rule_id, "unknown"),
            "sample_message": rule_sample_message.get(rule_id, ""),
            "tier1_prev": t1_prev,
            "tier4_prev": t4_prev,
            "ratio": ratio if ratio != float("inf") else None,
            "class": classification,
        }

    return {
        "corpus": corpus,
        "by_rule_and_tier": by_rule_and_tier,
        "pattern_classification": pattern_classification,
    }


def print_tiered_summary(tiered: dict, file=sys.stderr):
    """Print human-readable tiered summary to stderr."""
    corpus = tiered["corpus"]
    patterns = tiered["pattern_classification"]

    print("", file=file)
    print("=== Corpus Summary ===", file=file)
    print(f"Total repos: {corpus['total'] + corpus['excluded']} "
          f"({corpus['excluded']} excluded)", file=file)
    for tier in [1, 2, 3, 4]:
        td = corpus["by_tier"][str(tier)]
        label = TIER_LABELS[tier]
        print(f"  Tier {tier} ({label:15s}): {td['count']:>5} repos, "
              f"{td['pyspark_files']:>6} PySpark files, "
              f"{td['total_findings']:>6} findings, "
              f"{td['avg_per_file_density']:>5.2f} findings/file", file=file)

    # Persistent patterns
    persistent = {r: p for r, p in patterns.items() if p["class"] == "persistent"}
    declining = {r: p for r, p in patterns.items() if p["class"] == "declining"}
    beginner = {r: p for r, p in patterns.items() if p["class"] == "beginner"}

    if persistent:
        print("", file=file)
        print("=== Persistent Patterns (ratio >= 0.7) ===", file=file)
        for rule_id, p in sorted(persistent.items(),
                                  key=lambda x: x[1].get("ratio") or 0, reverse=True):
            ratio_str = f"{p['ratio']:.2f}" if p["ratio"] is not None else "N/A"
            arrow = "RISING" if (p["ratio"] or 0) > 1.0 else ""
            msg = p["sample_message"][:50]
            print(f"  {rule_id:6s} {msg:50s}  T1: {p['tier1_prev']:>5.1f}%  "
                  f"T4: {p['tier4_prev']:>5.1f}%  ratio: {ratio_str}"
                  f"{'  <- ' + arrow if arrow else ''}", file=file)

    if beginner:
        print("", file=file)
        print("=== Beginner Patterns (ratio < 0.3) ===", file=file)
        for rule_id, p in sorted(beginner.items(),
                                  key=lambda x: x[1].get("ratio") or 0):
            ratio_str = f"{p['ratio']:.2f}" if p["ratio"] is not None else "N/A"
            msg = p["sample_message"][:50]
            print(f"  {rule_id:6s} {msg:50s}  T1: {p['tier1_prev']:>5.1f}%  "
                  f"T4: {p['tier4_prev']:>5.1f}%  ratio: {ratio_str}", file=file)

    if declining:
        print("", file=file)
        print("=== Declining Patterns ===", file=file)
        for rule_id, p in sorted(declining.items(),
                                  key=lambda x: x[1].get("ratio") or 0, reverse=True):
            ratio_str = f"{p['ratio']:.2f}" if p["ratio"] is not None else "N/A"
            msg = p["sample_message"][:50]
            print(f"  {rule_id:6s} {msg:50s}  T1: {p['tier1_prev']:>5.1f}%  "
                  f"T4: {p['tier4_prev']:>5.1f}%  ratio: {ratio_str}", file=file)

    print("", file=file)


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
        "--metadata-dir",
        type=Path,
        default=None,
        help="Directory containing per-repo metadata JSONs for tiered aggregation",
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
    print(f"  Results dir:  {results_dir}", file=sys.stderr)
    print(f"  Result files: {result_count}", file=sys.stderr)
    if args.metadata_dir:
        print(f"  Metadata dir: {args.metadata_dir.resolve()}", file=sys.stderr)
    print("", file=sys.stderr)

    # Run base aggregation
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

    # Tiered aggregation (when metadata is provided)
    if args.metadata_dir:
        metadata_dir = args.metadata_dir.resolve()
        if not metadata_dir.exists():
            print(f"  Warning: Metadata directory not found: {metadata_dir}",
                  file=sys.stderr)
        else:
            print("", file=sys.stderr)
            print("  Aggregating tiered results...", file=sys.stderr)

            metadata_index = load_metadata_index(metadata_dir)
            print(f"    Loaded metadata for {len(metadata_index)} repos", file=sys.stderr)

            tiered = aggregate_tiered(results_dir, metadata_index)

            # Write tiered aggregate
            tiered_path = output_dir / "tiered-aggregate.json"
            with open(tiered_path, "w") as f:
                json.dump(tiered, f, indent=2)
            print(f"  Tiered JSON saved to: {tiered_path}", file=sys.stderr)

            # Print human-readable summary
            print_tiered_summary(tiered)

    print("", file=sys.stderr)
    print("  Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
