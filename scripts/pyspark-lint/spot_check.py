#!/usr/bin/env python3
"""
Spot-Check Lint Findings — Sample findings and display the actual source code.

Usage:
  python3 spot_check.py --results-dir lint-results --rule CY003 --count 5
  python3 spot_check.py --results-dir lint-results --severity critical --count 10
  python3 spot_check.py --results-dir lint-results --list-rules
"""

import argparse
import json
import random
import sys
from collections import Counter
from pathlib import Path


def load_findings(results_dir: Path) -> list[dict]:
    """Load all findings from lint result files, enriched with repo name."""
    all_findings = []
    for result_file in sorted(results_dir.glob("*.json")):
        try:
            data = json.loads(result_file.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        repo_name = data.get("repo", result_file.stem.replace("__", "/"))
        cy_output = data.get("cy_output", {})

        for finding in cy_output.get("findings", []):
            finding["_repo"] = repo_name
            all_findings.append(finding)

    return all_findings


def filter_findings(
    findings: list[dict],
    rule: str | None = None,
    severity: str | None = None,
) -> list[dict]:
    """Filter findings by rule and/or severity."""
    result = findings
    if rule:
        result = [f for f in result if f.get("rule_id") == rule]
    if severity:
        result = [f for f in result if f.get("severity") == severity]
    return result


def print_rules_summary(findings: list[dict]):
    """Print a summary of rule counts."""
    by_rule: dict[str, Counter] = {}
    for f in findings:
        rule_id = f.get("rule_id", "unknown")
        severity = f.get("severity", "unknown")
        if rule_id not in by_rule:
            by_rule[rule_id] = Counter()
        by_rule[rule_id]["count"] += 1
        by_rule[rule_id][severity] += 1

    print(f"{'Rule':<8} {'Count':>6}  {'Crit':>5} {'Warn':>5} {'Info':>5}  Sample message")
    print("─" * 90)
    for rule_id in sorted(by_rule, key=lambda r: by_rule[r]["count"], reverse=True):
        c = by_rule[rule_id]
        # Find a sample message for this rule
        sample = next(
            (f.get("message", "")[:50] for f in findings if f.get("rule_id") == rule_id),
            "",
        )
        print(
            f"{rule_id:<8} {c['count']:>6}  "
            f"{c.get('critical', 0):>5} {c.get('warning', 0):>5} {c.get('info', 0):>5}  "
            f"{sample}"
        )
    print(f"\nTotal: {len(findings)} findings across {len(by_rule)} rules")


def read_source_lines(filepath: str) -> list[str] | None:
    """Read source file, returning lines or None if unavailable."""
    p = Path(filepath)
    if not p.exists():
        return None
    try:
        return p.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None


def print_finding(finding: dict, index: int, total: int, context: int):
    """Print a single finding with source code context."""
    repo = finding.get("_repo", "unknown")
    rule_id = finding.get("rule_id", "unknown")
    severity = finding.get("severity", "unknown")
    filepath = finding.get("filepath", "")
    line = finding.get("line", 0)
    message = finding.get("message", "")
    suggestion = finding.get("suggestion", "")

    # Derive a short display path (relative within repo)
    display_path = filepath
    # Try to extract just the repo-relative part
    for marker in ("/repos/",):
        idx = filepath.find(marker)
        if idx >= 0:
            # Skip past "repos/owner__repo/"
            rest = filepath[idx + len(marker):]
            parts = rest.split("/", 1)
            if len(parts) == 2:
                display_path = parts[1]
            break

    print(f"\n{'━' * 60}")
    print(f"[{index}/{total}]  {repo}  {rule_id} ({severity})")
    print(f"File: {display_path}:{line}")
    print(f"{'━' * 60}")
    print(f"Message:    {message}")
    if suggestion:
        print(f"Suggestion: {suggestion}")
    print()

    # Read and display source
    lines = read_source_lines(filepath)
    if lines is None:
        print(f"  (source file not found: {filepath})")
        return

    start = max(0, line - context - 1)
    end = min(len(lines), line + context)

    for i in range(start, end):
        line_num = i + 1
        source_line = lines[i]
        if line_num == line:
            print(f">>> {line_num:>4} │ {source_line}")
        else:
            print(f"    {line_num:>4} │ {source_line}")


def main():
    parser = argparse.ArgumentParser(
        description="Sample lint findings and display source code for spot-checking"
    )
    parser.add_argument(
        "--results-dir", type=Path, default=Path("lint-results"),
        help="Directory containing per-repo lint result JSONs (default: lint-results/)",
    )
    parser.add_argument(
        "--rule", type=str, default=None,
        help="Filter to a specific rule ID (e.g., CY003)",
    )
    parser.add_argument(
        "--severity", type=str, default=None,
        choices=["critical", "warning", "info"],
        help="Filter by severity level",
    )
    parser.add_argument(
        "--count", type=int, default=5,
        help="Number of findings to sample (default: 5)",
    )
    parser.add_argument(
        "--context", type=int, default=5,
        help="Lines of code context above/below the finding (default: 5)",
    )
    parser.add_argument(
        "--list-rules", action="store_true",
        help="List all rules with counts and exit",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducible sampling",
    )
    args = parser.parse_args()

    results_dir = args.results_dir.resolve()
    if not results_dir.exists():
        print(f"Error: Results directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)

    # Load all findings
    findings = load_findings(results_dir)
    if not findings:
        print("No findings found in results directory.", file=sys.stderr)
        sys.exit(1)

    # List rules mode
    if args.list_rules:
        print_rules_summary(findings)
        return

    # Filter
    filtered = filter_findings(findings, rule=args.rule, severity=args.severity)
    if not filtered:
        filters = []
        if args.rule:
            filters.append(f"rule={args.rule}")
        if args.severity:
            filters.append(f"severity={args.severity}")
        print(f"No findings match filters: {', '.join(filters)}", file=sys.stderr)
        print(f"Use --list-rules to see available rules.", file=sys.stderr)
        sys.exit(1)

    # Sample
    if args.seed is not None:
        random.seed(args.seed)

    count = min(args.count, len(filtered))
    sample = random.sample(filtered, count)

    filters_desc = []
    if args.rule:
        filters_desc.append(f"rule={args.rule}")
    if args.severity:
        filters_desc.append(f"severity={args.severity}")
    filter_str = f" ({', '.join(filters_desc)})" if filters_desc else ""

    print(f"Sampled {count} of {len(filtered)} matching findings{filter_str}")

    for i, finding in enumerate(sample, 1):
        print_finding(finding, i, count, args.context)

    print()


if __name__ == "__main__":
    main()
