# Scan Harness

Scans public GitHub repos for PySpark code, lints them with **`cy lint`**, and aggregates the results to measure anti-pattern prevalence across real-world PySpark projects.

## Architecture

```
Search & Clone          Lint                    Aggregate
┌──────────────┐       ┌───────────────┐       ┌───────────────┐
│ GitHub API   │       │               │       │               │
│ search for   │──────▶│  cy lint      │──────▶│  Stats &      │
│ PySpark code │       │  (per repo)   │       │  hit rates    │
└──────────────┘       └───────────────┘       └───────────────┘
     Python                 CLI                     Python

corpus-pyspark/        lint-results/            lint-report/
├── search-results.json├── owner__repo.json     ├── stats.json
├── repo-summary.json  └── ...                  └── stats.md
└── repos/
    └── owner__repo/
```

## Quick Start

### Step 1: Search GitHub & Clone Repos

```bash
pip install -r scripts/pyspark-lint/requirements.txt

python3 scripts/pyspark-lint/search_pyspark.py \
  --output-dir corpus-pyspark \
  --max-repos 500 \
  --token $GITHUB_TOKEN
```

Searches GitHub for Python files using PySpark, clones matching repos (shallow), and identifies candidate `.py` files.

### Step 2: Lint All Repos

```bash
./scripts/pyspark-lint/run_lint.sh \
  --repos-dir corpus-pyspark/repos \
  --results-dir lint-results
```

Runs `cy lint --format json` on each repo directory. Produces one JSON file per repo in `lint-results/`, wrapping the raw `cy lint` output with metadata (repo name, timestamp, PySpark file list).

### Step 3: Aggregate Results

```bash
python3 scripts/pyspark-lint/aggregate_lint.py \
  --results-dir lint-results \
  --format both
```

Outputs `lint-report/stats.json` and `lint-report/stats.md` with:

- **Project hit rate** — % of repos with at least one finding
- **File hit rate** — % of PySpark files with at least one finding
- **Findings by severity** — critical / warning / info counts and hit rates
- **Findings by rule** — per-rule prevalence across files and repos
- **Top repos** — ranked by finding count

## Output Schema

Each per-repo result in `lint-results/` looks like:

```json
{
  "repo": "owner/repo-name",
  "scanned_at": "2026-03-04T...",
  "pyspark_files": ["path/to/file.py", ...],
  "cy_output": {
    "files_scanned": 5,
    "total_findings": 19,
    "counts": { "info": 6, "warning": 6, "critical": 7 },
    "findings": [
      {
        "rule_id": "CY003",
        "severity": "critical",
        "message": ".withColumn() inside a loop creates O(n²) plan complexity.",
        "filepath": "etl/pipeline.py",
        "line": 54,
        "col": 17,
        "suggestion": "Use .select([...]) with all column expressions instead."
      }
    ]
  }
}
```

## Dependencies

- **Python 3.9+**
- **[cy](https://clusteryield.com)** CLI on PATH (for linting)
- **Git** (for cloning repos)
- **requests** (for GitHub API — `pip install requests`)
