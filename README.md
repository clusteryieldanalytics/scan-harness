# Scan Harness

Scans public GitHub repos for PySpark code, lints them with **`cy lint`**, classifies repos by maturity tier, and aggregates the results to measure anti-pattern prevalence across real-world PySpark projects — segmented by project sophistication.

## Architecture

```
Search & Clone     Lint            Metadata        Classify       Aggregate
┌────────────┐   ┌───────────┐   ┌───────────┐   ┌──────────┐   ┌──────────────┐
│ GitHub API │   │           │   │ GitHub API│   │          │   │ Stats &      │
│ search for │──▶│ cy lint   │──▶│ stars,    │──▶│ Tier 1-4 │──▶│ tiered       │
│ PySpark    │   │ (per repo)│   │ CI, tests │   │ classify │   │ hit rates    │
└────────────┘   └───────────┘   └───────────┘   └──────────┘   └──────────────┘

corpus-pyspark/  lint-results/    metadata/                      lint-report/
├── repos/       └── *.json       ├── *.json                     ├── stats.json
└── search-                       └── tier-summary.json          ├── stats.md
    results.json                                                 └── tiered-aggregate.json
```

## Quick Start

```bash
pip install -r scripts/pyspark-lint/requirements.txt
```

### Step 1: Search GitHub & Clone Repos

```bash
python3 scripts/pyspark-lint/search_pyspark.py \
  --output-dir corpus-pyspark \
  --max-repos 500 \
  --token $GITHUB_TOKEN
```

Searches GitHub for Python files using PySpark, clones matching repos (shallow), and identifies candidate `.py` files. Uses GitHub Code Search API with 10 different query patterns to maximize corpus coverage.

Search results accumulate across runs — re-running merges new repos into the existing `search-results.json`. The `--max-repos` flag limits only **new** clones per run; already-cached repos are always processed.

**Targeting mature repos:** Use `--min-stars 50` to switch to GitHub Repository Search (which supports star filters) and find higher-tier repos:

```bash
python3 scripts/pyspark-lint/search_pyspark.py \
  --output-dir corpus-pyspark \
  --max-repos 200 \
  --min-stars 50 \
  --token $GITHUB_TOKEN
```

**Disk management:** Use `--cleanup` with `--lint-results-dir` to delete cloned repos that have already been linted, freeing disk space without losing lint data. Subsequent runs will skip re-cloning those repos:

```bash
# Delete already-linted clones, then clone 200 new repos
python3 scripts/pyspark-lint/search_pyspark.py \
  --output-dir corpus-pyspark \
  --skip-search --max-repos 200 \
  --lint-results-dir lint-results --cleanup
```

### Step 2: Lint All Repos

```bash
python3 scripts/pyspark-lint/run_lint.py \
  --repos-dir corpus-pyspark/repos \
  --results-dir lint-results
```

Runs `cy lint --format json` on each repo's filtered PySpark files. Produces one JSON file per repo in `lint-results/`, wrapping the raw `cy lint` output with metadata (repo name, timestamp, PySpark file list). Skips repos that already have a result file — safe to interrupt and resume.

**Test code filtering:** Both the search and lint steps exclude test files, examples, benchmarks, and fixtures to avoid false positives from intentionally bad code:
- Directories: `test/`, `tests/`, `examples/`, `samples/`, `demo/`, `benchmark/`, `fixtures/`, `mock/`, `venv/`, `docs/`
- File names: `test_*.py`, `*_test.py`, `conftest.py`, `setup.py`

### Step 3: Collect GitHub Metadata

```bash
python3 scripts/pyspark-lint/collect_metadata.py \
  --results-dir lint-results \
  --output-dir metadata \
  --token $GITHUB_TOKEN
```

Fetches per-repo metadata from the GitHub API (~5 calls per repo): stars, forks, contributor count, commit count, and a recursive file tree to detect CI, tests, packaging, and deployment signals. Also fetches README content for keyword matching. Caches results to disk — safe to interrupt and resume.

### Step 4: Classify Tiers

```bash
python3 scripts/pyspark-lint/classify_tiers.py \
  --metadata-dir metadata
```

Assigns each repo a maturity tier (1–4) based on its metadata:

| Tier | Label | Criteria |
|------|-------|----------|
| 4 | Prod-adjacent | stars > 200, org-owned, deployment or >10 contributors or >500 commits |
| 3 | Serious | stars > 50 or >3 contributors, plus CI or tests or packaging |
| 2 | Personal | stars >= 5 or >= 2 contributors or >= 10 commits |
| 1 | Learning/Hobby | Everything else, or keyword-demoted (homework, tutorial, coursework) |

Repos that are clones of official Spark/Databricks examples or generated code are excluded entirely.

### Step 5: Aggregate Results

```bash
python3 scripts/pyspark-lint/aggregate_lint.py \
  --results-dir lint-results \
  --metadata-dir metadata \
  --format both
```

Produces three output files in `lint-report/`:

- **`stats.json`** + **`stats.md`** — Overall (non-tiered) statistics: project/file hit rates, findings by severity, by rule, top repos
- **`tiered-aggregate.json`** — Per-tier breakdowns:
  - **Corpus distribution** — repo count, file count, total findings, and average per-file density per tier
  - **Findings by rule x tier** — prevalence, density, and repo counts per tier for each rule
  - **Pattern persistence** — classifies each rule as *persistent* (ratio >= 0.7), *declining* (0.3–0.7), or *beginner* (< 0.3) based on Tier 4 vs Tier 1 prevalence

### Spot-Checking Findings

```bash
# List all rules with counts
python3 scripts/pyspark-lint/spot_check.py \
  --results-dir lint-results --list-rules

# Sample 5 findings for a specific rule and display source code
python3 scripts/pyspark-lint/spot_check.py \
  --results-dir lint-results --rule CY003 --count 5

# Filter by severity, show more context
python3 scripts/pyspark-lint/spot_check.py \
  --results-dir lint-results --severity critical --context 10
```

Samples lint findings and displays the actual source code around each flagged location, making it easy to verify whether rules are producing true or false positives.

## Output Schema

### Per-repo lint result (`lint-results/*.json`)

```json
{
  "repo": "owner/repo-name",
  "scanned_at": "2026-03-04T...",
  "pyspark_files": ["path/to/file.py"],
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

### Per-repo metadata (`metadata/*.json`)

```json
{
  "repo": "owner/repo-name",
  "stars": 342,
  "forks": 218,
  "contributor_count": 47,
  "commit_count": 891,
  "is_org": true,
  "is_fork": false,
  "description": "Example PySpark applications",
  "topics": ["spark", "pyspark"],
  "has_ci": true,
  "has_tests": true,
  "has_packaging": true,
  "has_deployment": true,
  "pyspark_file_count": 23,
  "readme_keywords_matched": [],
  "tier": 3
}
```

## Caching & Incremental Runs

Every step caches its output and skips already-processed items on re-run:

| Step | Cache | Behavior |
|------|-------|----------|
| Search | `search-results.json` | Merges new repos into existing results |
| Clone | `corpus-pyspark/repos/` | Skips repos with existing directories (or lint results with `--lint-results-dir`) |
| Lint | `lint-results/*.json` | Skips repos with existing result files |
| Metadata | `metadata/*.json` | Skips repos with existing metadata files |
| Classify | `metadata/*.json` | Overwrites `tier` field in-place every run |
| Aggregate | `lint-report/*` | Regenerates from scratch every run |

## Dependencies

- **Python 3.9+**
- **[cy](https://clusteryield.com)** CLI on PATH (for linting)
- **Git** (for cloning repos)
- **requests** (for GitHub API — `pip install requests`)
