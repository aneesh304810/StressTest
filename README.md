# SWP Stress Test Suite — CTO Performance Benchmark
## Data Pipeline Intelligence · Oracle 12c · dbt

A complete benchmark suite across 8 scenarios covering all permutations of:
- **Project Size**: 50 → 5,000 models
- **SQL Complexity**: simple → extreme (20+ CTEs, 100 columns)
- **Data Volume**: 1M → 5B rows
- **Execution Pattern**: sequential → parallel, full-refresh → incremental

---

## Prerequisites

```bash
pip install dbt-oracle==1.8.3 oracledb==2.3.0 faker==24.0.0 pandas==2.2.0 tqdm==4.66.0
```

Oracle 12c standalone must be running. Set environment variables:
```bash
export ORACLE_USER=dbt_stress
export ORACLE_PASSWORD=StressTest123
export ORACLE_HOST=localhost
export ORACLE_PORT=1521
export ORACLE_SERVICE=ORCLCDB
export ORACLE_SCHEMA=STRESS
```

---

## Quick start — run all 8 scenarios

```bash
cd swp_stress_suite
python run_all.py
```

This will:
1. Create Oracle schemas and tables
2. Generate and load sample data for each scenario
3. Run dbt for each project (compile + run + test)
4. Collect metrics (time, memory, CPU)
5. Generate the HTML report at reports/cto_report.html

---

## Scenarios

| ID | Name | Models | Volume | Pattern | Key Question |
|----|------|--------|--------|---------|--------------|
| S1 | Baseline | 50 | 1M rows | Sequential | What is the floor? |
| S2 | Current State | 200 | 50M rows | Parallel 4T | Where are we today? |
| S3 | Full Migration | 500 | 50M rows | Parallel 8T | Full SWP scope? |
| S4 | Complex SQL | 200 | 50M rows | Parallel 4T | Legacy RMJ impact? |
| S5 | Data at Scale | 200 | 500M rows | Parallel 8T | Volume without tuning? |
| S6 | Pruning ON | 200 | 500M rows | Parallel 8T | Partition pruning ROI? |
| S7 | Enterprise | 1500 | 500M rows | Parallel 8T | Full platform? |
| S8 | Worst Case | 5000 | 5B rows | Parallel 8T | Absolute ceiling? |

---

## Output

`reports/cto_report.html` — interactive HTML with:
- Executive summary with traffic-light status
- Side-by-side metric comparison table
- Charts: run time, memory, CPU, rows/sec
- Partition pruning ROI (S5 vs S6)
- Recommendations
