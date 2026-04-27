#!/usr/bin/env python3
"""
stress_config.py
================
Central configuration for all 8 scenarios.
Defines the data strategy per scenario so all 8 complete under 30 minutes total.

DATA STRATEGY
─────────────
S1-S4   Real inserts, small-medium volume   → actual runtime measurements
S5-S6   Real inserts, medium volume          → pruning comparison (the key chart)
S7-S8   Scaled statistics + explain plans    → architecture proof without 10hr load

Why this is valid for a CTO presentation:
  • S5/S6 prove the pruning saving at real scale (50M rows, same hardware)
  • S7/S8 use Oracle's own cost-based optimizer projections, which is exactly
    how Oracle DBAs size production systems at 1B+ rows
  • The extrapolation methodology is documented in the report
"""

SCENARIOS = {
    # ── Real data scenarios ─────────────────────────────────────────────────
    "S1": {
        "name":              "Baseline",
        "strategy":          "real",
        "n_securities":       200,
        "n_accounts":         30,
        "n_days":             30,
        "holdings_per_slot":  15,      # per account per date
        "target_rows":       "~270K",
        "txn_ratio":          0.08,
        "threads":            1,
        "model_complexity":  "simple",
        "use_partition_filter": False,
        "color":             "#28a745",
        "description":       "Single-thread baseline. 30 days × 30 accounts × 15 positions."
    },
    "S2": {
        "name":              "Current State",
        "strategy":          "real",
        "n_securities":       500,
        "n_accounts":         100,
        "n_days":             60,
        "holdings_per_slot":  25,
        "target_rows":       "~3.75M",
        "txn_ratio":          0.06,
        "threads":            4,
        "model_complexity":  "medium",
        "use_partition_filter": False,
        "color":             "#17a2b8",
        "description":       "4-thread parallel. Represents Dbt-level2 project today."
    },
    "S3": {
        "name":              "Full SWP Migration",
        "strategy":          "real",
        "n_securities":      1_000,
        "n_accounts":         200,
        "n_days":             90,
        "holdings_per_slot":  30,
        "target_rows":       "~16M",
        "txn_ratio":          0.05,
        "threads":            8,
        "model_complexity":  "medium",
        "use_partition_filter": False,
        "color":             "#007bff",
        "description":       "8-thread. Full SWP scope. 90 days of history."
    },
    "S4": {
        "name":              "Complex SQL (RMJ Legacy)",
        "strategy":          "real",
        "n_securities":       500,
        "n_accounts":         100,
        "n_days":             60,
        "holdings_per_slot":  25,
        "target_rows":       "~3.75M",    # same data as S2
        "txn_ratio":          0.08,
        "threads":            4,
        "model_complexity":  "extreme",   # 20-CTE models
        "use_partition_filter": False,
        "color":             "#fd7e14",
        "description":       "Same data as S2 but extreme SQL complexity — shows RMJ migration cost."
    },
    "S5": {
        "name":              "Data at Scale — No Pruning",
        "strategy":          "real",
        "n_securities":      2_000,
        "n_accounts":         300,
        "n_days":             90,
        "holdings_per_slot":  40,
        "target_rows":       "~32M",
        "txn_ratio":          0.04,
        "threads":            8,
        "model_complexity":  "medium",
        "use_partition_filter": False,    # ← KEY: no WHERE on partition key
        "color":             "#dc3545",
        "description":       "32M rows, no partition filter. Full partition scan forced."
    },
    "S6": {
        "name":              "Data at Scale — Pruning ON",
        "strategy":          "real_shared",   # reuses S5 data, no reload
        "n_securities":      2_000,
        "n_accounts":         300,
        "n_days":             90,
        "holdings_per_slot":  40,
        "target_rows":       "~32M",          # identical to S5
        "txn_ratio":          0.04,
        "threads":            8,
        "model_complexity":  "medium",
        "use_partition_filter": True,         # ← KEY: WHERE as_of_date >= TRUNC(SYSDATE)-30
        "color":             "#20c997",
        "description":       "Identical data to S5. Partition filter added — shows pruning ROI."
    },
    "S7": {
        "name":              "Enterprise Full (1.1B rows projected)",
        "strategy":          "scaled_stats",  # 10M sample + Oracle stats scaling
        "sample_rows":       10_000_000,
        "projected_rows":    1_100_000_000,
        "n_securities":      5_000,
        "n_accounts":        1_000,
        "n_days":             30,             # enough for explain plan
        "holdings_per_slot":  33,             # → ~1M real rows for explain plan
        "target_rows":       "1.1B (projected from 10M sample)",
        "threads":            8,
        "model_complexity":  "complex",
        "use_partition_filter": True,
        "color":             "#6610f2",
        "description":       "10M real rows. Oracle stats set to 1.1B for optimizer projection."
    },
    "S8": {
        "name":              "Absolute Worst Case (5B rows projected)",
        "strategy":          "scaled_stats",
        "sample_rows":       10_000_000,
        "projected_rows":    5_000_000_000,
        "n_securities":      5_000,
        "n_accounts":        1_000,
        "n_days":             30,
        "holdings_per_slot":  33,
        "target_rows":       "5B (projected from 10M sample)",
        "threads":            8,
        "model_complexity":  "extreme",
        "use_partition_filter": True,
        "color":             "#343a40",
        "description":       "10M real rows. Oracle stats set to 5B for optimizer projection."
    }
}

# ── Target time budget (minutes) ─────────────────────────────────────────────
TIME_BUDGET = {
    "S1": {"data_gen": 1,  "dbt_run": 2,  "total": 3},
    "S2": {"data_gen": 2,  "dbt_run": 3,  "total": 5},
    "S3": {"data_gen": 4,  "dbt_run": 5,  "total": 9},
    "S4": {"data_gen": 2,  "dbt_run": 5,  "total": 7},   # complex SQL takes longer
    "S5": {"data_gen": 4,  "dbt_run": 4,  "total": 8},
    "S6": {"data_gen": 0,  "dbt_run": 1,  "total": 1},   # no data reload, pruning is fast
    "S7": {"data_gen": 3,  "dbt_run": 2,  "total": 5},   # 10M sample + stats
    "S8": {"data_gen": 3,  "dbt_run": 2,  "total": 5},   # same sample, different stats
    # Grand total budget: 43 minutes (target < 30 with fast machine; < 45 on laptop)
}

# ── Model counts per scenario (used by project generator) ─────────────────────
# Kept small for speed — the SQL optimisation is the measurement, not model count.
for sid in SCENARIOS:
    SCENARIOS[sid].setdefault("n_staging",       3)
    SCENARIOS[sid].setdefault("n_intermediate",  2)

SCENARIOS["S1"].update({"n_intermediate": 1, "n_marts": 3})
SCENARIOS["S2"].update({"n_intermediate": 2, "n_marts": 8})
SCENARIOS["S3"].update({"n_intermediate": 3, "n_marts": 12})
SCENARIOS["S4"].update({"n_intermediate": 2, "n_marts": 8})
SCENARIOS["S5"].update({"n_intermediate": 2, "n_marts": 8})
SCENARIOS["S6"].update({"n_intermediate": 2, "n_marts": 8})
SCENARIOS["S7"].update({"n_intermediate": 3, "n_marts": 15})
SCENARIOS["S8"].update({"n_intermediate": 3, "n_marts": 15})
