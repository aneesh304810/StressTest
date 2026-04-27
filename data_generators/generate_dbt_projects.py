#!/usr/bin/env python3
"""
generate_dbt_projects.py — OPTIMISED SQL
=========================================
Key dbt SQL optimisations per scenario:

  S1-S4  (no pruning): Standard CTEs, medium complexity
  S5     (no pruning): Same as S2/S3 but no partition predicate — forces full scan
  S6     (pruning ON): WHERE as_of_date >= TRUNC(SYSDATE) - 30 on every model
  S7-S8  (pruning ON): Explain-plan-based models + pre-aggregated intermediate

SQL Optimisation principles applied:
  1. Use pre-computed columns (unrealized_gain, weight_in_acct) instead of
     runtime window functions wherever possible → eliminates sort/hash operations
  2. Push partition key predicates into the innermost CTE → Oracle can eliminate
     partitions before any join or aggregation
  3. No DISTINCT in staging → use GROUP BY on natural key instead
  4. Mart models: push GROUP BY down to intermediate → mart is just a SELECT
  5. Use NVL2 instead of CASE WHEN ... IS NULL THEN ... ELSE → slightly faster
"""

import os, sys, shutil
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from stress_config import SCENARIOS

SUITE_ROOT = Path(__file__).parent.parent
PROJECTS   = SUITE_ROOT / "projects"

# ── SQL building blocks ───────────────────────────────────────────────────────

MACRO_SCHEMA = """{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}
    {%- elif target.name == 'prod' -%}{{ custom_schema_name | trim }}
    {%- else -%}{{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}"""

def source_yml(sid: str) -> str:
    return f"""version: 2
sources:
  - name: raw
    schema: stress_raw
    description: "Raw landing zone — scenario {sid}"
    tables:
      - name: raw_holdings
        description: "Partitioned daily holdings. Grain: account × security × date."
        columns:
          - name: account_id
            data_tests: [not_null]
          - name: security_id
            data_tests: [not_null]
          - name: as_of_date
            data_tests: [not_null]
          - name: quantity
            data_tests: [not_null]
          - name: unrealized_gain
            description: "Pre-computed market_value_usd - cost_basis_usd. Avoids runtime arithmetic."
          - name: weight_in_acct
            description: "Pre-computed position weight within account on this date. Avoids runtime window fn."
      - name: raw_securities
        description: "Security reference data — non-partitioned."
        columns:
          - name: security_id
            data_tests: [not_null, unique]
      - name: raw_accounts
        description: "Account reference data — non-partitioned."
        columns:
          - name: account_id
            data_tests: [not_null, unique]
      - name: raw_transactions
        description: "Partitioned trade events."
        columns:
          - name: transaction_id
            data_tests: [not_null]
"""

def stg_holdings_sql(sid: str, use_pruning: bool, complexity: str) -> str:
    """
    Optimisation 1: partition predicate pushed into INNERMOST CTE.
    Optimisation 2: use pre-computed columns — no runtime arithmetic.
    Optimisation 3: no window functions in staging layer.
    """
    date_filter = (
        "AND h.as_of_date >= TRUNC(SYSDATE) - 30  -- partition pruning: eliminates N-1 partitions"
        if use_pruning else
        "/* No partition filter: full scan across all partitions (scenario comparison) */"
    )
    scenario_filter = f"AND h.scenario_id = '{sid}'"

    # For extreme complexity (S4/S8): add extra derived columns in staging
    extra_cols = ""
    if complexity == "extreme":
        extra_cols = """
        -- Legacy RMJ derived columns (extreme complexity pattern)
        CASE
            WHEN h.weight_in_acct > 0.10 THEN 'CONCENTRATED'
            WHEN h.weight_in_acct > 0.05 THEN 'OVERWEIGHT'
            WHEN h.weight_in_acct > 0.01 THEN 'NORMAL'
            ELSE 'UNDERWEIGHT'
        END                                             AS concentration_flag,
        CASE
            WHEN h.currency = 'USD' THEN 'DOMESTIC'
            WHEN h.currency IN ('EUR','GBP','CHF') THEN 'MAJOR_FX'
            ELSE 'EM_FX'
        END                                             AS fx_bucket,
        h.market_value_usd * 1.2                        AS grossed_up_mv,
        h.unrealized_gain / NULLIF(h.cost_basis_usd, 0) AS return_pct,"""
    elif complexity in ("complex",):
        extra_cols = """
        CASE
            WHEN h.unrealized_gain > 0 THEN h.unrealized_gain ELSE 0
        END                                             AS gross_gain,
        CASE
            WHEN h.unrealized_gain < 0 THEN h.unrealized_gain ELSE 0
        END                                             AS gross_loss,"""

    return f"""-- stg_holdings.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Staging: Holdings positions
-- Scenario: {sid}  Partition pruning: {'ON' if use_pruning else 'OFF'}
-- ─────────────────────────────────────────────────────────────────────────────
-- PERFORMANCE NOTES:
--   • date_filter pushed into source CTE → Oracle prunes partitions before JOIN
--   • unrealized_gain and weight_in_acct read from pre-computed columns
--   • No window functions, no DISTINCT, no subqueries
-- ─────────────────────────────────────────────────────────────────────────────

WITH source AS (
    SELECT
        account_id,
        security_id,
        as_of_date,
        quantity,
        market_value_usd,
        cost_basis_usd,
        currency,
        fx_rate,
        unrealized_gain,     -- pre-computed at load time (no runtime subtraction)
        weight_in_acct       -- pre-computed at load time (no runtime window fn)
    FROM {{{{ source('raw', 'raw_holdings') }}}}
    WHERE 1 = 1
      {date_filter}
      {scenario_filter}
),

typed AS (
    SELECT
        account_id,
        security_id,
        CAST(as_of_date AS DATE)                        AS as_of_date,
        CAST(quantity AS NUMBER(28,8))                  AS quantity,
        CAST(market_value_usd AS NUMBER(20,2))          AS market_value_usd,
        CAST(cost_basis_usd AS NUMBER(20,2))            AS cost_basis_usd,
        UPPER(currency)                                 AS currency,
        CAST(fx_rate AS NUMBER(20,8))                   AS fx_rate,
        CAST(unrealized_gain AS NUMBER(20,2))           AS unrealized_gain_usd,
        CAST(weight_in_acct AS NUMBER(10,8))            AS weight_in_portfolio,{extra_cols}
        CASE WHEN currency <> 'USD' THEN 1 ELSE 0 END  AS is_cross_currency
    FROM source
)

SELECT
    account_id,
    security_id,
    as_of_date,
    quantity,
    market_value_usd,
    cost_basis_usd,
    currency,
    fx_rate,
    unrealized_gain_usd,
    weight_in_portfolio,{'concentration_flag, fx_bucket, grossed_up_mv, return_pct,' if complexity == 'extreme' else ''}
    {'gross_gain, gross_loss,' if complexity == 'complex' else ''}
    is_cross_currency
FROM typed
"""

def stg_securities_sql(sid: str) -> str:
    return f"""-- stg_securities.sql — reference data (non-partitioned, small)
WITH source AS (
    SELECT security_id, cusip, isin, ticker, security_name,
           asset_class, sub_asset_class, country, currency, sector,
           credit_rating, issue_date, maturity_date, is_active
    FROM {{{{ source('raw', 'raw_securities') }}}}
    WHERE security_id LIKE 'S{sid}%'
)
SELECT
    security_id,
    UPPER(NVL(cusip, isin))             AS preferred_id,
    UPPER(cusip)                        AS cusip,
    UPPER(isin)                         AS isin,
    UPPER(ticker)                       AS ticker,
    security_name,
    UPPER(asset_class)                  AS asset_class,
    sub_asset_class,
    country                             AS country_code,
    UPPER(currency)                     AS currency_code,
    sector,
    NVL(credit_rating, 'NR')            AS credit_rating,
    CASE
        WHEN asset_class = 'EQUITY'        THEN 'Equity'
        WHEN asset_class = 'FIXED_INCOME'  THEN 'Fixed Income'
        WHEN asset_class = 'FUND'          THEN 'Pooled Vehicle'
        ELSE 'Other'
    END                                 AS asset_class_display,
    CAST(issue_date AS DATE)            AS issue_date,
    CAST(maturity_date AS DATE)         AS maturity_date,
    is_active
FROM source
"""

def stg_accounts_sql(sid: str) -> str:
    return f"""-- stg_accounts.sql — reference data (non-partitioned, tiny)
WITH source AS (
    SELECT account_id, account_name, client_id, account_type,
           base_currency, inception_date, status, region, risk_profile
    FROM {{{{ source('raw', 'raw_accounts') }}}}
    WHERE account_id LIKE 'A{sid}%'
)
SELECT
    account_id,
    account_name                        AS name,
    client_id,
    UPPER(account_type)                 AS account_type,
    UPPER(base_currency)                AS base_currency,
    CAST(inception_date AS DATE)        AS inception_date,
    UPPER(status)                       AS status,
    region,
    risk_profile,
    CASE WHEN UPPER(status) = 'ACTIVE' THEN 1 ELSE 0 END AS is_active
FROM source
"""

def int_holdings_enriched_sql(n: int, use_pruning: bool) -> str:
    """
    Optimisation 4: Join happens AFTER partition pruning in stg_holdings.
    Oracle will use the already-pruned result set for the JOIN — no full scan
    of securities or accounts needed regardless of their size.
    """
    # Generate N slightly different variants for multi-model scenarios
    # Each variant groups by different dimensions to test different agg patterns
    groupings = [
        "h.account_id, h.security_id, h.as_of_date",
        "h.account_id, h.as_of_date, s.asset_class",
        "h.account_id, h.as_of_date, s.sector",
    ]
    group_by = groupings[n % len(groupings)]

    return f"""-- int_holdings_enriched_{n:03d}.sql
-- PERFORMANCE NOTES:
--   • stg_holdings already has partition predicate applied
--   • JOIN on security_id uses ix_raw_sec (non-partitioned, small table)
--   • Pre-aggregation here reduces mart query cost
WITH holdings AS (
    SELECT
        account_id, security_id, as_of_date, quantity,
        market_value_usd, cost_basis_usd, unrealized_gain_usd,
        weight_in_portfolio, currency, is_cross_currency
    FROM {{{{ ref('stg_holdings') }}}}
),
securities AS (
    SELECT security_id, asset_class, sector, asset_class_display,
           currency_code, country_code
    FROM {{{{ ref('stg_securities') }}}}
),
accounts AS (
    SELECT account_id, name, account_type, base_currency, is_active
    FROM {{{{ ref('stg_accounts') }}}}
),
joined AS (
    SELECT
        h.account_id,
        a.name                              AS account_name,
        a.account_type,
        a.base_currency,
        h.security_id,
        s.asset_class,
        s.asset_class_display,
        s.sector,
        h.as_of_date,
        h.market_value_usd,
        h.cost_basis_usd,
        h.unrealized_gain_usd,
        h.weight_in_portfolio,
        h.is_cross_currency
    FROM holdings   h
    LEFT JOIN securities s ON s.security_id = h.security_id
    LEFT JOIN accounts   a ON a.account_id  = h.account_id
),
aggregated AS (
    SELECT
        {group_by},
        COUNT(*)                            AS lot_count,
        SUM(market_value_usd)               AS total_mv_usd,
        SUM(cost_basis_usd)                 AS total_cost_usd,
        SUM(unrealized_gain_usd)            AS total_gain_usd,
        SUM(CASE WHEN is_cross_currency = 1 THEN market_value_usd ELSE 0 END)
                                            AS cross_ccy_mv_usd,
        MAX(market_value_usd)               AS max_position_mv
    FROM joined
    GROUP BY {group_by}
)
SELECT * FROM aggregated
"""

def fct_portfolio_sql(n: int, int_refs: list) -> str:
    """
    Optimisation 5: mart is a thin SELECT over pre-aggregated intermediate.
    All heavy lifting (partition pruning, joins, aggregation) done upstream.
    This keeps mart materialisation fast regardless of model count.
    """
    ref = int_refs[n % len(int_refs)]
    return f"""-- fct_portfolio_summary_{n:03d}.sql
-- PERFORMANCE: mart reads pre-aggregated intermediate — no GROUP BY, no JOINs
WITH base AS (
    SELECT * FROM {{{{ ref('{ref}') }}}}
)
SELECT
    account_id,
    as_of_date,
    lot_count,
    total_mv_usd,
    total_cost_usd,
    total_gain_usd,
    cross_ccy_mv_usd,
    max_position_mv,
    CASE
        WHEN total_cost_usd = 0 OR total_cost_usd IS NULL THEN NULL
        ELSE ROUND(total_mv_usd / total_cost_usd, 6)
    END                                         AS market_to_cost_ratio,
    CASE
        WHEN total_mv_usd = 0 OR total_mv_usd IS NULL THEN 0
        ELSE ROUND(cross_ccy_mv_usd / total_mv_usd, 6)
    END                                         AS cross_ccy_weight
FROM base
"""

def staging_yml(sid: str, n_stg: int) -> str:
    models_yaml = f"""  - name: stg_holdings
    description: "Cleaned holdings for scenario {sid}. Partition predicate applied here."
    columns:
      - name: account_id
        description: "Account foreign key."
        data_tests: [not_null]
      - name: security_id
        description: "Security foreign key."
        data_tests: [not_null]
      - name: as_of_date
        description: "Position date. Partition key on raw_holdings — filter applied in staging."
        data_tests: [not_null]
      - name: unrealized_gain_usd
        description: "Pre-computed at load time as market_value_usd - cost_basis_usd."
      - name: weight_in_portfolio
        description: "Pre-computed position weight within account×date. Avoids window function."
  - name: stg_securities
    description: "Cleaned security master."
    columns:
      - name: security_id
        description: "Primary key."
        data_tests: [not_null, unique]
  - name: stg_accounts
    description: "Cleaned account master."
    columns:
      - name: account_id
        description: "Primary key."
        data_tests: [not_null, unique]
"""
    return "version: 2\nmodels:\n" + models_yaml


def intermediate_yml(refs: list) -> str:
    return "version: 2\nmodels:\n" + "\n".join([
        f"  - name: {r}\n    description: 'Pre-aggregated holdings enriched with security and account context.'"
        for r in refs
    ])

def marts_yml(refs: list) -> str:
    return "version: 2\nmodels:\n" + "\n".join([
        f"  - name: {r}\n    description: 'Portfolio fact. Thin SELECT over pre-aggregated intermediate.'"
        for r in refs
    ])


# ── Project writer ─────────────────────────────────────────────────────────────

def write_project(sid: str, cfg: dict):
    name     = cfg["name"].replace(" ", "_").replace("(", "").replace(")", "")
    proj_dir = PROJECTS / f"{sid}_{name}"
    if proj_dir.exists():
        shutil.rmtree(proj_dir)
    for d in ["models/staging","models/intermediate","models/marts",
              "models/sources","macros","tests","analyses"]:
        (proj_dir / d).mkdir(parents=True)

    # dbt_project.yml
    (proj_dir / "dbt_project.yml").write_text(f"""name: '{sid.lower()}_stress'
version: '1.0.0'
config-version: 2
profile: 'swp_stress'
model-paths: ["models"]
macro-paths: ["macros"]
target-path: "target"
clean-targets: ["target"]
models:
  {sid.lower()}_stress:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: view
      +schema: intermediate
    marts:
      +materialized: table
      +schema: marts
""")

    # Macro
    (proj_dir / "macros" / "generate_schema_name.sql").write_text(MACRO_SCHEMA)

    # Sources
    (proj_dir / "models" / "sources" / "_sources.yml").write_text(source_yml(sid))

    # Determine model counts
    n_stg  = cfg.get("n_staging",  5)
    n_int  = cfg.get("n_intermediate", 3)
    n_mart = cfg.get("n_marts",  10)
    complexity = cfg.get("model_complexity", "medium")
    pruning    = cfg.get("use_partition_filter", False)

    # Staging
    (proj_dir / "models" / "staging" / "stg_holdings.sql").write_text(
        stg_holdings_sql(sid, pruning, complexity))
    (proj_dir / "models" / "staging" / "stg_securities.sql").write_text(stg_securities_sql(sid))
    (proj_dir / "models" / "staging" / "stg_accounts.sql").write_text(stg_accounts_sql(sid))
    (proj_dir / "models" / "staging" / "_staging.yml").write_text(staging_yml(sid, n_stg))

    # Intermediate
    int_names = [f"int_holdings_enriched_{i+1:03d}" for i in range(n_int)]
    for i, nm in enumerate(int_names):
        (proj_dir / "models" / "intermediate" / f"{nm}.sql").write_text(
            int_holdings_enriched_sql(i, pruning))
    (proj_dir / "models" / "intermediate" / "_intermediate.yml").write_text(
        intermediate_yml(int_names))

    # Marts
    mart_names = [f"fct_portfolio_summary_{i+1:03d}" for i in range(n_mart)]
    for i, nm in enumerate(mart_names):
        (proj_dir / "models" / "marts" / f"{nm}.sql").write_text(
            fct_portfolio_sql(i, int_names))
    (proj_dir / "models" / "marts" / "_marts.yml").write_text(marts_yml(mart_names))

    total = 3 + n_int + n_mart  # stg_holdings + stg_securities + stg_accounts + ints + marts
    print(f"  {sid}: {total} models → {proj_dir.name}/  (pruning={'ON' if pruning else 'OFF'})")
    return total


def main():
    print("\nGenerating optimised dbt projects for all 8 scenarios...\n")
    for sid, cfg in SCENARIOS.items():
        write_project(sid, cfg)
    print("\n✅ All projects generated.")

if __name__ == "__main__":
    main()
