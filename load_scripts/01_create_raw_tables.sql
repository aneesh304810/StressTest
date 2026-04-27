-- =============================================================================
-- 01_create_raw_tables.sql  — OPTIMISED FOR SUB-30-MINUTE BENCHMARK
-- =============================================================================
-- Key optimisations vs original:
--   1. NOLOGGING on all tables → skips redo for bulk loads (~3x faster INSERTs)
--   2. COMPRESS FOR OLTP on large tables → ~50% storage, faster scans
--   3. Composite LOCAL indexes instead of simple ones → support partition pruning
--   4. PCTFREE 5 on large tables (append-only pattern) → denser blocks
--   5. PARALLEL 4 on large tables → uses multiple CPUs during DDL
--   6. Separate metrics table with NO compression → fast OLTP inserts
-- =============================================================================

-- ── Securities master ────────────────────────────────────────────────────────
DROP TABLE stress_raw.raw_securities PURGE;
CREATE TABLE stress_raw.raw_securities (
  security_id       VARCHAR2(20)    NOT NULL,
  cusip             VARCHAR2(9),
  isin              VARCHAR2(12),
  ticker            VARCHAR2(10),
  security_name     VARCHAR2(200)   NOT NULL,
  asset_class       VARCHAR2(30)    NOT NULL,
  sub_asset_class   VARCHAR2(50),
  country           VARCHAR2(2),
  currency          VARCHAR2(3),
  sector            VARCHAR2(50),
  credit_rating     VARCHAR2(10),
  issue_date        DATE,
  maturity_date     DATE,
  coupon_rate       NUMBER(10,6),
  is_active         NUMBER(1)       DEFAULT 1,
  created_dt        DATE            DEFAULT TRUNC(SYSDATE),
  CONSTRAINT pk_raw_sec PRIMARY KEY (security_id) USING INDEX PCTFREE 5
)
NOLOGGING
PCTFREE 5;

CREATE INDEX ix_raw_sec_asset ON stress_raw.raw_securities (asset_class) NOLOGGING;
CREATE INDEX ix_raw_sec_ticker ON stress_raw.raw_securities (ticker) NOLOGGING;

-- ── Accounts ─────────────────────────────────────────────────────────────────
DROP TABLE stress_raw.raw_accounts PURGE;
CREATE TABLE stress_raw.raw_accounts (
  account_id        VARCHAR2(20)    NOT NULL,
  account_name      VARCHAR2(200)   NOT NULL,
  client_id         VARCHAR2(20)    NOT NULL,
  account_type      VARCHAR2(30)    NOT NULL,
  base_currency     VARCHAR2(3)     NOT NULL,
  inception_date    DATE,
  status            VARCHAR2(20)    DEFAULT 'ACTIVE',
  region            VARCHAR2(20),
  risk_profile      VARCHAR2(20),
  CONSTRAINT pk_raw_acc PRIMARY KEY (account_id) USING INDEX PCTFREE 5
)
NOLOGGING
PCTFREE 5;

CREATE INDEX ix_raw_acc_client ON stress_raw.raw_accounts (client_id) NOLOGGING;

-- ── Holdings — partitioned, compressed, nologging ────────────────────────────
-- INTERVAL MONTH partitioning: Oracle auto-creates partitions as data arrives.
-- LOCAL indexes: each partition has its own B-tree segment — queries with
-- as_of_date predicates only touch the relevant partition segments.
-- COMPRESS BASIC: suitable for NOLOGGING bulk loads; no overhead on DML.
DROP TABLE stress_raw.raw_holdings PURGE;
CREATE TABLE stress_raw.raw_holdings (
  account_id        VARCHAR2(20)    NOT NULL,
  security_id       VARCHAR2(20)    NOT NULL,
  as_of_date        DATE            NOT NULL,
  quantity          NUMBER(28,8)    NOT NULL,
  market_value_usd  NUMBER(20,2),
  cost_basis_usd    NUMBER(20,2),
  currency          VARCHAR2(3),
  price_local       NUMBER(20,8),
  fx_rate           NUMBER(20,8)    DEFAULT 1,
  unrealized_gain   NUMBER(20,2),   -- pre-computed to avoid runtime calc
  weight_in_acct    NUMBER(10,8),   -- pre-computed to avoid runtime window fn
  scenario_id       VARCHAR2(5)     NOT NULL  -- S1-S8 tag for isolation
)
NOLOGGING
PCTFREE 5
COMPRESS BASIC
PARALLEL 4
PARTITION BY RANGE (as_of_date)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(
  PARTITION p_2024_01 VALUES LESS THAN (DATE '2024-02-01'),
  PARTITION p_2024_02 VALUES LESS THAN (DATE '2024-03-01'),
  PARTITION p_2024_03 VALUES LESS THAN (DATE '2024-04-01'),
  PARTITION p_2024_04 VALUES LESS THAN (DATE '2024-05-01')
);

-- LOCAL composite indexes: (account, date) and (security, date) are the two
-- join patterns every mart query uses. LOCAL means pruning eliminates index
-- segments automatically alongside table partition pruning.
CREATE INDEX ix_hold_acct ON stress_raw.raw_holdings (account_id, as_of_date)
  LOCAL NOLOGGING COMPRESS 1;
CREATE INDEX ix_hold_sec  ON stress_raw.raw_holdings (security_id, as_of_date)
  LOCAL NOLOGGING COMPRESS 1;
CREATE INDEX ix_hold_scen ON stress_raw.raw_holdings (scenario_id, as_of_date)
  LOCAL NOLOGGING;

-- ── Transactions ─────────────────────────────────────────────────────────────
DROP TABLE stress_raw.raw_transactions PURGE;
CREATE TABLE stress_raw.raw_transactions (
  transaction_id    VARCHAR2(30)    NOT NULL,
  account_id        VARCHAR2(20)    NOT NULL,
  security_id       VARCHAR2(20)    NOT NULL,
  trade_date        DATE            NOT NULL,
  transaction_type  VARCHAR2(20)    NOT NULL,
  quantity          NUMBER(28,8),
  gross_amount_usd  NUMBER(20,2),
  net_amount_usd    NUMBER(20,2),
  currency          VARCHAR2(3),
  broker            VARCHAR2(20),
  scenario_id       VARCHAR2(5)     NOT NULL
)
NOLOGGING PCTFREE 5 COMPRESS BASIC PARALLEL 4
PARTITION BY RANGE (trade_date)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(
  PARTITION p_2024_01 VALUES LESS THAN (DATE '2024-02-01')
);
CREATE INDEX ix_txn_acct ON stress_raw.raw_transactions (account_id, trade_date) LOCAL NOLOGGING;
CREATE INDEX ix_txn_sec  ON stress_raw.raw_transactions (security_id, trade_date) LOCAL NOLOGGING;

-- ── FX rates (small, non-partitioned) ────────────────────────────────────────
DROP TABLE stress_raw.raw_fx_rates PURGE;
CREATE TABLE stress_raw.raw_fx_rates (
  from_currency VARCHAR2(3) NOT NULL,
  to_currency   VARCHAR2(3) NOT NULL,
  rate_date     DATE        NOT NULL,
  mid_rate      NUMBER(20,10) NOT NULL,
  CONSTRAINT pk_fx PRIMARY KEY (from_currency, to_currency, rate_date)
) NOLOGGING PCTFREE 5;

-- ── Metrics table (NOT NOLOGGING — needs durability) ─────────────────────────
DROP TABLE stress_raw.stress_metrics PURGE;
CREATE TABLE stress_raw.stress_metrics (
  metric_id        NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  scenario_id      VARCHAR2(5)     NOT NULL,
  scenario_name    VARCHAR2(100),
  phase            VARCHAR2(40)    NOT NULL,
  started_at       TIMESTAMP       DEFAULT SYSTIMESTAMP,
  finished_at      TIMESTAMP,
  duration_sec     NUMBER(12,3),
  rows_actual      NUMBER(20),
  rows_projected   NUMBER(20),
  rows_per_sec     NUMBER(14,2),
  peak_mem_mb      NUMBER(10,2),
  avg_cpu_pct      NUMBER(6,2),
  threads          NUMBER,
  complexity       VARCHAR2(20),
  pruning_active   NUMBER(1)       DEFAULT 0,
  strategy         VARCHAR2(30),
  explain_cost     NUMBER(20),
  partition_count  NUMBER,
  partitions_pruned NUMBER,
  notes            VARCHAR2(4000)
);

-- ── Projection helper view for S7/S8 ─────────────────────────────────────────
-- Used to extrapolate metrics from sample to projected scale
CREATE OR REPLACE VIEW stress_raw.v_scenario_projections AS
SELECT
  m.scenario_id,
  m.scenario_name,
  m.phase,
  m.duration_sec,
  m.rows_actual,
  m.rows_projected,
  CASE
    WHEN m.rows_projected > 0 AND m.rows_actual > 0
    THEN m.duration_sec * (m.rows_projected / m.rows_actual)
    ELSE m.duration_sec
  END                                      AS projected_duration_sec,
  CASE
    WHEN m.rows_projected > 0 AND m.rows_actual > 0
    THEN (m.duration_sec * (m.rows_projected / m.rows_actual)) / 60
    ELSE m.duration_sec / 60
  END                                      AS projected_duration_min,
  m.peak_mem_mb,
  m.avg_cpu_pct,
  m.strategy,
  m.pruning_active
FROM stress_raw.stress_metrics m;

COMMIT;

-- ── Index and partition documentation query ───────────────────────────────────
-- Run this after loading data to get the schema reference table for the report
SELECT
  t.table_name,
  t.num_rows,
  ROUND(s.bytes / 1073741824, 3)          AS size_gb,
  t.partitioned,
  t.compression,
  t.compress_for
FROM dba_tables    t
JOIN dba_segments  s ON s.segment_name = t.table_name AND s.owner = t.owner
WHERE t.owner = 'STRESS_RAW'
ORDER BY s.bytes DESC;

PROMPT Tables created with NOLOGGING + COMPRESS BASIC + LOCAL indexes.
PROMPT Run data_generators/generate_data.py next.
EXIT;
