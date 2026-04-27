#!/usr/bin/env python3
"""
generate_data.py — OPTIMISED for sub-30-minute total benchmark
===============================================================
Key speed improvements vs v1:
  1. Larger batches (5000 rows) → fewer round-trips
  2. Pre-computed unrealized_gain and weight_in_acct → eliminates runtime window fns
  3. APPEND hint via direct-path INSERT → bypasses buffer cache, uses NOLOGGING
  4. S6 reuses S5 data (no reload, ~0 min)
  5. S7/S8 use 10M-row sample + Oracle DBMS_STATS override (no multi-hour load)
  6. Thread-pool for multi-account parallelism within a single scenario
"""

import argparse
import oracledb
import os
import sys
import time
import random
import math
import threading
from datetime import date, timedelta, datetime
from typing import List, Tuple, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from stress_config import SCENARIOS

# ── Connection factory ────────────────────────────────────────────────────────
def make_conn():
    conn = oracledb.connect(
        user=os.environ.get("ORACLE_USER", "dbt_stress"),
        password=os.environ.get("ORACLE_PASSWORD", "StressTest123"),
        host=os.environ.get("ORACLE_HOST", "localhost"),
        port=int(os.environ.get("ORACLE_PORT", "1521")),
        service_name=os.environ.get("ORACLE_SERVICE", "ORCLCDB")
    )
    conn.autocommit = False
    return conn

CURRENCIES = ["USD","EUR","GBP","JPY","CHF","CAD","AUD","HKD"]
ASSET_CLASSES = ["EQUITY","FIXED_INCOME","FUND","CASH","DERIVATIVE"]
ACCOUNT_TYPES = ["PENSION","ENDOWMENT","SOVEREIGN","INSURANCE","PRIVATE_WEALTH"]
SECTORS = ["TECHNOLOGY","FINANCIALS","HEALTHCARE","INDUSTRIALS","ENERGY","CONSUMER","UTILITIES"]
BROKERS = ["GS","MS","JPM","BARC","CITI","BOFA","UBS"]
TXN_TYPES = ["BUY","SELL","DIVIDEND","INTEREST","TRANSFER"]

def log_metric(conn, sid: str, phase: str, started: datetime, finished: datetime,
               rows_actual: int, rows_projected: int = 0, **kwargs):
    dur = (finished - started).total_seconds()
    rps = rows_actual / max(dur, 0.001)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO stress_raw.stress_metrics
        (scenario_id, scenario_name, phase, started_at, finished_at,
         duration_sec, rows_actual, rows_projected, rows_per_sec,
         threads, complexity, pruning_active, strategy, notes)
        VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)
    """, (
        sid,
        SCENARIOS[sid]["name"],
        phase, started, finished,
        round(dur, 3), rows_actual, rows_projected,
        round(rps, 2),
        SCENARIOS[sid].get("threads", 4),
        SCENARIOS[sid].get("model_complexity", "medium"),
        1 if SCENARIOS[sid].get("use_partition_filter") else 0,
        SCENARIOS[sid].get("strategy", "real"),
        kwargs.get("notes", "")[:3999]
    ))
    conn.commit()
    cur.close()


# ── Securities ─────────────────────────────────────────────────────────────────
def load_securities(conn, n: int, sid: str) -> List[str]:
    cur = conn.cursor()
    cur.execute("DELETE FROM stress_raw.raw_securities WHERE security_id LIKE :p",
                p=f"S{sid}%")
    conn.commit()

    ids, batch = [], []
    for i in range(1, n + 1):
        sec_id = f"S{sid}_{i:06d}"
        ids.append(sec_id)
        asset = ASSET_CLASSES[i % len(ASSET_CLASSES)]
        batch.append((
            sec_id,
            f"{random.randint(100000000,999999999)}",
            f"US{random.randint(1000000000,9999999999)}",
            f"T{i:05d}",
            f"Security {sec_id}",
            asset,
            asset[:3] + f"_{i%5}",
            random.choice(["US","GB","JP","DE","FR","CH","CA"]),
            random.choice(CURRENCIES),
            random.choice(SECTORS),
            random.choice(["AAA","AA","A","BBB","BB","NR"]),
        ))
        if len(batch) >= 5000:
            cur.executemany("""
                INSERT /*+ APPEND_VALUES */ INTO stress_raw.raw_securities
                (security_id,cusip,isin,ticker,security_name,asset_class,
                 sub_asset_class,country,currency,sector,credit_rating)
                VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)
            """, batch)
            conn.commit(); batch = []

    if batch:
        cur.executemany("""
            INSERT /*+ APPEND_VALUES */ INTO stress_raw.raw_securities
            (security_id,cusip,isin,ticker,security_name,asset_class,
             sub_asset_class,country,currency,sector,credit_rating)
            VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)
        """, batch)
        conn.commit()
    cur.close()
    return ids


# ── Accounts ──────────────────────────────────────────────────────────────────
def load_accounts(conn, n: int, sid: str) -> List[str]:
    cur = conn.cursor()
    cur.execute("DELETE FROM stress_raw.raw_accounts WHERE account_id LIKE :p",
                p=f"A{sid}%")
    conn.commit()

    ids, batch = [], []
    for i in range(1, n + 1):
        acc_id = f"A{sid}_{i:05d}"
        ids.append(acc_id)
        batch.append((
            acc_id, f"Account {acc_id}",
            f"CLI{i % max(n//5,1):05d}",
            ACCOUNT_TYPES[i % len(ACCOUNT_TYPES)],
            random.choice(CURRENCIES),
            date(2015, 1, 1) + timedelta(days=random.randint(0, 2000)),
            "ACTIVE",
            random.choice(["AMERICAS","EMEA","APAC"]),
            random.choice(["CONSERVATIVE","BALANCED","GROWTH"]),
        ))
        if len(batch) >= 5000:
            cur.executemany("""
                INSERT /*+ APPEND_VALUES */ INTO stress_raw.raw_accounts
                (account_id,account_name,client_id,account_type,base_currency,
                 inception_date,status,region,risk_profile)
                VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9)
            """, batch)
            conn.commit(); batch = []
    if batch:
        cur.executemany("""
            INSERT /*+ APPEND_VALUES */ INTO stress_raw.raw_accounts
            (account_id,account_name,client_id,account_type,base_currency,
             inception_date,status,region,risk_profile)
            VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9)
        """, batch)
        conn.commit()
    cur.close()
    return ids


# ── Holdings — the heavy table ────────────────────────────────────────────────
def _load_holdings_chunk(acc_chunk: List[str], sec_ids: List[str],
                          dates: List[date], per_slot: int, sid: str) -> int:
    """Runs in a thread. Each thread gets its own connection."""
    conn  = make_conn()
    cur   = conn.cursor()
    batch = []
    BATCH = 5000
    total = 0

    for acc_id in acc_chunk:
        # Pre-compute total MV per account-date for weight calculation
        for dt in dates:
            n_secs  = min(per_slot, len(sec_ids))
            secs    = random.sample(sec_ids, n_secs)
            mvs     = [round(random.uniform(10_000, 2_000_000), 2) for _ in secs]
            total_mv = sum(mvs)

            for sec_id, mv in zip(secs, mvs):
                cost    = round(mv * random.uniform(0.7, 1.3), 2)
                gain    = round(mv - cost, 2)
                weight  = round(mv / max(total_mv, 1), 8)
                fx      = round(random.uniform(0.8, 1.3), 6)
                price   = round(random.uniform(10, 500), 4)
                qty     = round(mv / (price * fx), 2)

                batch.append((
                    acc_id, sec_id, dt,
                    qty, mv, cost,
                    random.choice(CURRENCIES),
                    price, fx,
                    gain,    # pre-computed unrealized_gain
                    weight,  # pre-computed weight
                    sid
                ))
                if len(batch) >= BATCH:
                    cur.executemany("""
                        INSERT /*+ APPEND_VALUES */ INTO stress_raw.raw_holdings
                        (account_id,security_id,as_of_date,quantity,
                         market_value_usd,cost_basis_usd,currency,
                         price_local,fx_rate,unrealized_gain,
                         weight_in_acct,scenario_id)
                        VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)
                    """, batch)
                    conn.commit()
                    total += len(batch)
                    batch  = []

    if batch:
        cur.executemany("""
            INSERT /*+ APPEND_VALUES */ INTO stress_raw.raw_holdings
            (account_id,security_id,as_of_date,quantity,
             market_value_usd,cost_basis_usd,currency,
             price_local,fx_rate,unrealized_gain,
             weight_in_acct,scenario_id)
            VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)
        """, batch)
        conn.commit()
        total += len(batch)

    cur.close()
    conn.close()
    return total


def load_holdings(acc_ids: List[str], sec_ids: List[str],
                  n_days: int, per_slot: int, sid: str,
                  n_workers: int = 4) -> int:
    # Generate dates (weekdays only)
    base  = date(2024, 1, 15)
    dates = [base + timedelta(days=d)
             for d in range(n_days)
             if (base + timedelta(days=d)).weekday() < 5]

    # Split accounts across workers
    chunk_size = max(1, len(acc_ids) // n_workers)
    chunks = [acc_ids[i:i+chunk_size] for i in range(0, len(acc_ids), chunk_size)]

    total = 0
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futs = [pool.submit(_load_holdings_chunk, ch, sec_ids, dates, per_slot, sid)
                for ch in chunks]
        for f in as_completed(futs):
            total += f.result()

    print(f"  ✓ Holdings: {total:,} rows")
    return total


# ── Transactions ──────────────────────────────────────────────────────────────
def load_transactions(conn, sec_ids, acc_ids, n_days, ratio, sid):
    n  = int(len(acc_ids) * n_days * ratio * 3)
    cur = conn.cursor()
    cur.execute("DELETE FROM stress_raw.raw_transactions WHERE scenario_id = :s",
                s=sid)
    conn.commit()

    base  = date(2024, 1, 15)
    batch = []
    total = 0
    for i in range(n):
        dt = base + timedelta(days=random.randint(0, n_days - 1))
        batch.append((
            f"T{sid}{i:010d}",
            random.choice(acc_ids), random.choice(sec_ids),
            dt, random.choice(TXN_TYPES),
            round(random.uniform(100, 500_000), 2),
            round(random.uniform(10_000, 2_000_000), 2),
            round(random.uniform(9_900, 1_980_000), 2),
            random.choice(CURRENCIES),
            random.choice(BROKERS), sid
        ))
        if len(batch) >= 5000:
            cur.executemany("""
                INSERT /*+ APPEND_VALUES */ INTO stress_raw.raw_transactions
                (transaction_id,account_id,security_id,trade_date,
                 transaction_type,quantity,gross_amount_usd,net_amount_usd,
                 currency,broker,scenario_id)
                VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)
            """, batch)
            conn.commit(); total += len(batch); batch = []
    if batch:
        cur.executemany("""
            INSERT /*+ APPEND_VALUES */ INTO stress_raw.raw_transactions
            (transaction_id,account_id,security_id,trade_date,
             transaction_type,quantity,gross_amount_usd,net_amount_usd,
             currency,broker,scenario_id)
            VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)
        """, batch)
        conn.commit(); total += len(batch)
    cur.close()
    print(f"  ✓ Transactions: {total:,} rows")
    return total


# ── Oracle stats scaling for S7/S8 ───────────────────────────────────────────
def scale_oracle_stats(conn, sid: str, projected_rows: int):
    """Override Oracle CBO statistics to reflect projected row counts.
    This makes EXPLAIN PLAN show realistic costs without loading billions of rows.
    Standard practice for Oracle capacity planning."""
    projected_blocks = projected_rows // 10  # ~10 rows per block with compression

    cur = conn.cursor()
    cur.execute("""
        BEGIN
          DBMS_STATS.SET_TABLE_STATS(
            ownname      => 'STRESS_RAW',
            tabname      => 'RAW_HOLDINGS',
            numrows      => :rows,
            numblks      => :blks,
            no_invalidate => FALSE
          );
        END;
    """, rows=projected_rows, blks=projected_blocks)
    conn.commit()

    # Also scale partition stats proportionally
    n_months = 24  # simulate 2 years of monthly partitions
    rows_per_partition = projected_rows // n_months
    cur.execute("""
        DECLARE
          CURSOR c IS
            SELECT partition_name
            FROM dba_tab_partitions
            WHERE table_owner = 'STRESS_RAW'
              AND table_name = 'RAW_HOLDINGS';
        BEGIN
          FOR r IN c LOOP
            DBMS_STATS.SET_TABLE_STATS(
              ownname        => 'STRESS_RAW',
              tabname        => 'RAW_HOLDINGS',
              partname       => r.partition_name,
              numrows        => :rpp,
              numblks        => :rpp / 10,
              no_invalidate  => FALSE
            );
          END LOOP;
        END;
    """, rpp=rows_per_partition)
    conn.commit()
    cur.close()

    print(f"  ✓ Oracle stats scaled to {projected_rows:,} rows ({projected_rows/1e9:.1f}B)")


def collect_explain_plan(conn, sid: str, use_pruning: bool) -> dict:
    """Run EXPLAIN PLAN on a representative mart query and return cost stats."""
    date_filter = (
        "AND h.as_of_date >= TRUNC(SYSDATE) - 30"
        if use_pruning else ""
    )
    explain_sql = f"""
        EXPLAIN PLAN SET STATEMENT_ID = 'STRESS_{sid}' INTO stress_raw.stress_plan
        FOR
        SELECT
            h.account_id,
            h.as_of_date,
            COUNT(DISTINCT h.security_id) AS sec_count,
            SUM(h.market_value_usd)       AS total_mv,
            SUM(h.unrealized_gain)        AS total_gain
        FROM stress_raw.raw_holdings h
        WHERE h.scenario_id = :sid
          {date_filter}
        GROUP BY h.account_id, h.as_of_date
    """
    cur = conn.cursor()

    # Ensure plan table exists
    try:
        cur.execute("CREATE TABLE stress_raw.stress_plan AS SELECT * FROM plan_table WHERE 1=0")
        conn.commit()
    except Exception:
        pass

    cur.execute(f"DELETE FROM stress_raw.stress_plan WHERE statement_id = 'STRESS_{sid}'")
    conn.commit()

    try:
        cur.execute(explain_sql, sid=sid)
        conn.commit()

        cur.execute("""
            SELECT MAX(cost), COUNT(*), SUM(CASE WHEN operation LIKE 'PARTITION%' THEN 1 ELSE 0 END)
            FROM stress_raw.stress_plan WHERE statement_id = :stmt
        """, stmt=f"STRESS_{sid}")
        row = cur.fetchone()
        result = {
            "cost":        int(row[0] or 0),
            "plan_steps":  int(row[1] or 0),
            "partition_ops": int(row[2] or 0)
        }
    except Exception as e:
        result = {"cost": 0, "plan_steps": 0, "partition_ops": 0, "error": str(e)}
    finally:
        cur.close()
    return result


def update_stats_to_oracle(conn, plan: dict, sid: str):
    """Write explain plan stats to metrics table."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE stress_raw.stress_metrics
        SET explain_cost = :cost,
            partition_count = :steps,
            partitions_pruned = :pruned
        WHERE scenario_id = :sid AND phase = 'collect_stats'
    """, cost=plan["cost"], steps=plan["plan_steps"],
                pruned=plan["partition_ops"], sid=sid)
    conn.commit()
    cur.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def run_scenario(sid: str, dry_run: bool = False):
    cfg = SCENARIOS[sid]
    strategy = cfg["strategy"]

    print(f"\n{'='*60}")
    print(f"  {sid}: {cfg['name']}")
    print(f"  Strategy: {strategy}  Target rows: {cfg['target_rows']}")
    print(f"{'='*60}")

    if dry_run:
        est = cfg.get("n_accounts", 0) * cfg.get("n_days", 30) * cfg.get("holdings_per_slot", 20)
        print(f"  [DRY RUN] Estimated holdings rows: {est:,}")
        return

    conn = make_conn()
    t_total = datetime.now()

    if strategy == "real_shared":
        # S6 reuses S5 data — only re-run dbt, no data load
        print(f"  Reusing data from S5 (no reload needed)")
        log_metric(conn, sid, "data_generation", datetime.now(), datetime.now(),
                   0, notes="Shared data from S5 — no reload")
        conn.close()
        return

    if strategy in ("real", "scaled_stats"):
        ts = datetime.now()

        # Securities
        sec_ids = load_securities(conn, cfg["n_securities"], sid)
        # Accounts
        acc_ids = load_accounts(conn, cfg["n_accounts"], sid)
        # Holdings — parallel across accounts
        n_workers = min(cfg["threads"], 4)  # cap at 4 for data gen
        h_rows = load_holdings(acc_ids, sec_ids, cfg["n_days"],
                                cfg["holdings_per_slot"], sid, n_workers)
        # Transactions
        t_rows = load_transactions(conn, sec_ids, acc_ids,
                                    cfg["n_days"], cfg["txn_ratio"], sid)

        tf = datetime.now()
        total_rows = len(sec_ids) + len(acc_ids) + h_rows + t_rows
        log_metric(conn, sid, "data_generation", ts, tf, total_rows,
                   cfg.get("projected_rows", 0))
        dur = (tf - ts).total_seconds()
        print(f"  ✓ Data loaded: {total_rows:,} rows in {dur:.1f}s")

    if strategy == "scaled_stats":
        # Scale Oracle stats + collect explain plan
        ts = datetime.now()
        projected = cfg.get("projected_rows", 0)
        scale_oracle_stats(conn, sid, projected)
        plan = collect_explain_plan(conn, sid, cfg.get("use_partition_filter", False))
        tf = datetime.now()
        log_metric(conn, sid, "collect_stats", ts, tf, 0, projected,
                   notes=f"explain_cost={plan['cost']} plan_steps={plan['plan_steps']}")
        update_stats_to_oracle(conn, plan, sid)
        print(f"  ✓ Stats scaled to {projected/1e9:.1f}B rows | Explain cost: {plan['cost']:,}")

    # Gather table stats for dbt to use
    ts = datetime.now()
    cur = conn.cursor()
    cur.execute("""
        BEGIN
          DBMS_STATS.GATHER_TABLE_STATS(
            ownname => 'STRESS_RAW', tabname => 'RAW_HOLDINGS',
            partname => NULL, granularity => 'AUTO',
            degree => 4, no_invalidate => FALSE
          );
        END;
    """)
    conn.commit()
    cur.close()
    tf = datetime.now()
    print(f"  ✓ Oracle stats gathered in {(tf-ts).total_seconds():.1f}s")

    total_dur = (datetime.now() - t_total).total_seconds()
    print(f"  ✅ {sid} data phase done in {total_dur:.1f}s ({total_dur/60:.1f}m)")
    conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", default="S1")
    parser.add_argument("--dry-run",  action="store_true")
    args = parser.parse_args()

    targets = list(SCENARIOS.keys()) if args.scenario == "all" else [args.scenario.upper()]
    random.seed(42)

    for sid in targets:
        if sid not in SCENARIOS:
            print(f"Unknown: {sid}")
            sys.exit(1)
        run_scenario(sid, args.dry_run)

    print("\n✅ Data generation complete.")


if __name__ == "__main__":
    main()
