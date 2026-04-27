#!/usr/bin/env python3
"""
run_all.py
==========
Master orchestrator for the SWP stress test suite.
Runs all 8 scenarios in sequence, collects metrics, generates the CTO report.

Usage:
    python run_all.py                          # full run (data + dbt + report)
    python run_all.py --scenarios S1 S2        # specific scenarios
    python run_all.py --skip-data              # dbt only (data already loaded)
    python run_all.py --skip-dbt               # data only
    python run_all.py --report-only            # just regenerate report from DB
"""

import argparse
import oracledb
import os
import sys
import subprocess
import time
import json
import psutil
import threading
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

SUITE_ROOT = Path(__file__).parent
PROJECTS   = SUITE_ROOT / "projects"
REPORTS    = SUITE_ROOT / "reports"
REPORTS.mkdir(exist_ok=True)

# ── Oracle connection ─────────────────────────────────────────────────────────
def get_conn():
    return oracledb.connect(
        user=os.environ.get("ORACLE_USER", "dbt_stress"),
        password=os.environ.get("ORACLE_PASSWORD", "StressTest123"),
        host=os.environ.get("ORACLE_HOST", "localhost"),
        port=int(os.environ.get("ORACLE_PORT", "1521")),
        service_name=os.environ.get("ORACLE_SERVICE", "ORCLCDB")
    )

SCENARIO_META = {
    "S1": {"name": "Baseline",                  "models": 58,   "threads": 1, "volume_est": "1M",    "complexity": "simple",   "pruning": False},
    "S2": {"name": "Current State",             "models": 200,  "threads": 4, "volume_est": "20M",   "complexity": "medium",   "pruning": False},
    "S3": {"name": "Full Migration",            "models": 500,  "threads": 8, "volume_est": "99M",   "complexity": "medium",   "pruning": False},
    "S4": {"name": "Complex SQL",               "models": 200,  "threads": 4, "volume_est": "20M",   "complexity": "extreme",  "pruning": False},
    "S5": {"name": "Data at Scale (No Pruning)","models": 200,  "threads": 8, "volume_est": "500M",  "complexity": "medium",   "pruning": False},
    "S6": {"name": "Data at Scale (Pruning ON)","models": 200,  "threads": 8, "volume_est": "500M",  "complexity": "medium",   "pruning": True},
    "S7": {"name": "Enterprise Full",           "models": 1500, "threads": 8, "volume_est": "1.1B",  "complexity": "complex",  "pruning": True},
    "S8": {"name": "Absolute Worst Case",       "models": 5000, "threads": 8, "volume_est": "5B*",   "complexity": "extreme",  "pruning": True},
}

# ── CPU/Memory sampler ────────────────────────────────────────────────────────
class ResourceSampler:
    def __init__(self):
        self.samples: List[Tuple[float, float]] = []  # (cpu_pct, mem_mb)
        self._stop  = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.samples = []
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self):
        proc = psutil.Process(os.getpid())
        while not self._stop.is_set():
            try:
                cpu = psutil.cpu_percent(interval=1)
                mem = proc.memory_info().rss / (1024 * 1024)
                self.samples.append((cpu, mem))
            except Exception:
                pass

    def summary(self) -> Dict:
        if not self.samples:
            return {"avg_cpu": 0, "peak_cpu": 0, "peak_mem_mb": 0, "avg_mem_mb": 0}
        cpus = [s[0] for s in self.samples]
        mems = [s[1] for s in self.samples]
        return {
            "avg_cpu":    round(sum(cpus) / len(cpus), 1),
            "peak_cpu":   round(max(cpus), 1),
            "avg_mem_mb": round(sum(mems) / len(mems), 1),
            "peak_mem_mb":round(max(mems), 1),
        }


# ── dbt runner ────────────────────────────────────────────────────────────────

def run_dbt(project_dir: Path, command: str, threads: int = 4,
            extra_args: List[str] = None) -> Tuple[bool, str, float]:
    """Run a dbt command. Returns (success, output, duration_seconds)."""
    cmd = [
        "dbt", command,
        "--project-dir", str(project_dir),
        "--profiles-dir", str(SUITE_ROOT),
        "--threads", str(threads),
    ]
    if extra_args:
        cmd.extend(extra_args)

    env = {**os.environ, "DBT_PROFILES_DIR": str(SUITE_ROOT)}
    t0  = time.time()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=7200)
        duration = time.time() - t0
        success  = result.returncode == 0
        output   = result.stdout + "\n" + result.stderr
        return success, output, duration
    except subprocess.TimeoutExpired:
        return False, "TIMEOUT after 2h", time.time() - t0
    except FileNotFoundError:
        return False, "dbt not found — is it installed?", 0.0


def count_dbt_rows(output: str) -> Dict[str, int]:
    """Parse dbt run output for row counts."""
    import re
    counts = {}
    for line in output.split("\n"):
        m = re.search(r"(\d+) rows affected", line, re.IGNORECASE)
        if m:
            counts["rows"] = counts.get("rows", 0) + int(m.group(1))
    return counts


def count_models_in_output(output: str) -> Tuple[int, int, int]:
    """Returns (pass, warn, error) from dbt run output."""
    import re
    m = re.search(r"Completed with (\d+) warnings? and (\d+) error", output)
    if m:
        warns  = int(m.group(1))
        errors = int(m.group(2))
        passes = 0
    else:
        m2 = re.search(r"(\d+) of (\d+) OK", output)
        passes = int(m2.group(1)) if m2 else 0
        warns  = output.count("WARN")
        errors = output.count("ERROR")
    return passes, warns, errors


def log_metric_to_oracle(conn, scenario_id: str, phase: str, started: datetime,
                          finished: datetime, rows: int, resource: Dict, notes: str = ""):
    dur = (finished - started).total_seconds()
    rps = rows / max(dur, 0.001)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO stress_raw.stress_run_metrics
            (scenario_id, scenario_name, phase, started_at, finished_at,
             duration_seconds, rows_processed, rows_per_second,
             peak_memory_mb, avg_cpu_pct, dbt_models_count, dbt_threads, notes)
            VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)
        """, (
            scenario_id,
            SCENARIO_META.get(scenario_id, {}).get("name", scenario_id),
            phase, started, finished,
            round(dur, 3), rows, round(rps, 2),
            resource.get("peak_mem_mb", 0),
            resource.get("avg_cpu", 0),
            SCENARIO_META.get(scenario_id, {}).get("models", 0),
            SCENARIO_META.get(scenario_id, {}).get("threads", 4),
            notes[:3999] if notes else ""
        ))
        conn.commit()
    except Exception as e:
        print(f"    [metric log error] {e}")
    finally:
        cur.close()


# ── Per-scenario runner ───────────────────────────────────────────────────────

def run_scenario(scenario_id: str, skip_data: bool, skip_dbt: bool,
                 conn, results: List[Dict]):
    meta     = SCENARIO_META[scenario_id]
    proj_dir = next(PROJECTS.glob(f"{scenario_id}_*"), None)
    sampler  = ResourceSampler()

    print(f"\n{'═'*70}")
    print(f"  {scenario_id}: {meta['name']}")
    print(f"  Models: {meta['models']}  Threads: {meta['threads']}  Volume: {meta['volume_est']}")
    print(f"  Complexity: {meta['complexity']}  Partition pruning: {meta['pruning']}")
    print(f"{'═'*70}")

    result = {
        "scenario_id":   scenario_id,
        "scenario_name": meta["name"],
        "models":        meta["models"],
        "threads":       meta["threads"],
        "volume_est":    meta["volume_est"],
        "complexity":    meta["complexity"],
        "pruning":       meta["pruning"],
        "phases":        {}
    }

    # ── Data generation ─────────────────────────────────────────────────────
    if not skip_data:
        print(f"\n  [1/4] Generating data...")
        sampler.start()
        ts = datetime.now()
        try:
            subprocess.run(
                [sys.executable, str(SUITE_ROOT / "data_generators" / "generate_data.py"),
                 "--scenario", scenario_id],
                check=True, timeout=86400
            )
            success = True
        except Exception as e:
            print(f"    ERROR: {e}")
            success = False
        tf = datetime.now()
        sampler.stop()
        res = sampler.summary()
        dur = (tf - ts).total_seconds()
        result["phases"]["data_generation"] = {"duration_s": round(dur, 1), "success": success, **res}
        log_metric_to_oracle(conn, scenario_id, "data_generation", ts, tf, 0, res)
        print(f"  ✓ Data generation: {dur:.1f}s  Peak memory: {res['peak_mem_mb']:.0f}MB  CPU: {res['avg_cpu']:.1f}%")

    if not proj_dir:
        print(f"  ⚠ Project directory not found for {scenario_id} — generating now...")
        subprocess.run([sys.executable, str(SUITE_ROOT / "data_generators" / "generate_dbt_projects.py")])
        proj_dir = next(PROJECTS.glob(f"{scenario_id}_*"), None)
    if not proj_dir:
        print(f"  ✗ Cannot find project for {scenario_id}. Skipping.")
        return

    if not skip_dbt:
        # ── dbt compile ───────────────────────────────────────────────────────
        print(f"\n  [2/4] dbt compile...")
        sampler.start()
        ts = datetime.now()
        ok, out, dur = run_dbt(proj_dir, "compile", meta["threads"])
        tf = datetime.now()
        sampler.stop()
        res = sampler.summary()
        result["phases"]["dbt_compile"] = {"duration_s": round(dur, 1), "success": ok, **res, "output_tail": out[-500:]}
        log_metric_to_oracle(conn, scenario_id, "dbt_compile", ts, tf, 0, res, out[-2000:])
        status = "✓" if ok else "✗"
        print(f"  {status} dbt compile: {dur:.1f}s  Memory: {res['peak_mem_mb']:.0f}MB  CPU: {res['avg_cpu']:.1f}%")
        if not ok:
            print(f"    [compile errors - continuing]\n{out[-300:]}")

        # ── dbt run ───────────────────────────────────────────────────────────
        print(f"\n  [3/4] dbt run ({meta['threads']} threads)...")
        sampler.start()
        ts = datetime.now()
        ok, out, dur = run_dbt(proj_dir, "run", meta["threads"], ["--full-refresh"])
        tf = datetime.now()
        sampler.stop()
        res = sampler.summary()
        passes, warns, errors = count_models_in_output(out)
        result["phases"]["dbt_run"] = {
            "duration_s": round(dur, 1), "success": ok,
            "models_pass": passes, "models_warn": warns, "models_error": errors,
            **res, "output_tail": out[-500:]
        }
        log_metric_to_oracle(conn, scenario_id, "dbt_run", ts, tf,
                              passes, res, out[-2000:])
        status = "✓" if ok else "✗"
        print(f"  {status} dbt run: {dur:.1f}s  Pass={passes} Warn={warns} Err={errors}")
        print(f"     Memory peak: {res['peak_mem_mb']:.0f}MB  Avg CPU: {res['avg_cpu']:.1f}%  Peak CPU: {res['peak_cpu']:.1f}%")

        # ── dbt test ─────────────────────────────────────────────────────────
        print(f"\n  [4/4] dbt test...")
        sampler.start()
        ts = datetime.now()
        ok, out, dur = run_dbt(proj_dir, "test", meta["threads"])
        tf = datetime.now()
        sampler.stop()
        res = sampler.summary()
        passes, warns, errors = count_models_in_output(out)
        result["phases"]["dbt_test"] = {
            "duration_s": round(dur, 1), "success": ok,
            "tests_pass": passes, "tests_warn": warns, "tests_error": errors,
            **res
        }
        log_metric_to_oracle(conn, scenario_id, "dbt_test", ts, tf, passes, res)
        status = "✓" if ok else "✗"
        print(f"  {status} dbt test: {dur:.1f}s  Pass={passes} Err={errors}")

    results.append(result)
    print(f"\n  ✅ {scenario_id} complete")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SWP Stress Test Harness")
    parser.add_argument("--scenarios", nargs="+", default=list(SCENARIO_META.keys()))
    parser.add_argument("--skip-data", action="store_true")
    parser.add_argument("--skip-dbt",  action="store_true")
    parser.add_argument("--report-only", action="store_true")
    args = parser.parse_args()

    print("\n" + "═"*70)
    print("  SWP Stress Test Suite — CTO Performance Benchmark")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═"*70)

    conn    = get_conn()
    results = []

    if not args.report_only:
        # Generate projects if missing
        if not any(PROJECTS.glob("S1_*")):
            print("\nGenerating dbt projects first...")
            subprocess.run([sys.executable, str(SUITE_ROOT / "data_generators" / "generate_dbt_projects.py")])

        for sid in args.scenarios:
            if sid not in SCENARIO_META:
                print(f"Unknown scenario: {sid}")
                continue
            run_scenario(sid, args.skip_data, args.skip_dbt, conn, results)

    # Always regenerate report with fresh DB data
    from reports.generate_report import generate_html_report
    report_path = generate_html_report(conn, results)
    print(f"\n{'═'*70}")
    print(f"  📊 CTO Report: {report_path}")
    print(f"  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═"*70)
    conn.close()


if __name__ == "__main__":
    main()
