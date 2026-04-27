# quickstart.ps1
# Run this from the swp_stress_suite folder.
# Pre-requisites: Oracle DB accessible, Python 3.10+, dbt-oracle installed.
# Uses python-oracledb (no Oracle Instant Client required for thin mode).

$env:ORACLE_HOST     = "localhost"
$env:ORACLE_PORT     = "1521"
$env:ORACLE_SERVICE  = "ORCLCDB"
$env:ORACLE_USER     = "dbt_stress"
$env:ORACLE_PASSWORD = "StressTest123"
$env:ORACLE_SCHEMA   = "STRESS_DEV"
$env:DBT_PROFILES_DIR = (Get-Location).Path

Write-Host "=== SWP Stress Test Suite ===" -ForegroundColor Cyan
Write-Host "Step 1: Install Python dependencies..."
pip install oracledb==2.3.0 dbt-oracle==1.8.3 faker==24.0.0 pandas==2.2.0 tqdm==4.66.0 psutil==5.9.8 --quiet

Write-Host "Step 2: Run Oracle setup as SYSDBA..."
Write-Host "  sqlplus sys/YourSysPassword@localhost:1521/ORCLCDB as sysdba @load_scripts/00_oracle_setup.sql" -ForegroundColor Yellow
Read-Host "Press Enter after running oracle_setup.sql as SYSDBA..."

Write-Host "Step 3: Create raw tables..."
Write-Host "  sqlplus dbt_stress/StressTest123@localhost:1521/ORCLCDB @load_scripts/01_create_raw_tables.sql" -ForegroundColor Yellow
Read-Host "Press Enter after creating tables..."

Write-Host "Step 4: Generate dbt project files..."
python data_generators/generate_dbt_projects.py

Write-Host "Step 5: Run S1 baseline (quick test ~2 min)..."
python run_all.py --scenarios S1

Write-Host ""
Write-Host "Step 6: Run all 8 scenarios (~40 min total)..."
$ans = Read-Host "Run all 8 now? (y/n)"
if ($ans -eq "y") {
    python run_all.py
} else {
    Write-Host "To run all later:          python run_all.py"
    Write-Host "Pruning comparison only:   python run_all.py --scenarios S5 S6"
    Write-Host "Specific scenarios:        python run_all.py --scenarios S1 S2 S3"
}

Write-Host ""
Write-Host "Report: reports/cto_report.html" -ForegroundColor Green
Write-Host "Open:   Start-Process reports/cto_report.html"
