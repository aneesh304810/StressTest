-- =============================================================================
-- oracle_setup.sql
-- Run as SYSDBA: sqlplus sys/password@localhost:1521/ORCLCDB as sysdba @oracle_setup.sql
-- Creates the stress-test user, tablespaces, and base grants.
-- =============================================================================

-- ── Drop existing user if re-running ────────────────────────────────────────
BEGIN
  EXECUTE IMMEDIATE 'DROP USER dbt_stress CASCADE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

-- ── Tablespaces ──────────────────────────────────────────────────────────────
-- Use existing USERS tablespace for 12c standalone simplicity.
-- In production use dedicated tablespaces per schema.

-- ── Create stress test user ──────────────────────────────────────────────────
CREATE USER dbt_stress IDENTIFIED BY "StressTest123"
  DEFAULT TABLESPACE USERS
  TEMPORARY TABLESPACE TEMP
  QUOTA UNLIMITED ON USERS;

-- ── Grants ───────────────────────────────────────────────────────────────────
GRANT CREATE SESSION     TO dbt_stress;
GRANT CREATE TABLE       TO dbt_stress;
GRANT CREATE VIEW        TO dbt_stress;
GRANT CREATE SEQUENCE    TO dbt_stress;
GRANT CREATE PROCEDURE   TO dbt_stress;
GRANT CREATE TYPE        TO dbt_stress;
GRANT CREATE MATERIALIZED VIEW TO dbt_stress;
GRANT ALTER SESSION      TO dbt_stress;
GRANT CREATE SYNONYM     TO dbt_stress;

-- Allow dbt to create schemas (needed for +schema: model config)
GRANT CREATE USER        TO dbt_stress;
GRANT ALTER USER         TO dbt_stress;

-- Performance views needed for metric collection
GRANT SELECT ON v_$session    TO dbt_stress;
GRANT SELECT ON v_$sql        TO dbt_stress;
GRANT SELECT ON v_$sesstat    TO dbt_stress;
GRANT SELECT ON v_$statname   TO dbt_stress;
GRANT SELECT ON v_$mystat     TO dbt_stress;
GRANT SELECT ON v_$process    TO dbt_stress;
GRANT SELECT ON v_$pgastat    TO dbt_stress;
GRANT SELECT ON dba_segments  TO dbt_stress;
GRANT SELECT ON dba_tables    TO dbt_stress;
GRANT SELECT ON dba_indexes   TO dbt_stress;
GRANT SELECT ON dba_tab_partitions TO dbt_stress;

-- ── Create sub-schemas for each layer ────────────────────────────────────────
-- dbt will create these via the generate_schema_name macro, but we pre-create
-- them so Oracle doesn't reject the first run.
DECLARE
  PROCEDURE ensure_user(p_user VARCHAR2) IS
  BEGIN
    BEGIN
      EXECUTE IMMEDIATE 'CREATE USER ' || p_user ||
        ' IDENTIFIED BY "StressTest123" DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP QUOTA UNLIMITED ON USERS';
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
    BEGIN EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO ' || p_user; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'GRANT CREATE TABLE TO ' || p_user; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'GRANT CREATE VIEW TO ' || p_user; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'GRANT CREATE MATERIALIZED VIEW TO ' || p_user; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO ' || p_user; EXCEPTION WHEN OTHERS THEN NULL; END;
    -- Grant dbt_stress access to create objects in sub-schemas
    BEGIN EXECUTE IMMEDIATE 'GRANT CREATE ANY TABLE TO dbt_stress'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'GRANT CREATE ANY VIEW TO dbt_stress'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'GRANT INSERT ANY TABLE TO dbt_stress'; EXCEPTION WHEN OTHERS THEN NULL; END;
  END;
BEGIN
  -- Raw landing schemas
  ensure_user('STRESS_RAW');
  -- dbt output schemas (non-prod prefix)
  ensure_user('STRESS_STAGING');
  ensure_user('STRESS_INTERMEDIATE');
  ensure_user('STRESS_MARTS');
END;
/

-- ── Verify ───────────────────────────────────────────────────────────────────
SELECT username, created FROM dba_users WHERE username LIKE 'STRESS%' ORDER BY username;

PROMPT Setup complete. Run load_scripts/01_create_raw_tables.sql next.
EXIT;
