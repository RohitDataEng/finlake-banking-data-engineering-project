-- Day 3 - Databricks SQL setup/checks

-- Change these names if your workspace uses a different catalog/schema.
USE CATALOG workspace;
USE SCHEMA default;

CREATE VOLUME IF NOT EXISTS finlake_volume;

-- Volume path after creation:
-- /Volumes/workspace/default/finlake_volume/

-- After running the PySpark notebook, run this row count check:

SELECT 'bronze_branch_master' AS table_name, COUNT(*) AS row_count FROM workspace.default.bronze_branch_master
UNION ALL
SELECT 'bronze_customers', COUNT(*) FROM workspace.default.bronze_customers
UNION ALL
SELECT 'bronze_loan_applications', COUNT(*) FROM workspace.default.bronze_loan_applications
UNION ALL
SELECT 'bronze_loan_accounts', COUNT(*) FROM workspace.default.bronze_loan_accounts
UNION ALL
SELECT 'bronze_emi_payments', COUNT(*) FROM workspace.default.bronze_emi_payments
UNION ALL
SELECT 'bronze_delinquency', COUNT(*) FROM workspace.default.bronze_delinquency;

-- Audit check

SELECT
    pipeline_layer,
    source_system,
    COUNT(*) AS record_count
FROM workspace.default.bronze_customers
GROUP BY pipeline_layer, source_system;
