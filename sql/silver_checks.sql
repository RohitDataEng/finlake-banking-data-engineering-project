-- Silver Checks

SELECT 'silver_branch_master' AS table_name, COUNT(*) AS row_count FROM workspace.default.silver_branch_master
UNION ALL
SELECT 'silver_customers', COUNT(*) FROM workspace.default.silver_customers
UNION ALL
SELECT 'silver_loan_applications', COUNT(*) FROM workspace.default.silver_loan_applications
UNION ALL
SELECT 'silver_loan_accounts', COUNT(*) FROM workspace.default.silver_loan_accounts
UNION ALL
SELECT 'silver_emi_payments', COUNT(*) FROM workspace.default.silver_emi_payments
UNION ALL
SELECT 'silver_delinquency', COUNT(*) FROM workspace.default.silver_delinquency
UNION ALL
SELECT 'silver_rejected_records', COUNT(*) FROM workspace.default.silver_rejected_records;

SELECT
    source_table,
    rule_code,
    rejection_reason,
    COUNT(*) AS rejected_count
FROM workspace.default.silver_rejected_records
GROUP BY source_table, rule_code, rejection_reason
ORDER BY source_table, rule_code;

SELECT customer_id, COUNT(*) AS cnt
FROM workspace.default.silver_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT *
FROM workspace.default.silver_loan_accounts
WHERE loan_amount <= 0;

SELECT p.*
FROM workspace.default.silver_emi_payments p
LEFT JOIN workspace.default.silver_loan_accounts l
    ON p.loan_id = l.loan_id
WHERE l.loan_id IS NULL;
