--Gold Checks

SELECT 'gold_customer_loan_summary' AS table_name, COUNT(*) AS row_count FROM workspace.default.gold_customer_loan_summary
UNION ALL
SELECT 'gold_branch_performance_summary', COUNT(*) FROM workspace.default.gold_branch_performance_summary
UNION ALL
SELECT 'gold_monthly_collection_summary', COUNT(*) FROM workspace.default.gold_monthly_collection_summary
UNION ALL
SELECT 'gold_loan_default_risk_summary', COUNT(*) FROM workspace.default.gold_loan_default_risk_summary
UNION ALL
SELECT 'gold_portfolio_kpis', COUNT(*) FROM workspace.default.gold_portfolio_kpis;

SELECT loan_id, customer_id, customer_name, loan_type, loan_amount, collection_rate, missed_count, max_days_past_due, total_overdue_amount, risk_score, risk_category
FROM workspace.default.gold_loan_default_risk_summary
ORDER BY risk_score DESC, total_overdue_amount DESC
LIMIT 10;

SELECT branch_id, branch_name, city, region, total_loans, total_disbursed_amount, collection_rate, default_rate, total_overdue_amount
FROM workspace.default.gold_branch_performance_summary
ORDER BY total_disbursed_amount DESC;

SELECT * FROM workspace.default.gold_portfolio_kpis;
