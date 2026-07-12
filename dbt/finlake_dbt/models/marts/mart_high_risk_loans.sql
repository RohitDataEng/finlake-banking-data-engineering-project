select
    loan_id,
    customer_id,
    customer_name,
    branch_id,
    branch_name,
    region,
    loan_type,
    loan_amount,
    collection_rate,
    missed_count,
    max_days_past_due,
    total_overdue_amount,
    risk_score,
    risk_category,
    true as requires_collection_attention
from {{ ref('stg_loan_default_risk_summary') }}
where risk_category in ('CRITICAL', 'HIGH')
