select
    emi_due_month,
    loan_type,
    total_emi_records,
    total_loans,
    total_emi_due,
    total_amount_paid,
    collection_rate,
    missed_rate,
    paid_count,
    late_count,
    partial_count,
    missed_count,
    case
        when collection_rate >= 95 then 'EXCELLENT'
        when collection_rate >= 85 then 'GOOD'
        when collection_rate >= 70 then 'NEEDS_ATTENTION'
        else 'POOR'
    end as collection_performance_band
from {{ ref('stg_monthly_collection_summary') }}
