select
    branch_id,
    branch_name,
    city,
    state,
    region,
    total_loans,
    total_borrowers,
    total_disbursed_amount,
    collection_rate,
    default_rate,
    total_overdue_amount,
    npa_records,
    case
        when default_rate >= 10 or collection_rate < 75 then 'HIGH_RISK_BRANCH'
        when default_rate >= 5 or collection_rate < 90 then 'WATCHLIST_BRANCH'
        else 'HEALTHY_BRANCH'
    end as branch_health_status
from {{ ref('stg_branch_performance_summary') }}
