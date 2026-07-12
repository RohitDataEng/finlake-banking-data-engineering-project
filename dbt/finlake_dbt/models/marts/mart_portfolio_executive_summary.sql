select
    kpi_date,
    total_loans,
    total_customers,
    total_loan_portfolio,
    total_emi_due,
    total_amount_collected,
    total_overdue_amount,
    active_loans,
    closed_loans,
    defaulted_loans,
    high_risk_loans,
    avg_risk_score,
    collection_rate,
    default_rate,
    high_risk_loan_rate,
    case
        when high_risk_loan_rate >= 15 or default_rate >= 10 then 'HIGH_PORTFOLIO_RISK'
        when high_risk_loan_rate >= 8 or default_rate >= 5 then 'MEDIUM_PORTFOLIO_RISK'
        else 'LOW_PORTFOLIO_RISK'
    end as portfolio_health_status
from {{ ref('stg_portfolio_kpis') }}
