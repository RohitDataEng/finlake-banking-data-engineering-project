select *
from {{ ref('stg_loan_default_risk_summary') }}
where collection_rate < 0 or collection_rate > 100
