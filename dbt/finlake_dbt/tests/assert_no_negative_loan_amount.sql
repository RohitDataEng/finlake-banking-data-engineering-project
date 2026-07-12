select *
from {{ ref('stg_loan_default_risk_summary') }}
where loan_amount < 0
