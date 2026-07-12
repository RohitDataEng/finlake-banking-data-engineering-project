select *
from {{ source('snowflake_gold', 'GOLD_LOAN_DEFAULT_RISK_SUMMARY') }}
