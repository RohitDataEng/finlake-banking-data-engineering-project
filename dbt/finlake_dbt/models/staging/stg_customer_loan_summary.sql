select *
from {{ source('snowflake_gold', 'GOLD_CUSTOMER_LOAN_SUMMARY') }}
