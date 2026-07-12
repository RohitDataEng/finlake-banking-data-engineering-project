select *
from {{ source('snowflake_gold', 'GOLD_BRANCH_PERFORMANCE_SUMMARY') }}
