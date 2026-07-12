select *
from {{ source('snowflake_gold', 'GOLD_MONTHLY_COLLECTION_SUMMARY') }}
