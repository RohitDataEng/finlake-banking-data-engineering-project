select *
from {{ source('snowflake_gold', 'GOLD_PORTFOLIO_KPIS') }}
