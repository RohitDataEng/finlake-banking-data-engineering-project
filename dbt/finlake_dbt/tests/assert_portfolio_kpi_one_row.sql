select count(*) as row_count
from {{ ref('stg_portfolio_kpis') }}
having count(*) <> 1
