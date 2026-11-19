-- Revenue amounts are never negative.

select *
from {{ ref('fact_revenue') }}
where amount < 0
