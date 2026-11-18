-- The "highest revenue category" model must return exactly one row.

with result as (

    select count(*) as row_count from {{ ref('highest_revenue_category') }}

)

select row_count
from result
where row_count != 1
