-- fact_rental must contain exactly one row per staged rental.

with expected as (

    select count(*) as row_count from {{ ref('stg_rental') }}

),

actual as (

    select count(*) as row_count from {{ ref('fact_rental') }}

)

select
    expected.row_count as expected_count,
    actual.row_count as actual_count
from expected
cross join actual
where expected.row_count != actual.row_count
