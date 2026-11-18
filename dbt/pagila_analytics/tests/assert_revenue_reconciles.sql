-- Payments must reconcile between the staging layer and fact_revenue.

with expected as (

    select
        count(*) as payment_count,
        coalesce(sum(amount), 0) as total_amount
    from {{ ref('stg_payment') }}

),

actual as (

    select
        count(*) as payment_count,
        coalesce(sum(amount), 0) as total_amount
    from {{ ref('fact_revenue') }}

)

select
    expected.payment_count as expected_payments,
    actual.payment_count as actual_payments,
    expected.total_amount as expected_amount,
    actual.total_amount as actual_amount
from expected
cross join actual
where expected.payment_count != actual.payment_count
   or expected.total_amount != actual.total_amount
