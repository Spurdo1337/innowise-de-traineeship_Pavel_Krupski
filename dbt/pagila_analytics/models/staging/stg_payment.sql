with source as (

    select * from {{ source('pagila', 'payment') }}

)

select
    payment_id::integer as payment_id,
    customer_id::integer as customer_id,
    staff_id::integer as staff_id,
    rental_id::integer as rental_id,
    amount::number(10, 2) as amount,
    payment_date::timestamp_ntz as payment_date
from source
