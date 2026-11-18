with source as (

    select * from {{ source('pagila', 'rental') }}

)

select
    rental_id::integer as rental_id,
    rental_date::timestamp_ntz as rental_date,
    inventory_id::integer as inventory_id,
    customer_id::integer as customer_id,
    return_date::timestamp_ntz as return_date,
    staff_id::integer as staff_id,
    last_update::timestamp_ntz as last_update
from source
