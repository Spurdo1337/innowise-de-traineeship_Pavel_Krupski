with source as (

    select * from {{ source('pagila', 'store') }}

)

select
    store_id::integer as store_id,
    manager_staff_id::integer as manager_staff_id,
    address_id::integer as address_id,
    last_update::timestamp_ntz as last_update
from source
