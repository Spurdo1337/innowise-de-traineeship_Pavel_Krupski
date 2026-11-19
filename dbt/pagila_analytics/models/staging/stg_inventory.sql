with source as (

    select * from {{ source('pagila', 'inventory') }}

)

select
    inventory_id::integer as inventory_id,
    film_id::integer as film_id,
    store_id::integer as store_id,
    last_update::timestamp_ntz as last_update
from source
