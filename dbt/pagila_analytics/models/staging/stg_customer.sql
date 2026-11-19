with source as (

    select * from {{ source('pagila', 'customer') }}

)

select
    customer_id::integer as customer_id,
    store_id::integer as store_id,
    first_name::varchar(45) as first_name,
    last_name::varchar(45) as last_name,
    email::varchar(50) as email,
    address_id::integer as address_id,
    activebool::boolean as activebool,
    create_date::date as create_date,
    last_update::timestamp_ntz as last_update,
    active::integer as active
from source
