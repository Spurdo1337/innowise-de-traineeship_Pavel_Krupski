with source as (

    select * from {{ source('pagila', 'staff') }}

)

select
    staff_id::integer as staff_id,
    first_name::varchar(45) as first_name,
    last_name::varchar(45) as last_name,
    address_id::integer as address_id,
    email::varchar(50) as email,
    store_id::integer as store_id,
    active::boolean as active,
    username::varchar(16) as username,
    last_update::timestamp_ntz as last_update
from source
