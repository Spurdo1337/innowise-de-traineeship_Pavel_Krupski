with source as (

    select * from {{ source('pagila', 'address') }}

)

select
    address_id::integer as address_id,
    address::varchar(50) as address,
    address2::varchar(50) as address2,
    district::varchar(20) as district,
    city_id::integer as city_id,
    postal_code::varchar(10) as postal_code,
    phone::varchar(20) as phone,
    last_update::timestamp_ntz as last_update
from source
