with source as (

    select * from {{ source('pagila', 'city') }}

)

select
    city_id::integer as city_id,
    city::varchar(50) as city,
    country_id::integer as country_id,
    last_update::timestamp_ntz as last_update
from source
