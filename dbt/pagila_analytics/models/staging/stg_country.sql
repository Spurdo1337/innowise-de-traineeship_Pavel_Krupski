with source as (

    select * from {{ source('pagila', 'country') }}

)

select
    country_id::integer as country_id,
    country::varchar(50) as country,
    last_update::timestamp_ntz as last_update
from source
