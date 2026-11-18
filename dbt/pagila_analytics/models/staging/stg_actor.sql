with source as (

    select * from {{ source('pagila', 'actor') }}

)

select
    actor_id::integer as actor_id,
    first_name::varchar(45) as first_name,
    last_name::varchar(45) as last_name,
    last_update::timestamp_ntz as last_update
from source
