with source as (

    select * from {{ source('pagila', 'language') }}

)

select
    language_id::integer as language_id,
    name::varchar(20) as language_name,
    last_update::timestamp_ntz as last_update
from source
