with source as (

    select * from {{ source('pagila', 'category') }}

)

select
    category_id::integer as category_id,
    name::varchar(25) as category_name,
    last_update::timestamp_ntz as last_update
from source
