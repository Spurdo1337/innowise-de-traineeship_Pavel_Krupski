with source as (

    select * from {{ source('pagila', 'film_category') }}

)

select
    film_id::integer as film_id,
    category_id::integer as category_id,
    last_update::timestamp_ntz as last_update
from source
