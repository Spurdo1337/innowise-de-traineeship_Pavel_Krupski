with source as (

    select * from {{ source('pagila', 'film_actor') }}

)

select
    actor_id::integer as actor_id,
    film_id::integer as film_id,
    last_update::timestamp_ntz as last_update
from source
