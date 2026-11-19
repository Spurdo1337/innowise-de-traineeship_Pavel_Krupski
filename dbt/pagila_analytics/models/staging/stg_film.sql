with source as (

    select * from {{ source('pagila', 'film') }}

)

select
    film_id::integer as film_id,
    title::varchar(255) as title,
    description::varchar as description,
    release_year::integer as release_year,
    language_id::integer as language_id,
    rental_duration::integer as rental_duration,
    rental_rate::number(4, 2) as rental_rate,
    length::integer as length,
    replacement_cost::number(5, 2) as replacement_cost,
    rating::varchar(10) as rating,
    last_update::timestamp_ntz as last_update
from source
