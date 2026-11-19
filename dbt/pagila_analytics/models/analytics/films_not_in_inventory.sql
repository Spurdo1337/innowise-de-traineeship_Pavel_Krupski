-- Stage 1, task 4: films that never appeared in inventory.

with film as (

    select * from {{ ref('dim_film') }}

),

inventory as (

    select distinct film_id from {{ ref('stg_inventory') }}

)

select
    film.title
from film
left join inventory
    on film.film_id = inventory.film_id
where inventory.film_id is null
order by film.title
