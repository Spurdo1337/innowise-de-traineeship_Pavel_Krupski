-- Stage 1, task 2: top 10 actors by the number of rentals of their films.

with rental as (

    select * from {{ ref('fact_rental') }}

),

film_actor as (

    select * from {{ ref('int_film_actor_bridge') }}

)

select
    film_actor.first_name,
    film_actor.last_name,
    count(*) as rental_count
from rental
inner join film_actor
    on rental.film_id = film_actor.film_id
group by film_actor.actor_id, film_actor.first_name, film_actor.last_name
order by rental_count desc, film_actor.last_name, film_actor.first_name
limit 10
