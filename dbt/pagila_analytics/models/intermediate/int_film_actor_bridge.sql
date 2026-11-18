with film_actor as (

    select * from {{ ref('stg_film_actor') }}

),

actor as (

    select * from {{ ref('stg_actor') }}

),

film as (

    select * from {{ ref('stg_film') }}

),

film_category as (

    select * from {{ ref('stg_film_category') }}

),

category as (

    select * from {{ ref('stg_category') }}

)

select
    film_actor.film_id,
    film_actor.actor_id,
    actor.first_name,
    actor.last_name,
    film.title,
    category.category_name
from film_actor
inner join actor
    on film_actor.actor_id = actor.actor_id
inner join film
    on film_actor.film_id = film.film_id
left join film_category
    on film_actor.film_id = film_category.film_id
left join category
    on film_category.category_id = category.category_id
