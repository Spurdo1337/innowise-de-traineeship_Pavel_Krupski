with film as (

    select * from {{ ref('stg_film') }}

),

language as (

    select * from {{ ref('stg_language') }}

),

film_category as (

    select * from {{ ref('stg_film_category') }}

),

category as (

    select * from {{ ref('stg_category') }}

)

select
    film.film_id,
    film.title,
    film.release_year,
    language.language_name,
    category.category_name as category,
    film.rating,
    film.rental_rate,
    film.rental_duration,
    film.length,
    film.replacement_cost
from film
left join language
    on film.language_id = language.language_id
left join film_category
    on film.film_id = film_category.film_id
left join category
    on film_category.category_id = category.category_id
