-- Stage 1, task 5: top 3 actors by the number of "Children" films (ties kept).

with children_films as (

    select
        actor_id,
        first_name,
        last_name,
        film_id
    from {{ ref('int_film_actor_bridge') }}
    where category_name = 'Children'

),

actor_films as (

    select
        actor_id,
        first_name,
        last_name,
        count(distinct film_id) as movie_count
    from children_films
    group by actor_id, first_name, last_name

),

ranked as (

    select
        *,
        dense_rank() over (order by movie_count desc) as rnk
    from actor_films

)

select
    first_name,
    last_name,
    movie_count
from ranked
where rnk <= 3
order by movie_count desc, last_name, first_name
