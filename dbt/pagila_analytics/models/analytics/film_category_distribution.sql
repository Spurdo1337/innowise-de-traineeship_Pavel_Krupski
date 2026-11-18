-- Stage 1, task 1: number of films per category.

with film as (

    select * from {{ ref('dim_film') }}

)

select
    category,
    count(*) as movie_count
from film
group by category
order by movie_count desc
