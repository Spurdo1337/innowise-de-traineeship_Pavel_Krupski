-- Stage 1, task 3: category with the highest total revenue.

with revenue as (

    select * from {{ ref('fact_revenue') }}

),

film as (

    select * from {{ ref('dim_film') }}

)

select
    film.category,
    sum(revenue.amount) as total_spent
from revenue
inner join film
    on revenue.film_id = film.film_id
group by film.category
order by total_spent desc, film.category
limit 1
