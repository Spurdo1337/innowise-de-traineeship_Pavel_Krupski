-- Stage 1, task 7: category with the highest total rental hours per city,
-- for cities that start with "a" or contain a hyphen.

with city_groups as (

    select 'starts_with_a' as city_group, city_id, city
    from {{ ref('stg_city') }}
    where city ilike 'a%'

    union all

    select 'has_hyphen' as city_group, city_id, city
    from {{ ref('stg_city') }}
    where city like '%-%'

),

rental as (

    select
        rental.customer_id,
        film.category,
        rental.hours_rented,
        rental.is_returned
    from {{ ref('fact_rental') }} as rental
    inner join {{ ref('dim_film') }} as film
        on rental.film_id = film.film_id

),

city_rentals as (

    select
        city_groups.city_group,
        city_groups.city,
        rental.category,
        sum(rental.hours_rented) as total_rental_hours
    from city_groups
    inner join {{ ref('dim_customer') }} as customer
        on city_groups.city_id = customer.city_id
    inner join rental
        on customer.customer_id = rental.customer_id
    where rental.is_returned
    group by city_groups.city_group, city_groups.city, rental.category

),

ranked as (

    select
        *,
        dense_rank() over (
            partition by city_group, city
            order by total_rental_hours desc
        ) as rnk
    from city_rentals

)

select
    city_group,
    city,
    category,
    round(total_rental_hours, 2) as total_rental_hours
from ranked
where rnk = 1
order by city_group, city, category
