with rental as (

    select * from {{ ref('stg_rental') }}

),

inventory as (

    select * from {{ ref('stg_inventory') }}

),

film as (

    select * from {{ ref('stg_film') }}

),

customer as (

    select * from {{ ref('stg_customer') }}

),

staff as (

    select * from {{ ref('stg_staff') }}

)

select
    rental.rental_id,
    rental.rental_date,
    rental.return_date,
    rental.rental_date::date as rental_date_day,
    rental.return_date::date as return_date_day,
    inventory.film_id,
    inventory.store_id,
    rental.customer_id,
    rental.staff_id,
    film.title as film_title,
    film.rental_duration as allowed_rental_days,
    datediff('day', rental.rental_date, rental.return_date) as days_rented,
    datediff('second', rental.rental_date, rental.return_date) / 3600.0 as hours_rented,
    (rental.return_date is not null) as is_returned
from rental
inner join inventory
    on rental.inventory_id = inventory.inventory_id
inner join film
    on inventory.film_id = film.film_id
inner join customer
    on rental.customer_id = customer.customer_id
left join staff
    on rental.staff_id = staff.staff_id
