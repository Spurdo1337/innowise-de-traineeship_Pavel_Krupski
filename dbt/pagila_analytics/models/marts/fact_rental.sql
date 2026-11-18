with rental as (

    select * from {{ ref('int_rental_facts') }}

)

select
    rental_id,
    rental_date_day as rental_date,
    return_date_day as return_date,
    rental_date as rental_at,
    return_date as return_at,
    film_id,
    customer_id,
    store_id,
    staff_id,
    days_rented,
    hours_rented,
    is_returned,
    1 as rental_count
from rental
