-- Every rental date must exist in the calendar dimension.

select
    rental.rental_id,
    rental.rental_date
from {{ ref('fact_rental') }} as rental
left join {{ ref('dim_date') }} as calendar
    on rental.rental_date = calendar.date_day
where calendar.date_day is null
