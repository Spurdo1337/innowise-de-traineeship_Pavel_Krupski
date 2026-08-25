-- Pagila SQL tasks
-- Database: Pagila (PostgreSQL)


\set ON_ERROR_STOP on

\o /output/results.txt

\qecho '================================================================'
\qecho 'Task 1: Number of movies per category (descending)'
\qecho '================================================================'
SELECT
    c.name AS category,
    COUNT(fc.film_id) AS movie_count
FROM category c
JOIN film_category fc ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY movie_count DESC;

\qecho ''
\qecho '================================================================'
\qecho 'Task 2: Top 10 actors by total rental count of their movies'
\qecho '================================================================'
SELECT
    a.first_name,
    a.last_name,
    COUNT(r.rental_id) AS rental_count
FROM actor a
JOIN film_actor fa ON fa.actor_id = a.actor_id
JOIN inventory i ON i.film_id = fa.film_id
JOIN rental r ON r.inventory_id = i.inventory_id
GROUP BY a.actor_id, a.first_name, a.last_name
ORDER BY rental_count DESC, a.last_name, a.first_name
LIMIT 10;

\qecho ''
\qecho '================================================================'
\qecho 'Task 3: Category with the highest total amount spent'
\qecho '================================================================'
SELECT
    c.name AS category,
    SUM(p.amount) AS total_spent
FROM payment p
JOIN rental r ON r.rental_id = p.rental_id
JOIN inventory i ON i.inventory_id = r.inventory_id
JOIN film_category fc ON fc.film_id = i.film_id
JOIN category c ON c.category_id = fc.category_id
GROUP BY c.name
ORDER BY total_spent DESC, c.name
LIMIT 1;

\qecho ''
\qecho '================================================================'
\qecho 'Task 4: Movies not present in inventory (no IN operator)'
\qecho '================================================================'
SELECT
    f.title
FROM film f
WHERE NOT EXISTS (
    SELECT 1
    FROM inventory i
    WHERE i.film_id = f.film_id
)
ORDER BY f.title;

\qecho ''
\qecho '================================================================'
\qecho 'Task 5: Top 3 actors by appearances in the "Children" category'
\qecho '         (ties included)'
\qecho '================================================================'
WITH actor_children AS (
    SELECT
        a.actor_id,
        a.first_name,
        a.last_name,
        COUNT(DISTINCT fa.film_id) AS movie_count
    FROM actor a
    JOIN film_actor fa ON fa.actor_id = a.actor_id
    JOIN film_category fc ON fc.film_id = fa.film_id
    JOIN category c ON c.category_id = fc.category_id
    WHERE c.name = 'Children'
    GROUP BY a.actor_id, a.first_name, a.last_name
),
ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY movie_count DESC) AS rnk
    FROM actor_children
)
SELECT
    first_name,
    last_name,
    movie_count
FROM ranked
WHERE rnk <= 3
ORDER BY movie_count DESC, last_name, first_name;

\qecho ''
\qecho '================================================================'
\qecho 'Task 6: Cities by active/inactive customer count'
\qecho '         (sorted by inactive customers, descending)'
\qecho '================================================================'
SELECT
    ci.city,
    SUM(CASE WHEN cu.active = 1 THEN 1 ELSE 0 END) AS active_customers,
    SUM(CASE WHEN cu.active = 0 THEN 1 ELSE 0 END) AS inactive_customers
FROM city ci
LEFT JOIN address a ON a.city_id = ci.city_id
LEFT JOIN customer cu ON cu.address_id = a.address_id
GROUP BY ci.city_id, ci.city
ORDER BY inactive_customers DESC, ci.city;

\qecho ''
\qecho '================================================================'
\qecho 'Task 7: Category with highest total rental hours, per city,'
\qecho '         for cities starting with "a" and cities containing "-"'
\qecho '================================================================'
WITH city_groups AS (
    SELECT 'starts_with_a' AS city_group, ci.city_id, ci.city
    FROM city ci
    WHERE ci.city ILIKE 'a%'

    UNION ALL

    SELECT 'has_hyphen' AS city_group, ci.city_id, ci.city
    FROM city ci
    WHERE ci.city LIKE '%-%'
),
category_totals AS (
    SELECT
        cg.city_group,
        cg.city,
        c.name AS category,
        -- actual rented hours per rental = return_date - rental_date,
        -- NOT film.rental_duration (that's a per-film "days allowed" constant,
        -- unrelated to how long a specific rental actually lasted)
        SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600) AS total_rental_hours
    FROM city_groups cg
    JOIN address a ON a.city_id = cg.city_id
    JOIN customer cu ON cu.address_id = a.address_id
    JOIN rental r ON r.customer_id = cu.customer_id
    JOIN inventory i ON i.inventory_id = r.inventory_id
    JOIN film f ON f.film_id = i.film_id
    JOIN film_category fc ON fc.film_id = f.film_id
    JOIN category c ON c.category_id = fc.category_id
    WHERE r.return_date IS NOT NULL  -- skip rentals not yet returned
    GROUP BY cg.city_group, cg.city, c.name
),
ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY city_group, city
            ORDER BY total_rental_hours DESC
        ) AS rnk
    FROM category_totals
)
SELECT
    city_group,
    city,
    category,
    ROUND(total_rental_hours, 2) AS total_rental_hours
FROM ranked
WHERE rnk = 1
ORDER BY city_group, city, category;

\o
