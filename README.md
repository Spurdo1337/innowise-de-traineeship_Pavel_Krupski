# Task 5 - Spark

## Project structure

- `db_init/` — Pagila PostgreSQL init scripts
- `jars/` — PostgreSQL JDBC driver jar, committed directly 
- `work/` — PySpark notebook and script
- `docker-compose.yml` — local environment

## Run

```bash
docker compose up -d
```

Open JupyterLab at `http://127.0.0.1:8888/lab?token=mysecrettoken`, then open `work/run_task_queries.ipynb` and run cell.

Results are written to `output/` as CSV files.

## Assumptions

- Query 7 (top category by rental hours in a group of cities) ranks categories once across all cities matching the filter together, not separately per city.
- Rental duration is computed only for rentals with a non-null `return_date`; rentals still outstanding are excluded.
