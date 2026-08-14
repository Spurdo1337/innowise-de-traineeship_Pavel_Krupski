# Dorm Rooms & Students Reporting Service

Loads `rooms.json` and `students.json` into a PostgreSQL database and
generates room reports (student count per room, youngest rooms on
average, largest age gap, mixed-sex rooms) using raw SQL. Each report
is written to its own JSON file.

## Requirements

- Docker and Docker Compose v2 (`docker compose ...`)

## Run

```bash
docker compose up --build
```

That's it — no other steps needed. This starts the database, waits
until it's healthy, then runs the app once against the sample files
already included in `./data`.

## Check the result

Four files appear on the host in `output/`:

```
output/rooms_with_student_count.json
output/top5_rooms_smallest_avg_age.json
output/top5_rooms_largest_age_diff.json
output/mixed_sex_rooms.json
```
