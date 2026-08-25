FROM postgres:16

ENV POSTGRES_DB=pagila
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres

# docker-entrypoint-initdb.d runs *.sql files in alphabetical order
# on first container start (empty data directory only).
COPY db/01-schema.sql /docker-entrypoint-initdb.d/01-schema.sql
COPY db/02-data.sql   /docker-entrypoint-initdb.d/02-data.sql
COPY pagila_queries_with_descriptions.sql /docker-entrypoint-initdb.d/03-queries.sql

# Fallback target for \o /output/results.txt if the container is run
# without a mounted volume (report just won't persist in that case).
RUN mkdir -p /output
