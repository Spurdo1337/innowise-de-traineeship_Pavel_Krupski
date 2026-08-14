FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sql ./sql
COPY src ./src

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

# Default command: process the sample files bundled in ./data.
# Override at run time, e.g.:
#   docker compose run --rm app python src/main.py --students /data/students.json --rooms /data/rooms.json
CMD ["python", "src/main.py", "--students", "/data/students.json", "--rooms", "/data/rooms.json"]
