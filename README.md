# Airflow + MongoDB Reviews Pipeline

This project processes a CSV file with Apache Airflow and Pandas, then loads the processed data into MongoDB.

## Structure

```text
.
├── dags/
│   ├── process_reviews.py
│   └── load_reviews_to_mongo.py
├── data/
│   └── raw/tiktok_google_play_reviews.csv
├── queries/
│   └── mongo_queries.txt
├── scripts/
│   ├── bootstrap_airflow.sh
│   └── log_empty_file.sh
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Workflow Visualization
Here are the Graph views of the main processing DAGs:

![Data process DAG](airflow_dag_process_reviews.PNG)
![Data MongoDB loader DAG](airflow_dag_load_reviews_to_mongo.PNG)

## Run

```bash
docker compose up --build
```
Airflow UI: `http://localhost:8080`

## Which DAG to run first

Trigger `process_reviews` DAG

## MongoDB connection

Use this connection string in MongoDB Compass or `mongosh`:

```text
mongodb://airflow:airflow@localhost:27017/reviews_db?authSource=admin
```

Database: `reviews_db`
Collection: `google_play_reviews`

## MongoDB queries

All aggregation pipelines are stored in `queries/mongo_queries.txt`.
