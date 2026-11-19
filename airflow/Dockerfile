# Airflow runtime image for the SADA project.
#
# dbt lives in its own virtual environment (/opt/dbt) so its dependency tree
# cannot clash with the Airflow environment. The `dbt` entry point is linked
# into /usr/local/bin, which is what the Airflow DAGs call.

FROM apache/airflow:3.3.2-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-client \
        default-mysql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER root

COPY requirements-dbt.txt /requirements-dbt.txt
RUN python -m venv /opt/dbt \
    && /opt/dbt/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/dbt/bin/pip install --no-cache-dir -r /requirements-dbt.txt \
    && ln -s /opt/dbt/bin/dbt /usr/local/bin/dbt

USER airflow
