FROM apache/airflow:2.8.4-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.10.0 \
    psycopg2-binary==2.9.9 \
    clickhouse-driver==0.2.7
