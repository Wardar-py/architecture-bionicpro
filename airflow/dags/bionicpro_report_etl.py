from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
from airflow.operators.python import PythonOperator
import logging

default_args = {
    'owner': 'bionicpro',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bionicpro_report_etl',
    default_args=default_args,
    description='ETL для витрины отчётов протезов',
    schedule_interval='0 3 * * *',  # ежедневно в 3:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=['bionicpro'],
)

def extract_crm_users(**context):
    """Извлекает список пользователей из CRM PostgreSQL."""
    pg_hook = PostgresHook(postgres_conn_id='crm_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, full_name, email, prosthesis_model FROM users;")
    rows = cursor.fetchall()
    context['ti'].xcom_push(key='crm_users', value=rows)
    cursor.close()
    conn.close()

def load_crm_users_to_clickhouse(**context):
    """Загружает справочник пользователей в ClickHouse (dim_users)."""
    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    users = context['ti'].xcom_pull(key='crm_users')
    if not users:
        return
    # Создаём таблицу dim_users, если её нет
    ch_hook.run("""
    CREATE TABLE IF NOT EXISTS dim_users (
        user_id UInt64,
        full_name String,
        email String,
        prosthesis_model String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY user_id;
    """)
    # Очищаем и вставляем (для простоты полная перезагрузка раз в день)
    ch_hook.run("TRUNCATE TABLE dim_users;")
    ch_hook.run("INSERT INTO dim_users (user_id, full_name, email, prosthesis_model) VALUES",
                parameters=users)

def aggregate_telemetry_3(**context):
    """Агрегирует сырую телеметрию и объединяет с dim_users, формируя витрину."""
    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    # Создаём витрину, если её нет
    ch_hook.run("""
    CREATE TABLE IF NOT EXISTS report_customer_telemetry (
        date Date,
        user_id UInt64,
        full_name String,
        email String,
        prosthesis_model String,
        total_actions UInt32,
        avg_response_time_ms Float32,
        max_response_time_ms Float32,
        min_response_time_ms Float32,
        usage_duration_sec UInt32
    ) ENGINE = SummingMergeTree()
    ORDER BY (date, user_id);
    """)
    # Агрегация за предыдущий день
    yesterday = "{{ yesterday_ds }}"
    ch_hook.run(f"""
    INSERT INTO report_customer_telemetry
    SELECT
        toDate(timestamp) AS date,
        t.user_id,
        u.full_name,
        u.email,
        u.prosthesis_model,
        count() AS total_actions,
        avg(response_time_ms) AS avg_response_time_ms,
        max(response_time_ms) AS max_response_time_ms,
        min(response_time_ms) AS min_response_time_ms,
        sum(if(action_type = 'move', duration_ms, 0)) / 1000 AS usage_duration_sec
    FROM raw_telemetry t
    LEFT JOIN dim_users u ON t.user_id = u.user_id
    WHERE toDate(timestamp) = '{yesterday}'
    GROUP BY date, t.user_id, u.full_name, u.email, u.prosthesis_model;
    """)
def aggregate_telemetry(**context):
    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    ch_hook.run("""
    INSERT INTO report_customer_telemetry
    SELECT
        toDate(timestamp) AS date,
        t.user_id,
        u.full_name,
        u.email,
        u.prosthesis_model,
        count() AS total_actions,
        avg(response_time_ms) AS avg_response_time_ms,
        max(response_time_ms) AS max_response_time_ms,
        min(response_time_ms) AS min_response_time_ms,
        sum(if(action_type = 'move', duration_ms, 0)) / 1000 AS usage_duration_sec
    FROM raw_telemetry t
    LEFT JOIN dim_users u ON t.user_id = u.user_id
    WHERE toDate(timestamp) = '{yesterday}'
    GROUP BY date, t.user_id, u.full_name, u.email, u.prosthesis_model;
    """)

extract_task = PythonOperator(task_id='extract_crm_users', python_callable=extract_crm_users, dag=dag)
load_task = PythonOperator(task_id='load_crm_to_clickhouse', python_callable=load_crm_users_to_clickhouse, dag=dag)
aggregate_task = PythonOperator(task_id='aggregate_telemetry', python_callable=aggregate_telemetry, dag=dag)



extract_task >> load_task >> aggregate_task