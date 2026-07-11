from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from clickhouse_driver import Client


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
    schedule_interval='0 3 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['bionicpro'],
)

CLICKHOUSE_HOST = 'olap_db'
CLICKHOUSE_USER = 'demo'
CLICKHOUSE_PASSWORD = 'demo'
CLICKHOUSE_DATABASE = 'default'


def get_ch_client() -> Client:
    return Client(
        host=CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def ch_execute(query: str, params=None):
    client = get_ch_client()
    return client.execute(query, params or {})


def ensure_raw_telemetry(**context):
    ch_execute("""
    CREATE TABLE IF NOT EXISTS raw_telemetry (
        timestamp DateTime,
        user_id UInt64,
        response_time_ms Float32,
        action_type String,
        duration_ms UInt32
    ) ENGINE = MergeTree()
    ORDER BY (timestamp, user_id);
    """)
    row_count = ch_execute("SELECT count() FROM raw_telemetry")[0][0]
    if row_count == 0:
        ch_execute("""
        INSERT INTO raw_telemetry (timestamp, user_id, response_time_ms, action_type, duration_ms)
        SELECT
            now() - INTERVAL number DAY - INTERVAL (number % 24) HOUR,
            (number % 3) + 1,
            50 + (number % 40),
            if(number % 2 = 0, 'move', 'grip'),
            100 + (number % 500)
        FROM numbers(200);
        """)


def extract_crm_users(**context):
    pg_hook = PostgresHook(postgres_conn_id='crm_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, full_name, email, prosthesis_model FROM users;")
    rows = cursor.fetchall()
    context['ti'].xcom_push(key='crm_users', value=rows)
    cursor.close()
    conn.close()


def load_crm_users_to_clickhouse(**context):
    users = context['ti'].xcom_pull(key='crm_users')
    if not users:
        return

    ch_execute("""
    CREATE TABLE IF NOT EXISTS dim_users_etl (
        user_id UInt64,
        full_name String,
        email String,
        prosthesis_model String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY user_id;
    """)
    ch_execute("TRUNCATE TABLE dim_users_etl;")
    ch_execute(
        "INSERT INTO dim_users_etl (user_id, full_name, email, prosthesis_model) VALUES",
        users,
    )


def aggregate_telemetry(**context):
    yesterday = (context['data_interval_start'] - timedelta(days=1)).strftime('%Y-%m-%d')

    client = get_ch_client()
    client.execute("""
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

    client.execute(
        "ALTER TABLE report_customer_telemetry DELETE WHERE date = %(d)s",
        {'d': yesterday},
        settings={'mutations_sync': 1},
    )

    client.execute(f"""
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
    LEFT JOIN dim_users_etl u ON t.user_id = u.user_id
    WHERE toDate(timestamp) = '{yesterday}'
    GROUP BY date, t.user_id, u.full_name, u.email, u.prosthesis_model;
    """)


init_task = PythonOperator(
    task_id='ensure_raw_telemetry',
    python_callable=ensure_raw_telemetry,
    dag=dag,
)
extract_task = PythonOperator(
    task_id='extract_crm_users',
    python_callable=extract_crm_users,
    dag=dag,
)
load_task = PythonOperator(
    task_id='load_crm_to_clickhouse',
    python_callable=load_crm_users_to_clickhouse,
    dag=dag,
)
aggregate_task = PythonOperator(
    task_id='aggregate_telemetry',
    python_callable=aggregate_telemetry,
    dag=dag,
)

init_task >> extract_task >> load_task >> aggregate_task
