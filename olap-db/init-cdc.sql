-- Таблица для сырых данных из Kafka
CREATE TABLE IF NOT EXISTS dim_users_queue
(
    user_id UInt64,
    full_name String,
    email String,
    prosthesis_model String,
    __op String,   -- 'c' (create), 'u' (update), 'd' (delete)
    __source_ts DateTime
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'crm.public.users',
    kafka_group_name = 'clickhouse_users',
    kafka_format = 'JSONEachRow';

-- Материализованное представление для ReplacingMergeTree
CREATE TABLE IF NOT EXISTS dim_users
(
    user_id UInt64,
    full_name String,
    email String,
    prosthesis_model String,
    __op String,
    __source_ts DateTime
) ENGINE = ReplacingMergeTree(__source_ts)
ORDER BY user_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS dim_users_mv TO dim_users
AS SELECT
    user_id,
    full_name,
    email,
    prosthesis_model,
    __op,
    __source_ts
FROM dim_users_queue;