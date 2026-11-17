-- Очередь из Kafka-топика
CREATE TABLE IF NOT EXISTS dim_users_queue
(
    user_id UInt64,
    full_name String,
    email String,
    prosthesis_model String,
    __op String,
    __source_ts_ms UInt64,
    __deleted String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'crm.public.users',
    kafka_group_name = 'clickhouse_users',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- таблица справочника
CREATE TABLE IF NOT EXISTS dim_users
(
    user_id UInt64,
    full_name String,
    email String,
    prosthesis_model String,
    __op String,
    __source_ts_ms UInt64,
    __deleted String
) ENGINE = ReplacingMergeTree(__source_ts_ms)
ORDER BY user_id;

-- Перекачиваем сообщения из Kafka
CREATE MATERIALIZED VIEW IF NOT EXISTS dim_users_mv TO dim_users
AS SELECT
    user_id,
    full_name,
    email,
    prosthesis_model,
    __op,
    __source_ts_ms,
    __deleted
FROM dim_users_queue;

INSERT INTO dim_users (user_id, full_name, email, prosthesis_model, __op, __source_ts_ms, __deleted) VALUES
    (1, 'Prothetic One',   'prothetic1@example.com', 'BionicHand X1', 'r', 0, 'false'),
    (2, 'Prothetic Two',   'prothetic2@example.com', 'BionicHand X2', 'r', 0, 'false'),
    (3, 'Prothetic Three', 'prothetic3@example.com', 'BionicArm Pro', 'r', 0, 'false');
