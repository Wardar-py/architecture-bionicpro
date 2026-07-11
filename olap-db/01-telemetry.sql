-- Сырые данные телеметрии протезов (источник для витрины отчётности).
CREATE TABLE IF NOT EXISTS raw_telemetry
(
    timestamp        DateTime,
    user_id          UInt64,
    response_time_ms Float32,
    action_type      String,
    duration_ms      UInt32
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id);
