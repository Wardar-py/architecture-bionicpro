CREATE TABLE IF NOT EXISTS report_telemetry_agg
(
    date                 Date,
    user_id              UInt64,
    total_actions        AggregateFunction(count),
    avg_response_state   AggregateFunction(avg, Float32),
    max_response_state   AggregateFunction(max, Float32),
    min_response_state   AggregateFunction(min, Float32),
    usage_duration_state AggregateFunction(sum, UInt32)
) ENGINE = AggregatingMergeTree()
ORDER BY (date, user_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS report_telemetry_mv TO report_telemetry_agg
AS SELECT
    toDate(timestamp) AS date,
    user_id,
    countState() AS total_actions,
    avgState(response_time_ms) AS avg_response_state,
    maxState(response_time_ms) AS max_response_state,
    minState(response_time_ms) AS min_response_state,
    sumState(if(action_type = 'move', duration_ms, 0)) AS usage_duration_state
FROM raw_telemetry
GROUP BY date, user_id;

CREATE VIEW IF NOT EXISTS report_customer_telemetry_cdc AS
SELECT
    a.date AS date,
    a.user_id AS user_id,
    u.full_name AS full_name,
    u.email AS email,
    u.prosthesis_model AS prosthesis_model,
    countMerge(a.total_actions) AS total_actions,
    avgMerge(a.avg_response_state) AS avg_response_time_ms,
    maxMerge(a.max_response_state) AS max_response_time_ms,
    minMerge(a.min_response_state) AS min_response_time_ms,
    sumMerge(a.usage_duration_state) / 1000 AS usage_duration_sec
FROM report_telemetry_agg AS a
INNER JOIN (
    SELECT
        user_id,
        argMax(full_name, __source_ts_ms) AS full_name,
        argMax(email, __source_ts_ms) AS email,
        argMax(prosthesis_model, __source_ts_ms) AS prosthesis_model,
        argMax(__deleted, __source_ts_ms) AS last_deleted
    FROM dim_users
    GROUP BY user_id
    HAVING last_deleted != 'true'
) AS u ON a.user_id = u.user_id
GROUP BY a.date, a.user_id, u.full_name, u.email, u.prosthesis_model;

INSERT INTO raw_telemetry (timestamp, user_id, response_time_ms, action_type, duration_ms)
SELECT
    now() - INTERVAL number DAY - INTERVAL (number % 24) HOUR,
    (number % 3) + 1,
    50 + (number % 40),
    if(number % 2 = 0, 'move', 'grip'),
    100 + (number % 500)
FROM numbers(200);
