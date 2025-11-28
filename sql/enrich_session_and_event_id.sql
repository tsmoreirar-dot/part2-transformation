create or replace table enrich_session_and_event_id as (
WITH ordered AS (
    SELECT
        *,
        LAG(timestamp::TIMESTAMP) OVER (
            PARTITION BY client_id
            ORDER BY timestamp::TIMESTAMP
        ) AS prev_ts
    FROM raw_clean_data
),
session_flags AS (
    SELECT
        *,
        CASE
            WHEN prev_ts IS NULL THEN 1
            WHEN timestamp::TIMESTAMP - prev_ts > INTERVAL '15 minutes' THEN 1
            ELSE 0
        END AS new_session_flag
    FROM ordered
),
session_groups AS (
    SELECT
        *,
        SUM(new_session_flag) OVER (
            PARTITION BY client_id
            ORDER BY timestamp::TIMESTAMP
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_index
    FROM session_flags
)
SELECT
    uuid() as event_id,
    client_id,
    page_url,
    user_agent,
    timestamp,
    session_index,
    md5(client_id || '-' || session_index) AS session_id
FROM session_groups
--where client_id = '1740268999-k1PQxqt-fODp'
ORDER BY client_id, timestamp::TIMESTAMP asc, session_id asc)
