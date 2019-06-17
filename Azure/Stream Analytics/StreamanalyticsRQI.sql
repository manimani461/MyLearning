WITH 
T0 AS
(
    SELECT
        BlobName as blob_name,
        EventProcessedUtcTime as event_processed_utc_time,
        BlobLastModifiedUtcTime as blob_last_modified_utc_time,
        stat_name,
        cnt
    FROM
        [RQIApiConnectorStats]
),
T1 AS
(
    SELECT
        System.Timestamp AS time,
        SUM(CASE WHEN stat_name = 'session_blobs' THEN cnt ELSE NULL END) AS sessions
    FROM
        T0
    GROUP BY
        TumblingWindow(second, 1)
)
SELECT
    time,
    CASE WHEN sessions IS NULL THEN 0 ELSE sessions END AS sessions
INTO
    [LiveSessionPowerBI]
FROM
    T1;