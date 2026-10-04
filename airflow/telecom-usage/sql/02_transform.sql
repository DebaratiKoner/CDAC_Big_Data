USE telecom_db;

DROP TABLE IF EXISTS telecom_usage_final;

CREATE TABLE telecom_usage_final (
    usage_id STRING,
    customer_id STRING,
    plan STRING,
    call_minutes INT,
    sms_count INT,
    data_gb DOUBLE,
    usage_date STRING,
    circle STRING,
    usage_category STRING
);

INSERT INTO TABLE telecom_usage_final
SELECT
    usage_id,
    customer_id,
    plan,
    call_minutes,
    sms_count,
    data_gb,
    usage_date,
    circle,
    CASE
        WHEN data_gb >= 10 THEN 'HIGH'
        ELSE 'NORMAL'
    END AS usage_category
FROM telecom_usage_raw;
