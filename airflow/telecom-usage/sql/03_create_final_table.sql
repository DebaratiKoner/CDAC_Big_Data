DROP TABLE IF EXISTS telecom_usage_final;

CREATE TABLE telecom_usage_final
AS
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
