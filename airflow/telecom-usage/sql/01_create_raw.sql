CREATE DATABASE IF NOT EXISTS telecom_db;

USE telecom_db;

DROP TABLE IF EXISTS telecom_usage_raw;

CREATE EXTERNAL TABLE telecom_usage_raw (
    usage_id STRING,
    customer_id STRING,
    plan STRING,
    call_minutes INT,
    sms_count INT,
    data_gb DOUBLE,
    usage_date STRING,
    circle STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/vboxuser/telecom_usage/raw';
