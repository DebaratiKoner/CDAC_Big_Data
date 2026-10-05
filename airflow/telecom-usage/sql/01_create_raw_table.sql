DROP TABLE IF EXISTS telecom_usage_raw;

CREATE TABLE telecom_usage_raw (
    usage_id STRING,
    customer_id STRING,
    plan STRING,
    call_minutes INT,
    sms_count INT,
    data_gb DOUBLE,
    usage_date DATE,
    circle STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES (
    "skip.header.line.count"="1"
);
