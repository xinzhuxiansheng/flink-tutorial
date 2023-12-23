CREATE TABLE `kafka_source` (
    age INT NOT NULL,
    cnt BIGINT,
    PRIMARY KEY (age) NOT ENFORCED
)
WITH
(
    'connector' = 'upsert-kafka',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'format' = 'json',
    'topic' = 'chk_out_topic',
    'key,format' = 'json',
    'value.format' = 'json'
);