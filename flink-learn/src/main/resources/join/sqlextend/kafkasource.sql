CREATE TABLE `kafka_source` (
    name STRING,
    age INT
)
WITH
(
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'connector' = 'kafka',
    'format' = 'json',
    'topic' = 'chk_in_topic',
    'properties.group.id' = 'yzhougid02',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'latest',
    'json.fail-on-missing-field' = 'false',
    'json.igonore-parse-errors' = 'true'
);