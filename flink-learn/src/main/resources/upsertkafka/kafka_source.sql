CREATE TABLE kafka_source(
     name STRING,
     age INT NOT NULL
)WITH
(
 'connector' = 'kafka',
 'topic' = 'dt001',
 'properties.bootstrap.servers' = 'kafka:9092',
 'properties.group.id' = 'gid-sql-1',
 'scan.startup.mode' = 'latest-offset',
 'format' = 'json',
 'json.fail-on-missing-field' = 'false',
 'json.ignore-parse-errors' = 'true'
 )