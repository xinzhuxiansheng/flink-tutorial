CREATE TABLE `kafka_source` (
   `id` INT COMMENT '',
   `name` STRING COMMENT '',
   `address` STRING COMMENT '',
   `ext_field01` STRING COMMENT ''
)
WITH
(
    'properties.bootstrap.servers' = 'kafka:9092',
    'connector' = 'kafka',
    'format' = 'json',
    'topic' = 'yzhouJSONtp02',
    'properties.group.id' = 'yzhougid01',
    'scan.startup.mode' = 'latest-offset'
);