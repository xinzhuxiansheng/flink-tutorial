CREATE TABLE `kafka_source` (
    `id` INT COMMENT '',
    `name` STRING COMMENT '',
    `addressId` STRING COMMENT ''
)
WITH
(
'properties.bootstrap.servers' = 'kafka:9092',
'connector' = 'kafka',
'format' = 'json',
'topic' = 'yzhouJSONtp02',
'properties.group.id' = 'yzhougid02',
'scan.startup.mode' = 'latest-offset'
);