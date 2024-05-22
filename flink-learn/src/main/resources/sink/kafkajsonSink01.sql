CREATE TABLE `kafka_sink` (
    `id` INT COMMENT '',
    `count` BIGINT NOT NULL COMMENT ''
)
WITH
(
    'properties.bootstrap.servers' = 'kafka:9092',
    'connector' = 'kafka',
    'format' = 'json',
    'topic' = 'yzhouJSONtp03'
);