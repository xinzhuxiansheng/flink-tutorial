CREATE TABLE `kafka_source` (
    `TRANS_DATE` STRING COMMENT '',
    `PRD_CODE` STRING COMMENT '',
    `TOTAL_AMOUNT` STRING COMMENT '',
    `TURNOVER_AMOUNT` STRING COMMENT '',
    `op_type` STRING COMMENT ''
)
WITH
(
    'connector' = 'kafka',
    'properties.bootstrap.servers' = '192.168.0.201:9092',
    'format' = 'json',
    'topic' = 'yzhoutp01',
    'properties.group.id' = 'yzhougid01',
    'scan.startup.mode' = 'earliest-offset'
);