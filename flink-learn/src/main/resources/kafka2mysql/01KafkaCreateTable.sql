CREATE TABLE `kafka_source` (
   `TRANS_DATE` STRING COMMENT '',
   `PRD_CODE` STRING COMMENT '',
   `TOTAL_AMOUNT` STRING COMMENT '',
   `TURNOVER_AMOUNT` STRING COMMENT '',
   `op_type` STRING COMMENT ''
)
WITH
(
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'connector' = 'kafka',
    'format' = 'json',
    'topic' = 'yzhoutp01',
    'properties.group.id' = 'yzhougid01',
    'scan.startup.mode' = 'earliest-offset'
);