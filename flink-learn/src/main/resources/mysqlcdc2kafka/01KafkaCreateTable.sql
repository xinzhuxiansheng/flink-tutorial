CREATE TABLE `kafka_sink` (
   `TRANS_DATE` STRING COMMENT '',
   `PRD_CODE` STRING COMMENT '',
   `TOTAL_AMOUNT` STRING COMMENT '',
   `TURNOVER_AMOUNT` STRING COMMENT '',
   `op_type` STRING COMMENT ''
)
WITH
(
    'connector' = 'kafka',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'topic' = 'yzhoutp02',
    'value.format' = 'debezium-json'
);