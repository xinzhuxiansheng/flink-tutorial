CREATE TABLE payment_flow(
     order_id BIGINT  NOT NULL,
     pay_money BIGINT
)WITH
(
 'connector' = 'kafka',
 'topic' = 'payment_flow',
 'properties.bootstrap.servers' = 'kafka:9092',
 'properties.group.id' = 'gid-sql-payment',
 -- 为了便于演示，在这使用latest-offset，每次启动都使用最新的数据
 'scan.startup.mode' = 'latest-offset',
 'format' = 'json',
 'json.fail-on-missing-field' = 'false',
 'json.ignore-parse-errors' = 'true'
 )