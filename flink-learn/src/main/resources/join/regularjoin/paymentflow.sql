CREATE TABLE payment_flow (
    order_id BIGINT NOT NULL ,
    pay_money BIGINT
)
WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'localhost:9092',
    'topic' = 'payment_flow',
    'format' = 'json',
    'properties.group.id' = 'yzhougid01',
    -- 为了便于演示，在这使用 latest-offset, 每次启动都使用最新的数据
    -- 'scan.startup.mode' = 'group-offset',
    'scan.startup.mode' = 'latest-offset',
    -- 'properties.auto.offset.reset' = 'latest',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);