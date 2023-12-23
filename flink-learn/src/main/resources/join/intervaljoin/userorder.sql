CREATE TABLE user_order (
    order_id BIGINT NOT NULL ,
    -- 事件时间戳
    ts BIGINT,
    -- 转换事件时间时间戳为 TO_TIMESTAMP_LTZ(3) 类型
    d_timestamp AS TO_TIMESTAMP_LTZ(ts,3),
    user_id STRING,
    -- 通过 Watermark 定义事件时间，可以在这里指定允许的最大乱序时间
    -- 例如： WATERMARK FOR d_timestamp AS d_timestamp - INTERVAL '5' SECOND
    WATERMARK FOR d_timestamp AS d_timestamp
)
WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'localhost:9092',
    'topic' = 'user_order',
    'properties.group.id' = 'yzhougid01',
    'format' = 'json',
    -- 为了便于演示，在这使用 latest-offset, 每次启动都使用最新的数据
    -- 'scan.startup.mode' = 'group-offset',
    'scan.startup.mode' = 'latest-offset',
    -- 'properties.auto.offset.reset' = 'latest',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);