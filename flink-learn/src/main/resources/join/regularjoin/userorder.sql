CREATE TABLE user_order (
    order_id BIGINT,
    ts BIGINT,
    d_timestamp AS TO_TIMESTAMP_LTZ(ts,3)
    -- 注意：d_timestamp的值可以从原始数据中取，原始数据中没有的话也可以从kafka的元数据中取
    -- d_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
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