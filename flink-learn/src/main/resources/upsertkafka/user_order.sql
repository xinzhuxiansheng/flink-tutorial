CREATE TABLE user_order(
                           order_id BIGINT NOT NULL,
                           ts BIGINT,
                           d_timestamp AS TO_TIMESTAMP_LTZ(ts,3)
    -- 注意：d_timestamp的值可以从原始数据中取，原始数据中没有的话也可以从Kafka的元数据中取
    -- d_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
)WITH(
     'connector' = 'kafka',
     'topic' = 'user_order',
     'properties.bootstrap.servers' = 'kafka:9092',
     'properties.group.id' = 'gid-sql-order',
     -- 为了便于演示，在这使用latest-offset，每次启动都使用最新的数据
     'scan.startup.mode' = 'latest-offset',
     'format' = 'json',
     'json.fail-on-missing-field' = 'false',
     'json.ignore-parse-errors' = 'true'
     )