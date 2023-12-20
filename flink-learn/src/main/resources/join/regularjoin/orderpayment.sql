CREATE TABLE order_payment (
    order_id BIGINT NOT NULL ,
    d_timestamp TIMESTAMP_LTZ(3),
    pay_money BIGINT
)
WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'localhost:9092',
    'topic' = 'order_payment',
    'format' = 'json',
    'sink.partitioner' = 'default'
);