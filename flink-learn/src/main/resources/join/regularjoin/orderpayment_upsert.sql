CREATE TABLE order_payment (
    order_id BIGINT NOT NULL ,
    d_timestamp TIMESTAMP_LTZ(3),
    pay_money BIGINT,
    PRIMARY KEY(order_id) NOT ENFORCED
)
WITH (
    'connector' = 'upsert-kafka',
    'properties.bootstrap.servers' = 'localhost:9092',
    'topic' = 'order_payment',
    'key.format' = 'json',
    'value.format' = 'json'
);