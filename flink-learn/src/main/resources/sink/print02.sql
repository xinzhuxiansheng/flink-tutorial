CREATE TABLE sink_table (
    productId BIGINT,
    `all` BIGINT
) WITH
(
  'connector' = 'print'
  );