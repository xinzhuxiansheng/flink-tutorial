CREATE TABLE sink_table (
    `id` INT COMMENT '',
    `name` STRING COMMENT '',
    `addressId` STRING COMMENT '',
    `province` STRING COMMENT '',
    `city` STRING COMMENT ''
) WITH
(
  'connector' = 'print'
  );