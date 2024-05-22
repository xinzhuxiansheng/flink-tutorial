CREATE TABLE sink_table (
    `id` INT COMMENT '',
    `name` STRING COMMENT '',
    `addressId` STRING COMMENT '',
    `ext_field01` STRING COMMENT ''
) WITH
(
  'connector' = 'print'
  );