CREATE TABLE `mysql_sink` (
  `id` INT COMMENT '',
  `name` STRING COMMENT '',
  `address` STRING COMMENT '',
  `ext_field01` STRING COMMENT '',
   PRIMARY KEY (id) NOT ENFORCED
)
WITH
(
    'connector' = 'jdbc',
    'table-name' = 'sink_yzhou_test02',
    'sink.parallelism' = '1',
    'url' = 'jdbc:mysql://localhost:3306/yzhou_test',
    'username' = 'root',
    'password' = '12345678'
);