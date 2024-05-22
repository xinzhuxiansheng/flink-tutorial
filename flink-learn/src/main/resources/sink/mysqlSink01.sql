CREATE TABLE `mysql_sink` (
  bid INT,
  proctime TIMESTAMP(3),
  nowtime TIMESTAMP(3),
  localtime01 TIMESTAMP(3)
)
WITH
    (
    'connector' = 'jdbc',
    'table-name' = 'user_info_out',
    'sink.parallelism' = '1',
    'url' = 'jdbc:mysql://localhost:3306/yzhou_test',
    'username' = 'root',
    'password' = '12345678'
    );