CREATE TABLE IF NOT EXISTS mysql_test (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'jdbc',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'url' = 'jdbc:mysql://mysql:3306/imooc_test?useSSL=false&characterEncoding=utf-8&serverTimezone=Asia/Shanghai',
  'username' = 'root',
  'password' = '123456',
  'table-name' = 'mysql_test'
)