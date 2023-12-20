CREATE TABLE country_area(
     country STRING,
     area STRING
)
WITH(
 'connector' = 'jdbc',
 'driver' = 'com.mysql.cj.jdbc.Driver', -- mysql8.x使用这个driver class
 'url' = 'jdbc:mysql://localhost:3306/yzhou_test?serverTimezone=Asia/Shanghai', -- mysql8.x中需要指定时区
 'username' = 'root',
 'password' = '12345678',
 'table-name' = 'country_area',
 -- 通过lookup缓存可以减少Flink任务和数据库的请求次数，启用之后每个子任务中会保存一份缓存数据
 'lookup.cache.max-rows' = '100', -- 控制lookup缓存中最多存储的数据条数
 'lookup.cache.ttl' = '3600000', -- 控制lookup缓存中数据的生命周期(毫秒)，太大或者太小都不合适
 'lookup.max-retries' = '1' -- 查询数据库失败后重试的次数
 )