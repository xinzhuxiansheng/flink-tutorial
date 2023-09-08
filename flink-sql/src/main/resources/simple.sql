-- 内容查询基于 FlinkTable 进行查询，请先为将要查询的表创建对应 FlinkTable。
-- 您可以通过点击左侧数据目录中的表生成 FlinkTable 建表语句，或直接在 SQL IDE 中输入创建语句。
-- CREATE TABLE `yzhou_sink_yzhou_test01`
-- (
--     `id`      INT    NOT NULL COMMENT '',
--     `name`    STRING NOT NULL COMMENT '',
--     `address` STRING COMMENT '',
--     PRIMARY KEY (id) NOT ENFORCED
-- )
--     WITH
--         (
--         'password' = '12345678',
--         'connector' = 'jdbc',
--         'table-name' = 'users',
--         'sink.parallelism' = '1',
--         'url' = 'jdbc:mysql://127.0.0.1:3306/yzhou_test?useSSL=false',
--         'username' = 'root'
--         );
--
-- select * from yzhou_sink_yzhou_test01;

-- 内容查询基于 FlinkTable 进行查询，请先为将要查询的表创建对应 FlinkTable。
-- 您可以通过点击左侧数据目录中的表生成 FlinkTable 建表语句，或直接在 SQL IDE 中输入创建语句。
CREATE TABLE `yzhou_sink_yzhou_test01` (
   `id` BIGINT NOT NULL COMMENT '',
   `name` STRING NOT NULL COMMENT '',
   PRIMARY KEY (id) NOT ENFORCED
)
    WITH
        (
        'password' = '12345678',
        'connector' = 'jdbc',
        'table-name' = 'users',
        'sink.parallelism' = '1',
        'url' = 'jdbc:mysql://127.0.0.1:3306/yzhou_test?useSSL=false',
        'username' = 'root'
        );

select * from yzhou_sink_yzhou_test01;