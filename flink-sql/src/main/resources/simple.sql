-- 内容查询基于 FlinkTable 进行查询，请先为将要查询的表创建对应 FlinkTable。
-- 您可以通过点击左侧数据目录中的表生成 FlinkTable 建表语句，或直接在 SQL IDE 中输入创建语句。
CREATE TABLE `yzhou_sink_yzhou_test01` (
  `id` INT NOT NULL COMMENT '',
  `name` STRING NOT NULL COMMENT '',
  `address` STRING COMMENT '',
  PRIMARY KEY (id) NOT ENFORCED
)
WITH
    (
    'password' = '123456',
    'connector' = 'jdbc',
    'table-name' = 'yzhou_test01',
    'sink.parallelism' = '1',
    'url' = 'jdbc:mysql://xxx.xxx.xxx.xxx:3306/xx_test',
    'username' = 'xx_test'
    );

select * from yzhou_sink_yzhou_test01;

