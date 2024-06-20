CREATE TABLE `yzhou_test01` (
`id` INT NOT NULL COMMENT '',
`name` STRING NOT NULL COMMENT '',
`address` STRING COMMENT '',
`ext_field01` STRING COMMENT '',
PRIMARY KEY (id) NOT ENFORCED
)
WITH
(
'connector' = 'jdbc',
'table-name' = 'yzhou_test01',
'url' = 'jdbc:mysql://192.168.0.202:3306/yzhou_test',
'username' = 'root',
'password' = '123456',
'driver' = 'com.mysql.cj.jdbc.Driver'
);