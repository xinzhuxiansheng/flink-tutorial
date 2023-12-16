CREATE TABLE `mysql_sink` (
   TRANS_DATE VARCHAR(100),
   PRD_CODE VARCHAR(100),
   op_type VARCHAR(100),
   TOTAL_AMOUNT VARCHAR(100),
   TURNOVER_AMOUNT VARCHAR(100),
   PRIMARY KEY (PRD_CODE) NOT ENFORCED
)
WITH
(
    'connector' = 'jdbc',
    'table-name' = 'sink_yzhou_test04',
    'sink.parallelism' = '1',
    'url' = 'jdbc:mysql://localhost:3306/yzhou_test',
    'username' = 'root',
    'password' = '12345678'
);