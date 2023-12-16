CREATE TABLE `mysqlcdc_source` (
  TRANS_DATE VARCHAR(100),
  PRD_CODE VARCHAR(100),
  op_type VARCHAR(100),
  TOTAL_AMOUNT VARCHAR(100),
  TURNOVER_AMOUNT VARCHAR(100),
  PRIMARY KEY (PRD_CODE) NOT ENFORCED
)
WITH
(
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'source_yzhou_test03',
    'table-name' = 'your-table-name',
    'scan.startup.mode' = 'initial', -- 或者使用 'latest-offset', 'timestamp' 等
    'server-time-zone' = 'Asia/Shanghai' -- 根据你的时区设置
);