CREATE TABLE video_data(
   vid STRING,
   uid STRING,
   start_time BIGINT,
   country STRING,
   proc_time AS PROCTIME() -- 处理时间
)
WITH(
 'connector' = 'kafka',
 'topic' = 'yzhoutp01',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'gid-sql-video',
 'scan.startup.mode' = 'latest-offset',
 'format' = 'json',
 'json.fail-on-missing-field' = 'false',
 'json.ignore-parse-errors' = 'true'
 )