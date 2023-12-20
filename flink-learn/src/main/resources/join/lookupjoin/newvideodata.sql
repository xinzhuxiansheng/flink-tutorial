CREATE TABLE new_video_data(
   vid STRING,
   uid STRING,
   start_time BIGINT,
   area STRING
)
WITH(
 'connector' = 'kafka',
 'topic' = 'yzhoutp02',
 'properties.bootstrap.servers' = 'localhost:9092',
 'format' = 'json',
 'sink.partitioner' = 'default'
 )