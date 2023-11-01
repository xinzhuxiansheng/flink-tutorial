create table cust_http_post_source(
  id int,
  name string
)WITH(
 'connector' = 'http',
 'http.url' = 'http://localhost:5000/flink/test/post/order',
 'http.mode' = 'post',
 'read.streaming.enabled' = 'true',
 'read.streaming.check-interval' = '10',
 'format' = 'json'
 );

select * from cust_http_post_source;