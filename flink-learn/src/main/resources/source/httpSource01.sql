CREATE TABLE `http_source` (
    `id` INT COMMENT '',
    `name` STRING COMMENT '',
    `address` STRING COMMENT '',
    `ext_field01` STRING COMMENT ''
)
WITH
(
'connector' = 'http',
'http.url' = 'https://mock.apifox.com/m1/2474307-0-default/api/getUserInfo',
'http.method' = 'GET',
'read.streaming.enabled' = 'true',
'read.streaming.check-interval' = '10',
'format' = 'json'
);