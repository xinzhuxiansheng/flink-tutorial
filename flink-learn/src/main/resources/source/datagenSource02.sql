CREATE TABLE source_table (
    `bid` INT,
    `proctime` AS PROCTIME (),
    `nowtime` AS NOW(),
    `localtime01` AS LOCALTIMESTAMP
)
WITH(
    'connector' = 'datagen',
    'rows-per-second' = '3',
    'fields.bid.min' = '1',
    'fields.bid.max' = '10'
)