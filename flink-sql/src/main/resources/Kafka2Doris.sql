CREATE TABLE
    `kafka_source` (
                       `TRANS_DATE` STRING COMMENT '',
                       `PRD_CODE` STRING COMMENT '',
                       `TOTAL_AMOUNT` STRING COMMENT '',
                       `TURNOVER_AMOUNT` STRING COMMENT ''
)
WITH
(
'properties.bootstrap.servers' = 'localhost:9092',
'connector' = 'kafka',
'json.ignore-parse-errors' = 'false',
'format' = 'json',
'topic' = 'yzhoutp01',
'properties.group.id' = 'testGroup',
'scan.startup.mode' = 'earliest-offset',
'json.fail-on-missing-field' = 'false'
);

CREATE TABLE
    `doris_sink` (
                     `TRANS_DATE` STRING COMMENT '',
                     `PRD_CODE` STRING NOT NULL COMMENT '',
                     `TOTAL_AMOUNT` STRING COMMENT '',
                     `TURNOVER_AMOUNT` STRING COMMENT '',
                     PRIMARY KEY (PRD_CODE) NOT ENFORCED
)WITH (
 'connector' = 'doris',
 'fenodes' = 'xxxxxx:8030',
 'table.identifier' = 'xxx.yzhou_test01',
 'username' = 'root',
 'password' = '',
 'sink.max-retries' = '3',
 'sink.properties.format' = 'json',
 'sink.properties.strip_outer_array' = 'true',
 'sink.enable-delete'='true',
 'sink.label-prefix' = 'doris_label'
 );

INSERT INTO doris_sink SELECT * FROM kafka_source;