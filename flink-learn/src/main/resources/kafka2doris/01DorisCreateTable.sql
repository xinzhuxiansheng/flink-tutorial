CREATE TABLE `doris_sink` (
     `TRANS_DATE` STRING COMMENT '',
     `PRD_CODE` STRING NOT NULL COMMENT '',
     `TOTAL_AMOUNT` STRING COMMENT '',
     `TURNOVER_AMOUNT` STRING COMMENT '',
     `op_type` STRING COMMENT '',
     PRIMARY KEY (PRD_CODE) NOT ENFORCED
)WITH (
    'connector' = 'doris',
    'fenodes' = '192.168.0.201:8030',
    'table.identifier' = 'yzhou_test.yzhou_test01',
    'username' = 'root',
    'password' = '',
    'sink.max-retries' = '3',
    'sink.properties.format' = 'json',
    'sink.enable-delete'='true',
    'sink.label-prefix' = 'doris_label_yzhou_103',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.properties.customdelete_name' = 'op_type',
    'sink.properties.customdelete_value' = 'D'

);