CREATE TABLE source_table (
    productId BIGINT,
    income BIGINT
)
WITH(
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.productId.min' = '1',
    'fields.productId.max' = '2',
    'fields.income.min' = '1',
    'fields.income.max' = '2'
)