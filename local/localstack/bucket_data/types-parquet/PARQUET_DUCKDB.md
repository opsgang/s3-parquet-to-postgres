/tmp/test.csv:

See also local/localstack/bucket_data/types-parquet/full.csv

```csv
my_date_field,my_boolean,my_timestamp_field,my_varchar_field,my_small_int
"24/09/2024","true","24/09/2024 10:15:30.012","this is my varchar","2"
"01/08/2024","false","02/08/2022 09:21:01.123","this is NOT my varchar","3"
```

Note that the timestamp field created is MICROS
TODO: try with other TIMESTAMP configurations
```sql
CREATE TABLE test_parquet (
  my_date_field DATE,
  my_boolean BOOLEAN,
  my_timestamp_field TIMESTAMP,
  my_varchar_field VARCHAR,
  my_small_int SMALLINT
) ;
COPY test_parquet FROM 'test.csv' WITH (timestampformat '%d/%m/%Y %H:%M:%S.%g');

SELECT * FROM test_parquet;

COPY (SELECT * from test_parquet) TO 'test.parquet' (FORMAT 'parquet');
.exit
```
