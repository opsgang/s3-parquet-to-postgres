Maybe create vec of funcs that operate on the RowAccessor to retrieve the value type
We should be able to figure out based on Physical and Converted what the rust val should be
e.g. if physical is f64

* handle timestamps, dates, bools, smallints etc
    * check with generated parquet if can feed types in or not.
        * in a test, create table with types we'd like it to handle
        * verify the input data via psql
* Specify in README limits of supported types i.e. not bytea
    * compare to current field types in https://docs.rs/parquet/latest/parquet/record/enum.Field.html
    * add mapping to readme of parquet -> rust -> postgres type
* lib.rs to re-export all modules as pub
* ci/cd
    * pr build runs cargo test -- --nocapture
    * release build packages binary
