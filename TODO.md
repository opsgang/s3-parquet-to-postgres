* de-dupe parquet files
    * move all the parquet files to the bucket_data
    * set up all buckets in localstack
    * change tests to use source files in bucket_data/
* lib.rs to re-export all modules as pub
* ci/cd
    * pr build runs cargo test -- --nocapture
    * release build packages binary
