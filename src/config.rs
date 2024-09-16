use anyhow::Result;
use serde::Deserialize;
use serde_yml::from_reader;
use std::collections::HashMap;
use std::fs::File;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub db: DbConfig,
    pub s3: S3Config,
    pub parquet: ParquetConfig,
    pub parquet_to_db: Option<HashMap<String, Option<String>>>,
    pub work_lists: WorkListsConfig,
}

#[derive(Debug, Deserialize)]
pub struct DbConfig {
    pub table_name: String,
    pub conn_str: String,
}

#[derive(Debug, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub download_batch_size: usize,
    pub downloads_dir: String,
}

#[derive(Debug, Deserialize)]
pub struct ParquetConfig {
    pub desired_fields: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct WorkListsConfig {
    pub dir: String,
}

impl Config {
    pub fn from_yaml_file(filename: &str) -> Result<Self> {
        let file = File::open(filename)?;
        let config: Config = from_reader(file)?;
        // TODO: verify all elements non-empty (inc desired_fields list)
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use const_format::formatcp;
    use std::env;

    static TESTDATA_DIR: &str = formatcp!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "tests/testdata/unit-tests"
    );

    #[test]
    fn test_from_yaml_file_invalid_path() {
        let result = Config::from_yaml_file("non_existent_file.yml");
        assert!(
            result.is_err(),
            "Expected an error when reading a non-existent file."
        );
    }

    #[test]
    fn test_from_yaml_file_valid_yaml() -> Result<()> {
        let config_yml = format!("{}/{}", TESTDATA_DIR, "good.yml");
        let config: Config = Config::from_yaml_file(config_yml.as_str())?;

        // Verify the structure matches the expected values
        assert_eq!(
            config.db.conn_str,
            "host=127.0.0.1 password=postgres user=postgres dbname=warehouse"
        );
        assert_eq!(config.s3.bucket, "skilling-customer-io-s3-parquet-dumps");
        assert_eq!(config.s3.download_batch_size, 2);
        assert_eq!(config.s3.downloads_dir, "out");
        assert_eq!(
            config.parquet.desired_fields,
            vec!["delivery_id".to_string(), "body".to_string()]
        );
        assert_eq!(config.work_lists.dir, "work");

        Ok(())
    }

    #[test]
    fn test_from_yaml_file_missing_fields() -> Result<()> {
        let config_yml = format!("{}/{}", TESTDATA_DIR, "missing-fields.yml");
        let config: Result<Config> = Config::from_yaml_file(config_yml.as_str());

        // The deserialization should fail due to missing fields
        assert!(
            config.is_err(),
            "Expected an error due to missing required fields."
        );

        Ok(())
    }
}
