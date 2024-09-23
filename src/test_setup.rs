// This sets up locally running s3 and db to simplify our testing.
// We need to bring it into the runtime in main.rs but we can minimise
// its impact as its only needed during testing.
//
// Yes, using docker evidences that several of these "unit-tests" have external dependencies
// Still, this approach is simpler than the amount of code changes needed by mocking.
// I have faith in the versimilitude of localstack and docker postgres, both well-supported projects.
// Besides, I'm more interested in whether the thing works end-to-end, than in unit-testing.

#[cfg(test)]
pub mod tests {
    use anyhow::{bail, Result};
    use assert_fs::{fixture::TempDir, prelude::*};
    use const_format::formatcp;
    use csv::WriterBuilder;
    use lazy_static::lazy_static;
    use log::debug;
    use once_cell::sync::Lazy;
    use parquet::file::reader::SerializedFileReader;
    use std::collections::HashMap;
    use std::env;
    use std::fs::File;
    use std::io::Cursor;
    use std::path::Path;
    use std::process::Command;
    use tokio::fs;
    use tokio::io::AsyncReadExt;
    use tokio_postgres::Client;

    pub const GOOD_DB_CONN_STR: &str =
        "host=127.0.0.1 user=postgres password=postgres dbname=testing";

    pub const CARS_COLS_FOR_CREATE: &str = r#"
                model VARCHAR (255),
                miles_per_gallon FLOAT8,
                num_of_cyl INT,
                disp FLOAT8,
                hp INT,
                drat FLOAT8,
                wt FLOAT8,
                qsec FLOAT8,
                vs SMALLINT,
                am SMALLINT,
                gear INT,
                carb SMALLINT
    "#;

    pub const CUSTOMER_ORDER_COLS_FOR_CREATE: &str = r#"
                id BIGINT NOT NULL,
                customer_name VARCHAR (255),
                description VARCHAR (255),
                some_unsigned_float DOUBLE PRECISION,
                some_positive_int BIGINT,
                some_fraction FLOAT8
    "#;

    // customer order_00.parquet has rows with null some_fraction
    // so NOT NULL constraint on that col will cause failure
    pub const CUSTOMER_ORDER_VIOLATED_CONSTRAINT_COLS_FOR_CREATE: &str = r#"
                id BIGINT,
                customer_name VARCHAR (255),
                description VARCHAR (255),
                some_unsigned_float DOUBLE PRECISION,
                some_positive_int BIGINT,
                some_fraction FLOAT8 NOT NULL
    "#;

    pub const IRIS_COLS_FOR_CREATE: &str = r#"
                "sepal.length" FLOAT8,
                "sepal.width" FLOAT8,
                "petal.length" FLOAT8,
                "petal.width" FLOAT8,
                variety VARCHAR(255)
    "#;

    static PARQUET_SRC_DIR: &str = formatcp!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "tests/testdata/unit-tests/parquet_ops"
    );

    pub static LOCALSTACK_PARQUET_DIR_CUSTOMERS: &str = formatcp!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "local/localstack/bucket_data/customer-orders-parquet"
    );

    pub static LOCALSTACK_PARQUET_DIR_DELIVERIES: &str = formatcp!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "local/localstack/bucket_data/deliveries-parquet"
    );

    lazy_static! {
        static ref COLS_FOR_CREATE: HashMap<&'static str, &'static str> = {
            let mut m = HashMap::new();
            m.insert("car", CARS_COLS_FOR_CREATE);
            m.insert("customer_order", CUSTOMER_ORDER_COLS_FOR_CREATE);
            m.insert(
                "customer_order_violated_constraint",
                CUSTOMER_ORDER_VIOLATED_CONSTRAINT_COLS_FOR_CREATE,
            );
            m.insert("iris", IRIS_COLS_FOR_CREATE);
            m
        };
    }

    pub fn render_tmpl_str(template: &str, values: Vec<&str>) -> String {
        let mut result = template.to_string(); // Start with the template as a String
        let placeholder_iter = values.iter(); // Create an iterator over the vector

        // Replace each occurrence of "{}" with the next value from the iterator
        for value in placeholder_iter {
            if let Some(start) = result.find("{}") {
                result.replace_range(start..start + 2, value); // Replace the first "{}"
            } else {
                break; // No more placeholders to replace
            }
        }

        result
    }

    #[allow(dead_code)]
    pub fn print_files_recursively(dir: &Path) -> Result<()> {
        use std::fs;
        // Iterate over the directory entries
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            // If the entry is a directory, recursively call this function
            if path.is_dir() {
                print_files_recursively(&path)?;
            } else {
                // Print the file path
                debug!("{}", path.display());
            }
        }
        Ok(())
    }

    pub async fn create_table_return_client(
        table_name: String,
        schema_type: &str,
    ) -> Result<Client> {
        let (client, connection) =
            tokio_postgres::connect(GOOD_DB_CONN_STR, tokio_postgres::NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                let msg = format!("db connection error: {}", e);
                bail!("{}", msg);
            }
            Ok(())
        });

        let cols_for_create: &str = match COLS_FOR_CREATE.get(schema_type) {
            Some(cols_str) => cols_str,
            None => bail!(
                "schema type {} not found in hashmap COLS_FOR_CREATE",
                schema_type
            ),
        };

        client
            .batch_execute(
                format!(
                    "DROP TABLE IF EXISTS {}; CREATE TABLE {} ({});",
                    table_name.clone(),
                    table_name.clone(),
                    cols_for_create,
                )
                .as_str(),
            )
            .await
            .unwrap();
        Ok(client)
    }

    pub async fn parquet_cars_reader() -> Result<(TempDir, SerializedFileReader<File>)> {
        let tmp_dir = TempDir::new().unwrap();
        tmp_dir
            .copy_from(PARQUET_SRC_DIR, &["cars.parquet"])
            .unwrap();
        let parquet_file = format!("{}/cars.parquet", tmp_dir.path().display());
        let f = File::open(Path::new(parquet_file.as_str())).unwrap();
        let reader = SerializedFileReader::new(f).unwrap();

        Ok((tmp_dir, reader))
    }

    pub async fn parquet_iris_reader() -> Result<(TempDir, SerializedFileReader<File>)> {
        let tmp_dir = TempDir::new().unwrap();
        tmp_dir
            .copy_from(PARQUET_SRC_DIR, &["iris.parquet"])
            .unwrap();
        let parquet_file = format!("{}/iris.parquet", tmp_dir.path().display());
        let f = File::open(Path::new(parquet_file.as_str())).unwrap();
        let reader = SerializedFileReader::new(f).unwrap();

        Ok((tmp_dir, reader))
    }

    pub async fn get_rows_as_csv_string(client: &Client, sql: &str) -> Result<String> {
        let rows = client.query(sql, &[]).await.unwrap();

        let cols = rows
            .first()
            .map(|row| {
                row.columns()
                    .iter()
                    .map(|col| col.name().to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let mut wtr = WriterBuilder::new()
            .has_headers(true)
            .from_writer(Cursor::new(Vec::new()));

        wtr.write_record(&cols)?;
        for row in rows {
            let values: Vec<String> = (0..row.len())
                .map(|i| get_value_as_string(&row, i))
                .collect::<Result<Vec<String>, _>>()?;
            wtr.write_record(values)?;
        }

        let csv_string = String::from_utf8(wtr.into_inner()?.into_inner())?;

        Ok(csv_string)
    }

    // we use full type spec for row or compiler will think this is a parquet record row
    fn get_value_as_string(row: &tokio_postgres::Row, idx: usize) -> Result<String> {
        let column_type = row.columns()[idx].type_();
        let value_str = match *column_type {
            // Handle integers
            tokio_postgres::types::Type::INT2 => row
                .get::<_, Option<i16>>(idx)
                .map_or("".to_string(), |v| v.to_string()),
            tokio_postgres::types::Type::INT4 => row
                .get::<_, Option<i32>>(idx)
                .map_or("".to_string(), |v| v.to_string()),
            tokio_postgres::types::Type::INT8 => row
                .get::<_, Option<i64>>(idx)
                .map_or("".to_string(), |v| v.to_string()),

            // Handle floating point numbers
            tokio_postgres::types::Type::FLOAT4 => row
                .get::<_, Option<f32>>(idx)
                .map_or("".to_string(), |v| v.to_string()),
            tokio_postgres::types::Type::FLOAT8 => row
                .get::<_, Option<f64>>(idx)
                .map_or("".to_string(), |v| v.to_string()),

            // Handle string types
            tokio_postgres::types::Type::TEXT | tokio_postgres::types::Type::VARCHAR => row
                .get::<_, Option<String>>(idx)
                .map_or("".to_string(), |v| v),

            // Handle other types as needed
            tokio_postgres::types::Type::BOOL => row
                .get::<_, Option<bool>>(idx)
                .map_or("".to_string(), |v| v.to_string()),

            // Fallback for other types
            _ => "".to_string(),
        };

        Ok(value_str)
    }

    pub fn set_good_aws_vars() {
        unset_aws_env_vars(); // remove any inherited vars
        env::set_var("AWS_ACCESS_KEY_ID", "test");
        env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        env::set_var("AWS_DEFAULT_REGION", "us-west-1");
        env::set_var("AWS_ENDPOINT_URL", "http://127.0.0.1:4566");
    }

    pub fn unset_aws_env_vars() {
        let aws_keys: Vec<String> = env::vars()
            .filter(|(key, _)| key.starts_with("AWS_"))
            .map(|(key, _)| key)
            .collect();

        for key in aws_keys {
            env::remove_var(key);
        }
    }

    pub fn restore_env(original_env: HashMap<String, String>) {
        for (key, _) in env::vars() {
            env::remove_var(key);
        }

        for (key, value) in original_env {
            env::set_var(key, value);
        }
    }

    pub async fn get_downloaded_and_src_file_contents(
        src_file_path: String,
        downloaded_file_path: String,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut src_file = fs::File::open(src_file_path).await?;
        let mut src_file_contents = Vec::new();
        src_file.read_to_end(&mut src_file_contents).await?;

        // Read the contents of the second file asynchronously
        let mut downloaded_file = fs::File::open(downloaded_file_path).await?;
        let mut downloaded_file_contents = Vec::new();
        downloaded_file
            .read_to_end(&mut downloaded_file_contents)
            .await?;

        Ok((src_file_contents, downloaded_file_contents))
    }

    fn docker_exists() -> bool {
        Command::new("docker")
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn run_docker_compose_down() {
        let status = Command::new("docker")
            .args(["compose", "down", "--remove-orphans", "-v"])
            .status()
            .expect("Failed to run docker compose down");

        if !status.success() {
            panic!("docker compose down failed");
        }
    }

    fn run_docker_compose_up() {
        let status = Command::new("docker")
            .args(["compose", "up", "-d", "--wait"])
            .status()
            .expect("Failed to run docker compose up");

        if !status.success() {
            panic!("docker compose up failed");
        }
    }

    // Setup function that runs once before all tests
    pub fn setup_docker() {
        static DOCKER_SETUP: Lazy<()> = Lazy::new(|| {
            if !docker_exists() {
                panic!("Docker is not installed or not found in PATH");
            }

            // cd to docker-compose dir
            let cargo_manifest_dir =
                env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
            let local_dir = Path::new(&cargo_manifest_dir).join("local");

            let this_dir = env::current_dir().unwrap();
            env::set_current_dir(&local_dir).expect("Failed to change directory to local");

            run_docker_compose_down();
            run_docker_compose_up();

            env::set_current_dir(&this_dir).expect("Failed to change directory to original");
        });

        Lazy::force(&DOCKER_SETUP);
    }
}
