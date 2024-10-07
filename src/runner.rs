use anyhow::Result;
use log::{debug, info};
use parquet::file::reader::FileReader;

// don't need crate::cmd_args, as only handles things for binary
use crate::config;
use crate::db;
use crate::parquet_ops;
use crate::s3_download;
use crate::work_lists;

fn cfg(cfg_file: &str) -> Result<config::Config> {
    let cfg: config::Config = config::Config::from_yaml_file(cfg_file)?;
    Ok(cfg)
}

async fn parquet_rows_to_db(
    downloaded_file: String,
    desired_fields: Vec<String>,
    db: &db::Db,
) -> Result<()> {
    let mut parquet = parquet_ops::Parquet::new(downloaded_file.clone(), desired_fields.clone())?;

    // block controls that parquet file (dowloaded_file) is guaranteed closed at end of this scope
    {
        let reader = parquet.file_reader()?;

        debug!("{}: ... finding desired columns positions", downloaded_file);
        let (parquet_col_nums, pq_type_data) = parquet.get_desired_cols(&reader)?;

        debug!("{}: ... reading parquet rows", downloaded_file);
        let row_iter: parquet::record::reader::RowIter = reader.get_row_iter(None)?;

        info!("{}: ... writing rows to db", downloaded_file);
        let num_rows_added = db
            .write_rows(row_iter, &parquet_col_nums, &pq_type_data)
            .await?;

        info!(
            "{}: {} rows added to db successfully",
            downloaded_file, num_rows_added
        );
    } // shouldn't be anything still keeping the downloaded file open now - free to delete
    Ok(())
}

pub async fn run(cfg_file: &str) -> Result<()> {
    info!("reading cfg file: [{}]", cfg_file);
    let cfg = cfg(cfg_file)?;

    // files to process
    let work_lists_dir: &str = cfg.work_lists.dir.as_str();

    // s3 downloads
    let batch_size: usize = cfg.s3.download_batch_size;
    let bucket_name = cfg.s3.bucket;
    let output_dir = cfg.s3.downloads_dir;

    // parquet
    let desired_fields: Vec<String> = cfg.parquet.desired_fields;

    // db
    let table_name: String = cfg.db.table_name;
    let conn_str: &str = cfg.db.conn_str.as_str();

    let parquet_to_db = cfg.parquet_to_db;
    info!("connecting to db");
    let db = db::Db::connect(
        conn_str,
        table_name.as_str(),
        desired_fields.clone(),
        parquet_to_db,
    )
    .await?;

    info!(
        "Will write fields {} to database table {}",
        desired_fields.join(", "),
        table_name
    );

    let mut work_lists = work_lists::WorkLists::new(work_lists_dir, batch_size)?;
    loop {
        let wip_list = work_lists.next_batch()?.wip_list.clone();

        if wip_list.is_empty() {
            break;
        }

        let map_ids_to_downloads =
            s3_download::get(bucket_name.clone(), wip_list.clone(), output_dir.clone()).await?;
        info!("... downloaded files:");
        for file_id in &wip_list {
            info!("\t{}", map_ids_to_downloads.get(file_id).unwrap());
        }
        // parquet filename has the output_dir
        for id in &wip_list {
            let downloaded_file = map_ids_to_downloads.get(id.as_str()).unwrap();
            info!("{}: handling downloaded parquet file", downloaded_file);

            parquet_rows_to_db(downloaded_file.to_string(), desired_fields.clone(), &db).await?;

            debug!("{}: will mark {} as completed", downloaded_file, id);
            work_lists.mark_completed(id.to_string())?;

            debug!("{}: deleting downloaded file", downloaded_file);
            s3_download::delete(downloaded_file.clone())?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use assert_fs::{fixture::TempDir, prelude::*};
    use const_format::formatcp;
    use once_cell::sync::Lazy;
    use std::collections::HashMap;
    use std::env;
    use std::path::Path;
    use tokio::sync::Mutex;
    use tokio_postgres::Client;

    use crate::test_setup::tests::{
        create_table_return_client, get_rows_as_csv_string, render_tmpl_str, restore_env,
        set_good_aws_vars, setup_docker,
    };

    static RUNNER_TESTDATA: &str = formatcp!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "tests/testdata/unit-tests/runner"
    );

    static LOCK_ENV_RUNNER_TESTS: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    // test with orders parquet files
    // yaml file should use relative paths (as will be in a temp dir)
    // create a temp dir and cd to it
    // test run.rs creates expected csv

    async fn runner_tests_setup(test_name: &str, table_columns: &str) -> Result<(TempDir, Client)> {
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir_path = format!("{}", tmp_dir.path().display());

        let src_dir = format!("{}/{}", RUNNER_TESTDATA, test_name);
        tmp_dir.copy_from(src_dir, &["*", "todo"])?;

        // setup_docker(); // do this before creating test table!
        // create expected db table
        let db_client = create_table_return_client(test_name.to_string(), table_columns).await?;

        set_good_aws_vars();
        env::set_current_dir(Path::new(&tmp_dir_path)).expect("Failed to cd to temp dir for test");

        Ok((tmp_dir, db_client))
    }

    #[tokio::test]
    async fn test_run_happy_path_customer_orders() -> Result<()> {
        let test_name = "test_run_happy_path_customer_orders";
        let _env_lock = LOCK_ENV_RUNNER_TESTS.lock().await;
        let original_env: HashMap<String, String> = env::vars().collect();

        let (tmp_dir, db_client) = runner_tests_setup(test_name, "customer_order").await?;

        // env_logger::init(); // uncomment for logs during cargo test -- --nocapture
        run("config.yml").await?;
        tmp_dir.close().unwrap(); // can be deleted as read what we need, and we'll verify in db
        restore_env(original_env);

        // VERIFY DB RESULTS
        let sql = format!("SELECT count(id) AS total from {}", test_name);
        let exp_string = "\
            total\n\
            60\n\
        ";
        let csv_string = get_rows_as_csv_string(&db_client, sql.as_str())
            .await
            .unwrap();
        assert_eq!(
            csv_string,
            exp_string.to_string(),
            "Expected {} rows inserted into the db.",
            "60",
        );

        let sql_tmpl = "\
            (SELECT * FROM {} ORDER BY id ASC LIMIT 2) \
            UNION ALL \
            (SELECT * FROM {} ORDER BY id DESC LIMIT 2) \
            ORDER BY id ASC; \
        ";

        let sql = render_tmpl_str(sql_tmpl, vec![test_name, test_name]);

        let exp_string = "\
            id,customer_name,description,some_unsigned_float,some_positive_int,some_fraction\n\
            1,,\"Eldon Base for stackable storage shelf, platinum\",-213.25,3,0.8\n\
            2,,\"1.7 Cubic Foot Compact \"\"Cube\"\" Office Refrigerators\",457.81,293,0.58\n\
            59,,Accessory4,-267.01,5925,0.85\n\
            60,,Personal Creations� Ink Jet Cards and Labels,3.63,6016,0.36\n\
        ";

        let csv_string = get_rows_as_csv_string(&db_client, sql.as_str())
            .await
            .unwrap();

        assert_eq!(
            csv_string,
            exp_string.to_string(),
            "Fetched the first two and last two rows from the DB, but didn't get expected results",
        );

        // VERIFY NULL VALUES INSERTED IN DB
        let sql = format!("SELECT * from {} WHERE some_fraction IS NULL", test_name);
        let exp_string = "\
            id,customer_name,description,some_unsigned_float,some_positive_int,some_fraction\n\
            8,,\"SAFCO Mobile Desk Side File, Wire Frame\",127.7,613,\n\
            9,,\"SAFCO Commercial Wire Shelving, Black\",-695.26,643,\n\
        ";
        let csv_string = get_rows_as_csv_string(&db_client, sql.as_str())
            .await
            .unwrap();
        assert_eq!(
            csv_string,
            exp_string.to_string(),
            "Expected rows with ids 8 and 9 returned where some_fraction is null.",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_run_nested_s3_paths() -> Result<()> {
        let test_name = "test_run_nested_s3_paths";
        let _env_lock = LOCK_ENV_RUNNER_TESTS.lock().await;
        let original_env: HashMap<String, String> = env::vars().collect();

        let (tmp_dir, db_client) = runner_tests_setup(test_name, "delivery").await?;

        // env_logger::init(); // uncomment for logs during cargo test -- --nocapture
        run("config.yml").await?;
        tmp_dir.close().unwrap(); // can be deleted as read what we need, and we'll verify in db
        restore_env(original_env);

        // VERIFY DB RESULTS
        let sql = format!("SELECT count(id) AS total from {}", test_name);
        let exp_string = "\
            total\n\
            14\n\
        ";
        let csv_string = get_rows_as_csv_string(&db_client, sql.as_str())
            .await
            .unwrap();
        assert_eq!(
            csv_string,
            exp_string.to_string(),
            "Expected {} rows inserted into the db.",
            "14",
        );

        let sql_tmpl = "\
            (SELECT id FROM {} ORDER BY id ASC LIMIT 2) \
            UNION ALL \
            (SELECT id FROM {} ORDER BY id DESC LIMIT 2) \
            ORDER BY id ASC; \
        ";

        let sql = render_tmpl_str(sql_tmpl, vec![test_name, test_name]);

        let exp_string = "\
            id\n\
            dgS5mQYAAL6DGb2DGQGQZRse42PfgtS8aNdnjwQ=\n\
            dgS5mQYAANXiGNTiGAGQZRr0me0mxb0dVD3dfkI=\n\
            dgS5mQYBBPOdGPKdGAGQZxqvsOHBr_RDNe3aVFg=\n\
            dgS5mQYBBPOdGPKdGAGQZxruMdKhdZ919oxt23U=\n\
        ";

        let csv_string = get_rows_as_csv_string(&db_client, sql.as_str())
            .await
            .unwrap();

        assert_eq!(
            csv_string,
            exp_string.to_string(),
            "Fetched the first two and last two rows from the DB, but didn't get expected results",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_run_supported_parquet_data_types() -> Result<()> {
        let test_name = "test_run_supported_parquet_data_types";
        let _env_lock = LOCK_ENV_RUNNER_TESTS.lock().await;
        let original_env: HashMap<String, String> = env::vars().collect();

        let (tmp_dir, db_client) = runner_tests_setup(test_name, "types_full").await?;

        env_logger::init(); // uncomment for logs during cargo test -- --nocapture
        run("config.yml").await?;
        tmp_dir.close().unwrap(); // can be deleted as read what we need, and we'll verify in db
        restore_env(original_env);

        // VERIFY DB RESULTS
        let sql = format!("SELECT count(*) AS total from {}", test_name);
        let exp_count = 2;
        let exp_count_string = format!("total\n{}\n", exp_count);
        let csv_string = get_rows_as_csv_string(&db_client, sql.as_str())
            .await
            .unwrap();
        assert_eq!(
            csv_string,
            exp_count_string.to_string(),
            "Expected {} rows inserted into the db.",
            exp_count,
        );

        let sql = format!("SELECT * FROM {} ORDER BY my_date_field DESC", test_name);

        let exp_csv_string = "\
            my_date_field,my_boolean,my_timestamp_field,my_varchar_field,my_small_int\n\
            2024-09-24,true,,this is my varchar,2\n\
            2024-08-01,false,,this is NOT my varchar,3\n\
        ";

        let csv_string = get_rows_as_csv_string(&db_client, sql.as_str())
            .await
            .unwrap();

        assert_eq!(
            csv_string,
            exp_csv_string.to_string(),
            "Should have fetched all rows",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_run_customer_orders_constraint_violation() -> Result<()> {
        let test_name = "test_run_customer_orders_constraint_violation";
        let _env_lock = LOCK_ENV_RUNNER_TESTS.lock().await;
        let original_env: HashMap<String, String> = env::vars().collect();

        let (tmp_dir, _) =
            runner_tests_setup(test_name, "customer_order_violated_constraint").await?;

        // env_logger::init(); // uncomment for logs during cargo test -- --nocapture
        let result = run("config.yml").await;

        tmp_dir.close().unwrap(); // can be deleted as read what we need, and we'll verify in db
        restore_env(original_env);

        assert!(
            result.is_err(),
            "Should fail as table rejects rows with NULL customer_name vals"
        );

        if let Err(e) = result {
            let msg = e.to_string();
            assert!(
                msg.contains("violates not-null constraint"),
                "Failure should have been due to not-null constraint"
            );
        };

        Ok(())
    }
}
