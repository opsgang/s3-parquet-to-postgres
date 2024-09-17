use anyhow::{bail, Context, Result};
use aws_sdk_s3 as s3;
use futures::stream::{self, StreamExt};
use log::debug;
use std::collections::HashMap;
use std::fs::remove_file;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

fn get_dirpath(path_str: &str) -> Result<Option<String>> {
    let path = Path::new(path_str);

    // Check if the path ends with a trailing slash
    if path_str.ends_with('/') {
        bail!("the object path looks like a dir: {}", path_str);
    }

    // Get the parent of the path
    if let Some(parent) = path.parent() {
        // Convert parent to a string and check if it's non-empty and not "/"
        if let Some(parent_str) = parent.to_str() {
            if !parent_str.is_empty() && parent_str != "/" {
                return Ok(Some(parent_str.to_string()));
            }
        }
    }

    Ok(None)
}

// returns a Vec of the locally downloaded files to process
pub async fn get(
    bucket_name: String,
    s3_keys: Vec<String>,
    output_dir: String,
) -> Result<HashMap<String, String>> {
    let config = aws_config::load_from_env().await;
    let client = s3::Client::new(&config);

    debug!("Handling these parquet files:");
    for k in s3_keys.iter() {
        debug!("{}", k);
    }

    // Note that we can always clone a Vec<String> because String is cloneable (even if Vec is not)
    let map_ids_to_downloads: HashMap<String, String> = s3_keys
        .iter()
        .map(|k| (k.clone(), format!("{}/{}", output_dir.clone(), k)))
        .collect();

    // Create an mpsc channel to handle errors
    let (tx, mut rx) = mpsc::channel(1);

    let s3_get_object_requests = stream::iter(s3_keys.into_iter())
        .map(move |key| {
            let client = client.clone();
            let bucket_name = bucket_name.clone();
            // create local dirpath to match s3 object path
            if let Some(dirpath) = get_dirpath(key.as_str()).unwrap() {
                debug!("... creating dir {}", dirpath);
                std::fs::create_dir_all(format!("{}/{}", output_dir.as_str(), dirpath))
                    .with_context(|| format!("Failed to create dirpath {}", dirpath))
                    .unwrap();
            }
            let tx = tx.clone(); // Clone the sender for each async task
            let local_output_dir = output_dir.clone();
            async move {
                let result = client
                    .get_object()
                    .bucket(bucket_name)
                    .key(key.clone())
                    .send()
                    .await
                    .with_context(|| format!("Failed to get object with key: {}", key));

                match result {
                    Ok(mut output) => {
                        let file_name = format!("{}/{}", local_output_dir, key.clone());
                        let mut file = File::create(&file_name)
                            .await
                            .with_context(|| format!("Failed to create file {}", file_name))
                            .unwrap();
                        while let Some(bytes) = output
                            .body
                            .try_next()
                            .await
                            .context("Failed to read chunk")
                            .unwrap()
                        {
                            file.write_all(&bytes)
                                .await
                                .context("Failed to write to file")
                                .unwrap();
                        }
                    }
                    Err(err) => {
                        // Send the error to the channel
                        let _ = tx.send(Err(err)).await;
                    }
                }

                // Send a success message
                let _ = tx.send(Ok(())).await;
            }
        })
        .buffer_unordered(5);

    // Launch the concurrent processing
    tokio::spawn(async move {
        s3_get_object_requests
            .for_each_concurrent(None, |_| async {})
            .await;
    });

    // Wait for errors from the channel
    while let Some(result) = rx.recv().await {
        result?; // we'll propagate any errors
    }

    Ok(map_ids_to_downloads)
}

pub fn delete(filename: String) -> Result<()> {
    remove_file(filename)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{bail, Result};
    use assert_fs::fixture::TempDir;
    use const_format::formatcp;
    use once_cell::sync::Lazy;
    use std::env;
    use tokio::fs;
    use tokio::io::AsyncReadExt;
    use tokio::sync::Mutex;

    static ENV_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    static SRC_PARQUET_DIR_CUSTOMERS: &str = formatcp!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "local/localstack/bucket_data/customer-orders-parquet"
    );

    static SRC_PARQUET_DIR_DELIVERIES: &str = formatcp!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "local/localstack/bucket_data/deliveries-parquet"
    );

    macro_rules! vec_stringify {
        ($($x:expr),*) => (vec![$($x.to_string()),*]);
    }

    fn set_good_aws_vars() {
        unset_aws_env_vars(); // remove any inherited vars
        env::set_var("AWS_ACCESS_KEY_ID", "test");
        env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        env::set_var("AWS_DEFAULT_REGION", "us-west-1");
        env::set_var("AWS_ENDPOINT_URL", "http://127.0.0.1:4566");
    }

    fn unset_aws_env_vars() {
        let aws_keys: Vec<String> = env::vars()
            .filter(|(key, _)| key.starts_with("AWS_"))
            .map(|(key, _)| key)
            .collect();

        for key in aws_keys {
            env::remove_var(key);
        }
    }

    fn restore_env(original_env: HashMap<String, String>) {
        for (key, _) in env::vars() {
            env::remove_var(key);
        }

        for (key, value) in original_env {
            env::set_var(key, value);
        }
    }

    async fn get_downloaded_and_src_file_contents(
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

    // fn get_dirpath(path_str: &str) -> Result<Option<String>>
    #[test]
    fn test_get_dirpath_abs_path_to_file_returns_abs_dir_path() -> Result<()> {
        let path = "/some/path/to/file.parquet";

        let dirpath_result = get_dirpath(path);

        assert!(
            dirpath_result.is_ok(),
            "should return dirpath to file.parquet, with no trailing slash",
        );

        let Some(v) = dirpath_result.unwrap() else {
            bail!("dirpath_result should unwrap without issue")
        };
        assert_eq!(v, "/some/path/to");

        Ok(())
    }

    #[test]
    fn test_get_dirpath_rel_path_to_file_returns_rel_dir_path() -> Result<()> {
        let path = "some/path/to/file.parquet";

        let dirpath_result = get_dirpath(path);

        assert!(
            dirpath_result.is_ok(),
            "should return dirpath to file.parquet, with no trailing slash",
        );

        let Some(v) = dirpath_result.unwrap() else {
            bail!("dirpath_result should unwrap without issue")
        };
        assert_eq!(v, "some/path/to");

        Ok(())
    }

    #[test]
    fn test_get_dirpath_trailing_slash_will_err() -> Result<()> {
        let path = "some/path/to/dir/";

        let dirpath_result = get_dirpath(path);

        assert!(
            dirpath_result.is_err(),
            "should fail due to input path trailing slash"
        );

        Ok(())
    }

    #[test]
    fn test_get_dirpath_rel_filename_no_parent_dirs() -> Result<()> {
        let path = "parquet.file";

        let dirpath_result = get_dirpath(path);

        assert!(
            dirpath_result.is_ok(),
            "should return None as no parent dirs",
        );

        let v = dirpath_result.unwrap();
        assert_eq!(v, None);

        Ok(())
    }

    #[test]
    fn test_get_dirpath_abs_filename_in_root_dir() -> Result<()> {
        let path = "/parquet.file";

        let dirpath_result = get_dirpath(path);

        assert!(
            dirpath_result.is_ok(),
            "should return None as no parent dirs to create (only root dir)",
        );

        let v = dirpath_result.unwrap();
        assert_eq!(v, None);

        Ok(())
    }

    // NOTE: the paths being passed to get_dirpath() are meant to be
    // S3 object "keys" (the path-like id in an s3 bucket for an object).
    // The . and .. are meaningless in an S3 key context
    // so this tool doesn't care about handling them in some special way.
    // This test is here, just to remind anyone else of this fact.
    #[test]
    fn test_get_dirpath_rel_unix_dots() -> Result<()> {
        let path = "./../path/to/parquet.file";

        let dirpath_result = get_dirpath(path);

        assert!(
            dirpath_result.is_ok(),
            "should return dirpath to file.parquet, with no trailing slash",
        );

        let Some(v) = dirpath_result.unwrap() else {
            bail!("dirpath_result should unwrap without issue")
        };
        assert_eq!(v, "./../path/to");

        Ok(())
    }

    // pub async fn get(bucket_name: String, s3_keys: Vec<String>, output_dir: String,) -> Result<HashMap<String, String>>
    #[tokio::test]
    async fn test_bad_aws_creds() -> Result<()> {
        let _env_lock = ENV_MUTEX.lock().await;
        let original_env: HashMap<String, String> = env::vars().collect();
        unset_aws_env_vars();

        // set up aws env vars for localstack
        let res = get(
            String::from("no-such-bucket"),
            vec_stringify!["order_001.parquet", "order_002.parquet"],
            ".".to_string(),
        )
        .await;

        restore_env(original_env);

        assert!(res.is_err(), "aws api should fail as no valid creds");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_unknown_bucket() -> Result<()> {
        let _env_lock = ENV_MUTEX.lock().await;
        let original_env: HashMap<String, String> = env::vars().collect();
        set_good_aws_vars();

        // set up aws env vars for localstack
        let res = get(
            String::from("no-such-bucket"),
            vec_stringify!["order_001.parquet", "order_002.parquet"],
            ".".to_string(),
        )
        .await;

        restore_env(original_env);

        assert!(res.is_err(), "aws api should fail as no such bucket");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_unknown_key() -> Result<()> {
        let _env_lock = ENV_MUTEX.lock().await;
        let original_env: HashMap<String, String> = env::vars().collect();
        set_good_aws_vars();

        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir_path = format!("{}", tmp_dir.path().display());

        // set up aws env vars for localstack
        let res = get(
            String::from("customer-orders-parquet"),
            vec_stringify!["not-a-real-key", "order_01.parquet"], // [not real, real] key
            tmp_dir_path.clone(),
        )
        .await;

        restore_env(original_env);

        assert!(
            res.is_err(),
            "aws api might get order_01.parquet but will fail on not-a-real-key"
        );

        tmp_dir.close().unwrap(); // can be deleted as read what we need

        Ok(())
    }

    #[tokio::test]
    async fn test_get_happy_path_files_at_bucket_root() -> Result<()> {
        // set up aws env vars for localstack
        let _env_lock = ENV_MUTEX.lock().await;
        let original_env: HashMap<String, String> = env::vars().collect();
        set_good_aws_vars();

        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir_path = format!("{}", tmp_dir.path().display());
        let res = get(
            String::from("customer-orders-parquet"),
            vec_stringify!["order_00.parquet", "order_01.parquet"], // [real, real] key
            tmp_dir_path.clone(),
        )
        .await;

        restore_env(original_env);

        assert!(
            res.is_ok(),
            "should download order_00 and order_01 parquet files"
        );

        let my_map = res.unwrap();

        for (s3_key, downloaded_file) in &my_map {
            let (src_contents, downloaded_contents) = get_downloaded_and_src_file_contents(
                format!("{}/{}", SRC_PARQUET_DIR_CUSTOMERS, s3_key),
                downloaded_file.to_string(),
            )
            .await
            .unwrap();

            assert_eq!(src_contents, downloaded_contents,);
        }

        tmp_dir.close().unwrap(); // can be deleted as read what we need
        Ok(())
    }

    #[tokio::test]
    async fn test_get_happy_path_s3_keys_with_subdirs() -> Result<()> {
        // set up aws env vars for localstack
        let _env_lock = ENV_MUTEX.lock().await;
        let original_env: HashMap<String, String> = env::vars().collect();
        set_good_aws_vars();

        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir_path = format!("{}", tmp_dir.path().display());

        let s3_keys = vec_stringify![
            "parent_dir/subdir_b/001.parquet",
            "parent_dir/subdir_a/001.parquet"
        ];
        let res = get(
            String::from("deliveries-parquet"),
            s3_keys.clone(), // [real, real] key
            tmp_dir_path.clone(),
        )
        .await;

        restore_env(original_env);

        assert!(
            res.is_ok(),
            "should download, creating s3 path with local subdirs as needed"
        );

        let my_map = res.unwrap();

        for (s3_key, downloaded_file) in &my_map {
            let (src_contents, downloaded_contents) = get_downloaded_and_src_file_contents(
                format!("{}/{}", SRC_PARQUET_DIR_DELIVERIES, s3_key),
                downloaded_file.to_string(),
            )
            .await
            .unwrap();

            assert_eq!(src_contents, downloaded_contents,);
        }

        tmp_dir.close().unwrap(); // can be deleted as read what we need
        Ok(())
    }
}
