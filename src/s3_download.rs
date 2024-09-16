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
        // anyhow provides simple custom errors with bail! macro, if panic is sufficient
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
