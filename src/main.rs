use anyhow::Result;
use log::{debug, info};
use parquet::file::reader::FileReader;

mod cmd_args;
mod config;
mod db;
mod parquet_ops;
mod s3_download;
mod work_lists;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let cfg_file: String = cmd_args::config_yaml(|| std::env::args().collect::<Vec<String>>())?;
    info!("reading cfg file: [{}]", cfg_file);
    let cfg: config::Config = config::Config::from_yaml_file(cfg_file.as_str())?;

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
            s3_download::get(bucket_name.clone(), wip_list, output_dir.clone()).await?;
        info!("... downloaded files:");
        for downloaded_file in map_ids_to_downloads.values() {
            info!("\t{}", downloaded_file);
        }
        // parquet filename has the output_dir
        for (id, downloaded_file) in map_ids_to_downloads.iter() {
            info!("{}: handling downloaded parquet file", downloaded_file);
            let mut parquet =
                parquet_ops::Parquet::new(downloaded_file.to_string(), desired_fields.clone())?;

            {
                // controls that parquet file (dowloaded_file) is guaranteed closed at end of this scope
                let reader = parquet.file_reader()?;

                debug!("{}: ... finding desired columns positions", downloaded_file);
                let parquet_col_nums = parquet.get_desired_cols(&reader)?;

                debug!("{}: ... reading parquet rows", downloaded_file);
                let row_iter: parquet::record::reader::RowIter = reader.get_row_iter(None)?;

                info!("{}: ... writing rows to db", downloaded_file);
                let num_rows_added = db.write_rows(row_iter, &parquet_col_nums).await?;

                info!(
                    "{}: {} rows added to db successfully",
                    downloaded_file, num_rows_added
                );
            } // shouldn't be anything still keeping the downloaded file open now - free to delete

            debug!("{}: will mark {} as completed", downloaded_file, id);
            work_lists.mark_completed(id.to_string())?;

            debug!("{}: deleting downloaded file", downloaded_file);
            s3_download::delete(downloaded_file.clone())?;
        }
    }

    Ok(())
}
