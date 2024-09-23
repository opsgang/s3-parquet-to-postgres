use anyhow::Result;

mod cmd_args;
mod config;
mod db;
mod parquet_ops;
mod runner;
mod s3_download;
mod work_lists;

#[cfg(test)]
mod test_setup; // for docker-compose setup

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    // do this in main as command line arg collection only relevant to binary
    let cfg_file: String = cmd_args::config_yaml(|| std::env::args().collect::<Vec<String>>())?;

    runner::run(cfg_file.as_str()).await?;
    Ok(())
}
