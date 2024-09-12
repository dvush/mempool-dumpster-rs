use indicatif::{ProgressBar, ProgressFinish, ProgressStyle};
use polars::error::PolarsError;
use polars::frame::DataFrame;
use polars::prelude::{
    col, LazyFrame, NamedFrom, ParquetCompression, ParquetWriter, Series, StatisticsOptions,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tracing::{debug, info, warn};

// There are 3 types of files:
// - sourcelog: contains the source of the transaction
// - transaction-data: contains transaction data (gas, gas price, from, to, etc)
// - transactions: contains transaction data and raw transaction itself

pub struct Config {
    pub data_dir: PathBuf,
    pub base_url: String,
    pub progress: bool,
    pub overwrite: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            base_url: "https://mempool-dumpster.flashbots.net/ethereum/mainnet".to_string(),
            progress: true,
            overwrite: false,
        }
    }
}

impl Config {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            ..Default::default()
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_progress(mut self, progress: bool) -> Self {
        self.progress = progress;
        self
    }

    pub fn with_overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    // true if should skip
    fn check_file(&self, file_path: impl AsRef<Path>) -> eyre::Result<bool> {
        let file_path = file_path.as_ref();
        if file_path.exists() {
            if self.overwrite {
                info!("File {} already exists, overwriting", file_path.display());
            } else {
                info!(
                    "File {} already exists, skipping download",
                    file_path.display()
                );
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn download_sourcelog_file(&self, day: &str) -> eyre::Result<()> {
        info!("Downloading sourcelog file for {}", day);

        let file_path = path_source_log(&self.data_dir, day);
        let skip = self.check_file(&file_path)?;
        if skip {
            return Ok(());
        }

        let month = get_month(day);

        let url = format!("{}/{}/{}_sourcelog.csv.zip", self.base_url, month, day);

        let records = download_zip_csv_records::<SourcelogCSVRecord>(&url, self.progress)?;

        let df = DataFrame::new(vec![
            Series::new(
                "timestamp",
                records
                    .iter()
                    .map(|r| chrono::NaiveDateTime::from_timestamp_millis(r.timestamp_ms))
                    .collect::<Vec<_>>(),
            ),
            Series::new(
                "hash",
                records.iter().map(|r| r.hash.clone()).collect::<Vec<_>>(),
            ),
            Series::new(
                "source",
                records.iter().map(|r| r.source.clone()).collect::<Vec<_>>(),
            ),
        ])?;

        debug!("Writing sourcelog file to {}", file_path.display());
        write_dataframe_to_parquet(df, file_path, self.progress)?;

        Ok(())
    }

    pub fn download_transaction_file(&self, day: &str) -> eyre::Result<()> {
        info!("Downloading transaction file for {}", day);

        let file_path = path_transactions(&self.data_dir, day);
        let skip = self.check_file(&file_path)?;
        if skip {
            return Ok(());
        }

        let month = get_month(day);

        let url = format!("{}/{}/{}.parquet", self.base_url, month, day);

        let reader = ureq::get(&url).call()?.into_reader();
        let mut reader: Box<dyn Read> = if self.progress {
            Box::new(
                progress_bar_template()
                    .with_message(format!("Downloading file: {}.parquet", day))
                    .wrap_read(reader),
            )
        } else {
            Box::new(reader)
        };

        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)?;
        file.write_all(&buffer)?;

        Ok(())
    }

    pub fn download_transaction_data_file(&self, day: &str) -> eyre::Result<()> {
        info!("Downloading transaction file for {}", day);

        let file_path = path_transaction_data(&self.data_dir, day);
        let skip = self.check_file(&file_path)?;
        if skip {
            return Ok(());
        }

        // https://mempool-dumpster.flashbots.net/ethereum/mainnet/2023-08/2023-08-31.csv.zip

        let month = get_month(day);

        let url = format!("{}/{}/{}.csv.zip", self.base_url, month, day);
        let records = download_zip_csv_records::<TransactionDataCSVRecord>(&url, true)?;

        let df = DataFrame::new(vec![
            Series::new(
                "timestamp",
                records
                    .iter()
                    .map(|r| chrono::NaiveDateTime::from_timestamp_millis(r.timestamp_ms))
                    .collect::<Vec<_>>(),
            ),
            Series::new(
                "hash",
                records.iter().map(|r| r.hash.clone()).collect::<Vec<_>>(),
            ),
            Series::new(
                "chainId",
                records
                    .iter()
                    .map(|r| r.chain_id.clone())
                    .collect::<Vec<_>>(),
            ),
            Series::new(
                "from",
                records
                    .iter()
                    .map(|r| r.from.to_lowercase())
                    .collect::<Vec<_>>(),
            ),
            Series::new(
                "to",
                records
                    .iter()
                    .map(|r| r.to.to_lowercase())
                    .collect::<Vec<_>>(),
            ),
            Series::new(
                "value",
                records.iter().map(|r| r.value.clone()).collect::<Vec<_>>(),
            ),
            Series::new(
                "nonce",
                records.iter().map(|r| r.nonce.clone()).collect::<Vec<_>>(),
            ),
            Series::new(
                "gas",
                records.iter().map(|r| r.gas.clone()).collect::<Vec<_>>(),
            ),
            Series::new(
                "gasPrice",
                records
                    .iter()
                    .map(|r| r.gas_price.clone())
                    .collect::<Vec<_>>(),
            ),
            Series::new(
                "gasTipCap",
                records
                    .iter()
                    .map(|r| r.gas_tip_cap.clone())
                    .collect::<Vec<_>>(),
            ),
            Series::new(
                "gasFeeCap",
                records
                    .iter()
                    .map(|r| r.gas_fee_cap.clone())
                    .collect::<Vec<_>>(),
            ),
            Series::new(
                "dataSize",
                records.iter().map(|r| r.data_size).collect::<Vec<_>>(),
            ),
            Series::new(
                "data4Bytes",
                records
                    .iter()
                    .map(|r| r.data_4bytes.clone())
                    .collect::<Vec<_>>(),
            ),
        ])?;

        write_dataframe_to_parquet(df, file_path, self.progress)?;

        Ok(())
    }
}

pub fn get_month_list() -> eyre::Result<Vec<String>> {
    let resp = ureq::get("https://mempool-dumpster.flashbots.net/index.html")
        .call()?
        .into_string()?;

    let fragment = scraper::Html::parse_document(&resp);
    let selector = scraper::Selector::parse("ul.root-months li a").unwrap();

    let result = fragment
        .select(&selector)
        .map(|e| e.inner_html())
        .collect::<Vec<_>>();

    if result.is_empty() {
        Err(eyre::eyre!("failed to get month list"))
    } else {
        Ok(result)
    }
}

pub fn get_day_list(month: &str) -> eyre::Result<Vec<String>> {
    let resp = ureq::get(&format!(
        "https://mempool-dumpster.flashbots.net/ethereum/mainnet/{}/index.html",
        month
    ))
    .call()?
    .into_string()?;

    let fragment = scraper::Html::parse_document(&resp);
    let selector = scraper::Selector::parse("table.pure-table tbody tr.c1 td.fn a").unwrap();

    let result = fragment
        .select(&selector)
        .filter_map(|e| {
            e.inner_html()
                .strip_suffix(".csv.zip")
                .map(|s| s.to_string())
        })
        .collect::<Vec<_>>();

    if result.is_empty() {
        Err(eyre::eyre!("failed to get day list"))
    } else {
        Ok(result)
    }
}

#[derive(Error, Debug)]
pub enum TransactionRangeError {
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error("day file not found: {0}")]
    DayFileNotFound(String),
    #[error(transparent)]
    PolarsError(#[from] PolarsError),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RawTransaction {
    pub timestamp_ms: i64,
    pub raw_tx: Vec<u8>,
}

pub fn get_raw_transactions(
    data_dir: impl AsRef<Path>,
    from_timestamp_ms: i64,
    to_timestamp_ms: i64,
) -> Result<Vec<RawTransaction>, TransactionRangeError> {
    let from_time = chrono::NaiveDateTime::from_timestamp_millis(from_timestamp_ms)
        .ok_or(TransactionRangeError::InvalidTimestamp)?;
    let to_time = chrono::NaiveDateTime::from_timestamp_millis(to_timestamp_ms)
        .ok_or(TransactionRangeError::InvalidTimestamp)?;

    tracing::trace!("Getting raw transactions from {} to {}", from_time, to_time);

    // get all days in range
    let mut days = Vec::new();
    let mut current_day = from_time.date();
    while current_day <= to_time.date() {
        days.push(current_day.format("%Y-%m-%d").to_string());
        current_day = current_day
            .succ_opt()
            .ok_or(TransactionRangeError::InvalidTimestamp)?;
    }

    tracing::trace!("Fetching transactions for days: {:?}", days);

    // check all day files
    for day in &days {
        let path = path_transactions(&data_dir, day);
        if !path.exists() {
            return Err(TransactionRangeError::DayFileNotFound(day.to_string()));
        }
    }

    let mut raw_transactions = Vec::new();

    for day in &days {
        let path = path_transactions(&data_dir, day);
        let df = LazyFrame::scan_parquet(path, Default::default())?;
        let result = df
            .filter(
                col("timestamp")
                    .gt(from_timestamp_ms)
                    .and(col("timestamp").lt(to_timestamp_ms)),
            )
            .select(&[col("timestamp"), col("rawTx")])
            .collect()?;
        let raw_tx_column = result.column("rawTx")?.binary()?;
        let timestamp_column = result.column("timestamp")?.datetime()?;

        for i in 0..raw_tx_column.len() {
            let bytes = raw_tx_column.get(i).ok_or_else(|| {
                TransactionRangeError::PolarsError(PolarsError::NoData("rawTx".into()))
            })?;
            let timestamp = timestamp_column.get(i).ok_or_else(|| {
                TransactionRangeError::PolarsError(PolarsError::NoData("timestamp".into()))
            })?;

            raw_transactions.push(RawTransaction {
                timestamp_ms: timestamp,
                raw_tx: bytes.to_vec(),
            })
        }
    }

    raw_transactions.sort_by_key(|r| r.timestamp_ms);

    Ok(raw_transactions)
}

fn write_dataframe_to_parquet(
    mut df: DataFrame,
    file_path: impl AsRef<Path>,
    progress: bool,
) -> eyre::Result<()> {
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&file_path)?;
    let writer: Box<dyn Write> = if progress {
        Box::new(
            progress_bar_template()
                .with_message(format!("Writing file: {}", file_path.as_ref().display()))
                .wrap_write(file),
        )
    } else {
        Box::new(file)
    };
    ParquetWriter::new(writer)
        .with_statistics(StatisticsOptions {
            min_value: true,
            max_value: true,
            distinct_count: false,
            null_count: false,
        })
        .with_compression(ParquetCompression::Gzip(None))
        .finish(&mut df)?;

    Ok(())
}

fn progress_bar_template() -> ProgressBar {
    let style = ProgressStyle::default_spinner()
        .template("{spinner:.green} [{elapsed_precise}] [{bytes}]  [{bytes_per_sec}] {msg}")
        .unwrap();
    ProgressBar::new_spinner()
        .with_style(style)
        .with_finish(ProgressFinish::AndLeave)
}

pub fn get_month(day: &str) -> String {
    day.split('-').take(2).collect::<Vec<_>>().join("-")
}

fn path_transaction_data(data_dir: impl AsRef<Path>, day: &str) -> PathBuf {
    data_dir
        .as_ref()
        .join(format!("transaction-data/{}_transaction-data.parquet", day))
}

fn path_source_log(data_dir: impl AsRef<Path>, day: &str) -> PathBuf {
    data_dir
        .as_ref()
        .join(format!("sourcelog/{}_sourcelog.parquet", day))
}

fn path_transactions(data_dir: impl AsRef<Path>, day: &str) -> PathBuf {
    data_dir
        .as_ref()
        .join(format!("transactions/{}.parquet", day))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct SourcelogCSVRecord {
    timestamp_ms: i64,
    hash: String,
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct TransactionDataCSVRecord {
    timestamp_ms: i64,
    hash: String,
    chain_id: String,
    from: String,
    to: String,
    value: String,
    nonce: String,
    gas: String,
    gas_price: String,
    gas_tip_cap: String,
    gas_fee_cap: String,
    data_size: i64,
    data_4bytes: String,
}

fn download_zip_csv_records<R: DeserializeOwned>(
    url: &str,
    progress: bool,
) -> eyre::Result<Vec<R>> {
    debug!("Downloading .zip.csv from {}", url);

    // we download the file in memory because its small and zip::ZipArchive requires a Seek + Read
    let response_bytes = {
        let mut response_bytes = Vec::new();

        let mut reader = ureq::get(url).call()?.into_reader();
        let mut read: Box<dyn Read> = if progress {
            Box::new(
                progress_bar_template()
                    .with_message("Downloading ")
                    .wrap_read(&mut reader),
            )
        } else {
            Box::new(reader)
        };
        let read_bytes = read.read_to_end(&mut response_bytes)?;
        debug!("Downloaded {} bytes", read_bytes);
        response_bytes
    };

    let mut zip = {
        let zip = Cursor::new(response_bytes);
        zip::ZipArchive::new(zip)?
    };

    let mut csv = {
        // we only have one file in the zip
        let file = zip.by_index(0)?;
        csv::Reader::from_reader(file)
    };

    let progress_bar = ProgressBar::new_spinner().with_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] Records: [{pos}]")
            .unwrap(),
    );

    let mut result = Vec::new();
    for record in csv.deserialize() {
        if progress {
            progress_bar.inc(1);
        }
        let record: R = match record {
            Ok(r) => r,
            Err(e) => {
                warn!("Failed to deserialize record: {}", e);
                continue;
            }
        };
        result.push(record)
    }
    if progress {
        progress_bar.finish();
    }

    debug!("Read {} records", result.len());

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_get_month_list() {
        let month = get_month_list().expect("failed to get month list");
        assert!(month.iter().find(|m| *m == "2023-08").is_some());
        assert!(month.iter().find(|m| *m == "2023-09").is_some());
    }

    #[test]
    fn test_get_days_list() {
        let days = get_day_list("2023-08").expect("failed to get day list");
        assert!(days.iter().find(|m| *m == "2023-08-08").is_some());
        assert!(days.iter().find(|m| *m == "2023-08-31").is_some());
    }

    #[ignore]
    #[test]
    fn test_download_sourcelog_file() {
        let env = tracing_subscriber::EnvFilter::builder()
            .parse("mempool_dumpster=debug")
            .unwrap();
        tracing_subscriber::fmt().with_env_filter(env).init();

        Config::new("data")
            .with_progress(true)
            .with_base_url("http://localhost:8000")
            .with_overwrite(true)
            .download_sourcelog_file("2023-09-07")
            .unwrap();
    }

    #[ignore]
    #[test]
    fn test_download_transactions_file() {
        let env = tracing_subscriber::EnvFilter::builder()
            .parse("mempool_dumpster=debug")
            .unwrap();
        tracing_subscriber::fmt().with_env_filter(env).init();

        Config::new("data")
            .with_progress(true)
            .with_base_url("http://localhost:8000")
            .with_overwrite(true)
            .download_transaction_file("2023-09-09")
            .unwrap();
    }

    #[ignore]
    #[test]
    fn test_download_transaction_data_file() {
        let env = tracing_subscriber::EnvFilter::builder()
            .parse("mempool_dumpster=debug")
            .unwrap();
        tracing_subscriber::fmt().with_env_filter(env).init();

        Config::new("data")
            .with_progress(true)
            .with_base_url("http://localhost:8000")
            .with_overwrite(true)
            .download_transaction_data_file("2023-08-08")
            .unwrap();
    }

    #[ignore]
    #[test]
    fn test_get_raw_transactions() {
        let env = tracing_subscriber::EnvFilter::builder()
            .parse("mempool_dumpster=trace")
            .unwrap();
        tracing_subscriber::fmt().with_env_filter(env).init();

        // 2023-09-09 01:00:00
        let start_time = NaiveDate::from_ymd_opt(2023, 9, 9)
            .unwrap()
            .and_hms_opt(1, 0, 0)
            .unwrap()
            .timestamp_millis();
        // 2023-09-09 01:05:00
        let end_time = NaiveDate::from_ymd_opt(2023, 9, 9)
            .unwrap()
            .and_hms_opt(1, 5, 0)
            .unwrap()
            .timestamp_millis();

        let res = get_raw_transactions("data", start_time, end_time).unwrap();
        debug!("Got {} transactions", res.len());
        debug!("First transaction: {:?}", res.first());
        debug!("Last transaction: {:?}", res.last());
    }
}
