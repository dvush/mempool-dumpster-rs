use polars::frame::DataFrame;
use polars::prelude::{NamedFrom, ParquetCompression, ParquetWriter, Series};
use rayon::prelude::*;
use reth_primitives::{Bytes, TransactionSigned};
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use tracing::info;

// There are 3 types of files:
// - sourcelog: contains the source of the transaction
// - transaction-data: contains transaction data (gas, gas price, from, to, etc)
// - transactions: contains transaction data and raw transaction itself

fn get_month(day: &str) -> String {
    day.split('-').take(2).collect::<Vec<_>>().join("-")
}

fn path_transaction_data(data_dir: impl AsRef<Path>, day: &str) -> PathBuf {
    data_dir
        .as_ref()
        .join(format!("{}_transaction-data.parquet", day))
}

fn path_source_log(data_dir: impl AsRef<Path>, day: &str) -> PathBuf {
    data_dir.as_ref().join(format!("{}_sourcelog.parquet", day))
}

fn path_transactions(data_dir: impl AsRef<Path>, day: &str) -> PathBuf {
    data_dir
        .as_ref()
        .join(format!("{}_transactions.parquet", day))
}

async fn get_month_list() -> eyre::Result<Vec<String>> {
    let resp = reqwest::get("https://mempool-dumpster.flashbots.net/index.html")
        .await?
        .text()
        .await?;

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

async fn get_days(month: &str) -> eyre::Result<Vec<String>> {
    let resp = reqwest::get(format!(
        "https://mempool-dumpster.flashbots.net/ethereum/mainnet/{}/index.html",
        month
    ))
    .await?
    .text()
    .await?;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct SourcelogCSVRecord {
    timestamp_ms: u64,
    hash: String,
    source: String,
}

// downloads the sourcelog file for the given day and convert it to parquet format
async fn download_sourcelog_file(data_dir: impl AsRef<Path>, day: &str) -> eyre::Result<()> {
    info!("Downloading sourcelog file for {}", day);

    let file_path = path_source_log(data_dir, day);
    if file_path.exists() {
        info!(
            "File {} already exists, skipping download",
            file_path.display()
        );
        return Ok(());
    }

    // https://mempool-dumpster.flashbots.net/ethereum/mainnet/2023-08/2023-08-31_sourcelog.csv.zip

    let month = get_month(day);

    let url = format!(
        "https://mempool-dumpster.flashbots.net/ethereum/mainnet/{}/{}_sourcelog.csv.zip",
        month, day
    );
    // let resp = reqwest::get(&url).await?.bytes().await?;
    let resp = fs::read("tmp/2023-09-07_sourcelog.csv.zip")?;
    info!("Downloaded {} bytes", resp.len());

    let mut zip = {
        let zip = Cursor::new(resp);
        let mut archive = zip::ZipArchive::new(zip)?;
        archive
    };
    let mut csv = {
        let file = zip.by_index(0)?;
        csv::Reader::from_reader(file)
    };

    let mut records = Vec::new();
    if !csv.has_headers() {
        csv.set_headers(csv::StringRecord::from(vec![
            "timestamp_ms",
            "hash",
            "source",
        ]));
    }
    for result in csv.deserialize() {
        let record: SourcelogCSVRecord = match result {
            Ok(r) => r,
            Err(e) => {
                info!("Failed to deserialize record: {}", e);
                continue;
            }
        };
        records.push(record);
    }

    info!("Read {} records", records.len());

    let mut df = DataFrame::new(vec![
        Series::new(
            "timestamp",
            records
                .iter()
                .map(|r| chrono::NaiveDateTime::from_timestamp_millis(r.timestamp_ms as i64))
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

    let file = File::create(&file_path)?;
    ParquetWriter::new(file)
        .with_statistics(true)
        .with_compression(ParquetCompression::Gzip(None))
        .finish(&mut df)?;

    Ok(())
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

// downloads the sourcelog file for the given day and convert it to parquet format
async fn download_transaction_data_file(data_dir: impl AsRef<Path>, day: &str) -> eyre::Result<()> {
    info!("Downloading transaction file for {}", day);

    let file_path = path_transaction_data(data_dir, day);
    if file_path.exists() {
        info!(
            "File {} already exists, skipping download",
            file_path.display()
        );
        return Ok(());
    }

    // https://mempool-dumpster.flashbots.net/ethereum/mainnet/2023-08/2023-08-31.csv.zip

    let month = get_month(day);

    let url = format!(
        "https://mempool-dumpster.flashbots.net/ethereum/mainnet/{}/{}.csv.zip",
        month, day
    );
    // let resp = reqwest::get(&url).await?.bytes().await?;
    let resp = fs::read("tmp/2023-08-08.csv.zip")?;
    info!("Downloaded {} bytes", resp.len());

    let mut zip = {
        let zip = Cursor::new(resp);
        let mut archive = zip::ZipArchive::new(zip)?;
        archive
    };
    let mut csv = {
        let file = zip.by_index(0)?;
        csv::Reader::from_reader(file)
    };

    let mut records = Vec::new();
    if !csv.has_headers() {
        csv.set_headers(csv::StringRecord::from(vec![
            "timestamp_ms",
            "hash",
            "chain_id",
            "from",
            "to",
            "value",
            "nonce",
            "gas",
            "gas_price",
            "gas_tip_cap",
            "gas_fee_cap",
            "data_size",
            "data_4bytes",
        ]));
    }
    for result in csv.deserialize() {
        let record: TransactionDataCSVRecord = match result {
            Ok(r) => r,
            Err(e) => {
                info!("Failed to deserialize record: {}", e);
                continue;
            }
        };
        records.push(record);
    }

    info!("Read {} records", records.len());

    let mut df = DataFrame::new(vec![
        Series::new(
            "timestamp",
            records
                .iter()
                .map(|r| chrono::NaiveDateTime::from_timestamp_millis(r.timestamp_ms as i64))
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

    let file = File::create(&file_path)?;
    ParquetWriter::new(file)
        .with_statistics(true)
        .with_compression(ParquetCompression::Gzip(None))
        .finish(&mut df)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::filter::FilterExt;

    #[tokio::test]
    async fn test_get_month_list() {
        let month = get_month_list().await.expect("failed to get month list");
        assert!(month.iter().find(|m| *m == "2023-08").is_some());
        assert!(month.iter().find(|m| *m == "2023-09").is_some());
    }

    #[tokio::test]
    async fn test_get_days_list() {
        let days = get_days("2023-08").await.expect("failed to get day list");
        assert!(days.iter().find(|m| *m == "2023-08-08").is_some());
        assert!(days.iter().find(|m| *m == "2023-08-31").is_some());
    }

    #[ignore]
    #[tokio::test]
    async fn test_download_sourcelog_file() {
        let env = tracing_subscriber::EnvFilter::builder()
            .parse("info")
            .unwrap();
        tracing_subscriber::fmt().with_env_filter(env).init();

        download_sourcelog_file("data", "2023-08-31").await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn test_download_transaction_data_file() {
        let env = tracing_subscriber::EnvFilter::builder()
            .parse("info")
            .unwrap();
        tracing_subscriber::fmt().with_env_filter(env).init();

        download_transaction_data_file("data", "2023-08-31")
            .await
            .unwrap();
    }
}
