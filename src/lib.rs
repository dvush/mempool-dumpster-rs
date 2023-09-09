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

    let zip = Cursor::new(resp);
    let mut archive = zip::ZipArchive::new(zip)?;
    let file = archive.by_index(0)?;
    let mut csv = csv::Reader::from_reader(file);

    let mut records = Vec::new();
    csv.set_headers(csv::StringRecord::from(vec![
        "timestamp_ms",
        "hash",
        "source",
    ]));
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
struct CSVTransactionRecord {
    timestamp_ms: u64,
    hash: String,
    raw_transaction_hex: Bytes,
}

struct ParquetTransactionRecord {
    timestamp: chrono::NaiveDateTime,
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
    raw_tx: Vec<u8>,
}

// downloads the tx file for the given day and convert it to parquet format
async fn download_old_transaction_file(day: &str) -> eyre::Result<()> {
    info!("Downloading transaction file for {}", day);
    // https://mempool-dumpster.flashbots.net/ethereum/mainnet/2023-08/2023-08-08_transactions.csv.zip

    let month = day.split('-').take(2).collect::<Vec<_>>().join("-");

    let url = format!(
        "https://mempool-dumpster.flashbots.net/ethereum/mainnet/{}/{}_transactions.csv.zip",
        month, day
    );

    // let resp = reqwest::get(&url).await?.bytes().await?;
    // tmp
    let resp = fs::read("2023-08-08_transactions.csv.zip")?;
    info!("Downloaded {} bytes", resp.len());

    let zip = Cursor::new(resp);
    let mut archive = zip::ZipArchive::new(zip)?;
    let mut file = archive.by_index(0)?;
    let mut csv = csv::Reader::from_reader(file);

    let mut records = Vec::new();
    let headers = csv::StringRecord::from(vec!["timestamp_ms", "hash", "raw_transaction_hex"]);
    for result in csv.records().take(2000) {
        let record = result?;
        let record: CSVTransactionRecord = record.deserialize(Some(&headers))?;
        records.push(record);
    }
    info!("Read {} records", records.len());

    // deserialize transactions
    let txs = records
        .into_par_iter()
        .filter_map(|r| {
            // let recovered_tx = TransactionSigned::decode_enveloped(r.raw_transaction_hex.clone())
            //     .ok()?
            //     .into_ecrecovered()?;

            let recovered_tx = TransactionSigned::decode_enveloped(r.raw_transaction_hex.clone())
                .unwrap()
                .into_ecrecovered()
                .unwrap();

            let data_4bytes = if recovered_tx.input().len() >= 4 {
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&recovered_tx.input()[..4]);
                // prepend 0x
                format!("0x{}", reth_primitives::hex::encode(buf))
            } else {
                "".to_string()
            };

            let timestamp = chrono::NaiveDateTime::from_timestamp_millis(r.timestamp_ms as i64)?;

            Some(ParquetTransactionRecord {
                timestamp,
                hash: recovered_tx.hash().to_string(),
                chain_id: recovered_tx.chain_id().unwrap_or(0).to_string(),
                from: recovered_tx.signer().to_string().to_lowercase(),
                to: recovered_tx
                    .to()
                    .map(|t| t.to_string().to_lowercase())
                    .unwrap_or("".to_string()),
                value: recovered_tx.value().to_string(),
                nonce: recovered_tx.nonce().to_string(),
                gas: recovered_tx.gas_limit().to_string(),
                gas_price: recovered_tx.max_fee_per_gas().to_string(),
                gas_tip_cap: recovered_tx
                    .max_priority_fee_per_gas()
                    .unwrap_or(0)
                    .to_string(),
                gas_fee_cap: recovered_tx.max_fee_per_gas().to_string(),
                data_size: recovered_tx.input().len() as i64,
                data_4bytes,
                raw_tx: r.raw_transaction_hex.to_vec(),
            })
        })
        .collect::<Vec<_>>();

    let mut df = DataFrame::new(vec![
        Series::new(
            "timestamp",
            txs.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
        ),
        Series::new(
            "hash",
            txs.iter().map(|r| r.hash.clone()).collect::<Vec<_>>(),
        ),
        Series::new(
            "chainId",
            txs.iter().map(|r| r.chain_id.clone()).collect::<Vec<_>>(),
        ),
        Series::new(
            "from",
            txs.iter().map(|r| r.from.clone()).collect::<Vec<_>>(),
        ),
        Series::new("to", txs.iter().map(|r| r.to.clone()).collect::<Vec<_>>()),
        Series::new(
            "value",
            txs.iter().map(|r| r.value.clone()).collect::<Vec<_>>(),
        ),
        Series::new(
            "nonce",
            txs.iter().map(|r| r.nonce.clone()).collect::<Vec<_>>(),
        ),
        Series::new("gas", txs.iter().map(|r| r.gas.clone()).collect::<Vec<_>>()),
        Series::new(
            "gasPrice",
            txs.iter().map(|r| r.gas_price.clone()).collect::<Vec<_>>(),
        ),
        Series::new(
            "gasTipCap",
            txs.iter()
                .map(|r| r.gas_tip_cap.clone())
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "gasFeeCap",
            txs.iter()
                .map(|r| r.gas_fee_cap.clone())
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "dataSize",
            txs.iter().map(|r| r.data_size).collect::<Vec<_>>(),
        ),
        Series::new(
            "data4Bytes",
            txs.iter()
                .map(|r| r.data_4bytes.clone())
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "rawTx",
            txs.iter().map(|r| r.raw_tx.clone()).collect::<Vec<_>>(),
        ),
    ])?;

    let path = format!("{}_transactions.parquet", day);
    if std::path::Path::new(&path).exists() {
        info!("File {} already exists, removing", path);
        std::fs::remove_file(&path)?;
        return Ok(());
    }
    let file = File::create(&path)?;
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
    async fn test_download_old_transaction_file() {
        let env = tracing_subscriber::EnvFilter::builder()
            .parse("info")
            .unwrap();
        tracing_subscriber::fmt().with_env_filter(env).init();

        download_old_transaction_file("2023-08-31").await.unwrap();
    }
}
