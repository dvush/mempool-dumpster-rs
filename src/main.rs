use clap::Parser;
use std::fs::create_dir;
use std::path::PathBuf;

#[derive(Parser, Debug)]
struct Cli {
    #[clap(
        short,
        long,
        env = "MEMPOOL_DATADIR",
        default_value = "./data",
        help = "Directory to store data"
    )]
    datadir: PathBuf,
    #[clap(
        short,
        long,
        default_value = "false",
        help = "Overwrite existing files"
    )]
    overwrite: bool,
    #[clap(
        short,
        long,
        default_value = "false",
        help = "Skip errors and continue"
    )]
    ignore_errors: bool,
    #[clap(subcommand)]
    subcmd: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    #[clap(name = "list-months", about = "List available months")]
    ListMonths,
    #[clap(name = "list-days", about = "List available days in a month")]
    ListDays { month: String },
    #[clap(name = "get", about = "Download data")]
    Get {
        day_or_month: String,
        #[clap(
            long,
            default_value = "false",
            help = "Download sourcelog files (on by default)"
        )]
        sourcelog: bool,
        #[clap(
            long,
            default_value = "false",
            help = "Download transaction data files (on by default)"
        )]
        transaction_data: bool,
        #[clap(
            long,
            default_value = "false",
            help = "Download transaction files (off by default)"
        )]
        transactions: bool,
    },
}

fn main() -> eyre::Result<()> {
    let env = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt().with_env_filter(env).init();
    let cmd = Cli::parse();

    match cmd.subcmd {
        Commands::ListMonths => {
            let months = mempool_dumpster::get_month_list()?;
            for month in months {
                println!("{}", month);
            }
        }
        Commands::ListDays { month } => {
            let days = mempool_dumpster::get_day_list(&month)?;
            for day in days {
                println!("{}", day);
            }
        }
        Commands::Get {
            day_or_month,
            sourcelog,
            transaction_data,
            transactions,
        } => {
            // check if datadir exists
            if !cmd.datadir.exists() {
                return Err(eyre::eyre!(
                    "datadir does not exist: {}",
                    cmd.datadir.display()
                ));
            }

            let sourcelog_path = cmd.datadir.join("sourcelog");
            if !sourcelog_path.exists() {
                create_dir(&sourcelog_path)?;
            }
            let transaction_data_path = cmd.datadir.join("transaction-data");
            if !transaction_data_path.exists() {
                create_dir(&transaction_data_path)?;
            }
            let transactions_path = cmd.datadir.join("transactions");
            if !transactions_path.exists() {
                create_dir(&transactions_path)?;
            }

            let (sourcelog, transaction_data, transactions) =
                if sourcelog || transaction_data || transactions {
                    (sourcelog, transaction_data, transactions)
                } else {
                    (true, true, false)
                };

            let config = mempool_dumpster::Config::new(&cmd.datadir)
                .with_progress(true)
                .with_overwrite(cmd.overwrite);

            let month = if day_or_month.split('-').count() == 3 {
                None
            } else {
                Some(
                    day_or_month
                        .split('-')
                        .take(2)
                        .collect::<Vec<_>>()
                        .join("-"),
                )
            };

            let days = if let Some(month) = month {
                mempool_dumpster::get_day_list(&month)?
            } else {
                vec![day_or_month]
            };

            for day in days {
                if sourcelog {
                    let result = config.download_sourcelog_file(&day);
                    if let Err(e) = result {
                        if cmd.ignore_errors {
                            tracing::error!("Error: {}", e);
                        } else {
                            return Err(e);
                        }
                    }
                }
                if transaction_data {
                    let result = config.download_transaction_data_file(&day);
                    if let Err(e) = result {
                        if cmd.ignore_errors {
                            tracing::error!("Error: {}", e);
                        } else {
                            return Err(e);
                        }
                    }
                }
                if transactions {
                    let result = config.download_transaction_file(&day);
                    if let Err(e) = result {
                        if cmd.ignore_errors {
                            tracing::error!("Error: {}", e);
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
