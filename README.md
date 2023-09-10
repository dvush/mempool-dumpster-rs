# Intro

This is a helper library and cli tool to work with https://github.com/flashbots/mempool-dumpster/, provider of historical 
mempool data for Ethereum.

There are 3 kind of files that are provided for each day:
* **sourcelog** - has timestamp when transactions arrived with the source of transactions
* **transaction-data** - has the earliest timestamp when transaction was seen and transaction data (from, to, nonce, etc)
* **transactions** - has the same data as transaction-data with a full raw transaction


# CLI

CLI help with downloading files and converting all of them to the parquet format.

Set directory to store downloaded files with `MEMPOOL_DATADIR` or cli arg `--datadir`.


```
mempool-dumpster list-months # list available months
mempool-dumpster list-days 2023-09 # list available days in a month


mempool-dumpster get 2023-09-01 # download sourcelog and transactions files for a day
mempool-dumpster get 2023-09-01 --sourcelog 
mempool-dumpster get 2023-09-01 --sourcelog --transaction-data 
mempool-dumpster get 2023-09-01 --transactions 

mempool-dumpster --datadir ./data get 2023-09-01 --transactions 

mempool-dumpster get 2023-09 # download sourcelog and transactions files for a month
```

## Install

```shell
cargo install mempool-dumpster

# from local repo
cargo install --path .
```

# Data Format

Files will be downloaded from mempool-dumpster website, converted to the compressed parquet files if needed 
and stored in the data directory. There would be one file per file type per day and each type would be in separate directory.

```text
├── sourcelog
│   └── 2023-08-31_sourcelog.parquet
├── transaction-data
│   └── 2023-08-31_transaction-data.parquet
└── transactions
    └── 2023-09-08.parquet
```

## Sourcelog

Path: 

`$DATA_DIR/sourcelog/`

Columns: 

* `INT64 timestamp (TIMESTAMP(MILLIS,false))` timestamp with millisecond precision
* `BYTE_ARRAY hash (STRING)` 0x-prefixed hex-encoded tx hash
* `BYTE_ARRAY source (STRING)` source of transactions 

Example:
```text
┌─────────────────────────┬────────────────────────────────────────────────────────────────────┬─────────┐
│        timestamp        │                                hash                                │ source  │
│        timestamp        │                              varchar                               │ varchar │
├─────────────────────────┼────────────────────────────────────────────────────────────────────┼─────────┤
│ 2023-09-07 00:00:00.11  │ 0x8d7fbf19e135fe78cf27cd44865679eedb1b15b1816d97babfaa6a55a06b9261 │ infura  │
│ 2023-09-07 00:00:00.121 │ 0xc12ac4752baf7cebd9fe81d9c82bc1b2cc263f70d0099e4969ed32b2a7d55557 │ alchemy │
│ 2023-09-07 00:00:00.13  │ 0xce024c2799b5e47011e68b62daa610c54dabdd8327e3bcce97642bf416eb4842 │ blx     │
│ 2023-09-07 00:00:00.159 │ 0xae47341cce1913e794548330759fc8a99e3740112365c4ba49fbe8bcbafcddd6 │ local   │
│ 2023-09-07 00:00:00.16  │ 0xe45826dd9f37236f05a3fb4ff8fd5d04f8460cae1ea6eaccbdbd93337229155f │ local   │
└─────────────────────────┴────────────────────────────────────────────────────────────────────┴─────────┘
```

## Transaction Data

Path: 

`$DATADIR/transaction-data/`

Columns:

* `INT64 timestamp (TIMESTAMP(MILLIS,false))` earliest timestamp when transaction appeared
* `BYTE_ARRAY hash (STRING)` 0x-prefixed hex-encoded tx hash
* `BYTE_ARRAY chainId (STRING)` string of chain id (base 10)
* `BYTE_ARRAY from (STRING)` 0x-prefixed hex-encoded from address
* `BYTE_ARRAY to (STRING)` 0x-prefixed hex-encoded to address(empty for contract creation)
* `BYTE_ARRAY value (STRING)` string of value in wei (base 10)
* `BYTE_ARRAY nonce (STRING)` string of nonce (base 10)
* `BYTE_ARRAY gas (STRING)` string of gas limit (base 10)
* `BYTE_ARRAY gasPrice (STRING)` string of gas price in wei (base 10)
* `BYTE_ARRAY gasTipCap (STRING)` string of gas tip cap in wei (base 10)
* `BYTE_ARRAY gasFeeCap (STRING)` string of gas fee cap in wei (base 10)
* `INT64 dataSize` size of data field in bytes
* `BYTE_ARRAY data4Bytes (STRING)` first 4 bytes of data field (0x-prefixed hex-encoded)


Example:
```text
┌──────────────────────┬──────────────────────┬─────────┬──────────────────────┬──────────────────────┬───┬─────────────┬───────────┬─────────────┬──────────┬────────────┐
│      timestamp       │         hash         │ chainId │         from         │          to          │ … │  gasPrice   │ gasTipCap │  gasFeeCap  │ dataSize │ data4Bytes │
│      timestamp       │       varchar        │ varchar │       varchar        │       varchar        │   │   varchar   │  varchar  │   varchar   │  int64   │  varchar   │
├──────────────────────┼──────────────────────┼─────────┼──────────────────────┼──────────────────────┼───┼─────────────┼───────────┼─────────────┼──────────┼────────────┤
│ 2023-08-08 00:00:0…  │ 0xfd357043a6dca490…  │ 1       │ 0x9bd59d85cdd41693…  │ 0xb2d513b9a54a9999…  │ … │ 10000000000 │ 500000000 │ 10000000000 │        0 │            │
│ 2023-08-08 00:00:0…  │ 0x8c334ff637d8df9f…  │ 1       │ 0x329b4a3343da7b5b…  │ 0xb2d513b9a54a9999…  │ … │ 10000000000 │ 500000000 │ 10000000000 │        0 │            │
│ 2023-08-08 00:00:0…  │ 0x5cf6f66511451fa3…  │ 1       │ 0xa0915532179371db…  │ 0xb2d513b9a54a9999…  │ … │ 10000000000 │ 500000000 │ 10000000000 │        0 │            │
│ 2023-08-08 00:00:0…  │ 0x91c3e7d726f5834a…  │ 1       │ 0xd8aa8f3be2fb0c79…  │ 0x0ed1bcc400acd345…  │ … │ 23385350014 │ 444240358 │ 23385350014 │        4 │ 0x98e5b12a │
│ 2023-08-08 00:00:0…  │ 0xdbbf976af37eb599…  │ 1       │ 0xd8aa8f3be2fb0c79…  │ 0x2659dbe2d2e6f880…  │ … │ 23385350014 │ 444240358 │ 23385350014 │        4 │ 0x98e5b12a │
├──────────────────────┴──────────────────────┴─────────┴──────────────────────┴──────────────────────┴───┴─────────────┴───────────┴─────────────┴──────────┴────────────┤
```

## Transactions

Path: 
`$DATADIR/transactions/`

Columns:

* `INT64 timestamp (TIMESTAMP(MILLIS,false))` earliest timestamp when transaction appeared
* `BYTE_ARRAY hash (STRING)` 0x-prefixed hex-encoded tx hash
* `BYTE_ARRAY chainId (STRING)` string of chain id (base 10)
* `BYTE_ARRAY from (STRING)` 0x-prefixed hex-encoded from address
* `BYTE_ARRAY to (STRING)` 0x-prefixed hex-encoded to address(empty for contract creation)
* `BYTE_ARRAY value (STRING)` string of value in wei (base 10)
* `BYTE_ARRAY nonce (STRING)` string of nonce (base 10)
* `BYTE_ARRAY gas (STRING)` string of gas limit (base 10)
* `BYTE_ARRAY gasPrice (STRING)` string of gas price in wei (base 10)
* `BYTE_ARRAY gasTipCap (STRING)` string of gas tip cap in wei (base 10)
* `BYTE_ARRAY gasFeeCap (STRING)` string of gas fee cap in wei (base 10)
* `INT64 dataSize` size of data field in bytes
* `BYTE_ARRAY data4Bytes (STRING)` first 4 bytes of data field (0x-prefixed hex-encoded)
* `BYTE_ARRAY rawTx` raw transaction bytes

Example:
```text
┌──────────────────────┬──────────────────────┬─────────┬──────────────────────┬──────────────────────┬───┬─────────────┬──────────┬────────────┬──────────────────────┐
│      timestamp       │         hash         │ chainId │         from         │          to          │ … │  gasFeeCap  │ dataSize │ data4Bytes │        rawTx         │
│      timestamp       │       varchar        │ varchar │       varchar        │       varchar        │   │   varchar   │  int64   │  varchar   │         blob         │
├──────────────────────┼──────────────────────┼─────────┼──────────────────────┼──────────────────────┼───┼─────────────┼──────────┼────────────┼──────────────────────┤
│ 2023-09-08 00:00:0…  │ 0x07017d7bc566bdb9…  │ 1       │ 0x08dc8ffc2db71ea0…  │ 0x6982508145454ce3…  │ … │ 24000000000 │       68 │ 0xa9059cbb │ \x02\xF8\xB2\x01\x…  │
│ 2023-09-08 00:00:0…  │ 0x28ea098d9dfd59e3…  │ 1       │ 0x19777274b0e613bd…  │ 0x28c6c06298d514db…  │ … │ 24000000000 │        0 │            │ \x02\xF8s\x01\x02\…  │
│ 2023-09-08 00:00:0…  │ 0xb03b8cff6dc46893…  │ 1       │ 0xe67fc443fa1d4927…  │ 0x514910771af9ca65…  │ … │ 24000000000 │       68 │ 0xa9059cbb │ \x02\xF8\xB2\x01\x…  │
│ 2023-09-08 00:00:0…  │ 0x272c5580ed4ba3f4…  │ 1       │ 0xf6492394b174f678…  │ 0x95ad61b0a150d792…  │ … │ 24000000000 │       68 │ 0xa9059cbb │ \x02\xF8\xB0\x01\x…  │
│ 2023-09-08 00:00:0…  │ 0x13fc9ac90f6eede4…  │ 1       │ 0x61b7e18ba8ba0413…  │ 0xdac17f958d2ee523…  │ … │ 24000000000 │       68 │ 0xa9059cbb │ \x02\xF8\xB2\x01\x…  │
├──────────────────────┴──────────────────────┴─────────┴──────────────────────┴──────────────────────┴───┴─────────────┴──────────┴────────────┴──────────────────────┤
```

