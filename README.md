# Crypto Market Data Pipeline

A small real-time market data pipeline for collecting Binance Spot raw trades and storing them in ClickHouse.

The current version focuses on BTCUSDT Spot trades. It was built to better understand exchange WebSocket data, trade-level market data storage, reconnect handling, and basic data quality validation.

## What this project does

This collector connects to the Binance Spot raw trade WebSocket stream, parses incoming trade messages, and writes them into ClickHouse in batches.

It currently supports:

* Binance Spot raw trade collection through WebSocket
* Async message handling with `asyncio`
* Automatic WebSocket reconnect
* Batch insertion into ClickHouse
* Trade ID continuity checks
* REST backfill for missing trades after reconnect
* Basic SQL checks for duplicates, missing trade IDs, and latency

## Motivation

Raw trade data is useful for understanding short-term market behavior, liquidity, aggressive buy/sell flow, and data quality issues in real-time trading systems.

This project is not a trading bot. It is a data collection and validation layer that can later be used for market microstructure research, strategy backtesting, or feature engineering.

## Tech stack

* Python
* asyncio
* websockets
* httpx
* pandas
* ClickHouse
* Docker

## Data source

The current collector uses Binance Spot raw trade stream:

```text
btcusdt@trade
```

Each message contains a single raw trade with fields such as trade ID, event time, trade time, price, quantity, and buyer-maker flag.

## ClickHouse schema

The current table stores:

| Column           | Description                               |
| ---------------- | ----------------------------------------- |
| `exchange`       | Exchange name, currently `binance`        |
| `market_type`    | Market type, currently `spot`             |
| `symbol`         | Trading pair, e.g. `BTCUSDT`              |
| `trade_id`       | Binance raw trade ID                      |
| `event_ts`       | Exchange event timestamp                  |
| `trade_time`     | Trade execution timestamp                 |
| `recv_ts`        | Local receive timestamp                   |
| `price`          | Trade price                               |
| `qty`            | Trade quantity                            |
| `is_buyer_maker` | Whether the buyer is the market maker     |
| `aggressor_side` | Inferred aggressive side, `buy` or `sell` |
| `run_id`         | Unique ID for each collector run          |

## Pipeline flow

```text
Binance WebSocket
        ↓
Python asyncio collector
        ↓
Parse raw trade message
        ↓
Check trade_id continuity
        ↓
Backfill missing trades with REST API if needed
        ↓
Batch insert into ClickHouse
        ↓
Run SQL data quality checks
```

## Reconnect and backfill logic

The collector keeps track of the latest `trade_id` received during the current run.

If the WebSocket disconnects and reconnects, the next received trade is compared with the previous `trade_id`.

Example:

```text
last trade before disconnect: 100
first trade after reconnect: 108
```

The collector detects that trades `101` to `107` are missing and requests them through Binance REST historical trade data before continuing with the live stream.

This is useful because WebSocket streams are real-time feeds and do not automatically replay messages missed during a disconnection.

## Data quality checks

After collection, I use SQL queries in ClickHouse to check whether the stored data is usable.

### Check total rows

```sql
SELECT count()
FROM market_data.spot_trades_btcusdt_test;
```

### Check duplicate trade IDs

```sql
SELECT
    trade_id,
    count() AS c
FROM market_data.spot_trades_btcusdt_test
GROUP BY trade_id
HAVING c > 1
LIMIT 10;
```

### Check missing trade IDs

```sql
SELECT
    min(trade_id) AS min_id,
    max(trade_id) AS max_id,
    count() AS rows,
    max_id - min_id + 1 AS expected_rows,
    expected_rows - rows AS possible_missing
FROM market_data.spot_trades_btcusdt_test;
```

### Check latency

```sql
SELECT
    avg(dateDiff('millisecond', event_ts, recv_ts)) AS avg_latency_ms,
    quantile(0.5)(dateDiff('millisecond', event_ts, recv_ts)) AS p50_latency_ms,
    quantile(0.95)(dateDiff('millisecond', event_ts, recv_ts)) AS p95_latency_ms,
    max(dateDiff('millisecond', event_ts, recv_ts)) AS max_latency_ms
FROM market_data.spot_trades_btcusdt_test;
```

## Test result

In one test run, the collector stored 18,705 BTCUSDT raw trades.

The data quality checks showed:

```text
rows = 18,705
expected_rows = 18,705
possible_missing = 0
duplicate trade IDs = 0
```

The median latency was around 135 ms.

During a Wi-Fi disconnection test, the collector reconnected and backfilled missing trades. Those backfilled records had higher latency because their `trade_time` came from the original exchange trade time, while `recv_ts` was recorded when the historical trades were fetched.

This behavior is expected, but it also shows why the next version should explicitly label whether a row came from the live WebSocket stream or from REST backfill.

## Current limitations

* Only BTCUSDT Spot trades are collected for now.
* Backfilled trades currently use `trade_time` as `event_ts`, because Binance REST historical trade data does not provide the same event timestamp as the WebSocket stream.
* Rows are not yet labeled by data source, so live WebSocket data and REST backfill data are mixed in the same table.
* The collector is currently a prototype and has not been tested as a long-running production service.

## Next steps

* Add a `data_source` column, such as `websocket` or `rest_backfill`
* Support multiple symbols
* Add Binance `bookTicker` best bid/ask collection
* Add order book depth collection
* Write research notebooks for trade imbalance, volume bursts, latency, and short-term price movement
* Move configuration such as symbol, batch size, and table name into a separate config file

## Status

Working prototype.
