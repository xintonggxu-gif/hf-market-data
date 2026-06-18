import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

import clickhouse_connect
import pandas as pd
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

import logging
from logging.handlers import RotatingFileHandler



logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse123"
CLICKHOUSE_DATABASE = "market_data"


SYMBOLS_FILE = "symbols_binance_perp.txt"

# 一张表存所有 Binance perp BBO
TABLE_NAME = "binance_perp_bbo_test"

BATCH_SIZE = 10000
FLUSH_INTERVAL = 3.0
QUEUE_MAXSIZE = 100000

DROP_TABLE_ON_START = False

COLUMNS = [
    "exchange",
    "market_type",
    "symbol",
    "update_id",
    "recv_ts",
    "exchange_ts",
    "engine_ts",
    "bid_price",
    "bid_qty",
    "ask_price",
    "ask_qty",
    "micro_p",
    "run_id",
    "data_source",
]

def setup_logging(log_file: str = "binance_perp_bbo.log"):
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=100 * 1024 * 1024,
        backupCount=5,
    )
    file_handler.setFormatter(formatter)

    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
def load_symbols_from_file(path: str) -> list[str]:
    symbols = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line:
                continue

            if line.startswith("#"):
                continue

            symbols.append(line.lower())

    # 去重，但保持原顺序
    symbols = list(dict.fromkeys(symbols))

    if not symbols:
        raise ValueError(f"No symbols loaded from {path}")

    return symbols

def build_combined_ws_url(symbols: list[str]) -> str:
    streams = "/".join(
        f"{symbol.lower()}@bookTicker"
        for symbol in symbols
    )
    return f"wss://fstream.binance.com/public/stream?streams={streams}"


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def create_bbo_table(client):
    if DROP_TABLE_ON_START:
        client.command(f"DROP TABLE IF EXISTS {TABLE_NAME}")

    client.command(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME}
    (
        exchange String,
        market_type String,
        symbol String,

        update_id UInt64,
        recv_ts DateTime64(3, 'UTC'),
        exchange_ts DateTime64(3, 'UTC'),
        engine_ts DateTime64(3, 'UTC'),

        bid_price Float64,
        bid_qty Float64,
        ask_price Float64,
        ask_qty Float64,

        micro_p Float64,
        run_id String,
        data_source String
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMMDD(recv_ts)
    ORDER BY (exchange, market_type, symbol, recv_ts, update_id)
    """)

    logger.info("Table %s is ready.", TABLE_NAME)


async def upload_bbo_df(client, df: pd.DataFrame):
    if df.empty:
        return

    await asyncio.to_thread(
        client.insert_df,
        TABLE_NAME,
        df,
    )

    logger.info("Inserted %s rows into ClickHouse.", len(df))


async def flush_batch(client, batch):
    if not batch:
        return

    df = pd.DataFrame(batch, columns=COLUMNS)
    await upload_bbo_df(client, df)
    batch.clear()


async def write_to_clickhouse(client, queue: asyncio.Queue):
    batch = []

    try:
        while True:
            try:
                row = await asyncio.wait_for(
                    queue.get(),
                    timeout=FLUSH_INTERVAL,
                )

            except asyncio.TimeoutError:
                if batch:
                    try:
                        await flush_batch(client, batch)
                    except Exception:
                        logger.exception("ClickHouse flush error")
                        await asyncio.sleep(3)
                continue

            if row is None:
                break

            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                try:
                    await flush_batch(client, batch)
                except Exception:
                    logger.exception("ClickHouse flush error")
                    await asyncio.sleep(3)

    except asyncio.CancelledError:
        logger.info("Writer cancelled. Flushing remaining batch...")
        raise

    finally:
        if batch:
            try:
                await flush_batch(client, batch)
            except Exception:
                logger.exception("Final flush error")

        logger.info("ClickHouse writer stopped.")


async def get_binance_perp_bbo_multi(
    ws_url: str,
    run_id: str,
    symbols: list[str],
    queue: asyncio.Queue,
):
    expected_symbols = {s.upper() for s in symbols}

    # 关键变化：每个 symbol 各自维护 last_update_id
    last_update_id_by_symbol = {}

    async for ws in connect(
        ws_url,
        ping_interval=20,
        ping_timeout=60,
        close_timeout=5,
    ):
        logger.info("Connected to Binance perp BBO WebSocket.")
        logger.info(
            "Subscribed symbols: %s",
            ", ".join(sorted(expected_symbols)),
        )

        try:
            async for message in ws:
                recv_ts = datetime.now(timezone.utc)

                payload = json.loads(message)

                # combined stream:
                # {"stream": "btcusdt@bookTicker", "data": {...}}
                data = payload.get("data", payload)

                symbol = data["s"]

                if symbol not in expected_symbols:
                    logger.warning("Unexpected symbol skipped: %s", symbol)
                    continue

                current_update_id = int(data["u"])
                last_update_id = last_update_id_by_symbol.get(symbol)

                if (
                    last_update_id is not None
                    and current_update_id <= last_update_id
                ):
                    logger.warning(
                        "Duplicate or out-of-order BBO skipped: "
                        "symbol=%s, current=%s, last=%s",
                        symbol,
                        current_update_id,
                        last_update_id,
                    )
                    continue

                exchange_ts = ms_to_utc(int(data["E"]))
                engine_ts = ms_to_utc(int(data["T"]))

                bid_price = float(data["b"])
                bid_qty = float(data["B"])
                ask_price = float(data["a"])
                ask_qty = float(data["A"])

                qty_sum = bid_qty + ask_qty
                if qty_sum <= 0:
                    continue

                micro_p = (
                    bid_price * ask_qty
                    + ask_price * bid_qty
                ) / qty_sum

                row = {
                    "exchange": "binance",
                    "market_type": "perp",
                    "symbol": symbol,

                    "update_id": current_update_id,
                    "recv_ts": recv_ts,
                    "exchange_ts": exchange_ts,
                    "engine_ts": engine_ts,

                    "bid_price": bid_price,
                    "bid_qty": bid_qty,
                    "ask_price": ask_price,
                    "ask_qty": ask_qty,

                    "micro_p": micro_p,
                    "run_id": run_id,
                    "data_source": "websocket",
                }

                await queue.put(row)

                last_update_id_by_symbol[symbol] = current_update_id

        except ConnectionClosed as e:
            logger.warning("Connection closed: %s. Reconnecting...", e)
            continue

        except asyncio.CancelledError:
            logger.info("Reader cancelled.")
            raise

        except KeyError:
            logger.exception(
                "Missing expected Binance field. Reconnecting after 3 seconds."
            )
            await asyncio.sleep(3)
            continue

        except Exception:
            logger.exception(
                "Unexpected error. Reconnecting after 3 seconds."
            )
            await asyncio.sleep(3)
            continue


async def main():
    setup_logging()
    SYMBOLS = load_symbols_from_file(SYMBOLS_FILE)
    symbols = [s.lower() for s in SYMBOLS]
    ws_url = build_combined_ws_url(symbols)

    queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

    client = get_clickhouse_client()
    create_bbo_table(client)

    run_id = str(uuid4())

    logger.info("run_id = %s", run_id)
    logger.info("Connecting to Binance perp BBO WebSocket...")
    logger.info("ws_url = %s", ws_url)

    writer_task = asyncio.create_task(
        write_to_clickhouse(client, queue)
    )

    try:
        await get_binance_perp_bbo_multi(
            ws_url=ws_url,
            run_id=run_id,
            symbols=symbols,
            queue=queue,
        )

    except asyncio.CancelledError:
        logger.info("Main cancelled.")
        raise

    finally:
        logger.info("Stopping writer...")

        await queue.put(None)
        await writer_task

        logger.info("Program exited cleanly.")


if __name__ == "__main__":
    asyncio.run(main())