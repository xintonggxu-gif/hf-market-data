import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

import clickhouse_connect
import pandas as pd
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse123"
CLICKHOUSE_DATABASE = "market_data"

SYMBOL = "solusdc"
WS_URL = f"wss://fstream.binance.com/public/stream?streams={SYMBOL}@bookTicker"

BATCH_SIZE = 100
TABLE_NAME = "perp_bbo_solusdc_test"

COLUMNS = [
    "exchange",
    "market_type",
    "symbol",
    "update_id",
    "event_ts",
    "transaction_ts",
    "recv_ts",
    "bid_price",
    "bid_qty",
    "ask_price",
    "ask_qty",
    "run_id",
]


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def create_bbo_table(client):
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME}
    (
        exchange String,
        market_type String,
        symbol String,

        update_id UInt64,

        event_ts DateTime64(3, 'UTC'),
        transaction_ts DateTime64(3, 'UTC'),
        recv_ts DateTime64(3, 'UTC'),

        bid_price Float64,
        bid_qty Float64,
        ask_price Float64,
        ask_qty Float64,

        run_id String
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMMDD(event_ts)
    ORDER BY (exchange, market_type, symbol, event_ts, update_id)
    """)

    print(f"Table {TABLE_NAME} is ready.")


def ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def upload_bbo_df(client, df: pd.DataFrame):
    client.insert_df(TABLE_NAME, df)
    print(f"Inserted {len(df)} perp BBO rows into ClickHouse.")


def flush_buffer(client, buffer):
    if not buffer:
        return

    df = pd.DataFrame(buffer, columns=COLUMNS)
    upload_bbo_df(client, df)

    # 只有 insert 成功后才清空 buffer
    buffer.clear()


def safe_flush_buffer(client, buffer):
    try:
        flush_buffer(client, buffer)
    except Exception as e:
        print(f"Flush failed, keep buffer for retry: {e}")


async def collect_perp_bbo(ws_url, run_id, ch_client):
    buffer = []

    async for ws in connect(
        ws_url,
        ping_interval=20,
        ping_timeout=60,
        close_timeout=5,
    ):
        print(f"Connected to Binance USD-M Futures BBO WebSocket: {SYMBOL.upper()}")
        print(f"WS_URL = {ws_url}")

        try:
            async for message in ws:
                recv_ts = datetime.now(timezone.utc)

                payload = json.loads(message)
                data = payload["data"]

                # st = 1 means USD-M Futures. If st is missing, default to 1.
                if data.get("st", 1) != 1:
                    continue

                row = {
                    "exchange": "binance",
                    "market_type": "um_perp",
                    "symbol": data["s"],

                    "update_id": int(data["u"]),

                    "event_ts": ms_to_utc(int(data["E"])),
                    "transaction_ts": ms_to_utc(int(data["T"])),
                    "recv_ts": recv_ts,

                    "bid_price": float(data["b"]),
                    "bid_qty": float(data["B"]),
                    "ask_price": float(data["a"]),
                    "ask_qty": float(data["A"]),

                    "run_id": run_id,
                }

                # 方案 A：不检查 update_id 顺序，收到什么就全部存
                buffer.append(row)

                if len(buffer) >= BATCH_SIZE:
                    safe_flush_buffer(ch_client, buffer)

        except ConnectionClosed as e:
            print(f"Perp connection closed: {e}. Reconnecting...")
            safe_flush_buffer(ch_client, buffer)
            continue

        except asyncio.CancelledError:
            print("Perp collector cancelled. Flushing buffer and exiting...")
            safe_flush_buffer(ch_client, buffer)
            raise

        except Exception as e:
            print(f"Perp unexpected error: {e}. Reconnecting after 3 seconds...")
            safe_flush_buffer(ch_client, buffer)
            await asyncio.sleep(3)
            continue


async def main():
    ch_client = get_clickhouse_client()
    create_bbo_table(ch_client)

    run_id = str(uuid4())

    print(f"run_id = {run_id}")
    print("Starting Binance USD-M Futures SOLUSDC BBO collector...")

    await collect_perp_bbo(
        ws_url=WS_URL,
        run_id=run_id,
        ch_client=ch_client,
    )


if __name__ == "__main__":
    asyncio.run(main())