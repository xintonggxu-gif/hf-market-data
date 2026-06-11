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

WS_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade"

BATCH_SIZE = 100
TABLE_NAME = "spot_trades_btcusdt_test"


COLUMNS = [
    "exchange",
    "market_type",
    "symbol",
    "trade_id",
    "event_ts",
    "trade_time",
    "recv_ts",
    "price",
    "qty",
    "is_buyer_maker",
    "aggressor_side",
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


def create_trades_table(client):
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME}
    (
        exchange String,
        market_type String,
        symbol String,

        trade_id UInt64,

        event_ts DateTime64(3, 'UTC'),
        trade_time DateTime64(3, 'UTC'),
        recv_ts DateTime64(3, 'UTC'),

        price Float64,
        qty Float64,

        is_buyer_maker Bool,
        aggressor_side String,

        run_id String
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMMDD(trade_time)
    ORDER BY (exchange, market_type, symbol, trade_time, trade_id)
    """)
    print(f"Table {TABLE_NAME} is ready.")


def upload_trades_df(client, df: pd.DataFrame):
    client.insert_df(TABLE_NAME, df)
    print(f"Inserted {len(df)} rows into ClickHouse.")


def ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def flush_buffer(client, buffer):
    if not buffer:
        return

    df = pd.DataFrame(buffer, columns=COLUMNS)
    upload_trades_df(client, df)
    buffer.clear()


async def get_binance_spot_trades(run_id, client):
    buffer = []

    async for ws in connect(
        WS_URL,
        ping_interval=20,
        ping_timeout=60,
        close_timeout=10,
    ):
        print("Connected to Binance BTCUSDT spot trade WebSocket.")

        try:
            async for message in ws:
                recv_ts = datetime.now(timezone.utc)

                payload = json.loads(message)
                data = payload["data"]

                row = {
                    "exchange": "binance",
                    "market_type": "spot",
                    "symbol": data["s"],

                    "trade_id": int(data["t"]),

                    "event_ts": ms_to_utc(int(data["E"])),
                    "trade_time": ms_to_utc(int(data["T"])),
                    "recv_ts": recv_ts,

                    "price": float(data["p"]),
                    "qty": float(data["q"]),

                    "is_buyer_maker": bool(data["m"]),
                    "aggressor_side": "sell" if data["m"] else "buy",

                    "run_id": run_id,
                }

                buffer.append(row)

                if len(buffer) >= BATCH_SIZE:
                    flush_buffer(client, buffer)

        except ConnectionClosed as e:
            print(f"Connection closed: {e}. Reconnecting...")
            flush_buffer(client, buffer)
            continue

        except asyncio.CancelledError:
            print("Cancelled. Flushing buffer and exiting...")
            flush_buffer(client, buffer)
            raise

        except Exception as e:
            print(f"Unexpected error: {e}. Reconnecting after 5 seconds...")
            flush_buffer(client, buffer)
            await asyncio.sleep(5)
            continue


async def main():
    client = get_clickhouse_client()

    create_trades_table(client)

    run_id = str(uuid4())

    print(f"run_id = {run_id}")
    print("Connecting to Binance BTCUSDT spot trade WebSocket...")

    await get_binance_spot_trades(run_id, client)


if __name__ == "__main__":
    asyncio.run(main())