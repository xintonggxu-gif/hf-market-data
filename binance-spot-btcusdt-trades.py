import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import clickhouse_connect
import pandas as pd
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse123"
CLICKHOUSE_DATABASE = "market_data"

SYMBOL = "btcusdt"
WS_URL = f"wss://stream.binance.com:9443/stream?streams={SYMBOL}@trade"

BATCH_SIZE = 100
TABLE_NAME = "spot_trades_btcusdt_test"

# 如果 historicalTrades 返回 401 / 403，可能需要填 Binance API key
# 只读 market data，不需要 secret
BINANCE_API_KEY = ""

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
    """
    buffer 满了、断线、取消程序时，把已经收到但还没 insert 的数据写入 ClickHouse。
    """
    if not buffer:
        return

    df = pd.DataFrame(buffer, columns=COLUMNS)
    upload_trades_df(client, df)
    buffer.clear()


def check_data_gap(currentid, lastid):
    """
    正常情况：
        currentid == lastid + 1

    缺数据情况：
        currentid > lastid + 1
    """
    return lastid is not None and currentid > lastid + 1


async def fill_data_gap(symbol, fromid, toid, run_id, ch_client):
    """
    用 Binance REST historicalTrades 补 WebSocket 断线期间缺失的 trade。

    fromid: 缺失区间第一条 trade_id
    toid:   缺失区间最后一条 trade_id
    """
    if fromid > toid:
        return

    url = "https://api.binance.com/api/v3/historicalTrades"

    headers = {}
    if BINANCE_API_KEY:
        headers["X-MBX-APIKEY"] = BINANCE_API_KEY

    next_id = fromid
    total_backfilled = 0

    print(f"Start backfilling trades from {fromid} to {toid}...")

    async with httpx.AsyncClient(timeout=20) as http_client:
        while next_id <= toid:
            params = {
                "symbol": symbol.upper(),
                "limit": 1000,
                "fromId": next_id,
            }

            resp = await http_client.get(
                url,
                params=params,
                headers=headers if headers else None,
            )
            resp.raise_for_status()

            trades = resp.json()

            if not trades:
                print(f"No trades returned when backfilling fromId={next_id}")
                break

            rows = []

            for trade in trades:
                trade_id = int(trade["id"])

                if trade_id > toid:
                    break

                trade_time = ms_to_utc(int(trade["time"]))

                row = {
                    "exchange": "binance",
                    "market_type": "spot",
                    "symbol": symbol.upper(),

                    "trade_id": trade_id,

                    # REST historicalTrades 没有 WebSocket 里的 event time E
                    # 所以这里暂时用 trade_time 填 event_ts，保证类型是 DateTime64
                    "event_ts": trade_time,
                    "trade_time": trade_time,
                    "recv_ts": datetime.now(timezone.utc),

                    "price": float(trade["price"]),
                    "qty": float(trade["qty"]),

                    "is_buyer_maker": bool(trade["isBuyerMaker"]),
                    "aggressor_side": "sell" if trade["isBuyerMaker"] else "buy",

                    "run_id": run_id,
                }

                rows.append(row)

            if rows:
                df = pd.DataFrame(rows, columns=COLUMNS)
                upload_trades_df(ch_client, df)
                total_backfilled += len(rows)

            last_returned_id = int(trades[-1]["id"])
            next_id = last_returned_id + 1

            if last_returned_id >= toid:
                break

            await asyncio.sleep(0.1)

    print(
        f"Finished backfill. "
        f"Backfilled {total_backfilled} trades from {fromid} to {toid}."
    )


async def get_binance_spot_trades(ws_url, run_id, client, symbol):
    buffer = []

    # 这里只记录本次程序运行期间的 lastid
    # WebSocket 断线但程序没退出时，lastid 会保留
    lastid = None

    async for ws in connect(
        ws_url,
        ping_interval=20,
        ping_timeout=60,
        close_timeout=5,
    ):
        print(f"Connected to Binance {symbol.upper()} spot trade WebSocket.")

        try:
            async for message in ws:
                recv_ts = datetime.now(timezone.utc)

                payload = json.loads(message)
                data = payload["data"]

                currentid = int(data["t"])

                # 1. 先检查 currentid 和 lastid 是否连续
                if check_data_gap(currentid, lastid):
                    missing_from = lastid + 1
                    missing_to = currentid - 1
                    missing_count = missing_to - missing_from + 1

                    print(
                        f"Trade gap detected: "
                        f"missing {missing_from} to {missing_to}, "
                        f"count={missing_count}"
                    )

                    # 先把当前 buffer 里已有的正常数据写入 ClickHouse
                    flush_buffer(client, buffer)

                    # 再补缺失的 trade
                    await fill_data_gap(
                        symbol=symbol,
                        fromid=missing_from,
                        toid=missing_to,
                        run_id=run_id,
                        ch_client=client,
                    )

                elif lastid is not None and currentid <= lastid:
                    print(
                        f"Duplicate or out-of-order trade skipped: "
                        f"current={currentid}, last={lastid}"
                    )
                    continue

                # 2. gap 处理完，再构造当前 WebSocket trade
                row = {
                    "exchange": "binance",
                    "market_type": "spot",
                    "symbol": data["s"],

                    "trade_id": currentid,

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

                # 3. 当前 trade 成功进入 buffer 后，更新 lastid
                lastid = currentid

                # 4. buffer 满 100 条就写入 ClickHouse
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
            print(f"Unexpected error: {e}. Reconnecting after 3 seconds...")
            flush_buffer(client, buffer)
            await asyncio.sleep(3)
            continue


async def main():
    client = get_clickhouse_client()

    create_trades_table(client)

    run_id = str(uuid4())

    print(f"run_id = {run_id}")
    print(f"Connecting to Binance {SYMBOL.upper()} spot trade WebSocket...")

    await get_binance_spot_trades(
        ws_url=WS_URL,
        run_id=run_id,
        client=client,
        symbol=SYMBOL,
    )


if __name__ == "__main__":
    asyncio.run(main())