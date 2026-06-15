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

BATCH_SIZE = 200
FLUSH_INTERVAL = 1.0
QUEUE_MAXSIZE = 10000

TABLE_NAME = "spot_trades_btcusdt_test"

# 测试阶段可以 True：每次运行都删表重建
# 正式采集时一定改成 False，否则每次运行都会删掉历史数据
DROP_TABLE_ON_START = True

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
    "data_source",
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
    if DROP_TABLE_ON_START:
        client.command(f"DROP TABLE IF EXISTS {TABLE_NAME}")

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

        run_id String,
        data_source String
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMMDD(trade_time)
    ORDER BY (exchange, market_type, symbol, trade_time, trade_id)
    """)

    print(f"Table {TABLE_NAME} is ready.")


def ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


async def upload_trades_df(client, df: pd.DataFrame):
    if df.empty:
        return

    await asyncio.to_thread(
        client.insert_df,
        TABLE_NAME,
        df,
    )

    print(f"Inserted {len(df)} rows into ClickHouse.")


async def flush_batch(client, batch):
    if not batch:
        return

    df = pd.DataFrame(batch, columns=COLUMNS)
    await upload_trades_df(client, df)
    batch.clear()


async def write_to_clickhouse(client, queue: asyncio.Queue):
    """
    Consumer / writer:
    专门从 queue 里拿 row，攒成 batch，然后写 ClickHouse。
    """
    batch = []

    try:
        while True:
            try:
                row = await asyncio.wait_for(
                    queue.get(),
                    timeout=FLUSH_INTERVAL,
                )

            except asyncio.TimeoutError:
                # 1 秒内没有新 row，但 batch 里有旧数据，也先写掉
                if batch:
                    try:
                        await flush_batch(client, batch)
                    except Exception as e:
                        print(f"ClickHouse flush error: {e}")
                        await asyncio.sleep(3)
                continue

            # None 是停止信号
            if row is None:
                break

            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                try:
                    await flush_batch(client, batch)
                except Exception as e:
                    print(f"ClickHouse flush error: {e}")
                    await asyncio.sleep(3)

    except asyncio.CancelledError:
        print("Writer cancelled. Flushing remaining batch...")
        raise

    finally:
        if batch:
            try:
                await flush_batch(client, batch)
            except Exception as e:
                print(f"Final flush error: {e}")

        print("ClickHouse writer stopped.")


def check_data_gap(currentid, lastid):
    """
    正常情况：
        currentid == lastid + 1

    缺数据情况：
        currentid > lastid + 1
    """
    return lastid is not None and currentid > lastid + 1


async def fill_data_gap(symbol, fromid, toid, run_id, queue: asyncio.Queue):
    """
    用 Binance REST historicalTrades 补 WebSocket 断线期间缺失的 trade。

    注意：
    这里不直接写 ClickHouse。
    REST 补回来的 row 也放入 queue，由 writer 统一写库。
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

            last_used_id = None

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
                    # 所以这里暂时用 trade_time 填 event_ts
                    "event_ts": trade_time,
                    "trade_time": trade_time,
                    "recv_ts": datetime.now(timezone.utc),

                    "price": float(trade["price"]),
                    "qty": float(trade["qty"]),

                    "is_buyer_maker": bool(trade["isBuyerMaker"]),
                    "aggressor_side": "sell" if trade["isBuyerMaker"] else "buy",

                    "run_id": run_id,
                    "data_source": "rest_backfill",
                }

                await queue.put(row)

                total_backfilled += 1
                last_used_id = trade_id

            last_returned_id = int(trades[-1]["id"])

            if last_returned_id >= toid:
                break

            if last_used_id is None:
                next_id = last_returned_id + 1
            else:
                next_id = last_used_id + 1

            await asyncio.sleep(0.1)

    print(
        f"Finished backfill. "
        f"Backfilled {total_backfilled} trades from {fromid} to {toid}."
    )


async def get_binance_spot_trades(ws_url, run_id, symbol, queue: asyncio.Queue):
    """
    Producer / reader:
    只负责：
    1. 收 WebSocket
    2. 检查 trade_id gap
    3. 需要时 REST backfill
    4. 把 row 放入 queue

    不直接写 ClickHouse。
    """
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

                # 1. 检查 currentid 和 lastid 是否连续
                if check_data_gap(currentid, lastid):
                    missing_from = lastid + 1
                    missing_to = currentid - 1
                    missing_count = missing_to - missing_from + 1

                    print(
                        f"Trade gap detected: "
                        f"missing {missing_from} to {missing_to}, "
                        f"count={missing_count}"
                    )

                    try:
                        await fill_data_gap(
                            symbol=symbol,
                            fromid=missing_from,
                            toid=missing_to,
                            run_id=run_id,
                            queue=queue,
                        )
                    except Exception as e:
                        print(f"Backfill failed: {e}. Continue with current trade.")

                elif lastid is not None and currentid <= lastid:
                    print(
                        f"Duplicate or out-of-order trade skipped: "
                        f"current={currentid}, last={lastid}"
                    )
                    continue

                # 2. 构造当前 WebSocket trade
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
                    "data_source": "websocket",
                }

                await queue.put(row)

                # 3. 当前 trade 成功进入 queue 后，更新 lastid
                lastid = currentid

        except ConnectionClosed as e:
            print(f"Connection closed: {e}. Reconnecting...")
            continue

        except asyncio.CancelledError:
            print("Reader cancelled.")
            raise

        except Exception as e:
            print(f"Unexpected error: {e}. Reconnecting after 3 seconds...")
            await asyncio.sleep(3)
            continue


async def main():
    queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

    client = get_clickhouse_client()
    create_trades_table(client)

    run_id = str(uuid4())

    print(f"run_id = {run_id}")
    print(f"Connecting to Binance {SYMBOL.upper()} spot trade WebSocket...")

    writer_task = asyncio.create_task(
        write_to_clickhouse(client, queue)
    )

    try:
        await get_binance_spot_trades(
            ws_url=WS_URL,
            run_id=run_id,
            symbol=SYMBOL,
            queue=queue,
        )

    except asyncio.CancelledError:
        print("Main cancelled.")
        raise

    finally:
        print("Stopping writer...")

        # 给 writer 一个停止信号，让它 flush 剩余 batch 后退出
        await queue.put(None)

        await writer_task

        print("Program exited cleanly.")


if __name__ == "__main__":
    asyncio.run(main())