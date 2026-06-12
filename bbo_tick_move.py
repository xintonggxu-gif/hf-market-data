#!/usr/bin/env python3
"""
Compare how often the mid moves >= 1 tick for two instruments from BBO logs.

Usage:
  python bbo_tick_move.py EURUSDT bbo_eur.csv 0.0001  SOLUSDC bbo_sol.csv 0.01

CSV format (one row per BBO update):
  timestamp,bid,ask
  2026-06-11T01:19:48.760Z,1.15440,1.15470
timestamp = ISO8601 (UTC). bid/ask = best bid / best ask price.
"""
import sys, pandas as pd, numpy as np

def load(path):
    df = pd.read_csv(path)

    # 兼容你刚刚从 ClickHouse 导出的列名
    if "timestamp" not in df.columns and "recv_ts" in df.columns:
        df = df.rename(columns={
            "recv_ts": "timestamp",
            "bid_price": "bid",
            "ask_price": "ask",
        })

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["mid"] = (df["bid"] + df["ask"]) / 2
    return df

def event_based(df, tick):
    d = df['mid'].diff().dropna()
    dticks = (d / tick).round()
    n = len(dticks)
    span_min = (df['timestamp'].iloc[-1] - df['timestamp'].iloc[0]).total_seconds()/60
    ge1 = (dticks.abs() >= 1).sum()
    return dict(updates=n, span_min=span_min,
                pct_ge1=100*ge1/n if n else float('nan'),
                ge1_per_min=ge1/span_min if span_min else float('nan'),
                updates_per_min=n/span_min if span_min else float('nan'))

def time_based(df, tick, bar='1s'):
    s = df.set_index('timestamp')['mid'].resample(bar).last().ffill()
    dticks = (s.diff().dropna() / tick).round()
    n = len(dticks)
    ge1 = (dticks.abs() >= 1).sum()
    span_min = n * pd.Timedelta(bar).total_seconds()/60
    return dict(bars=n, pct_ge1=100*ge1/n if n else float('nan'),
                ge1_per_min=ge1/span_min if span_min else float('nan'))

def bps_based(df, bar='1s', thr_bps=1.0):
    s = df.set_index('timestamp')['mid'].resample(bar).last().ffill()
    dbps = (s.diff()/s.shift()).dropna().abs()*1e4
    n=len(dbps); ge=(dbps>=thr_bps).sum()
    return dict(pct_ge=100*ge/n if n else float('nan'), thr_bps=thr_bps)

def report(label, path, tick):
    df = load(path)
    print(f"\n===== {label}  (tick={tick}, mid≈{df['mid'].mean():.5f}, "
          f"1 tick≈{tick/df['mid'].mean()*1e4:.2f} bps) =====")
    e = event_based(df, tick)
    print(f"  Event-based: {e['updates']} updates over {e['span_min']:.1f} min "
          f"({e['updates_per_min']:.1f}/min)")
    print(f"     mid moved >=1 tick on {e['pct_ge1']:.0f}% of updates  "
          f"=> {e['ge1_per_min']:.1f} moves/min")
    for bar in ['500ms','1s','5s']:
        t = time_based(df, tick, bar)
        print(f"  Time-based [{bar:>5} bars]: {t['pct_ge1']:.0f}% of bars move >=1 tick "
              f"=> {t['ge1_per_min']:.1f} moves/min")
    b = bps_based(df, '1s', 1.0)
    print(f"  Normalized: {b['pct_ge']:.0f}% of 1s bars move >= {b['thr_bps']:.1f} bps (same bar for both)")
    return df

if __name__ == '__main__':
    a = sys.argv[1:]
    if len(a) >= 6:
        report(a[0], a[1], float(a[2]))
        report(a[3], a[4], float(a[5]))
    else:
        print(__doc__)
