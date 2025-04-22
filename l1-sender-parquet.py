#!/usr/bin/env python3
from datetime import datetime, timezone
import time
import os
import sys
from glob import glob
import argparse
import pyarrow.parquet as pq
import psycopg as pg
from questdb.ingress import Sender, TimestampNanos

# global state for simulate mode and record counter
simulation_initialized = False
simulation_first_event_ts = None
simulation_base_ns = None
total = 0

def int_with_underscores(value):
    return int(value.replace("_", ""))

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Replay Parquet files into QuestDB via PyArrow, streaming row-groups."
    )
    parser.add_argument(
        "file_pattern", nargs="?", default="./*.parquet",
        help="Glob pattern for Parquet files"
    )
    parser.add_argument(
        "--limit", type=int_with_underscores, default=1_000_000,
        help="Max records to process before exiting"
    )
    parser.add_argument("--host",      required=True, help="QuestDB host")
    parser.add_argument("--pg-port",   default="8812",   help="QuestDB pg port")
    parser.add_argument("--user",      default="admin",  help="QuestDB pg user")
    parser.add_argument("--password",  default="quest",  help="QuestDB pg password")
    parser.add_argument(
        "--http-conn-host", required=True,
        help="QuestDB HTTP host, e.g. 172.31.42.41:9000"
    )
    parser.add_argument("--http-token", required=True, help="QuestDB HTTP token")
    parser.add_argument(
        "--timestamp-replace", default="realtime",
        help="Timestamp mode: realtime|original|simulate|YYYY-MM-DD"
    )
    parser.add_argument(
        "--delay", type=int, default=0,
        help="Delay between events in ms (realtime only)"
    )
    parser.add_argument(
        "--table-suffix", type=str, default="",
        help="Optional suffix for table and view names"
    )
    return parser.parse_args()

def ensure_table_exists(args):
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"
    ddl = f"""
    CREATE TABLE IF NOT EXISTS top_of_book{args.table_suffix} (
        timestamp          TIMESTAMP,
        symbol             SYMBOL CAPACITY 15000,
        venue              SYMBOL,
        expiry_date        TIMESTAMP,
        strike             DOUBLE,
        option_type        SYMBOL,
        side               SYMBOL,
        action             SYMBOL,
        depth              LONG,
        price              DOUBLE,
        size               LONG,
        bid_size           LONG,
        ask_size           LONG,
        bid_price          DOUBLE,
        ask_price          DOUBLE,
        bid_count          LONG,
        ask_count          LONG,
        ts_event           TIMESTAMP,
        ts_recv            TIMESTAMP,
        venue_description  VARCHAR
    ) timestamp(timestamp)
      PARTITION BY HOUR;
    """
    with pg.connect(conn_str, autocommit=True) as conn:
        conn.execute(ddl)

def ensure_mat_views_exist(args):
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"
    mv1 = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS OHLC_30s{args.table_suffix} AS (
      SELECT timestamp,
             symbol,
             first(price) AS open,
             max(price)   AS high,
             min(price)   AS low,
             last(price)  AS close,
             sum(size)    AS volume
        FROM top_of_book{args.table_suffix}
       WHERE size > 0
       SAMPLE BY 30s
    ) PARTITION BY DAY;
    """
    mv2 = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS OHLC_1h{args.table_suffix} AS (
      SELECT timestamp,
             symbol,
             first(open)  AS open,
             max(high)    AS high,
             min(low)     AS low,
             last(close)  AS close,
             sum(volume)  AS volume
        FROM OHLC_30s{args.table_suffix}
       SAMPLE BY 1h
    ) PARTITION BY DAY;
    """
    with pg.connect(conn_str, autocommit=True) as conn:
        conn.execute(mv1)
        conn.execute(mv2)

def get_adjusted_timestamp(record_ns, ts_mode, literal_date):
    global simulation_initialized, simulation_first_event_ts, simulation_base_ns
    if ts_mode == "realtime":
        return TimestampNanos.now()
    if ts_mode == "original":
        return TimestampNanos(record_ns)
    if ts_mode == "simulate":
        if not simulation_initialized:
            simulation_first_event_ts = record_ns
            simulation_base_ns = int(TimestampNanos.now().nanos)
            simulation_initialized = True
        offset = record_ns - simulation_first_event_ts
        return TimestampNanos(simulation_base_ns + offset)
    # literal-date case
    original_dt = datetime.fromtimestamp(record_ns / 1e9, tz=timezone.utc)
    new_dt = datetime.combine(literal_date, original_dt.timetz())
    return TimestampNanos(int(new_dt.timestamp() * 1e9))

import pyarrow.parquet as pq
# … other imports …

def insert_from_parquet(path, limit, sender, ts_mode, delay_ms, suffix):
    global total, simulation_initialized

    # parse literal-date if provided…
    literal_date = None
    if ts_mode not in ("realtime","original","simulate"):
        try:
            literal_date = datetime.strptime(ts_mode, "%Y-%m-%d").date()
        except:
            print(f"Invalid --timestamp-replace: {ts_mode}", file=sys.stderr)
            sys.exit(1)

    pqf = pq.ParquetFile(path)
    for rg in range(pqf.num_row_groups):
        table = pqf.read_row_group(rg).combine_chunks()

        # pull each column once as a single Array (lower per-row overhead)
        arr_symbol      = table.column("symbol")
        arr_venue       = table.column("venue")
        arr_option_type = table.column("option_type")
        arr_side        = table.column("side")
        arr_action      = table.column("action")

        arr_expiry      = table.column("expiry_date")
        arr_strike      = table.column("strike")
        arr_depth       = table.column("depth")
        arr_price       = table.column("price")
        arr_size        = table.column("size")
        arr_bid_size    = table.column("bid_size")
        arr_ask_size    = table.column("ask_size")
        arr_bid_price   = table.column("bid_price")
        arr_ask_price   = table.column("ask_price")
        arr_bid_count   = table.column("bid_count")
        arr_ask_count   = table.column("ask_count")
        arr_ts_event    = table.column("ts_event")
        arr_ts_recv     = table.column("ts_recv")
        arr_venue_desc  = table.column("venue_description")

        n = table.num_rows
        for i in range(n):
            total += 1
            if total % 1_000_000 == 0:
                print(f"{total:_} records", flush=True)
            if total > limit:
                print(f"Reached limit of {limit:_} records. Exiting.", flush=True)
                sender.flush(); sender.close(); sys.exit()

            # Extract once per row
            symbol      = arr_symbol[i].as_py()
            venue       = arr_venue[i].as_py()
            option_type = arr_option_type[i].as_py()
            side        = arr_side[i].as_py()
            action      = arr_action[i].as_py()

            symbols = {
                "symbol":      symbol,
                "venue":       venue,
                "option_type": option_type,
                "side":        side,
                "action":      action,
            }

            expiry_date       = arr_expiry[i].as_py()
            strike            = arr_strike[i].as_py()
            depth             = arr_depth[i].as_py()
            price             = arr_price[i].as_py()
            size              = arr_size[i].as_py()
            bid_size          = arr_bid_size[i].as_py()
            ask_size          = arr_ask_size[i].as_py()
            bid_price         = arr_bid_price[i].as_py()
            ask_price         = arr_ask_price[i].as_py()
            bid_count         = arr_bid_count[i].as_py()
            ask_count         = arr_ask_count[i].as_py()
            ts_event          = arr_ts_event[i].as_py()
            ts_recv           = arr_ts_recv[i].as_py()
            venue_description = arr_venue_desc[i].as_py()

            columns = {
                "expiry_date":      expiry_date,
                "strike":           strike,
                "depth":            depth,
                "price":            price,
                "size":             size,
                "bid_size":         bid_size,
                "ask_size":         ask_size,
                "bid_price":        bid_price,
                "ask_price":        ask_price,
                "bid_count":        bid_count,
                "ask_count":        ask_count,
                "ts_event":         ts_event,
                "ts_recv":          ts_recv,
                "venue_description": venue_description,
            }

            # compute ingestion timestamp
            record_ns = int(ts_event.timestamp() * 1e9)
            at = get_adjusted_timestamp(record_ns, ts_mode, literal_date)

            sender.row(f"top_of_book{suffix}", symbols=symbols, columns=columns, at=at)

            if ts_mode == "realtime" and delay_ms > 0:
                time.sleep(delay_ms / 1000.0)

if __name__ == "__main__":
    args = parse_arguments()

    questdb_conf = f"{args.http_conn_host};token={args.http_token};tls_verify=unsafe_off;auto_flush_rows=75000;auto_flush_interval=1000;"
    sender = Sender.from_conf(questdb_conf)
    sender.establish()

    ensure_table_exists(args)
    ensure_mat_views_exist(args)

    total = 0
    files = sorted(glob(args.file_pattern))
    for f in files:
        print(f"[{datetime.utcnow().isoformat()}] Loading {f}...", flush=True)
        insert_from_parquet(f, args.limit, sender,
                            args.timestamp_replace, args.delay, args.table_suffix)

    sender.flush()
    sender.close()
    print(f"Done. Total records sent: {total:,}")
