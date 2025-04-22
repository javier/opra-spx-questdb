#!/usr/bin/env python3
from datetime import datetime, timezone
import time, sys
import os
from glob import glob
import argparse
import pyarrow.parquet as pq
import psycopg as pg
from questdb.ingress import Sender, TimestampNanos
from concurrent.futures import ProcessPoolExecutor, as_completed

# This version precomputes row-group sizes and assigns contiguous chunks,
# enforcing a global limit without per-row locks.

def int_with_underscores(value):
    return int(value.replace("_", ""))


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Replay Parquet files into QuestDB with parallel senders and global limit, minimal overhead."
    )
    parser.add_argument("file_pattern", nargs="?", default="./*.parquet",
                        help="Glob pattern for Parquet files")
    parser.add_argument("--limit", type=int_with_underscores, default=None,
                        help="Max rows to send globally (default: all)")
    parser.add_argument("--host", required=True, help="QuestDB host")
    parser.add_argument("--pg-port", default="8812", help="QuestDB pg port")
    parser.add_argument("--user", default="admin", help="QuestDB pg user")
    parser.add_argument("--password", default="quest", help="QuestDB pg password")
    parser.add_argument("--http-conn-host", required=True,
                        help="QuestDB HTTP host, e.g. 172.31.42.41:9000")
    parser.add_argument("--http-token", required=True, help="QuestDB HTTP token")
    parser.add_argument("--timestamp-replace", default="realtime",
                        help="Timestamp mode: realtime|original|simulate|YYYY-MM-DD")
    parser.add_argument("--delay", type=int, default=0,
                        help="Delay between events in ms (realtime only)")
    parser.add_argument("--table-suffix", type=str, default="",
                        help="Optional table suffix for testing")
    parser.add_argument("--workers", "-w", type=int, default=1,
                        help="Number of parallel sender processes")
    return parser.parse_args()


def ensure_table_exists(args):
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"
    ddl = f"""
    CREATE TABLE IF NOT EXISTS top_of_book{args.table_suffix} (
        timestamp TIMESTAMP,
        symbol SYMBOL CAPACITY 15000,
        venue SYMBOL,
        expiry_date TIMESTAMP,
        strike DOUBLE,
        option_type SYMBOL,
        side SYMBOL,
        action SYMBOL,
        depth LONG,
        price DOUBLE,
        size LONG,
        bid_size LONG,
        ask_size LONG,
        bid_price DOUBLE,
        ask_price DOUBLE,
        bid_count LONG,
        ask_count LONG,
        ts_event TIMESTAMP,
        ts_recv TIMESTAMP,
        venue_description VARCHAR
    ) timestamp(timestamp) PARTITION BY HOUR;
    """
    with pg.connect(conn_str, autocommit=True) as conn:
        conn.execute(ddl)


def ensure_mat_views_exist(args):
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"
    mv1 = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS OHLC_30s{args.table_suffix} AS (
      SELECT timestamp, symbol,
             first(price) AS open, max(price) AS high, min(price) AS low,
             last(price) AS close, sum(size) AS volume
        FROM top_of_book{args.table_suffix} WHERE size>0 SAMPLE BY 30s
    ) PARTITION BY DAY;
    """
    mv2 = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS OHLC_1h{args.table_suffix} AS (
      SELECT timestamp, symbol,
             first(open) AS open, max(high) AS high,
             min(low) AS low, last(close) AS close,
             sum(volume) AS volume
        FROM OHLC_30s{args.table_suffix} SAMPLE BY 1h
    ) PARTITION BY DAY;
    """
    with pg.connect(conn_str, autocommit=True) as conn:
        conn.execute(mv1)
        conn.execute(mv2)


def get_adjusted_timestamp(record_ns, ts_mode, literal_date):
    if ts_mode == "realtime":
        return TimestampNanos.now()
    if ts_mode == "original":
        return TimestampNanos(record_ns)
    if ts_mode == "simulate":
        if not hasattr(get_adjusted_timestamp, 'first_ns'):
            get_adjusted_timestamp.first_ns = record_ns
            get_adjusted_timestamp.base_ns = int(TimestampNanos.now().nanos)
        offset = record_ns - get_adjusted_timestamp.first_ns
        return TimestampNanos(get_adjusted_timestamp.base_ns + offset)
    # literal date replacement
    original_dt = datetime.fromtimestamp(record_ns/1e9, tz=timezone.utc)
    new_dt = original_dt.replace(year=literal_date.year,
                                 month=literal_date.month,
                                 day=literal_date.day)
    return TimestampNanos(int(new_dt.timestamp()*1e9))


def process_chunk(chunk, config):
    """Each process ingests its assigned (path, rg, max_rows, start_offset)."""
    limit, ts_mode, delay_ms, suffix, http_conn_host, http_token = config
    # init sender
    cfg = f"{http_conn_host};token={http_token};tls_verify=unsafe_off;" \
          + "auto_flush_rows=75000;auto_flush_interval=1000;"
    sender = Sender.from_conf(cfg); sender.establish()

    literal_date = None
    if ts_mode not in ("realtime","original","simulate"):
        literal_date = datetime.strptime(ts_mode, "%Y-%m-%d").date()

    for path, rg, max_rows, start_offset in chunk:
        pqf = pq.ParquetFile(path)
        tbl = pqf.read_row_group(rg)
        arrs = {c: tbl.column(c) for c in tbl.column_names}
        n = tbl.num_rows
        # enforce per-group max
        to_process = n if max_rows is None else min(n, max_rows)
        for i in range(to_process):
            global_idx = start_offset + i
            if limit is not None and global_idx >= limit:
                sender.flush(); sender.close(); return
            if global_idx and global_idx % 1_000_000 == 0:
                print(f"[PID {os.getpid()}] {global_idx:_} total records", flush=True)

            sy = { 'symbol': arrs['symbol'][i].as_py(),
                   'venue': arrs['venue'][i].as_py(),
                   'option_type': arrs['option_type'][i].as_py(),
                   'side': arrs['side'][i].as_py(),
                   'action': arrs['action'][i].as_py() }
            cols = { k: arrs[k][i].as_py() for k in (
                'expiry_date','strike','depth','price','size',
                'bid_size','ask_size','bid_price','ask_price',
                'bid_count','ask_count','ts_event','ts_recv','venue_description') }
            record_ns = int(cols['ts_event'].timestamp()*1e9)
            at = get_adjusted_timestamp(record_ns, ts_mode, literal_date)
            sender.row(f"top_of_book{suffix}", symbols=sy, columns=cols, at=at)
            if ts_mode == "realtime" and delay_ms>0:
                time.sleep(delay_ms/1000.0)
        del tbl, arrs

    sender.flush(); sender.close()

if __name__ == '__main__':
    args = parse_arguments()
    ensure_table_exists(args)
    ensure_mat_views_exist(args)

    # first, collect tasks with row counts
    tasks = []
    cum = 0
    for path in sorted(glob(args.file_pattern)):
        pqf = pq.ParquetFile(path)
        for rg in range(pqf.num_row_groups):
            n = pqf.metadata.row_group(rg).num_rows
            if args.limit is None or cum < args.limit:
                remaining = None if args.limit is None else args.limit - cum
                max_rows = remaining if (remaining is not None and remaining < n) else None
                tasks.append((path, rg, max_rows, cum))
                # increase cum by the actual number of rows we plan to process
                cum += (max_rows if max_rows is not None else n)
            else:
                break
        if args.limit is not None and cum >= args.limit:
            break

    total_rows = min(cum, args.limit) if args.limit else cum
    print(f"Total rows to send: {total_rows}")

    # split contiguous tasks evenly
    workers = args.workers
    chunk_size = (len(tasks) + workers - 1) // workers
    chunks = [tasks[i*chunk_size:(i+1)*chunk_size] for i in range(workers)]

    config = (args.limit, args.timestamp_replace, args.delay,
              args.table_suffix, args.http_conn_host, args.http_token)

    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as exe:
            futures = [exe.submit(process_chunk, chunk, config) for chunk in chunks]
            for fut in as_completed(futures): fut.result()
    else:
        process_chunk(chunks[0], config)

    print(f"Done. {total_rows:_} rows sent via {workers} senders.")
