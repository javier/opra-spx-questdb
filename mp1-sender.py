from datetime import datetime, timezone
import time
import os
import sys
from glob import glob
import argparse
import json
import databento as db
import psycopg as pg
from questdb.ingress import Sender, TimestampNanos

simulation_initialized = False
simulation_first_event_ts = None
simulation_base_ns = None


def int_with_underscores(value):
    return int(value.replace("_", ""))

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Replay DBN files and print every million trades until a limit is reached."
    )
    parser.add_argument(
        "file_pattern",
        nargs="?",
        default="./*.dbn.*",
        help="Path to the files containing .dbn.zst files (default: current directory)"
    )
    parser.add_argument(
        "--limit",
        type=int_with_underscores,
        default=1_000_000,
        help="Maximum number of trades to process before exiting (default: 1_000_000). You can use underscores."
    )
    parser.add_argument(
        "--host",
        required=True,
        help="QuestDB host name or IP."
    )
    parser.add_argument(
        "--pg-port",
        default="8812",
        help="QuestDB pg port. Defaults to 8812"
    )
    parser.add_argument(
        "--user",
        default="admin",
        help="QuestDB pg user. Defaults to admin"
    )
    parser.add_argument(
        "--password",
        default="quest",
        help="QuestDB pg password. Defaults to quest"
    )
    parser.add_argument(
        "--http-conn-host",
        required=True,
        help="QuestDB HTTP connection host. Defaults to https::addr=172.31.42.41:9000"
    )
    parser.add_argument(
        "--http-token",
        required=True,
        help="QuestDB HTTP token"
    )
    parser.add_argument(
        "--publisher-file",
        default="./publishers.json",
        help="Path to the json file with the publishers. (default: ./publishers.json)"
    )
    parser.add_argument(
        "--timestamp-replace",
        default="realtime",
        help=("Timestamp replacement mode. Options are:\n"
              "  'realtime' (default): use current time,\n"
              "  'original': use the event's original timestamp,\n"
              "  'simulate': replay events preserving relative intervals,\n"
              "  or a literal UTC date (YYYY-MM-DD) to replace the date part.")
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=0,
        help="Delay between events in milliseconds (only valid for 'realtime' mode). Default is 0."
    )
    return parser.parse_args()

def ensure_table_exists(args):
    conn_str = f'user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb'

    with pg.connect(conn_str, autocommit=True) as connection:
        with connection.cursor() as cur:
            cur.execute(
            """
            CREATE TABLE IF NOT EXISTS 'top_of_book' (
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
            )

def ensure_mat_views_exist(args):
    conn_str = f'user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb'

    with pg.connect(conn_str, autocommit=True) as connection:
        with connection.cursor() as cur:
            cur.execute(
            """
            CREATE MATERIALIZED VIEW 'OHLC_30s'(
                SELECT timestamp, symbol, first(price) as open, max(price) as high , min(price) as low,
                       last(price) as close, sum(size) as volume
                FROM top_of_book
                WHERE size > 0
                SAMPLE BY 30s
            ) PARTITION BY DAY;
            """
            )

            cur.execute(
            """
            CREATE MATERIALIZED VIEW 'OHLC_1h' WITH BASE 'OHLC_30s'  AS (
                SELECT timestamp, symbol, first(open) as open, max(high) as high , min(low) as low,
                       last(close) as close, sum(volume) as volume
                FROM OHLC_30s
                SAMPLE by 1h
            ) PARTITION BY DAY;
            """
            )

def build_publisher_lookup(json_file, lookup):
    """
    Reads a JSON file containing a list of publisher records and returns a lookup dictionary
    indexed by publisher_id. Each key maps to a dictionary with venue and description.

    Args:
        json_file (str): The filename of the JSON file.

    Returns:
        dict: A dictionary keyed by publisher_id.
    """
    with open(json_file, 'r') as f:
        publishers = json.load(f)

    for publisher in publishers:
        publisher_id = publisher.get("publisher_id")
        lookup[publisher_id] = {
            "venue": publisher.get("venue", ""),
            "description": publisher.get("description", "")
        }

    return lookup

def load_instruments_from_symbology(symbology, lookup):
    """
    Gets the symbology from the DBN metadata and maps symbol id
    to instrument details including symbol name, expiry, strike, and more.

    Args:
        symbology (dict): Contains the 'mappings' key where each key is a composite
                          symbol string (e.g., "SPX   250919C05450000") and each value
                          is a list of mapping dictionaries.
        lookup (dict): Dictionary to populate with instrument data.

    Returns:
        dict: Updated lookup dictionary, keyed by symbol id.
    """
    for symbol_name, mappings in symbology['mappings'].items():
        # Parse the symbol_name (example: "SPX   250919C05450000")
        underlying = symbol_name[:6].strip()       # "SPX"
        expiry_str = symbol_name[6:12]               # "250919" => YYMMDD
        option_type = symbol_name[12]                # "C" for Call, "P" for Put
        strike_str = symbol_name[13:21]              # "05450000"

        # Convert expiry string to a date object
        expiry_date = datetime.strptime(expiry_str, "%y%m%d")
        # Convert strike string to a float (divide by 1000 per OCC convention)
        strike = int(strike_str) / 1000.0

        # Iterate over each mapping for this symbol
        for mapping in mappings:
            symbol_id = int(mapping['symbol'])
            lookup[symbol_id] = {
                'symbol': symbol_name,
                'underlying': underlying,
                'expiry_date': expiry_date,
                'option_type': option_type,
                'strike': strike,
                #'start_date': mapping.get('start_date'),
                #'end_date': mapping.get('end_date')
            }
    return lookup

def insert_into_questdb_from_file(mbp1_file, limit, sender, ts_mode, delay_ms):
    global simulation_initialized, simulation_first_event_ts, simulation_base_ns

    # If ts_mode is a literal date (and not one of the keywords), try parsing it.
    literal_date = None
    if ts_mode not in ["realtime", "original", "simulate"]:
        try:
            literal_date = datetime.strptime(ts_mode, "%Y-%m-%d").date()
        except Exception as e:
            print(f"Error parsing literal date from --timestamp-replace: {e}")
            sys.exit(1)

    def get_adjusted_timestamp(record_ts):
        """
        Given the original event timestamp (record_ts in nanoseconds),
        return the adjusted timestamp based on the ts_mode.
        """
        # record_ts is assumed to be an integer nanoseconds value.
        if ts_mode == "realtime":
            return TimestampNanos.now()
        elif ts_mode == "original":
            return TimestampNanos(record_ts)
        elif ts_mode == "simulate":
            global simulation_initialized, simulation_first_event_ts, simulation_base_ns
            if not simulation_initialized:
                simulation_first_event_ts = record_ts
                simulation_base_ns = int(TimestampNanos.now().nanos)  # Get current ns as int
                simulation_initialized = True
            # Calculate offset in ns from the first event
            offset = record_ts - simulation_first_event_ts
            return TimestampNanos(simulation_base_ns + offset)
        elif literal_date is not None:
            # Convert record_ts to a datetime object (assume UTC)
            original_dt = datetime.fromtimestamp(record_ts / 1e9, tz=timezone.utc)
            # Replace the date part with the literal date while preserving time-of-day
            new_dt = datetime.combine(literal_date, original_dt.time())
            # Convert back to nanoseconds (assuming new_dt is UTC)
            new_ts = int(new_dt.timestamp() * 1e9)
            return TimestampNanos(new_ts)
        else:
            # Fallback to realtime if something goes wrong
            return TimestampNanos.now()

    for record in mbp1_file:
        db.total += 1
        if db.total % 1_000_000 == 0:
            print(f"{db.total:_} records")
        if db.total > limit:
            print(f"Reached limit of {limit:_} records. Exiting.")
            sender.flush()
            sender.close()
            sys.exit()

        # Build the symbols dictionary
        symbols = {
            'symbol': instruments[record.instrument_id]['symbol'],
            'venue': publishers[record.publisher_id]['venue'],
            'option_type': instruments[record.instrument_id]['option_type'],
            'side': record.side,
            'action': record.action
        }

        # Build the columns dictionary
        columns = {
            'ts_event': datetime.fromtimestamp(record.ts_event / 1e9),
            'ts_recv': datetime.fromtimestamp(record.ts_recv / 1e9),
            #'ts_ns_delta': record.ts_in_delta,
            'expiry_date': instruments[record.instrument_id]['expiry_date'],
            'strike': instruments[record.instrument_id]['strike'],
            'depth': record.depth,
            'price': record.price * 1e-9,
            'size': record.size,
            'bid_size': record.levels[0].bid_sz,
            'ask_size': record.levels[0].ask_sz,
            'bid_price': record.levels[0].bid_px * 1e-9,
            'ask_price': record.levels[0].ask_px * 1e-9,
            'bid_count': record.levels[0].bid_ct,
            'ask_count': record.levels[0].ask_ct,
            'venue_description': publishers[record.publisher_id]['description']
        }

        #print("Symbols:", symbols)
        #print("Columns:", columns)

        sender.row(
            'top_of_book',
            symbols=symbols,
            columns=columns,
                at=get_adjusted_timestamp(record.ts_event)
                #at=TimestampNanos.now() #TimestampNanos(record.ts_event)
        )
        # If in realtime mode and a delay is specified, pause before processing the next event.
        if ts_mode == "realtime" and delay_ms > 0:
            time.sleep(delay_ms / 1000.0)


args = parse_arguments()

instruments = {}
publishers = {}

questdb_conf = f"{args.http_conn_host};token={args.http_token};tls_verify=unsafe_off;auto_flush_rows=150000;auto_flush_interval=1500;"
sender = Sender.from_conf(questdb_conf)
sender.establish()


publishers = build_publisher_lookup(args.publisher_file, publishers)
ensure_table_exists(args)
# ensure_mat_views_exist(args)

db.total = 0
files = sorted(glob(args.file_pattern))

# Process each file
for file in files:
    print(f"Loading {file}...")
    store = db.DBNStore.from_file(file)
    load_instruments_from_symbology(store.symbology, instruments)
    insert_into_questdb_from_file(store, args.limit, sender, args.timestamp_replace, args.delay)

print(f"Total trades processed: {db.total:,}")
