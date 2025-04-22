# opra-spx-questdb
Ingest SPX data from DBN files into QuestDB


TODO

example

```
python l1-sender-parquet.py "../parquet_src/top_of_book~15.2025-04-20T15*parquet"  --host 179.31.49.41 --http-conn-host https::addr=179.31.49.41:9000 --http-token <your_token> --limit 2_000_000 --table-suffix XXX --workers 6
```
