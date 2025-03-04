# tpc-ds-orc

Converts [TPC-DS](https://www.tpc.org/tpcds/) .dat files into [.parquet files](https://orc.apache.org/) using [PyArrow](https://arrow.apache.org/docs/python/index.html). TPC-DS schema information is hardcoded in `tableinfo.py` for validation at conversion time.

Usage: `python3 write.py {path to .dat dir} {output dir} [snappy|zlib|lz4|zstd]`