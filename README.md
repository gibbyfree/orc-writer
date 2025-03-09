# orc-writer

Converts [TPC-DS](https://www.tpc.org/tpcds/) .dat files and [IMDB](https://datasets.imdbws.com/) .tsv files into [.orc files](https://orc.apache.org/) using [PyArrow](https://arrow.apache.org/docs/python/index.html). Reports compression runtime and decompression runtime for each file.

TPC-DS schema information is hardcoded in `tableinfo.py` for validation at conversion time. IMDB schema information is hardcoded in `imdb_tableinfo.py`.

Usage: `python3 write.py {imdb|tpcds} {path to .dat dir} {output dir} [snappy|zlib|lz4|zstd]`