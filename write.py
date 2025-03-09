import glob
import os
import sys
import time
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.orc as porc
import pandas as pd

import tableinfo


def convert_dat_to_orc(dat_directory, output_directory, compression):
    os.makedirs(output_directory, exist_ok=True)

    for dat_path in glob.glob(os.path.join(dat_directory, "*.dat")):
        table_name = os.path.splitext(os.path.basename(dat_path))[0]
        schema = tableinfo.schemas.get(table_name)

        if not schema:
            print(f"Skipping {dat_path} - no schema found")
            continue

        # Skip if ORC already exists
        if os.path.exists(
            os.path.join(output_directory, f"{table_name}-{compression}.orc")
        ):
            print(f"Skipping {dat_path} - ORC already exists")
            continue

        table = pcsv.read_csv(
            dat_path,
            parse_options=pcsv.ParseOptions(delimiter="|"),
            read_options=pcsv.ReadOptions(
                autogenerate_column_names=True, skip_rows=0  # No headers
            ),
        )

        # Handle trailing delimiter
        expected_cols = len(schema.names)
        if table.num_columns > expected_cols:
            table = table.drop(table.column_names[expected_cols:])

        # Rename columns to match schema
        if table.num_columns == len(schema.names):
            table = table.rename_columns(schema.names)
        else:
            print(
                f"Column count mismatch in {table_name}. Expected {len(schema.names)}, found {table.num_columns}"
            )
            continue

        # Cast
        try:
            table = table.cast(schema)
        except pa.ArrowInvalid as e:
            print(f"Type conversion failed for {table_name}: {str(e)}")
            continue

        orc_path = os.path.join(output_directory, f"{table_name}-{compression}.orc")
        start = time.time()
        porc.write_table(table, orc_path, compression=compression)
        end = time.time()
        print(f"Converted {dat_path} to {orc_path}")
        print(f"Compression took {(end - start) * 1e3:.6f} ms for {orc_path}")


def read_orc(orc_dir):
    for orc_path in glob.glob(os.path.join(orc_dir, "*.orc")):
        try:
            start = time.time()
            table = porc.read_table(orc_path)
            end = time.time()

            print(f"Decompression took {(end - start) * 1e3:.6f} ms for {orc_path}")
        except Exception as e:
            print(f"Failed to read ORC file {orc_path}: {str(e)}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(
            "Usage: python3 write.py {/path/to/dat/files} {/path/to/orc/output} [compression]"
        )
        sys.exit(1)

    dat_path = sys.argv[1]
    orc_path = sys.argv[2]
    # Can be uncompressed, gzip, zstd, snappy
    compression = sys.argv[3] if len(sys.argv) > 3 else "uncompressed"

    convert_dat_to_orc(dat_path, orc_path, compression)
    read_orc(orc_path)
