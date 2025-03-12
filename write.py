import glob
import os
import sys
import tempfile
import time
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.orc as porc

import tpcds_tableinfo
import imdb_tableinfo


def convert_to_orc(mode, input_directory, output_directory, compression):
    os.makedirs(output_directory, exist_ok=True)

    pattern = "*.dat" if mode == "tpcds" else "*.tsv"
    schemas = tpcds_tableinfo.schemas if mode == "tpcds" else imdb_tableinfo.schemas
    delimiter = "|" if mode == "tpcds" else "\t"

    for path_to_convert in glob.glob(os.path.join(input_directory, pattern)):
        table_name = os.path.splitext(os.path.basename(path_to_convert))[0]
        schema = schemas.get(table_name)

        if not schema:
            print(f"Skipping {path_to_convert} - no schema found")
            continue

        # Skip if ORC already exists
        if os.path.exists(
            os.path.join(output_directory, f"{table_name}-{compression}.orc")
        ):
            print(f"Skipping {path_to_convert} - ORC already exists")
            continue

        if mode == "imdb":
            # IMDB input (particularly title.basics) contains random quotes which break read_csv
            with open(path_to_convert, "r") as file:
                content = file.read().replace('"', "")

            # Write the cleaned content to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as temp_file:
                temp_file.write(content)
                temp_file_path = temp_file.name

            file_to_read = temp_file_path
        else:
            file_to_read = path_to_convert

        table = pcsv.read_csv(
            file_to_read,
            parse_options=pcsv.ParseOptions(delimiter=delimiter, quote_char='"'),
            read_options=pcsv.ReadOptions(
                autogenerate_column_names=(mode == "tpcds"), skip_rows=0  # No headers
            ),
            convert_options=pcsv.ConvertOptions(null_values=["\\N", ""]),
        )

        # Handle trailing delimiter if needed
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
            for column in table.columns:
                try:
                    column.cast(schema.field(column.name).type)
                except pa.ArrowInvalid as col_e:
                    print(
                        f"Type conversion failed for column {column.name} with value {column.to_pylist()}: {str(col_e)}"
                    )
            continue

        orc_path = os.path.join(output_directory, f"{table_name}-{compression}.orc")
        start = time.time()
        porc.write_table(table, orc_path, compression=compression)
        end = time.time()
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
            "Usage: python3 write.py {imdb|tpcds} {/path/to/files} {/path/to/orc/output} [compression]"
        )
        sys.exit(1)

    mode = sys.argv[1]
    dat_path = sys.argv[2]
    orc_path = sys.argv[3]
    # Can be uncompressed, gzip, zstd, snappy
    compression = sys.argv[4] if len(sys.argv) > 4 else "uncompressed"

    convert_to_orc(mode, dat_path, orc_path, compression)
    read_orc(orc_path)
