'''
Convert ais zip archive to parquet archive using DuckDB
'''
import duckdb
import glob
from os.path import join, exists
from os import makedirs, remove
import argparse

def get_header() -> dict:
    # https://coast.noaa.gov/data/marinecadastre/ais/2018DataDictionary.png
    schema = {
                'MMSI': 'VARCHAR',
                'BaseDateTime': 'TIMESTAMP',
                'LAT': 'FLOAT',
                'LON': 'FLOAT',
                'SOG': 'FLOAT',
                'COG': 'FLOAT',
                'Heading': 'FLOAT',
                'VesselName': 'VARCHAR',
                'IMO': 'VARCHAR',
                'CallSign': 'VARCHAR',
                'VesselType': 'INTEGER',
                'Status': 'INTEGER',
                'Length': 'FLOAT',
                'Width': 'FLOAT',
                'Draft': 'FLOAT',
                'Cargo': 'VARCHAR',
                'TransceiverClass': 'VARCHAR',
            }
    return schema

def make_path(data_path:str, to_path:str) -> str:
    '''
    Make a path where to store the data and return file name
    '''
    split_path = data_path.split('/')
    root_dir = split_path[-2]
    new_file = (split_path[-1]).split('.')[0] + '.parquet'

    new_dir = join(to_path, root_dir)
    if not exists(new_dir):
        makedirs(new_dir)

    return join(new_dir, new_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert AIS CSV files to Parquet using DuckDB.')
    parser.add_argument('tmp_dir', type=str, help='The directory containing the CSV files.')
    parser.add_argument('dest_dir', type=str, help='The destination directory for the Parquet files.')

    args = parser.parse_args()

    data_dir = join(args.tmp_dir, '*.csv')
    parquet_dir = args.dest_dir

    header = get_header()

    con = duckdb.connect(database=':memory:', read_only=False)

    for f in glob.glob(data_dir, recursive=True):
        print(f)

        # Create column definitions dictionary for SQL
        schema_items = [f"'{col}': '{dtype}'" for col, dtype in header.items()]
        schema_dict_str = "{" + ", ".join(schema_items) + "}"

        # Construct the SQL statement for creating a view and filtering data
        create_view_sql = f"""
            CREATE OR REPLACE VIEW ais_data AS
            SELECT *
            FROM read_csv ('{f}', HEADER=True, columns={schema_dict_str}, ignore_errors=true)
            WHERE MMSI != 'MMSI';
        """

        # Make the output path
        new_file = make_path(f, parquet_dir)

        # Construct the SQL statement for writing to parquet
        write_parquet_sql = f"COPY (SELECT * FROM ais_data) TO '{new_file}' (FORMAT 'parquet');"

        # Execute the SQL statements
        con.execute(create_view_sql)
        con.execute(write_parquet_sql)

        # Remove the CSV file
        remove(f)

    con.close()
    print('Done')