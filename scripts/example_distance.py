import duckdb

#import pandas as pd

import glob 
import os.path


# Connect to DB (and load geo extension)
conn = duckdb.connect() #config={'allow_unsigned_extensions' : 'true'}) 
conn.execute("INSTALL spatial; LOAD spatial;")
#conn = duckdb.connect()

# Get list of parquet files to import into duckdb
data_dir = '/Users/ella/Documents/luna/ais_data/**/*.parquet'
data_list = list(filter(lambda x: not os.path.isdir(x), glob.glob(data_dir, recursive=True) ))

# Create Table like thing
conn.execute(f"CREATE VIEW ais AS SELECT * FROM read_parquet({data_list})")

conn.execute("SELECT COUNT(*) FROM ais")

start = '2022-01-01 00:00:00'
end = '2022-01-06 00:00:00'
lat, lon = (33.755, -118.215) #Port of longbeach
max_dist = 500 
sql_s = f"\
            WITH timeTable AS ( \
                SELECT MMSI, BaseDateTime, LAT, LON \
                FROM ais \
                WHERE BaseDateTime BETWEEN '{start}' AND '{end}' \
            )\
            SELECT COUNT(*) FROM timeTable \
            WHERE ST_Distance_Sphere( ST_POINT(LON, LAT), ST_POINT({lon}, {lat}) ) < {max_dist} \
        "

# Remove COUNT(*) to get dataframe
#df = conn.execute(sql_s).df()

count = conn.execute("SELECT COUNT(*) FROM ais").fetchall()[0]
print(f'Number of records: {count}')

record_row = conn.execute("SELECT LAT, LON FROM ais LIMIT 1").fetchall()[0]
print(f'Row Data Looks like: {record_row}')

print(f'Number of records within 500 meters of long beach port: {conn.execute(sql_s).fetchall()[0]}')
