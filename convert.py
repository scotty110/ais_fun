'''
Convert ais zip archive to parquet archive
'''
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType, TimestampType

import glob
from os.path import join, exists
from os import makedirs, remove

def get_header() -> dict:
    # https://coast.noaa.gov/data/marinecadastre/ais/2018DataDictionary.png
    schema = StructType([
                StructField('MMSI', StringType(), False), 
                StructField('BaseDateTime', TimestampType(), False), 
                StructField('LAT', FloatType(), False), 
                StructField('LON', FloatType(), False),
                StructField('SOG', FloatType(), True),
                StructField('COG', FloatType(), True),
                StructField('Heading', FloatType(), True),
                StructField('VesselName', StringType(), True),
                StructField('IMO', StringType(), True),
                StructField('CallSign', StringType(), True),
                StructField('VesselType', IntegerType(), True),
                StructField('Status', IntegerType(), True),
                StructField('Length', FloatType(), True),
                StructField('Width', FloatType(), True),
                StructField('Draft', FloatType(), True),
                StructField('Cargo', StringType(), True),
                StructField('TransceiverClass', StringType(), True),
                ]
            )
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


def get_spark_session() -> SparkSession:
    spark = SparkSession.builder \
            .appName("Convert Zip Files") \
            .config('spark.executor.cores', 8) \
            .config('spark.executor.instances', 1) \
            .config('spark.executor.memory', '10g') \
            .getOrCreate()
    return spark


if __name__ == '__main__':
    data_dir = './data/*.csv'
    parquet_dir = './AIS'

    spark = get_spark_session()
    header = get_header()


    for f in glob.glob(data_dir, recursive=True):
        print(f)
        # Remove 1st row

        new_file = make_path(f, parquet_dir)
        df = spark.read.csv(f, header=False, schema=header)
        df = df.where( df.MMSI != 'MMSI' )
        df.coalesce(1).write.parquet(new_file)
        remove(f)


    print('Done')
