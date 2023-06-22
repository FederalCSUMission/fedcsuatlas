#!/usr/bin/python3
'''
jdodson
microsoft 
2023
'''
from pyspark.sql.types import StructType, StructField, StringType, BinaryType
from pyspark.sql.functions import col, udf
from pyspark.sql import SparkSession
import logging
import argparse
import atexit

parser = argparse.ArgumentParser()
parser.add_argument("-root_bin_dir", type=str, required=True, help="Source binary file directory in HDFS, e.g. 'abfss://container@account.dfs.core.usgovcloudapi.net/'")
parser.add_argument("-warehouse_dir", type=str, required=True, help="Target HDFS warehouse directory, e.g. 'hdfs:///warehouse/'")
parser.add_argument("-hive_table_name", type=str, required=True, help="Target table in Hive, e.g. warehouse.bin_metadata")
args, unks = parser.parse_known_args()

spark = SparkSession.builder\
                    .master("yarn")\
                    .appName("spark-bin-metadata")\
                    .config("spark.sql.warehouse.dir", args.warehouse_dir)\
                    .enableHiveSupport()\
                    .getOrCreate()
def stop():
    '''Helper to gracefully stop the local sparkContext
    '''
    try:
        spark.sparkContext.stop()
    except:
        if not spark.spartContext._jsc.sc.isStopped():
            spark.stop()

def extract_filename(path: str) -> str:
    return path.split("/")[-1]

atexit.register(lambda: stop())

def main():
    logging.info(f"Beginning ingest from {args.root_bin_dir} to {args.warehouse_dir} in table {args.hive_table_name}") 

    metadata_df = spark.read\
                       .format("binaryFile")\
                       .load(f"{args.root_bin_dir}/*.bin")\
                       .select(udf(extract_filename)(col("path")).alias("filename"),
                               col("path"),
                               col("modificationTime").alias("updated_timestamp"),
                               col("length"))

    metadata_df.write\
               .mode("overwrite")\
               .format("hive")\
               .saveAsTable(f"{args.hive_table_name}")

if __name__ == "__main__":
    main()
