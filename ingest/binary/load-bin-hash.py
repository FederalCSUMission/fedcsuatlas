#!/usr/bin/python3
'''
jdodson
microsoft 
2023
'''
from pyspark.sql.types import StructType, StructField, StringType, BinaryType
from pyspark.sql.functions import col, udf
from pyspark.sql import SparkSession
import hashlib
import logging
import argparse
import atexit

parser = argparse.ArgumentParser()
parser.add_argument("-warehouse_dir", type=str, required=True, help="Target HDFS warehouse directory, e.g. 'hdfs:///warehouse'")
parser.add_argument("-source_hive_table_name", type=str, required=True, help="Source table in Hive, e.g. warehouse.bin_metadata")
parser.add_argument("-target_hive_table_name", type=str, required=True, help="Target table in Hive, e.g. warehouse.bin_hash")
parser.add_argument("-batch_size", type=int, default=500, help="Ingest batch size")
args, unks = parser.parse_known_args()

spark = SparkSession.builder\
                    .master("yarn")\
                    .appName("spark-bin-load-test")\
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

atexit.register(lambda: stop())

def main():

    schema = StructType([StructField("path", StringType(), True),
                         StructField("md5", StringType(), True)])

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {args.target_hive_table_name} (path STRING, md5 STRING)
              """)
    try:
        paths_df = spark.sql(f"""SELECT t1.path, t1.length
                                 FROM {args.source_hive_table_name} t1
                                 LEFT ANTI JOIN {args.target_hive_table_name} t2 
                                 ON t1.path = t2.path
                              """)\
                        .toPandas()
    except pyspark.sql.utils.AnalysisException:
        logging.error(f"No metadata table {args.source_hive_table_name} found! Exiting.")
        return

    batch = []
    for index, row in paths_df.iterrows():
        if index == 0 or index % args.batch_size != 0:
            batch += [row['path']]
            continue
        if index % args.batch_size == 0:
            df = spark.sparkContext\
                      .binaryFiles(",".join(batch))\
                      .map(lambda x: (x[0], x[1][0:100000000]))\
                      .map(lambda x: (x[0], hashlib.md5(x[1]).hexdigest()))\
                      .toDF(schema) 
            df.write\
              .mode("append")\
              .format("hive")\
              .saveAsTable(f"{args.target_hive_table_name}")
            batch = []

if __name__ == "__main__":
    main()
