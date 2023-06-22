#! /usr/bin/python3
''' jrdodson
2023

Simple Spark scripts to demonstrate how one might use the Spark API to submit remote jobs
'''

from pyspark.sql.functions import col, current_date
from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .master("yarn")\
                    .appName(f"job-temp")\
                    .config("spark.sql.warehouse.dir", "hdfs:///warehouse")\
                    .enableHiveSupport()\
                    .getOrCreate()

df = spark.sql("SELECT * FROM warehouse.jukebox_bin_metadata")\
          .limit(20)\
          .withColumn("current_date", current_date())

df.write\
  .mode("overwrite")\
  .format("hive")\
  .saveAsTable(f"warehouse.jukebox_sample_temp")