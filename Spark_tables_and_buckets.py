from pyspark.sql import *
from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkTables") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4J(spark)

    flight_df = spark.read \
        .format("parquet") \
        .load("Data/flight*.parquet")

    spark.sql("CREATE DATABASE IF NOT EXISTS FLIGHTS_DB")
    spark.catalog.setCurrentDatabase("FLIGHTS_DB")

    #manadged tables in spark
    flight_df.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "ORIGIN", "OP_CARRIER") \
        .sortBy("ORIGIN", "OP_CARRIER") \
        .saveAsTable("flights_data_table")

    logger.info(spark.catalog.listTables("FLIGHTS_DB"))
