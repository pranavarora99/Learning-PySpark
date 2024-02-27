from lib.logger import Log4J
from pyspark import SparkContext
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

if __name__=='__main__':
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkDataSync") \
        .getOrCreate()

    logger = Log4J(spark)

    flight_df = spark.read \
          .format("parquet") \
          .load("Data/flight*.parquet")

    logger.info("flight df partitions count before: " + str(flight_df.rdd.getNumPartitions()))
    flight_df.groupBy(spark_partition_id()).count().show()

    partitioned_df = flight_df.repartition(5)
    logger.info("flight df partitions count after: " + str(partitioned_df.rdd.getNumPartitions()))
    partitioned_df.groupBy(spark_partition_id()).count().show()

    # partitioned_df.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "DataSync/avro/") \
    #     .save()

    partitioned_df.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "DataSync/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 11000) \
        .save()
