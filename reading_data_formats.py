from pyspark.sql import SparkSession
from pyspark.sql.types import *
from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master('local[3]') \
        .appName('SparkSchemaDemo') \
        .getOrCreate()

    logger = Log4J(spark)

    flight_schema_def = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    #-------------reading from CSV-----------------------------------
    flight_csv_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(flight_schema_def) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "m/d/y") \
        .load("data/flight*.csv")

    flight_csv_df.show(10)
    logger.info("CSV Schema:" + flight_csv_df.schema.simpleString())

    # -------------reading from JSON-----------------------------------
    # flight_json_df = spark.read \
    #     .format("json") \
    #     .load("data/flight*.json")
    #
    # flight_json_df.show(10)
    # logger.info("JSON Schema:" + flight_json_df.schema.simpleString())

    # -------------reading from JSON-----------------------------------
    # flight_parq_df = spark.read \
    #     .format("parquet") \
    #     .load("data/flight*.parquet")
    #
    # flight_parq_df.show(10)
    # logger.info("Parquet Schema:" + flight_parq_df.schema.simpleString())
