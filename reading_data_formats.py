from pyspark.sql import SparkSession

from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master('local[3]') \
        .appName('SparkSchemaDemo') \
        .getOrCreate()

    logger = Log4J(spark)

    #-------------reading from csv-----------------------------------
    flight_csv_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/flight*.csv")
    
    flight_csv_df.show(10)
    logger.info("CSV Schema:" + flight_csv_df.schema.simpleString())

    #-------------reading from JSON-----------------------------------
    flight_json_df = spark.read \
        .format("json") \
        .load("data/flight*.json")
    
    flight_json_df.show(10)
    logger.info("JSON Schema:" + flight_json_df.schema.simpleString())

    #-------------reading from JSON-----------------------------------
    flight_parq_df = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    flight_parq_df.show(10)
    logger.info("Parquet Schema:" + flight_parq_df.schema.simpleString())
