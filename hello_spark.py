import sys

from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_customer_df, count_by_country

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <file_name>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")

    #conf_out = spark.sparkContext.getConf()
    #logger.info(conf_out.toDebugString())

    customer_survey_df = load_customer_df(spark, sys.argv[1])
    partitioned_survey_df = customer_survey_df.repartition(2)
    group_df = count_by_country(partitioned_survey_df)

    group_df.show()

    input("Press Enter")
    logger.info("Ending HelloSpark")
    spark.stop()
