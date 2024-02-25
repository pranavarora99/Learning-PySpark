from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
from lib.logger import Log4J

#CustomerRecord = namedtuple("CustomerRecord", ["Index","Customer_Id","First_Name","Last_Name","Company","City","Country","Phone_1","Phone_2","Email","Subscription_Date","Website"])

if __name__ == '__main__':
    conf = SparkConf() \
        .setMaster("local[3]") \
        .setAppName("HelloSparkSQL")
    #sc = SparkContext(conf = conf)

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSparkSQL <filename>")
        sys.exit(-1)

    customer_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(sys.argv[1])

    customer_df.createOrReplaceTempView("customers_table")

    count_df = spark.sql("select Country, count(1) as count from customers_table where Index <= 50 group by country")
    count_df.show()

