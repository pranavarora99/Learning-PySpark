from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
from lib.logger import Log4J

CustomerRecord = namedtuple("CustomerRecord", ["Index","Customer_Id","First_Name","Last_Name","Company","City","Country","Phone_1","Phone_2","Email","Subscription_Date","Website"])

if __name__ == '__main__':
    conf = SparkConf() \
        .setMaster("local[3]") \
        .setAppName("HelloRDD")
    #sc = SparkContext(conf = conf)

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    sc = spark.sparkContext
    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloRDD <filename>")
        sys.exit(-1)

    linesRDD = sc.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)

    #map function to read each line and apply transformations(remove '"' and make a list of strings from a string by split
    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(","))
    selectRDD = colsRDD.map(lambda cols:CustomerRecord(int(cols[1], cols[2]), str(cols[3], cols[4])))
    filterRDD = selectRDD.filter(lambda i: i.Index <= 20)
    kvRDD = filterRDD.map(lambda c: (c.county, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1+v2)

    colsList = countRDD.collect()
    for x in colsList:
        logger.info(x)
