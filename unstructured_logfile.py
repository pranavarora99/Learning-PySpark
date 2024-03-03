import re

from pyspark.sql import *
from pyspark.sql.functions import *


if __name__=='__main__':
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("UnstructuredLog") \
        .getOrCreate()

    logs_raw_df = spark.read.text("Data/apache_logs.txt")

    logs_raw_df.printSchema()

    pattern = r'(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\d+) "([^"]*)"'

    #regex = re.compile(pattern)

    #method 1
    # with open("Data/apache_logs.txt", 'r') as file:
    #     for line in file:
    #         match = regex.match(line)
    #         if match:
    #             # Access matched groups using match.group(index)
    #             ip_address = match.group(1)
    #             date_time = match.group(4)
    #             request = match.group(6)
    #             referrer = match.group(10)
    #
    #             # Process or store the extracted information as needed
    #             print(
    #                 f"IP: {ip_address}, Date: {date_time}, Request: {request}, Referrer: {referrer}")

    #method 2
    logs_df = logs_raw_df.select(regexp_extract('value', pattern, 1).alias('ip'),
                            regexp_extract('value', pattern, 4).alias('date'),
                            regexp_extract('value', pattern, 6).alias('request'),
                            regexp_extract('value', pattern, 10).alias('referrer'))

    logs_df.printSchema()

    logs_df \
        .where("trim(referrer) != '-' ") \
        .withColumn("referrer", substring_index("referrer", "/", 3)) \
        .groupBy("referrer") \
        .count() \
        .show(100, truncate=False)