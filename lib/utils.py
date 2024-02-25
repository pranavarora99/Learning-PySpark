import configparser

from pyspark import SparkConf
def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key,val)

    return spark_conf

def load_customer_df(spark, data_file):
    return spark.read \
        .option("header", "true") \
        .option("inferschema", "true") \
        .csv(data_file)

def count_by_country(customer_survey_df):
     return customer_survey_df \
        .select("Company", "Last Name", "Country") \
        .groupBy("Country") \
        .count()
