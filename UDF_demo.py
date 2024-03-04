import re
from pyspark.sql import *
from pyspark.sql.functions import *
from lib.logger import *
from pyspark.sql.types import *

def fix_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "NA"

if __name__=='__main__':
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("UDF-demo") \
        .getOrCreate()

    logger = Log4J(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("Data/survey.csv")

    survey_df.printSchema()
    survey_df.show(10)

    fix_gender_udf = udf(fix_gender, returnType=StringType())
    logger.info("Catalog Entry:")
    [logger.info(r) for r in spark.catalog.listFunctions() if "fix_gender" in r.name]

    survey_df_2 = survey_df.withColumn("Gender", fix_gender_udf("Gender"))
    survey_df_2.show(10)

    spark.udf.register("fix_gender_udf", fix_gender, StringType())
    logger.info("Catalog Entry:")
    [logger.info(r) for r in spark.catalog.listFunctions() if "fix_gender" in r.name]

    survey_df_3 = survey_df.withColumn("Gender", expr("fix_gender_udf(Gender)"))
    survey_df_3.show(10)

