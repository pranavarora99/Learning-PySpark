from pyspark.sql import SparkSession
from lib.logger import Log4J
from pyspark.sql import functions as f

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Aggregations") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4J(spark)

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("Data/invoices.csv")

    invoice_df.select(f.count("*").alias("count"),
        f.sum("Quantity").alias("TotalQuantity"),
        f.avg("UnitPrices").alias("AvgPrice"),
        f.countDistinct("InvoiceNo").alias("DistinctInvoices")) \
        .show()

    invoice_df.createOrReplaceTempView("sales")
    summary_sql = spark.sql("""
        SELECT Country, InvoiceNo, 
        sum(Quantity) as TotalQuantity, 
        round(sum(Quantity * UnitPrice),2) as InvoiceValue
        FROM sales GROUP BY Country, InvoiceNo
    """)

    summary_sql.show()

    summary_df=invoice_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity * UnitPrice")),2).alias("InvoiceValue"),
             f.expr("round(sum(Quantity * UnitPrice), 2) as InvoiceValueExpr")
             )

    summary_df.show()

NumInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.round(f.sum(f.expr("Quantity * UnitPrice")),2).alias("InvoiceValue")

    final_summary_df = invoice_df.withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), 'MM-dd-yyyy')) \
                    .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
                    .groupBy("Country", "WeekNumber") \
                    .agg(NumInvoices, TotalQuantity, InvoiceValue)

    final_summary_df.write \
        .format("csv") \
        .mode("overwrite") \
        .option("path", "output/") \
        .save()
