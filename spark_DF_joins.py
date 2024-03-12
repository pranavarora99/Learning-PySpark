from pyspark.sql import SparkSession
from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkDFJoins") \
        .getOrCreate()

    logger = Log4J

    orders = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]

    order_df = spark.createDataFrame(orders).toDF('orderID', 'ProdID', 'unit_price', "qty")

    products = [("01", "Scroll Mouse", 250, 20),
                    ("02", "Optical Mouse", 350, 20),
                    ("03", "Wireless Mouse", 450, 50),
                    ("04", "Wireless Keyboard", 580, 50),
                    ("05", "Standard Keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]

    product_df = spark.createDataFrame(products).toDF('prodID', 'prod_name',
                                                      'list_price', 'qty')

    new_product_df = product_df.withColumn("qty", "reorder_qty")

    join_mapping = order_df.prodID == new_product_df.prodID

    #inner join
    order_df.join(new_product_df, join_mapping, 'inner') \
        .drop(new_product_df.prodID) \
        .select('orderID', 'prodID', 'prod_name', 'list_price', 'qty') \
        .show()
    
    # #outer join
    # order_df.join(product_renamed_df, join_mapping, "left") \
    #     .drop(product_renamed_df.prod_id) \
    #     .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
    #     .withColumn("prod_name", expr("coalesce(prod_name, prod_id)")) \
    #     .withColumn("list_price", expr("coalesce(list_price, unit_price)")) \
    #     .sort("order_id") \
    #     .show()
    
    #shuffle join
    demo_data_df1 = spark.read.json('Data/d1/')
    demo_data_df2 = spark.read.json('Data/d2/')
    
    spark.conf.set("spark/shuffle.partition", 3)
    
    join_mapping_shuffle = demo_data_df1.id == demo_data_df2.id
    
    join_df = demo_data_df1.join(demo_data_df2, join_mapping_shuffle, "inner")
    
    join_df.collect()
    
    
    #Bucket join
    df1 = spark.read.json('Data/d1/')
    df2 = spark.read.json('Data/d2/')
    
    '''
        spark.sql("CREATE DATABASE IF NOT EXIST MY_DB")
        spark.sql("USE MY_DB")
        
        df1.coalesce(1).write \
            .bucketBy(3, 'id') \
            .mode('overwrite') \
            .saveAsTable('MY_DB.DEMO_DATA_1')
            
        df2.coalesce(1).write \
            .bucketBy(3, 'id') \
            .mode('overwrite') \
            .saveAsTable('MY_DB.DEMO_DATA_2')
    '''
    
    df3 = spark.read.table("MY_DB.DEMO_DATA_1")
    df4 = spark.read.table("MY_DB.DEMO_DATA_2")
    
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) #to allow bucket fomation
    join_mapping_bucket = df3.id == df4.id
    
    bucket_join_df = df3.join(df4, join_mapping_bucket, "inner")
    
    bucket_join_df.collect()
    input("Press enter: ")
