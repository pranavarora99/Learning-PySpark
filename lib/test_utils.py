from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import load_customer_df, count_by_country

class UtilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
                .master("local[3]") \
                .appName("helloSparkTest") \
                .getOrCreate()

    def test_data_file(self):
        sample_df = load_customer_df(self.spark, "data/customers.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count, 1000, "record count should be 1000")

    # def test_count_function(self):
    #     sample_df = load_customer_df(self.spark, "data/customers.csv")
    #     count_list = count_by_country(sample_df).collect()
    #     count_dict = dict()
    #     for row in count_list:
    #         count_dict[row["Country"]] = row["count"]
    #     self.assertEqual()
    #


    # @classmethod
    # def tearDownClass(cls) -> None:
    #     cls.spark.stop()
