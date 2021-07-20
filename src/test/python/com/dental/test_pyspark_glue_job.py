import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
from src.main.python.com.dental.pyspark_glue_job import *
from chispa.dataframe_comparer import *
import configparser

CONF_FILE = '/Users/macbookpro/IdeaProjects/pyspark_glue_jobs/src/test/resources/config.ini'

config = configparser.ConfigParser()
config.read(CONF_FILE)
input_location = config.get("paths", 'input')
result_location = config.get("paths", 'output')

schema = StructType([StructField("count", LongType(), True), StructField("records", ArrayType(StructType(
    StructType([StructField("sequence", LongType(), True), StructField("start_date", StringType(), True),
         StructField("identifier", StringType(), True),
         StructField("names", ArrayType(StructType(
             StructType([StructField("given_name", StringType(), True), StructField("other_given_names", StringType(), True),
                  StructField("surname", StringType(), True),
                  StructField("type", StringType(), True)])), False), False), StructField("entitlements", ArrayType(
            StructType(StructType([StructField("identifiers", StructType(StructType([StructField("identifier", StringType(), True),
                                                                       StructField("status_histories", StructType(
                                                                           StructType([StructField("status", StringType(), True),
                                                                                StructField("start_date", StringType(),
                                                                                            True)])), False)])), False),
                            StructField("status_histories", StructType(StructType([StructField("status", StringType(), True),
                                                                            StructField("start_date", StringType(),
                                                                                        True)])), False)])), False),
                                                                                       False),
         StructField("year_of_birth", LongType(), True), StructField("month_of_birth", LongType(), True),
         StructField("day_of_birth", LongType(), True)])), False), False)
                     ])



@pytest.mark.usefixtures("spark_session")
def test(spark_session):
    input_df = spark_session.read.json(input_location)
    excepted_df = spark_session.read.schema(schema).json(result_location, multiLine=True)
    result_df = explode_nested_array(input_df)
    result_df = get_nested_columns(result_df)
    result_df = process_data(result_df)
    result_df = get_nested_output(result_df)
    assert_df_equality(result_df, excepted_df, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True)


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("TEST_GLUE_JOBS") \
        .master("local[*]") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    test(spark)
