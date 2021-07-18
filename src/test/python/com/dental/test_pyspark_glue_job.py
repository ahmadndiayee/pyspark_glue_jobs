import pytest
from pyspark.sql.types import StructType,StructField, StringType, LongType, ArrayType
from src.main.python.com.dental.pyspark_glue_job import *
from chispa.dataframe_comparer import *
import configparser

CONF_FILE = '/Users/macbookpro/IdeaProjects/pyspark_glue_jobs/src/test/resources/config.ini'

config = configparser.ConfigParser()
config.read(CONF_FILE)
input_location = config.get("paths", 'input')
result_location = config.get("paths", 'output')


@pytest.mark.usefixtures("spark_session")
def test(spark_session):
    input_df = spark_session.read.json(input_location)
    excepted_df = spark_session.read.json(result_location, multiLine=True)
    result_df = explode_nested_array(input_df)
    result_df = get_nested_columns(result_df)
    result_df = process_data(result_df)
    result_df = get_nested_output(result_df)
    result_df.printSchema()
    excepted_df.printSchema()
    #result_df.coalesce(1).write.json("/Users/macbookpro/IdeaProjects/pyspark_glue_jobs/src/test/resources/dataset/output/text")
    assert_df_equality(result_df, excepted_df, ignore_column_order=True, ignore_nullable=True)
    #assert result_df == excepted_df


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("TEST_GLUE_JOBS") \
        .master("local[2]") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    test(spark)