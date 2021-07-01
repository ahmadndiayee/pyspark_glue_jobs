from pyspark.sql.functions import col, explode, lit, from_json, struct, collect_list
from pyspark.sql import SparkSession
import configparser

from pyspark.sql.types import StructType, ArrayType

CONF_FILE = '/home/ahmad/IdeaProjects/pyspark_glue_jobs/src/main/resources/config.ini'

spark = SparkSession.builder \
    .appName("Melbourne") \
    .master("local[*]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()


config = configparser.ConfigParser()
config.read(CONF_FILE)
input_location = config.get("paths", 'input')

file = spark.read.json(input_location)

count = file.count()


def explode_nested_array(df):
    df = df.withColumn("_Entitlements_", explode(col("domainTree.Entitlements")))
    df = df.withColumn("_individualNames_", explode(col("domainTree.individualNames")))
    df = df.withColumn("_identifiers_", explode(col("_Entitlements_.identifiers")))
    df = df.withColumn("_entitlementStatusHistories_", explode("_Entitlements_.entitlementStatusHistories"))
    df = df.withColumn("_identifierStatusHistories_", explode(col("_identifiers_.identifierStatusHistories")))
    return df


file = explode_nested_array(file)


def get_nested_columns(df):
    df = df.select(lit(count).alias("count"), col("sequence"), col("activatedDate").alias("start_date"),
                   col("identifier"),
                   col("_individualNames_.attributes.FirstName").alias("given_name"),
                   col("_individualNames_.attributes.LastName").alias("other_given_names"),
                   col("_individualNames_.attributes.MiddleNames").alias("surname"),
                   col("_individualNames_.attributes.TypeCode").alias("type"),
                   col("_identifiers_.attributes.Identifier").alias("entitlements_identifier"),
                   col("_entitlementStatusHistories_.attributes.status").alias(
                       "entitlements_status_histories_status"),
                   col("_entitlementStatusHistories_.attributes.StartDate").alias(
                       "entitlements_status_histories_start_date"),
                   col("_identifierStatusHistories_.attributes.status"),
                   col("_identifierStatusHistories_.attributes.StartDate"), col("domainTree.attributes.DayOfBirth"),
                   col("domainTree.attributes.YearOfBirth"), col("domainTree.attributes.MonthOfBirth"))
    return df


def process_data(df):
    df = df.select(
        col("count"),
        col("sequence"),
        col("start_date"),
        col("identifier"),
        struct(
            col("given_name"),
            col("other_given_names"),
            col("surname"),
            col("type")
        ).alias("names"),
        struct(
            struct(
                col("entitlements_identifier").alias("identifier"),
                struct(
                    col("entitlements_status_histories_status").alias("status"),
                    col("entitlements_status_histories_start_date").alias("start_date"),
                ).alias("status_histories")
            ).alias("identifier"),
            struct(
                col("status").alias("status"),
                col("start_date").alias("start_date")
            ).alias("status_histories")
        ).alias("entitlements"),
        col("YearOfBirth"),
        col("MonthOfBirth"),
        col("DayOfBirth")
    ).groupBy(
        col("count"),
        col("sequence"),
        col("start_date"),
        col("identifier"),
        col("names"),
        col("entitlements"),
        col("YearOfBirth"),
        col("MonthOfBirth"),
        col("DayOfBirth")
    ).agg(
        collect_list("names").alias("names"),
        collect_list("entitlements").alias("entitlements")
    )
    return df



def main():
    df = get_nested_columns(file)
    df = process_data(df)
    output_location = config.get("paths", 'output')
    file.coalesce(1).write.json(output_location)
    df.printSchema()
    # file.show()


if __name__ == '__main__':
    main()



