import os
from pyspark.sql.functions import explode, lit, collect_list, col, struct
from pyspark.sql import SparkSession
import configparser

CONF_FILE = '/Users/macbookpro/IdeaProjects/pyspark_glue_jobs/src/main/resources/config.ini'

config = configparser.ConfigParser()
config.read(os.path.expanduser("~/.aws/credentials"))
access_id = config.get("glue_jobs", "aws_access_key_id")
access_key = config.get("glue_jobs", "aws_secret_access_key")

spark = SparkSession.builder \
    .appName("GLUE_JOBS") \
    .master("local[1]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.hadoop.fs.s3a.access.key", access_id) \
    .config("spark.hadoop.fs.s3a.secret.key", access_key) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

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
    df = df.select(col("sequence"), col("activatedDate").alias("start_date"),
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
            ).alias("identifiers"),
            struct(
                col("status").alias("status"),
                col("start_date").alias("start_date")
            ).alias("status_histories")
        ).alias("entitlements"),
        col("YearOfBirth").alias("year_of_birth"),
        col("MonthOfBirth").alias("month_of_birth"),
        col("DayOfBirth").alias("day_of_birth")
    ).groupBy(
        col("sequence"),
        col("start_date"),
        col("identifier"),
        col("year_of_birth"),
        col("month_of_birth"),
        col("day_of_birth")
    ).agg(
        collect_list("names").alias("names"),
        collect_list("entitlements").alias("entitlements")
    ).select(col("sequence"), col("start_date"), col("identifier"), col("names"), col("entitlements"),
             col("year_of_birth"), col("month_of_birth"), col("day_of_birth"))
    return df


def get_nested_output(df):
    df = df \
        .withColumn("count", lit(df.count()).cast("long"))
    df = df.select(
            col("count"),
            struct(
                col("sequence"), col("start_date"), col("identifier"), col("names"), col("entitlements"),
                col("year_of_birth"), col("month_of_birth"), col("day_of_birth")
            ).alias("records")
        ).groupBy(col("count"))\
        .agg(
            collect_list("records").alias("records")
        )
    return df


def main():
    df = get_nested_columns(file)
    df = process_data(df)
    df = get_nested_output(df)
    output_location = config.get("paths", 'output')
    df.coalesce(1).write.json(output_location)


if __name__ == '__main__':
    main()
