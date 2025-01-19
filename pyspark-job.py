from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def process_data():

    spark = SparkSession.builder.appName("Transformation Of NewsData")\
                    .getOrCreate()
    
    bucket = "snowflake_projects_sa"
    news_dat_path = f"gs://{bucket}/news_data_analysis/parquet_files/"
    destination_path = f"gs://{bucket}/news_data_analysis/destination/"
    df = spark.read.format('parquet').load(news_dat_path)

    new_df = df.groupBy(col("author")).agg(
                            count('*').alias("article_count")
                            ,max(col("timestamp")).alias("latest_article_date")
                            ,countDistinct(col("source")).alias("distinct_sources")
                                        ).filter(col("author").isNotNull())\
                .orderBy(col("article_count").desc())
    
    new_df.coalesce(1).write.format("parquet").mode("overwrite").save(destination_path)
    
    spark.stop()

if __name__ == "__main__":
    process_data()
    
