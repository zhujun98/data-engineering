from pyspark.sql import SparkSession


if __name__ == "__main__":

    spark = SparkSession.builder.appName("spark_test").getOrCreate()

    df = spark.read.csv("s3://spark-data-lake-123/cities.csv", header=True)
    df.printSchema()
    print("="*80, "\n", "Total number of records: ", df.count(), "\n", "="*80)

    spark.stop()
