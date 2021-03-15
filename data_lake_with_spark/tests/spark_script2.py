from pyspark.sql import SparkSession


if __name__ == "__main__":

    spark = SparkSession.builder.appName("test2").getOrCreate()

    df = spark.read.format('csv').option('header', 'true').load(
        "s3://spark-data-lake-123/cities.csv")
    print(df.head())

    spark.stop()
