from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField, StructType, StringType, FloatType, DateType
)


if __name__ == "__main__":

    spark = SparkSession.builder.appName("event-kafka-stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafkaRawStreamingDF = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "kafka:19092")\
        .option("subscribe", "stedi-events")\
        .option("startingOffsets", "earliest")\
        .load()

    schema = StructType([
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ])

    kafkaRawStreamingDF\
        .selectExpr("cast(value as string) value")\
        .withColumn("value", F.from_json(F.col("value"), schema))\
        .select(F.col("value.*"))\
        .createOrReplaceTempView("CustomerRisk")

    customerRiskStreamingDF = spark\
        .sql("select customer, score from CustomerRisk")

    customerRiskStreamingDF\
        .writeStream\
        .outputMode("append")\
        .format("console")\
        .start()\
        .awaitTermination()
