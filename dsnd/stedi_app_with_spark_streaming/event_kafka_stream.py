from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField, StructType, StringType, FloatType, DateType
)


def event_kafka_stream(sp):
    customerRiskSchema = StructType([
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ])

    kafkaRawStreamingDF = sp\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "kafka:19092")\
        .option("subscribe", "stedi-events")\
        .option("startingOffsets", "earliest")\
        .load()

    # Create a view with fields similar as follows
    # +------------+-----+-----------+
    # |    customer|score| riskDate  |
    # +------------+-----+-----------+
    # |"sam@tes"...| -1.4| 2020-09...|
    # +------------+-----+-----------+
    kafkaRawStreamingDF\
        .selectExpr("cast(value as string) value")\
        .withColumn("value", F.from_json(F.col("value"), customerRiskSchema))\
        .select(F.col("value.*"))\
        .createOrReplaceTempView("CustomerRisk")

    customerRiskStreamingDF = sp\
        .sql("SELECT customer, score FROM CustomerRisk")

    return customerRiskStreamingDF


if __name__ == "__main__":
    spark = SparkSession.builder.appName("event-kafka-stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    customerRiskStreamingDF = event_kafka_stream(spark)

    customerRiskStreamingDF \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start() \
        .awaitTermination()

# The output should look like this:
# +--------------------+-----+
# |            customer|score|
# +--------------------+-----+
# |Danny.Gonzalez@te...| 11.5|
# |Trevor.Huey@test.com|-11.0|
# |Frank.Spencer@tes...| 16.0|
# ...
