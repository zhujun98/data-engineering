from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from event_kafka_stream import event_kafka_stream
from redis_kafka_stream import redis_kafka_stream


if __name__ == "__main__":
    spark = SparkSession.builder.appName("kafka-join").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    customerRiskStreamingDF = event_kafka_stream(spark)
    emailAndBirthYearStreamingDF = redis_kafka_stream(spark)

    joinedStreamingDF = customerRiskStreamingDF.join(
        emailAndBirthYearStreamingDF, F.expr("customer = email"))

    # for debug
    # The output should look like this:
    # +--------------------+-----+--------------------+---------+
    # | customer           |score| email              |birthYear|
    # +--------------------+-----+--------------------+---------+
    # |Senthil.Abram@tes...| -2.0|Senthil.Abram@tes...| 1958    |
    # |Senthil.Abram@tes...| -4.0|Senthil.Abram@tes...| 1958    |
    # |Gail.Lincoln@test...| -3.0|Gail.Lincoln@test...| 1951    |
    # ...
    write_to_console = joinedStreamingDF \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    write_to_kafka = joinedStreamingDF\
        .selectExpr("CAST(customer AS STRING) AS key", "to_json(struct(*)) AS value")\
        .writeStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:19092")\
        .option("topic", "customer-risk")\
        .option("checkpointLocation", "/tmp/kafkacheckpoint")\
        .start()

    write_to_console.awaitTermination()
    write_to_kafka.awaitTermination()
