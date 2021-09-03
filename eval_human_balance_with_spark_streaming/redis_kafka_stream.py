from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField, StructType, StringType, BooleanType, ArrayType, FloatType,
    DateType
)


if __name__ == "__main__":

    # Note: The Redis Source for Kafka has redundant fields zSetEntries and
    # zsetentries, only one should be parsed.
    redisServerTopicSchema = StructType([
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("Ch", BooleanType()),
        StructField("Incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("Score", FloatType())
            ]))
        )
    ])

    customerSchema = StructType([
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", DateType())
    ])

    customerRiskSchema = StructType([
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ])

    spark = SparkSession.builder.appName("redis-kafka-stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafkaRawStreamingDF = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "kafka:19092")\
        .option("subscribe", "redis-server")\
        .option("startingOffsets", "earliest")\
        .load()

    # Create a view with fields similar as follows:
    # +------------+-----+-----------+------------+---------+-----+-----+-----------------+
    # |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
    # +------------+-----+-----------+------------+---------+-----+-----+-----------------+
    # |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
    # +------------+-----+-----------+------------+---------+-----+-----+-----------------+
    kafkaRawStreamingDF\
        .selectExpr("cast(value as string) value")\
        .withColumn("value", F.from_json(F.col("value"), redisServerTopicSchema))\
        .select(F.col("value.*")) \
        .withColumn("value", F.lit(None).cast(StringType())) \
        .withColumn("expiredType", F.lit(None).cast(StringType()))\
        .withColumn("expiredValue", F.lit(None).cast(StringType()))\
        .withColumnRenamed("Ch", "ch")\
        .withColumnRenamed("Incr", "incr") \
        .createOrReplaceTempView("RedisSortedSet")

    # Create a CustomerRecords view similar as follows:
    # +--------------+--------------------+----------+----------+
    # | customerName | email              | phone    | birthDay |
    # +--------------+--------------------+----------+----------+
    # |Danny Gonzalez|Danny.Gonzalez@te...|8015551212|1965-01-01|
    #
    # The reason we do it this way is that the syntax available select against
    # a view is different than a dataframe, and it makes it easy to select the
    # nth element of an array in a sql column.
    spark.sql(
        "SELECT key, zSetEntries[0].element AS encodedCustomer FROM RedisSortedSet")\
        .withColumn("customer", F.unbase64(F.col("encodedCustomer")).cast(StringType()))\
        .withColumn("customer", F.from_json(F.col("customer"), customerSchema))\
        .select(F.col("customer.*"))\
        .createOrReplaceTempView("CustomerRecords")

    emailAndBirthYearStreamingDF = spark.sql(
        """
        SELECT email, birthDay FROM CustomerRecords
        WHERE email IS NOT NULL AND birthDay IS NOT NULL
        """
        ).withColumn("birthYear", F.split(F.col("birthDay"), "-").getItem(0))\
        .select(F.col("email"), F.col("birthYear"))

    emailAndBirthYearStreamingDF\
        .writeStream\
        .outputMode("append")\
        .format("console")\
        .start()\
        .awaitTermination()

# The output should look like this:
# +--------------------+---------+
# |               email|birthYear|
# +--------------------+---------+
# |Danny.Gonzalez@te...|     1965|
# |Trevor.Huey@test.com|     1964|
# |Frank.Spencer@tes...|     1963|
# ...
