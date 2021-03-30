from datetime import datetime
import os.path as osp

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampType
)


def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("Sparkify ETL") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data_path, output_data_path):
    # get filepath to song data file
    song_data_path = osp.join(input_data_path, "song_data/*/*/*/*.json")

    # read song data file
    song_schema = StructType([
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", LongType()),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("year", LongType()),
    ])
    df = spark.read.json(song_data_path, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select(
        ['song_id',
         'title',
         'artist_id',
         'year',
         'duration']).drop_duplicates()

    # write songs table to parquet files partitioned by year
    # Note: The code got stuck at EMR when the parquet files are partitioned by
    #       both year and artist. The reason could be that there are too
    #       many entries with "year == 0".
    songs_table.write.partitionBy("year", "artist_id").mode(
        "overwrite").parquet(osp.join(output_data_path, "songs"))

    # extract columns to create artists table
    artists_table = df.selectExpr(
        ['artist_id',
         'artist_name as name',
         'artist_location as location',
         'artist_latitude as latitude',
         'artist_longitude as longitude']).drop_duplicates()

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(
        osp.join(output_data_path, "artists"))


def process_log_data(spark, input_data, output_data_path):
    # get filepath to log data file
    log_data_path = osp.join(input_data_path, "log_data/*/*/*.json")

    # read log data file
    log_schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", LongType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("sessionId", LongType()),
        StructField("song", StringType(), False),
        StructField("status", LongType()),
        StructField("ts", LongType(), False),
        StructField("userAgent", StringType()),
        StructField("userId", StringType(), False)
    ])
    df = spark.read.json(log_data_path, schema=log_schema)

    # filter by actions for song plays
    df = df.filter(col('page') == 'NextSong')

    # extract columns for users table
    users_table = df.selectExpr(
        ['userId as user_id',
         'firstName as first_name',
         'lastName as last_name',
         'gender',
         'level'])
    users_table.show(5)

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(
        osp.join(output_data_path, "users"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(0.001 * int(x)),
                        TimestampType())
    df = df.withColumn("start_time", get_timestamp(col("ts")))

    # create datetime column from original timestamp column
    df = df.withColumn("hour", F.hour("start_time")) \
        .withColumn("day", F.dayofmonth("start_time")) \
        .withColumn("week", F.weekofyear("start_time")) \
        .withColumn("month", F.month("start_time")) \
        .withColumn("year", F.year("start_time")) \
        .withColumn("weekday", F.dayofweek("start_time"))

    # extract columns to create time table
    time_table = df.select(
        ['start_time',
         'hour',
         'day',
         'week',
         'month',
         'year',
         'weekday']).drop_duplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(
        osp.join(output_data_path, "time"))

    # read in song data to use for songplays table
    songs_df = spark.read.load(osp.join(output_data_path, "songs"))

    # extract columns from joined song and log datasets to create songplays
    # table
    songplays_table = (
        df.join(songs_df.withColumnRenamed("year", "year_songs_table"),
                songs_df.title == col("song"))
            .selectExpr([
                "start_time",
                "userId as user_id",
                "level",
                "song as song_id",
                "artist_id",
                "sessionId as session_id",
                "location",
                "userAgent as user_agent",
                "month",
                "year"])
            .withColumn("songplay_id", F.monotonically_increasing_id())
    )

    # write songplays table to parquet files
    songplays_table.write.partitionBy("year", "month").mode(
        "overwrite").parquet(osp.join(output_data_path, "songplays"))


if __name__ == "__main__":
    spark = create_spark_session()
    input_data_path = "s3a://udacity-dend/"
    output_data_path = "s3a://spark-data-lake-123/"

    process_song_data(spark, input_data_path, output_data_path)
    process_log_data(spark, input_data_path, output_data_path)
