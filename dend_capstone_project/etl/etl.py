import os.path as osp
import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType
)
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Sparkify ETL").getOrCreate()


def process_trip_data(input_data_path, output_data_path):
    paths_old = osp.join(input_data_path, "old")
    paths_new = osp.join(input_data_path, "new")

    trip_data_new_schema = StructType([
        StructField('ride_id', StringType()),
        StructField('rideable_type', StringType()),
        StructField('started_at', TimestampType()),
        StructField('ended_at', TimestampType()),
        StructField('start_station_name', StringType()),
        StructField('start_station_id', LongType()),
        StructField('end_station_name', StringType()),
        StructField('end_station_id', LongType()),
        StructField('start_lat', DoubleType()),
        StructField('start_lng', DoubleType()),
        StructField('end_lat', DoubleType()),
        StructField('end_lng', DoubleType()),
        StructField("member_casual", StringType())
    ])

    trip_data_new = spark.read.csv(paths_new,
                                   header=True,
                                   schema=trip_data_new_schema)

    trip_data_old_schema = StructType([
        StructField('Duration', DoubleType()),
        StructField('Start date', TimestampType()),
        StructField('End date', TimestampType()),
        StructField('Start station number', LongType()),
        StructField('Start station', StringType()),
        StructField('End station number', LongType()),
        StructField('End station', StringType()),
        StructField('Bike number', StringType()),
        StructField("Member type", StringType())
    ])

    trip_data_old = spark.read.csv(paths_old,
                                   header=True,
                                   schema=trip_data_old_schema)

    station_data = trip_data_old.select(
        F.col("Start station number").alias("station_id"),
        F.col("Start station").alias("station_name")).distinct().union(
        trip_data_old.select("End station number",
                             "End station").distinct()).union(
        trip_data_new.select("start_station_id",
                             "start_station_name").distinct()).union(
        trip_data_new.select("end_station_id",
                             "end_station_name").distinct()).distinct().sort(
        "station_id", ascending=True).dropna(
        how="any", subset=["station_id"]).filter(
        F.col("station_id") != 0).dropDuplicates(subset=["station_id"])

    trip_data = trip_data_old.select(
        F.lit(None).alias("ride_id").cast(StringType()),
        F.lit(None).alias("rideable_type").cast(StringType()),
        F.col("Start date").alias("started_at"),
        F.col("End date").alias("ended_at"),
        F.col("Start station number").alias("start_station_id"),
        F.col("End station number").alias("end_station_id"),
        F.col("Member type").alias("member_casual")).union(
        trip_data_new.select(
            "ride_id", "rideable_type", "started_at", "ended_at",
            "start_station_id", "end_station_id", "member_casual"))

    # Clean up.
    trip_data = trip_data.dropna(
        how="any", subset=["start_station_id", "end_station_id"]).filter(
        (F.col("start_station_id") != 0) & (F.col("end_station_id") != 0))

    # Add primary key "tid" and foreign key "start_date".
    # FIXME: monotonically_increase_id() does not return a sequence!
    trip_data = trip_data.withColumn(
        "tid", F.monotonically_increasing_id()).withColumn(
        "start_date", F.to_date(F.col("started_at")))

    # station_data.show(5)
    # trip_data.show(5)

    # write station_data and trip_data to parquet files
    station_data.write.mode("overwrite").parquet(
        osp.join(output_data_path, "station_data"))

    # write trip_data to parquet files
    trip_data.write.partitionBy("start_station_id").mode(
        "overwrite").parquet(osp.join(output_data_path, "trip_data"))


def process_covid_data(input_data_path, output_data_path):
    # Select only interested columns
    covid_data = spark.read.json(input_data_path).select(
        "dataQualityGrade", "date", "state", "death", "deathIncrease",
        "hospitalizedCurrently", "hospitalizedDischarged",
        "hospitalizedIncrease",
        "positive", "positiveIncrease", "recovered"
    )
    # Select only data from Washington DC
    covid_data = covid_data.filter(F.col("state") == "DC").drop("state")

    # Drop columns which has a single value (e.g. null), which typically
    # means data is not available.
    covid_data = covid_data.drop(
        "dataQualityGrade", "hospitalizedDischarged", "hospitalizedIncrease")

    # Convert type of column "date" from `long` to `date`.
    func = F.udf(lambda x: datetime.strptime(str(x), '%Y%m%d'), DateType())
    covid_data = covid_data.withColumn("date", func(F.col("date")))

    covid_data = covid_data.fillna(0).orderBy("date")

    covid_data = covid_data.dropDuplicates(["date"])

    # covid_data.show(5)

    # write covid_data to parquet files
    covid_data.write.mode("overwrite").parquet(
        osp.join(output_data_path, "covid_data"))


def process_weather_data(input_data_path, output_data_path):
    weather_data_schema = StructType([
        StructField('STATION', StringType()),
        StructField('NAME', StringType()),
        StructField('DATE', DateType()),
        StructField('AWND', DoubleType()),
        StructField('TAVG', DoubleType()),
        StructField('TMAX', DoubleType()),
        StructField('TMIN', DoubleType()),
        StructField('TOBS', DoubleType()),
        StructField('WDF2', DoubleType()),
        StructField('WDF5', DoubleType()),
        StructField('WDMV', DoubleType()),
        StructField('WSF2', DoubleType()),
        StructField('WSF5', DoubleType()),
        StructField('WT01', StringType()),
        StructField('WT02', StringType()),
        StructField('WT03', StringType()),
        StructField('WT04', StringType()),
        StructField('WT05', StringType()),
        StructField('WT06', StringType()),
        StructField('WT08', StringType()),
        StructField('WT11', StringType())
    ])

    weather_data = spark.read.csv(
        input_data_path, header=True, schema=weather_data_schema).drop(
        "NAME", "TOBS", "WDF2", "WDF5", "WDMV", "WSF2", "WSF5")

    # Remove rows if any of the columns "AWND", "TAVG", "TMAX" and "TMIN"
    # contain null. Afterwards, replace null in WT?? with 0 and cast the
    # data type to boolean.

    weather_data = weather_data.filter(
        F.col("AWND").isNotNull()).filter(
        F.col("TAVG").isNotNull()).filter(
        F.col("TMAX").isNotNull()).filter(
        F.col("TMIN").isNotNull())

    for i in ['01', "02", "03", "04", "05", "06", "08", "11"]:
        col_name = f"WT{i}"
        orig_col_name = f"{col_name}_orig"
        weather_data = weather_data.fillna(
            '0', subset=[col_name]).withColumnRenamed(
            col_name, orig_col_name).withColumn(
            col_name, F.col(orig_col_name).cast(BooleanType())).drop(
            orig_col_name)

    # Select one of the three stations.
    weather_data = weather_data.filter(F.col("STATION") == "USW00093721").drop(
        "STATION")

    # weather_data.show(5)

    # write weather_data to parquet files
    weather_data.write.mode("overwrite").parquet(
        osp.join(output_data_path, "weather_data"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true")

    args = parser.parse_args()

    if args.local:
        root_folder = "/opt/workspace"
    else:
        root_folder = "s3://dend-capstone-project-workspace"

    output_folder = osp.join(root_folder, "processed")

    process_trip_data(f"{root_folder}/datasets/capitalbikeshare_tripdata",
                      output_folder)
    process_covid_data(f"{root_folder}/datasets/covid_data/daily.json",
                       output_folder)
    process_weather_data(f"{root_folder}/datasets/weather_data/*_daily.csv",
                         output_folder)
