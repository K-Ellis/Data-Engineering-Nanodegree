import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    hour,
    weekofyear,
    date_format,
    to_timestamp,
    from_unixtime,
    dayofweek,
    monotonically_increasing_id,
)


config = configparser.ConfigParser()

config.read(os.path.join(os.path.dirname(__file__), "dl.cfg"))

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    print("\n...processing song data:")

    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        "song_id", "title", "artist_id", "artist_name", "year", "duration"
    ).distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(
        f"{output_data}songs.parquet"
    )

    print("\t...processed songs table")

    # extract columns to create artists table
    artists_table = df.select(
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_longitude",
        "artist_latitude",
    ).distinct()

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(f"{output_data}artists.parquet")

    print("\t...processed artists table")


def process_log_data(spark, input_data, output_data):
    print("\n...processing log data:")

    # get filepath to log data file
    log_data = f"{input_data}log_data/*"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(col("page") == "NextSong")

    # extract columns for users table
    users_table = df.select(
        "userId", "firstName", "lastName", "gender", "level"
    ).distinct()

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(f"{output_data}users.parquet")

    print("\t...processed users table")

    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp", to_timestamp(from_unixtime(col("ts") / 1000)))

    # create datetime columns from original timestamp colum
    time_table = (
        df.select("timestamp")
        .withColumn("hour", hour("timestamp"))
        .withColumn("day", dayofmonth("timestamp"))
        .withColumn("week", weekofyear("timestamp"))
        .withColumn("weekday", dayofweek("timestamp"))
        .withColumn("weekday_name", date_format("timestamp", "E"))
        .withColumn("month", month("timestamp"))
        .withColumn("year", year("timestamp"))
        .distinct()
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(
        f"{output_data}time.parquet", mode="overwrite"
    )

    print("\t...processed time table")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(f"{output_data}songs.parquet").select(
        "song_id", "title", "artist_id", "artist_name"
    )

    # # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.select(
        col("artist").alias("artist_name"),
        "level",
        "location",
        col("sessionId").alias("session_id"),
        col("song").alias("title"),
        col("userAgent").alias("user_agent"),
        col("UserId").alias("user_id"),
        col("timestamp").alias("start_time"),
    )

    songplays_table = (
        songplays_table.join(song_df, ["title", "artist_name"], "inner",)
        .distinct()
        .withColumn("songplay_id", monotonically_increasing_id())
        .withColumn("year", year("start_time"))
        .withColumn("month", month("start_time"))
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(
        f"{output_data}songplays.parquet"
    )

    print("\t...processed songplays table")


def main():
    import os

    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    # input_data = "data/"

    output_data = "data/output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
