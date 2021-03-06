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


def process_song_data(spark, input_data, output_data):
    """
    Process the song input data into the songs and artists tables.
    
    Args:
        spark, 
        input_data, 
        output_data
    Returns:
        None
    """
    print("\n...processing song data:")

    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*"

    print("\t...reading song data")
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        "song_id", "title", "artist_id", "artist_name", "year", "duration"
    ).distinct()

    print("\t...writing song table")
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(
        f"{output_data}songs.parquet"
    )

    print("\t...processed songs table")
    print(songs_table.printSchema())

    # extract columns to create artists table
    artists_table = df.select(
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_longitude",
        "artist_latitude",
    ).distinct()

    print("\t...writing artists table")
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(f"{output_data}artists.parquet")

    print("\t...processed artists table")
    print(artists_table.printSchema())


def process_log_data(spark, input_data, output_data):
    """
    Process the log file input data into the users, time and songplays tables.
    
    Args:
        spark, 
        input_data, 
        output_data
    Returns:
        None
    """
    print("\n...processing log data:")

    # get filepath to log data file
    log_data = f"{input_data}log_data/*"

    # read log data file
    df = spark.read.json(log_data)
    print("\t...log data read")

    # filter by actions for song plays
    df = df.where(col("page") == "NextSong")

    # extract columns for users table
    users_table = df.select(
        "userId", "firstName", "lastName", "gender", "level"
    ).distinct()

    print("\t...writing users table")
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(f"{output_data}users.parquet")

    print("\t...processed users table")
    print(users_table.printSchema())

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

    print("\t...writing partitioned time parquet files")
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(
        f"{output_data}time.parquet", mode="overwrite"
    )

    print("\t...processed time table")
    print(time_table.printSchema())

    print("\t...reading song parquet files")
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
    ).distinct()

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
    print(songplays_table.printSchema())


def main():
    # Change dir to path of current file
    import os

    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    # create spark session
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3"
    ).getOrCreate()

    # For v4 AWS S3 Bucket, e.g. Frankfurt
    # spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    # spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")

    input_data = "s3a://udacity-dend/"  # Udacity's S3 bucket
    # input_data = "s3a://dend-sparkify-etl/"  # Private S3 bucket
    # input_data = "data/"   # Local files

    output_data = "s3a://dend-sparkify-etl/"  # Private S3 bucket
    # output_data = "data/output/"  # Local files

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
