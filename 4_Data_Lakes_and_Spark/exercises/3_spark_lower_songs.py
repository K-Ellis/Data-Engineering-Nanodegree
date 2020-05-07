from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
    Example program to show how to submit applications
    """

    spark = SparkSession.builder.appName("LowerSongTitles").getOrCreate()

    list_songs = ["Despacito", "Nice for what", "No tears left to cry", "Havana"]

    distributed_list_songs = spark.sparkContext.parallelize(list_songs)

    print(distributed_list_songs.map(lambda x: x.lower()).collect())

    spark.stop()
