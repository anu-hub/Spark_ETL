import configparser
from datetime import datetime
import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import DateType, TimestampType
import datetime
import pyspark.sql.functions as fs


config = configparser.ConfigParser()
config.read('dl.cfg')
    
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    """
    :return: spark connection
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function to read and process Song data from S3 storage with error handling
    :param spark: spark connection
    :param input_data: input file path - s3 bucket location to read json files
    :param output_data: output file path - s3 bucket location to store parquet files
    :return: song data frame which will be use to populate SongPlay table in the process_log_data function
    """
    # get file path to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    print(song_data)

    
    # read song data file
    df_song_data = spark.read.json(song_data)
    

    # extract columns & write songs data to parquet files partitioned by year and artist
    songs_output = output_data + "Songs_Table"
    print(songs_output)
    
    df_song_data.select("song_id", "title", "artist_id", "year", "duration") \
        .dropDuplicates().where(
        (df_song_data.song_id != ' ') & (df_song_data.artist_id != ' ') & (df_song_data.year != 0)) \
        .write.partitionBy("year", "artist_id") \
        .format("parquet") \
        .option("path", songs_output) \
        .mode('overwrite') \
        .saveAsTable("Songs_Table")

    
    # extract columns & write artists table to parquet files
    artists_output = output_data + "Artist_Table"
    print(artists_output)
    
    df_song_data.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude") \
        .dropDuplicates().where(df_song_data.artist_id != ' ') \
        .option("path", artists_output) \
        .mode('overwrite') \
        .saveAsTable("Artist_Table")
    
    return df_song_data


def process_log_data(spark, input_data, output_data, song_df):
    """
    Function to read and process Log data from S3 storage with error handling
    :param spark: spark connection
    :param input_data: input file path - s3 bucket location to read json files
    :param output_data: output file path - s3 bucket location to store parquet files
    :param song_df: song data frame which will be use to populate SongPlay table
    :return: none
    """
    # get file path to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df_log_data = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log_data = df_log_data.filter(df_log_data.page == "NextSong")
    
    # extract columns & write users table to parquet files
    users_output = output_data + "User_Table"
    print(users_output)
    
    df_log_data.select("userId", "firstName", "lastName", "level", "gender") \
        .dropDuplicates().where(df_log_data.userId != ' ') \
        .write.format("parquet") \
        .option("path", users_output) \
        .mode('overwrite') \
        .saveAsTable("User_Table")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0), TimestampType())
    
    # create datetime column from original timestamp column
    user_log_ts = df_log_data.where(df_log_data.ts > 0) \
        .withColumn("start_time", get_timestamp(df_log_data.ts)) \
        .select("start_time")
    
    # extract columns to create time table
    user_log_ts = user_log_ts.withColumn("month", fs.month("start_time")) \
        .withColumn("year", fs.year("start_time")) \
        .withColumn("hour", fs.hour("start_time")) \
        .withColumn("week", fs.weekofyear("start_time")) \
        .withColumn("day", fs.dayofmonth("start_time")) \
        .withColumn("weekday", fs.dayofweek("start_time"))
    
    # write time table to parquet files partitioned by year and month
    time_output = output_data + "Time_Table"
    print(time_output)
    
    user_log_ts.select("start_time", "hour", "day", "week", "month", "year") \
        .dropDuplicates() \
        .write.partitionBy("year", "month") \
        .format("parquet") \
        .option("path", time_output) \
        .mode('overwrite') \
        .saveAsTable("Time_Table")

    # read in song data to use for songplays table
    df_song_data = song_df 
    df_song_data.createOrReplaceTempView("df_song_table")

    # extract columns from joined song and log datasets to create songplays table
    df_songplay = df_log_data.where(df_log_data.ts > 0) \
        .withColumn("start_time", get_timestamp(df_log_data.ts)) \
        .select((fs.to_timestamp("start_time").alias('start_time')),
                "userId", "level", "sessionId", "location", "song", "length", "artist", "page")

    df_songplay = df_songplay.withColumn("month", fs.month("start_time")) \
        .withColumn("year", fs.year("start_time"))

    df_songplay.createOrReplaceTempView("df_songplay_table")


    # write songplays table to parquet files partitioned by year and month

    songplays_output = output_data + "Songplays_Table"
    print(songplays_output)

    songplays_table = spark.sql(''' 
              select distinct songplay_id,start_time, userId, level, sessionId, location, song_id, artist_id, month, year
              from (
              SELECT sp.start_time, sp.userId, sp.level, sp.sessionId,sp.location, s.song_id,s.artist_id,
              ROW_NUMBER () over (order by start_time) songplay_id,sp.month,sp.year,
              ROW_NUMBER () OVER ( PARTITION BY sp.userId, level ORDER BY sp.start_time desc ) rnk
              FROM df_songplay_table sp LEFT OUTER JOIN df_song_table s
              ON sp.song = s.title
              AND sp.artist = s.artist_name
              AND sp.length = s.duration 
              where sp.page = "NextSong" 
              AND sp.userId != 'None'
              AND s.song_id != 'None'
              AND s.artist_id != 'None'
              )  where rnk =1
              ''')

    songplays_table.select("songplay_id", "start_time", "userId", "level", "sessionId",
                           "location", "song_id", "artist_id", "month", "year") \
        .dropDuplicates() \
        .write.partitionBy("year", "month") \
        .format("parquet") \
        .option("path", songplays_output) \
        .saveAsTable("Songplays_Table")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-s3-output/"
    
    song_df = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, song_df)


if __name__ == "__main__":
    main()
