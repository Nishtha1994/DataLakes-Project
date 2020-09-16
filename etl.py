import configparser
from datetime import datetime
from pandas import DataFrame
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import  pyspark.sql.functions as F
import pandas as pd



config = configparser.ConfigParser()
#Normally this file should be in ~/.aws/credentials
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['KEYS']['AWS_SECRET_ACCESS_KEY']

input_data = "s3a://udacity-dend/"

'''Creating a Spark session'''
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

'''Load data from song_data dataset and extract columns
    for songs and artist tables and write the data into parquet
    files which will be loaded on s3. '''
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
#     song_data = "s3a://udacity-dend/song-data/A/B/N/TRABNQK128F14A2178.json" (for one file)
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    
   # read song data file
    df = spark.read.json(song_data)
       
    # extract columns to create songs table
    songs_table =df.select('song_id', 'title', 'artist_id',
                            'year', 'duration')
    #Dropping duplicates
    songs_table=songs_table.dropDuplicates()
    songs_table.createOrReplaceTempView('songs')
#     # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table =  df.select()'artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude')
    #Dropping duplicates
    artists_table=artists_table.dropDuplicates()
    artists_table.createOrReplaceTempView('artists')
    # write artists table to parquet files
    artists_table.write.format("parquet").save("data/artist.parquet")
   
'''Load data from log_data dataset and extract columns
    for songplays, users, time atbles and write the data into parquet
    files which will be loaded on s3. '''
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
#     log_data ="s3a://udacity-dend/log_data/2018/11/2018-11-13-events.json" (for one file)
    log_data = 'f{input_data}/log_data/'
    
    # read log data file
    df =  spark.read.json(log_data)
    df= df.filter(df.page == 'NextSong')
    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName',
                            'gender', 'level')
        
    #Dropping duplicates
    users_table=users_table.dropDuplicates()
     # write users table to parquet files
    users_table.write.format("parquet").save("data/users.parquet")
    users.createOrReplaceTempView('users')
    # create timestamp column from original timestamp column
    format_timestamp_udf = udf(lambda x: datetime.fromtimestamp(x/ 1000.0).strftime('%Y-%m-%d %H:00:00'))   
    df = df.withColumn('timestamp', format_timestamp_udf(df['ts']))

    # extract columns to create time table
    time_table = df.select('datetime') \
                           .withColumn('start_time', actions_df.datetime) \
                           .withColumn('hour', hour('datetime')) \
                           .withColumn('day', dayofmonth('datetime')) \
                           .withColumn('week', weekofyear('datetime')) \
                           .withColumn('month', month('datetime')) \
                           .withColumn('year', year('datetime')) \
                           .withColumn('weekday', dayofweek('datetime')) \
                           .dropDuplicates()
    #Dropping duplicates
    time_table=time_table.dropDuplicates()
#     # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").format("parquet").save("data/time.parquet")
    
    # read in song data to use for songplays table
    song_df = f'{input_data}/song_data/*/*/*/*.json'
    song_df.registerTempTable("songs")
    df.registerTempTable("logs")
    time_table.registerTempTable("time")
    users_table.createOrReplaceTempView("users")
    
    
    
     # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(
      "select users.userId,logs.level,songs.song_id,songs.artist_id,logs.sessionId,logs.location,logs.userAgent,time.year,time.month from songs join logs on songs.artist_id=logs.artist join users on users.userId=logs.userId join time on time.hour=logs.hour")
    
# alternate
# songplays_table = songs.join(users, col('users.artist') == col(
#         'songs.artist_name'), 'inner')

    #  write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month")
    songplays_table=songplays_table.select('userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent')
    #Dropping duplicates
    songplays_table=songplays_table.dropDuplicates()
    songplays_table.write.format("parquet").save("data/songplays.parquet")



def main():
    """
    Perform the following roles:
    1.) Get or create a spark session.
    1.) Read the song and log data from s3.
    2.) take the data and transform them to tables
    which will then be written to parquet files.
    3.) Load the parquet files on s3.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
