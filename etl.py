import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    songs_stage = spark.read.json(song_data)

    # extract columns to create songs table
    
    songs_table = songs_stage.select(col('song_id'),
                       col('title'),
                       col('artist_id'),
                       col('year'),
                       col('duration')).orderBy('song_id')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = songs_stage.select(col('artist_id'),
                       col('artist_name').alias('name'),
                       col('artist_location').alias('location'),
                       col('artist_latitude').alias('latitude'),
                       col('artist_longitude').alias('longitude')).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data

    # read log data file
    log_data_raw = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_data_stage = log_data_raw.filter(log_data_raw.page == 'NextSong')

    # extract columns for users table    
    users_table = log_data_stage.select(col('userId').alias('user_id'),
                       col('firstName').alias('firt_name'),
                       col('lastName').alias('last_name'),
                       col('gender'),
                       col('level')).distinct()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000))
    log_data_stage = log_data_stage.withColumn('timestamp', get_timestamp(log_data_stage.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    log_data_stage = log_data_stage.withColumn('date', get_datetime(log_data_stage.ts))
    
    # extract columns to create time table
    time_table = log_data_stage.select(col('date').alias('start_time'),
                       hour('date').alias('hour'),
                       dayofmonth('date').alias('day'),
                       weekofyear('date').alias('week'),
                       month('date').alias('month'),
                       year('date').alias('year'),
                       date_format('date','EEEE').alias('weekday')).distinct().orderBy('start_time')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    # song_df = spark.read.parquet(os.path.join(output_data,'/songs/'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_data_stage.join(\
                                      songs_stage,\
                                      (log_data_stage.artist == songs_stage.artist_name)\
                                      & (log_data_stage.song == songs_stage.title))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
