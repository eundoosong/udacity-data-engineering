import configparser
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from spark_sql_queries import songs_table_sql, artists_table_sql,\
    user_table_sql, songplays_table_sql, time_table_sql

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Return spark session
    :return: spark session
    """
    return SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()


def process_song_data(spark, input_data_path, output_data_path):
    """
    read song data in json and process to create songs, artists table
    :param spark: spark session
    :param input_data_path: input data path
    :param output_data_path: output data path
    """
    # get song json file from s3 and create staging_song table
    staging_song = spark.read.json(input_data_path)
    staging_song.createOrReplaceTempView("staging_song")

    # extract columns to create songs table
    songs_table = spark.sql(songs_table_sql)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id")\
        .parquet(output_data_path + "/songs")

    # extract columns to create artists table
    artists_table = spark.sql(artists_table_sql)

    # write artists table to parquet files
    artists_table.write.parquet(output_data_path + "/artists")


def process_log_data(spark, input_data_path, output_data_path):
    """
    read log data in json and process to create users, time, songplays table
    :param spark: spark session
    :param input_data_path: input data path
    :param output_data_path: output data path
    """
    # get song json file from s3 and create staging_song table
    staging_log = spark.read.json(input_data_path)
    staging_log.createOrReplaceTempView("staging_log")

    # extract columns for users table
    users_table = spark.sql(user_table_sql)
    # write users table to parquet files
    users_table.write.parquet(output_data_path + "/users")

    # extract columns from joined song and log datasets
    # to create songplays table
    songplays_table = spark.sql(songplays_table_sql)
    songplays_table = songplays_table.withColumn('songplay_id',
                                                 monotonically_increasing_id())
    songplays_table.createOrReplaceTempView("songplays")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month")\
        .parquet(output_data_path + "/songplays")

    # extract columns to create time table
    time_table = spark.sql(time_table_sql)

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month")\
        .parquet(output_data_path + "/time")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='input data path',
                        default='s3a://udacity-dend')
    parser.add_argument('-o', '--output', help='output data path',
                        default='output')
    args = parser.parse_args()
    spark = create_spark_session()

    # TODO: should give song and log input path respectively as argument.
    process_song_data(spark, args.input+"/song_data/*/*/*/*", args.output)
    process_log_data(spark, args.input+"/log_data/2018/11/", args.output)


if __name__ == "__main__":
    main()
