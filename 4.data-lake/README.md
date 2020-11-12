## Data lake with spark
This project is for building an ETL pipeline that extracts the data from S3,
processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

## Brief schema design
- There are 5 tables defined as star schema.
- songplays table: fact table that contains user log data associated with song data
- users, songs, artists, time: all are dimension tables and associated with songplays table(fact table) using reference id
  - users table: users in the app
  - songs table: songs in music database
  - artists: artists in music database
  - time: time table for each record in songplays, that is broken down into specific datetime units
- each table exists as parquet file after etl processing
 
## Prerequisites
- python >= 3.7
- pyspark

## How to run
1. fill out your AWS credentials into dl.cfg if you want to download or upload your data from storage like S3
2. run etl.py with appropriate input/output path
```shell script
$ python etl.py --help
usage: etl.py [-h] [-i INPUT] [-o OUTPUT]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        input data path
  -o OUTPUT, --output OUTPUT
                        output data path

$ python etl.py  -i s3a://your_s3_input_path -o s3a://your_s3_output_path
```
