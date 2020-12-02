## Data pipeline with airflow
This project is for building an data pipeline using airflow
that contains the workflows from data load to processing, transformation, visualize.

The workflows represents as dag consisting of a task and each task is briefly explained as below.
1. Load data from s3 to AWS redshift
2. Load fact table
3. Load dimension tables
4. Run data quality check

## Brief schema design
- There are 5 tables defined as star schema.
- songplays table: fact table that contains user log data associated with song data
- users, songs, artists, time: all are dimension tables and associated with songplays table(fact table) using reference id
  - users table: users in the app
  - songs table: songs in music database
  - artists: artists in music database
  - time: time table for each record in songplays, that is broken down into specific datetime units

## Prerequisites
- python >= 3.7
- airflow
- docker (with docker-compose)

## How to run
1. run airflow
```
docker build -t udacity-airflow .
docker-compose -f docker-compose-LocalExecutor.yml up
```
2. set up your redshift cluster on AWS
    - create tables using [airflow/create_tables.sql](airflow/create_tables.sql)
3. set up necessary resources from airflow dashboard 
    - create redshift_conn_id from admin connections
    - create aws_credentials_id from admin connections
4. trigger dag
