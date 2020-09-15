## Data modeling with data warehouse
This project is for building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for analytics.

## Brief schema design
- There are 5 tables defined as star schema.
- [songplays tables](sql_queries.py#L59-L71)
  - fact table that contains log data associated with song plays
- users, songs, artists, time: all are dimension tables and associated with songplays table(fact table) using reference id
  - [users](sql_queries.py#L73-L81): users in the app
  - [songs](sql_queries.py#L83-L87): songs in music database
  - [artists](sql_queries.py#L89-L93): artists in music database
  - [time](sql_queries.py#L95-L99): timestamps of records in songplays broken down into specific units

## ETL Pipeline
1. Load data from S3 to staging tables(staging_events, staging_songs) on Redshift.
2. Load data from staging tables to analytics tables on Redshift.
3. Test by running queries such as `insert`  

## Prerequistes 
- AWS
- Python >= 3.0


## How to run
1. Create Redshift cluster.
  - fill in cluster information in [ClUSTER] section in dwh.cfg
  - create IAM role to access Redshift cluster and fill in the ARN in [IAM_ROLE] section in dwh.cfg
2. Create tables
```
python create_tables.py
```

3. Run ETL
```
python etl.py
```
