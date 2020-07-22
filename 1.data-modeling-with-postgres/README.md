## Data modeling with Postgres
This project shows how to define fact and dimension tables for star schema and write an ETL pipeline that transfers data from files into tables in Postgres using Python and SQL.

Each python file represents:
- create_tables.py: it drops and creates the tables.
- sql_queries.py: it contains all the SQL queries.
- etl.py: it's main ETL. it reads and processes files from song_data and log_data and loads them into your tables.

## Brief schema design
- There are 5 tables defined as star schema.
- [songplays tables](sql_queries.py#L12-L18)
  - fact table that contains log data associated with song plays
- users, songs, artists, time: all are dimension tables and associated with songplays table(fact table) using reference id
  - [users](sql_queries.py#L22-L24): users in the app
  - [songs](sql_queries.py#L28-L30): songs in music database
  - [artists](sql_queries.py#L34-L36): artists in music database
  - [time](sql_queries.py#L40-L42): timestamps of records in songplays broken down into specific units

## Prerequistes 
- Postgres
- Python >= 3.0

## How to create tables
```
python create_tables.py
```

## How to run ETL
```
python etl.py
```
