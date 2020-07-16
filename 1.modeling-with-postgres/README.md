## Data modeling with Postgres
This project shows how to define fact and dimension tables for star schema and write an ETL pipeline that transfers data from files into tables in Postgres using Python and SQL.

## Brief schema design
- There are 5 tables defined as star schema.
- songplays tables: fact table that represents both song and log records.
- users, songs, artists, time: all are dimension tables and associated with songplays table(fact table) using id

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
