import configparser
import psycopg2
import os
from sql_queries import copy_table_queries, insert_table_queries


def debug(cur, conn):
    cur.execute(
        "select * from stl_load_errors order by starttime desc limit 1"
    )
    conn.commit()
    print(cur.fetchall())


def load_staging_tables(cur, conn):
    print("start copying from s3 to each table")
    for query in copy_table_queries:
        try:
            print("query: " + query)
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print("load_staging_tables error: " + str(e))
            conn.rollback()


def insert_tables(cur, conn):
    print("start inserting to each table")
    for query in insert_table_queries:
        try:
            print("query: " + query)
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print("insert_tables error: " + str(e))
            conn.rollback()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    if os.getenv('DEBUG') is not None:
        debug(cur, conn)
    else:
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
