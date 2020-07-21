import os
import glob
import psycopg2
import pandas as pd
import numpy as np
import logging
from sql_queries import songplay_table_insert, artist_table_insert, \
                song_table_insert, time_table_insert, song_select, \
                user_table_insert


logger = logging.getLogger(__name__)


def process_song_file(cur, filepath):
    """
    Load song file and process to insert song/artist table
    :param cur: Postgress cursor
    :param filepath: file path to load data
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df.loc[:, ['song_id', 'title', 'artist_id',
                           'year', 'duration']]
    cur.execute(song_table_insert, song_data.values[0])

    # insert artist record
    artist_data = df.loc[:, ['artist_id', 'artist_name', 'artist_location',
                             'artist_latitude', 'artist_longitude']]
    cur.execute(artist_table_insert, artist_data.values[0])


def process_log_file(cur, filepath):
    """
    Load log file and process to insert songplay table
    :param cur: Postgress cursor
    :param filepath: file path to load data
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    ts = pd.to_datetime(df['ts'], unit='ms')

    # covert millisecond to second timestamp
    df['ts'] = df['ts'].div(1000).astype(int)

    # insert time data records
    time_data = np.array([df['ts'], ts.dt.hour, ts.dt.day, ts.dt.weekofyear,
                          ts.dt.month, ts.dt.year, ts.dt.weekday])
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 'month',
                     'year', 'weekday')
    time_df = pd.DataFrame(time_data.T, columns=list(column_labels))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ['userId', 'firstName', 'lastName', 'gender', 'level',
                         'level']]

    # insert user records
    for _, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
            # insert songplay record
            songplay_data = (row.ts, row.userId, row.level, songid,
                             artistid, row.sessionId, row.location,
                             row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Load all json files and process each
    :param cur: Postgress cursor
    :param conn: Postgress connection
    :param filepath: file path to load data
    :param func: process function
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    logger.info('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        logger.info(datafile)
        func(cur, datafile)
        conn.commit()
        logger.info('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb \
                            user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
