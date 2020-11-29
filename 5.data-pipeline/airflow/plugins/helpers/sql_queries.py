class SqlQueries:
    """
    Sql queries that insert data into fact, dimension table
    """
    songplays_table_insert = ('songplays', """
        INSERT INTO songplays (
            SELECT
                    md5(events.start_time) songplay_id,
                    events.start_time,
                    events.userid,
                    events.level,
                    songs.song_id,
                    songs.artist_id,
                    events.sessionid,
                    events.location,
                    events.useragent
                    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                FROM staging_events
                WHERE page='NextSong') events
                LEFT JOIN staging_songs songs
                ON events.song = songs.title
                    AND events.artist = songs.artist_name
                    AND events.length = songs.duration
        )
    """)

    users_table_insert = ('users', """
        INSERT INTO users (
            SELECT distinct userid, firstname, lastname, gender, level
            FROM staging_events
            WHERE page='NextSong'
        )
    """)

    songs_table_insert = ('songs', """
        INSERT INTO songs (
            SELECT distinct song_id, title, artist_id, year, duration
            FROM staging_songs
        )
    """)

    artists_table_insert = ('artists', """
        INSERT INTO artists (
            SELECT distinct artist_id, artist_name, artist_location, artist_latitude,
            artist_longitude FROM staging_songs
        )
    """)

    time_table_insert = ('time', """
        INSERT INTO time (
            SELECT start_time, extract(hour from start_time), extract(day from start_time),
                extract(week from start_time), extract(month from start_time),
                extract(year from start_time), extract(dayofweek from start_time)
            FROM songplays
        )
    """)
