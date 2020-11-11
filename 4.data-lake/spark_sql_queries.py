
# spark sql query to extract song table
songs_table_sql = '''
select song_id, title, artist_id, year, duration
    from staging_song where song_id is not null
'''

# spark sql query to extract artists table
artists_table_sql = '''
select artist_id, artist_name as name, artist_location as location,
    artist_latitude as latitude, artist_longitude as longitude
    from staging_song where artist_id is not null
    '''

# spark sql query to extract user table
user_table_sql = '''
select userId as user_id, firstName as first_name,
    lastName as last_name, gender, level
    from staging_log where userId is not null
'''

# spark sql query to extract songplays table
songplays_table_sql = '''
select start_time, month(start_time) as month, year(start_time) as year,
    user_id, level, song_id, artist_id, session_id, location, user_agent from (
        select title, artist_name, song_id, artist_id from staging_song
    ) as song inner join (
        select from_unixtime(round(ts/1000, 0)) as start_time, song, artist,
        userId as user_id, level, sessionId as session_id, location,
        userAgent as user_agent from staging_log
    ) as log
    on song.title = log.song and song.artist_name = log.artist
'''

# spark sql query to extract time table
time_table_sql = '''
select start_time, hour(start_time) as hour, dayofmonth(start_time) as day,
    weekofyear(start_time) as week, month(start_time) as month,
    year(start_time) as year, dayofweek(start_time) as weekday from songplays
'''
