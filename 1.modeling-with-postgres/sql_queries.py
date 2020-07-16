# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

songplay_table_create = ("""
create table songplays
    (songplay_id int, start_time int, user_id varchar, level varchar, song_id varchar, artist_id varchar, session_id varchar, location varchar, user_agent varchar);
""")

user_table_create = ("""
create table users(user_id varchar, first_name varchar, last_name varchar, gender char, level varchar);
""")

song_table_create = ("""
create table songs(song_id varchar, title varchar, artist_id varchar, year int, duration int);
""")

artist_table_create = ("""
create table artists(artist_id varchar, name varchar, location varchar, latitude float, longtitude float);
""")

time_table_create = ("""
create table time(start_time int, hour int, day int, week int, month int, year int, weekday int);
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id,
location, user_agent) values(%s, %s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level) values(%s, %s, %s, %s, %s);
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration) values(%s, %s, %s, %s, %s);
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longtitude) values(%s, %s, %s, %s, %s);
""")


time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday) values(%s, %s, %s, %s, %s, %s, %s);
""")

# FIND SONGS

song_select = ("""
select (songs.song_id, songs.artist_id) from songs join artists on songs.artist_id = artists.artist_id where 
songs.title=%s and artists.name=%s and songs.duration=%s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]