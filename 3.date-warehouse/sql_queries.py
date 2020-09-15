import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")
ARN = config.get("IAM_ROLE", "ARN")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
REGION = config.get("CLUSTER", "REGION")

# DROP TABLES
staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES
staging_events_table_create = ("""
create table staging_events (
    artist varchar(255),
    auth varchar(255),
    first_name varchar(255),
    gender varchar(5),
    item_in_session integer,
    last_name varchar(255),
    length double precision,
    level  varchar(255),
    location varchar(255),
    method varchar(10),
    page varchar(255),
    registration BIGINT,
    session_id integer,
    song varchar(255),
    status integer,
    ts BIGINT,
    user_agent varchar(255),
    user_id integer
    )
    """)

staging_songs_table_create = ("""
create table staging_songs (
    artist_id varchar(255),
    artist_latitude REAL,
    artist_longitude REAL,
    artist_location varchar(255),
    artist_name varchar(255),
    duration REAL,
    num_songs integer,
    song_id varchar(255),
    title varchar(255),
    year integer
    )
    """)

songplay_table_create = ("""
create table songplay(
    songplay_id integer IDENTITY(0, 1) primary key,
    start_time timestamp NOT NULL,
    user_id integer NOT NULL,
    level varchar(255),
    song_id varchar(255) NOT NULL,
    artist_id varchar(255) NOT NULL,
    session_id integer,
    location varchar(255),
    user_agent varchar(255)
    )
    """)

user_table_create = ("""
create table users(
    user_id integer primary key,
    first_name varchar(255),
    last_name varchar(255),
    gender char,
    level varchar(255)
    )
    """)

song_table_create = ("""
create table songs
    (song_id varchar(255) primary key, title varchar(255),
    artist_id varchar(255), year integer, duration REAL);
    """)

artist_table_create = ("""
create table artists
    (artist_id varchar(255) primary key, name varchar(255),
    location varchar(255), latitude REAL, longtitude REAL);
    """)

time_table_create = ("""
create table time
    (start_time timestamp primary key, hour integer, day integer,
    week integer, month integer, year integer, weekday integer);
    """)

# STAGING TABLES
staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    format as json {}
    REGION {}
    """).format(ARN, LOG_JSONPATH, REGION)

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    json 'auto'
    REGION {}
    """).format(ARN, REGION)

# FINAL TABLES
songplay_table_insert = ("""
    insert into songplay
    (start_time, user_id, level, song_id, artist_id, session_id,
    location, user_agent)
    (select (timestamp 'epoch' + e.ts * interval '1 second') as start_time,
    e.user_id as user_id, e.level as level, s.song_id as song_id,
    s.artist_id as artist_id, e.session_id as session_id,
    e.location as location, e.user_agent as user_agent
    from staging_events as e join staging_songs as s
    on e.song = s.title and e.artist = s.artist_name
    where e.page='NextSong')
    """)

user_table_insert = ("""
    insert into users(user_id, first_name, last_name, gender, level)
        select user_id, first_name, last_name, gender, level from
        staging_events where user_id is NOT NULL
        """)

song_table_insert = ("""
    insert into songs
    (song_id, title, artist_id, year, duration)
    (
        select song_id, title, artist_id, year,
        duration from staging_songs where song_id is NOT NULL
    )
""")

artist_table_insert = ("""
    insert into artists
    (artist_id, name, location, latitude, longtitude)
    (
        select artist_id, artist_name, artist_location, artist_latitude,
        artist_longitude from staging_songs where artist_id is NOT NULL
    )
""")

time_table_insert = ("""
    insert into time
    (start_time, hour, day, week, month, year, weekday)
    (
        select start_time
        , extract(hour from start_time), extract(day from start_time)
        , extract(week from start_time), extract(month from start_time)
        , extract(year from start_time), extract(weekday from start_time)
        from songplay
    )
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]
drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
