import configparser
import sys
import os


os.chdir(sys.path[0])

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

S3_LOG_DATA = config.get("S3", "LOG_DATA")
S3_LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
S3_SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")
REGION_NAME = config.get("DWH", "REGION_NAME")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR ,
        auth VARCHAR ,
        first_name VARCHAR ,
        gender VARCHAR ,
        item_in_session INT,
        last_name VARCHAR ,
        length FLOAT,
        level VARCHAR ,
        location VARCHAR ,
        method VARCHAR ,
        page VARCHAR ,
        registration FLOAT,
        session_id INT,
        song VARCHAR ,
        status INT,
        ts TIMESTAMP,
        user_agent VARCHAR ,
        user_id INT
    )
    ;
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT, 
        artist_longitude FLOAT,
        artist_location VARCHAR ,
        artist_name VARCHAR ,
        song_id VARCHAR ,
        title VARCHAR , 
        duration FLOAT,
        year INT
    )
    ;
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id INT IDENTITY (1, 1),
        level VARCHAR ,
        location VARCHAR ,
        user_agent VARCHAR ,
        session_id INT,
        user_id INT REFERENCES users (user_id) NOT NULL,
        song_id VARCHAR  REFERENCES song (song_id) NOT NULL,
        artist_id VARCHAR  REFERENCES artist (artist_id) NOT NULL,
        start_time TIMESTAMP REFERENCES time (start_time) SORTKEY DISTKEY NOT NULL,
        PRIMARY KEY (songplay_id)
    )
    ;
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS users (
        user_id INT SORTKEY,
        first_name VARCHAR ,
        last_name VARCHAR ,
        gender VARCHAR ,
        level VARCHAR ,
        PRIMARY KEY (user_id)
    )
    ;
"""


song_table_create = """
    CREATE TABLE IF NOT EXISTS song (
        song_id	VARCHAR  SORTKEY,
        title	VARCHAR  NOT NULL,
        year	INTEGER NOT NULL,
        duration	FLOAT NOT NULL,
        artist_id	VARCHAR  NOT NULL REFERENCES artist (artist_id),
        PRIMARY KEY (song_id)
    )
    ;
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artist (
        artist_id	VARCHAR  SORTKEY,
        name	VARCHAR  NOT NULL,
        location	VARCHAR ,
        latitude	FLOAT,
        longitude	FLOAT,
        PRIMARY KEY (artist_id)
    )
    ;
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS time (
        start_time	TIMESTAMP SORTKEY DISTKEY,
        hour	INTEGER NOT NULL,
        day	    INTEGER NOT NULL,
        week	INTEGER NOT NULL,
        month	INTEGER NOT NULL,
        year	INTEGER NOT NULL,
        weekday	VARCHAR  NOT NULL,
        PRIMARY KEY (start_time)
    )
    ;
"""

# STAGING TABLES

staging_events_copy = f"""
    COPY staging_events
    FROM '{S3_LOG_DATA}'
    IAM_ROLE '{IAM_ROLE_ARN}'
    REGION 'us-west-2'
    JSON '{S3_LOG_JSONPATH}'
    TIMEFORMAT 'epochmillisecs'
    ;
"""

staging_songs_copy = f"""
    COPY staging_songs
    FROM '{S3_SONG_DATA}'
    IAM_ROLE '{IAM_ROLE_ARN}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS
    ;
"""

# FINAL TABLES

songplay_table_insert = """
    INSERT INTO songplay (
        level,
        location,
        user_agent,
        session_id,
        user_id,
        song_id,
        artist_id,
        start_time
    )
    SELECT DISTINCT
        se.level,
        se.location,
        se.user_agent,
        se.session_id,
        se.user_id,
        ss.song_id,
        ss.artist_id,
        se.ts
    FROM staging_events se
    INNER JOIN staging_songs ss
        ON se.song = ss.title AND se.artist = ss.artist_name
    WHERE se.page = 'NextSong'
    ;
"""

user_table_insert = """
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
    WHERE 
        page = 'NextSong'
        AND user_id IS NOT NULL
    ;
"""

song_table_insert = """
    INSERT INTO song (
        song_id,
        title,
        year,
        duration,
        artist_id
    )
    SELECT DISTINCT
        song_id,
        title,
        year,
        duration,
        artist_id
    FROM staging_songs
    WHERE song_id IS NOT NULL
    ;
"""

artist_table_insert = """
    INSERT INTO artist (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
    ;
"""

time_table_insert = """
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT DISTINCT
        start_time,
        EXTRACT (hour from start_time),
        EXTRACT (day from start_time),
        EXTRACT (week from start_time),
        EXTRACT (month from start_time),
        EXTRACT (year from start_time),
        EXTRACT (weekday from start_time)
    FROM songplay
    ;
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    time_table_create,
    artist_table_create,
    song_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    user_table_insert,
    artist_table_insert,
    song_table_insert,
    songplay_table_insert,
    time_table_insert,
]
