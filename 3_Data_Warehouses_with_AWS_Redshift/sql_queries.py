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
REGION_NAME = config.get("CLUSTER", "REGION_NAME")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR (100),
        auth VARCHAR (20),
        first_name VARCHAR (20),
        gender VARCHAR (10),
        item_in_session INT,
        last_name VARCHAR (20),
        length FLOAT,
        level VARCHAR (10),
        location VARCHAR (100),
        method VARCHAR (5),
        page VARCHAR (10)
        registration FLOAT,
        session_id INT,
        song VARCHAR (50),
        status INT,
        ts TIMESTAMP,
        user_agent VARCHAR (150),
        user_id INT
);"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT,
        artist_id VARCHAR (50),
        artist_latitude FLOAT, 
        artist_longitude FLOAT,
        artist_location VARCHAR (100),
        artist_name VARCHAR (100),
        song_id VARCHAR (50),
        title VARCHAR (100), 
        duration FLOAT,
        year INT
);"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id INT IDENTITY (1, 1),
        level VARCHAR (10),
        location VARCHAR (100),
        user_agent VARCHAR (150),
        session_id INT,
        user_id INT REFERENCES user (user_id),
        song_id VARCHAR (20) REFERENCES song (song_id),
        artist_id VARCHAR (20) REFERENCES artist (artist_id),
        start_time TIMESTAMP REFERENCES time (start_time) SORTKEY,
        PRIMARY KEY (songplay_id)
    )
;"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS user (
        user_id INT,
        first_name VARCHAR (20),
        last_name VARCHAR (20),
        gender VARCHAR (10),
        level VARCHAR (10),
        PRIMARY KEY (user_id)
    )
;"""


song_table_create = """
    CREATE TABLE IF NOT EXISTS song (
        song_id	VARCHAR (50),
        title	VARCHAR (100) NOT NULL,
        year	INTEGER NOT NULL,
        duration	FLOAT NOT NULL,
        artist_id	VARCHAR (50) NOT NULL REFERENCES artists (artist_id),
        PRIMARY KEY (song_id)
    )
;"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artist (
        artist_id	VARCHAR (50),
        name	VARCHAR (100) NOT NULL,
        location	VARCHAR (100),
        latitude	FLOAT,
        longitude	FLOAT,
        PRIMARY KEY (artist_id)
    )
;"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS time (
        start_time	TIMESTAMP,
        hour	INTEGER NOT NULL,
        day	    INTEGER NOT NULL,
        week	INTEGER NOT NULL,
        month	INTEGER NOT NULL,
        year	INTEGER NOT NULL,
        weekday	VARCHAR (10) NOT NULL,
        PRIMARY KEY (start_time)
    )
;"""

# STAGING TABLES

staging_events_copy = f"""
    COPY staging_events
    FROM {S3_LOG_DATA}
    IAM_ROLE {IAM_ROLE_ARN}
    REGION {REGION_NAME}
    JSON {S3_LOG_JSONPATH};
"""

staging_songs_copy = f"""
    COPY staging_songs
    FROM {S3_SONG_DATA}
    IAM_ROLE {IAM_ROLE_ARN}
    REGION {REGION_NAME}
    JSON 'auto';
"""

# FINAL TABLES

songplay_table_insert = """
"""

user_table_insert = """
"""

song_table_insert = """
"""

artist_table_insert = """
"""

time_table_insert = """
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
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
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
