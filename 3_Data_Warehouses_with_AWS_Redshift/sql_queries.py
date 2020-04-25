import configparser
import sys
import os


os.chdir(sys.path[0])

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')
DWH_IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR 100,
        auth VARCHAR 20,
        first_name VARCHAR 20,
        gender VARCHAR 5,
        item_in_session INT,
        last_name VARCHAR 20
        length FLOAT,
        level VARCHAR 5,
        location VARCHAR 100,
        method VARCHAR 5,
        page VARCHAR 10
        registration FLOAT,
        session_id INT,
        song VARCHAR 50
        status INT,
        ts TIMESTAMP,
        user_agent VARCHAR 50
        user_id INT
);""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT,
        artist_id VARCHAR 20,
        artist_latitude FLOAT, 
        artist_longitude FLOAT,
        artist_location VARCHAR 10,
        artist_name VARCHAR 50,
        song_id VARCHAR 20,
        title VARCHAR 100, 
        duration FLOAT,
        year INT
);""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay
;""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user
;""")


song_table_create = ("""
CREATE TABLE IF NOT EXISTS song
;""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist
;""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
;""")

# STAGING TABLES

staging_events_copy = ("""

""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
