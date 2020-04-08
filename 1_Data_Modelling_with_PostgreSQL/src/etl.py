import os
import glob
import psycopg2
import pandas as pd
import sys
from sql_queries import *


def process_song_file(cur, filepath):
    """Read and parse JSON song file file before inserting into database.
    
    Args:
        cur (object): the cursor object
        filepath (str): the filepath of the JSON song file.
    
    Returns:
        None
    """
    # open and parse song file
    df = pd.read_json(filepath, lines=True)

    # insert artist record. This must be executed before inserting song records, because the song table references the artist table.
    artist_data = df[
        ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    ].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df[
        ["song_id", "title", "artist_id", "year", "duration"]
    ].values[0].tolist()
    cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath):
    """Read and parse JSON log file file before inserting into database.
    
    Args:
        cur (object): the cursor object
        filepath (str): the filepath of the JSON log file.
    
    Returns:
        None
    """
    # open and parse the JSON log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query("page == 'NextSong'")

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.day_name())
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.concat(time_data, axis=1)
    time_df.columns = column_labels

    for _, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for _, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for _, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        # if results has a value, set the tuple it returns equal to the songid and artistid
        if results:
            songid, artistid = results 
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row[[
                "level", "sessionId", "location", "userAgent", "userId",
            ]].values.tolist()
            + [songid, artistid, pd.to_datetime(row["ts"], unit="ms")]
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Finds every file in the filepath and processes them with the given func.
    
    Args:
        cur (object): the cursor object.
        conn (object): the connection to the database.
        filepath (str): the root filepath of the data files.
        func (function): the processing function.
    
    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Connects to the postgres database and processes the raw song_data and log_data, inserting them into the database.
    
    Returns:
        None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='../data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='../data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))  # Change the working directory to that of this file (for VSCode)
    main()
