import pathlib

import psycopg2
import pandas as pd
from sql_queries import (
    song_table_insert, artist_table_insert, time_table_insert,
    user_table_insert, song_select, songplay_table_insert,
    song_table_check, artist_table_check, time_table_check,
    user_table_check, songplay_table_check
)


def process_song_file(cur, filepath):
    """Process a single song file and insert the records to db."""
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location",
                      "artist_latitude", "artist_longitude"]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Process a single log file and insert the records to db."""
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df['ts']

    # insert time data records
    dt = t.dt
    time_data = (t, dt.hour, dt.day, dt.isocalendar().week, dt.month,
                 dt.year, dt.weekday)
    column_labels = ("timestamp", "hour", "day", "week", "month",
                     "year", "weekday")
    time_df = pd.DataFrame({k: v for k, v in zip(column_labels, time_data)})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates('userId')

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for _, row in df.iterrows():
        # get song_id and artist_id from the song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = (
            row['ts'], row['userId'], row['level'], song_id, artist_id,
            row['sessionId'], row['location'], row['userAgent']
        )

        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()


def process_data(cur, conn, directory, func):
    """Process all files in the given directory."""
    # get all files matching extension from directory
    all_files = list(pathlib.Path(directory).rglob("*.json"))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, directory))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def run_test_queries():
    """Check results by querying all the tables."""
    for name, query in [('song', song_table_check),
                        ('artist', artist_table_check),
                        ('time', time_table_check),
                        ('user', user_table_check),
                        ('songplay', songplay_table_check)]:
        cur.execute(query)
        print(f"Query result of {name} table:")
        for row in cur.fetchall():
            print(row)

    cur.execute("SELECT COUNT(*) FROM songplays")
    print(f"Total number of records in the 'songplays' table is "
          f"{cur.fetchone()[0]}")


if __name__ == "__main__":
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, directory='data/song_data', func=process_song_file)
    process_data(cur, conn, directory='data/log_data', func=process_log_file)

    run_test_queries()

    conn.close()
