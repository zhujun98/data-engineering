from create_tables import connect_db
from sql_queries import (
    copy_table_queries, insert_table_queries,
    staging_events_table_check, staging_songs_table_check,
    song_table_check, artist_table_check, time_table_check,
    user_table_check, songplay_table_check
)


def load_staging_tables(cur, conn):
    """Load data from S3 storage to Redshift."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

    cur.execute(staging_events_table_check)
    print("Number of rows in the staging events table: ", cur.fetchone()[0])

    cur.execute(staging_songs_table_check)
    print("Number of rows in the staging songs table: ", cur.fetchone()[0])


def insert_tables(cur, conn):
    """Insert data into the fact and dimension tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


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
            print(" - ", row)


if __name__ == "__main__":
    cur, conn = connect_db()

    try:
        # load_staging_tables(cur, conn)
        insert_tables(cur, conn)
    except Exception as e:
        print(repr(e))

    run_test_queries()

    conn.close()
