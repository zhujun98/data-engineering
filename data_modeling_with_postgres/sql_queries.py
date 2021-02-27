# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (user_id int PRIMARY KEY,
     first_name varchar,
     last_name varchar,
     gender char (1),
     level varchar)
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (song_id varchar PRIMARY KEY,
     title varchar,
     artist_id varchar,
     year int,
     duration numeric)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (artist_id varchar PRIMARY KEY, 
     artist_name varchar, 
     artist_location varchar, 
     artist_latitude numeric, 
     artist_longitude numeric)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (timestamp timestamp PRIMARY KEY,    
     hour int,
     day int,
     week int,
     month int,
     year int,
     weekday int
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
    INSERT INTO users
    (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""
    INSERT INTO songs
    (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
    INSERT INTO artists
    (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    VALUES (%s, %s, %s, %s, %s)
""")


time_table_insert = ("""
    INSERT INTO time 
    (timestamp, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# CHECK RECORDS

song_table_check = ("""
    SELECT * FROM songs LIMIT 5
""")

artist_table_check = ("""
    SELECT * FROM artists LIMIT 5
""")

time_table_check = ("""
    SELECT * FROM time LIMIT 5
""")

user_tabel_check = ("""
    SELECT * from users LIMIT 5
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
