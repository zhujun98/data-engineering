from run_on_aws import get_iam_role_arns, s3_config


iam_role_arn = get_iam_role_arns()[0]

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist              varchar,
        auth                varchar,
        first_name          varchar,
        gender              char(1),
        item_in_session     integer,
        last_name           varchar,
        length              decimal,
        level               varchar,
        localtion           varchar,
        method              varchar,
        page                varchar,
        registration        decimal,
        session_id          integer,
        song                varchar,
        status              integer,
        ts                  varchar,
        user_agent          varchar,
        user_id              integer
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_song            integer,
        artist_id           varchar,
        artist_latitude     decimal,
        artist_longitude    decimal,
        artist_location     varchar,
        artist_name         varchar,
        song_id             varchar,
        title               varchar,
        duration            decimal,
        year                smallint 
    )
""")

# Redshift does not support 'SERIAL' type
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id    integer      IDENTITY(0,1)    PRIMARY KEY,
        start_time     timestamp    NOT NULL,
        user_id        integer      NOT NULL,
        level          varchar,
        song_id        varchar,
        artist_id      varchar,
        session_id     integer,
        location       varchar,
        user_agent     varchar
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id        integer      PRIMARY KEY,
        first_name     varchar,
        last_name      varchar,
        gender         char (1),
        level          varchar      NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id        varchar      PRIMARY KEY,
        title          varchar      NOT NULL,
        artist_id      varchar,
        year           smallint,
        duration       decimal
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id      varchar      PRIMARY KEY, 
        name           varchar      NOT NULL, 
        location       varchar, 
        latitude       decimal, 
        longitude      decimal
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time     timestamp    PRIMARY KEY,    
        hour           smallint,
        day            smallint,
        week           smallint,
        month          smallint,
        year           smallint,
        weekday        smallint
    )
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events 
    FROM '{s3_config['LOG_DATA']}'
    IAM_ROLE '{iam_role_arn}'
    REGION '{s3_config['REGION']}'
    FORMAT AS JSON '{s3_config['LOG_JSONPATH']}'
""")

staging_songs_copy = (f"""
    COPY staging_songs 
    FROM '{s3_config['SONG_DATA']}/A/A/A/TRAAAAK128F9318786.json'
    IAM_ROLE '{iam_role_arn}'
    REGION '{s3_config['REGION']}'
    FORMAT AS JSON 'auto'
""")

# FINAL TABLES

# songplay_id is assigned automatically
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
""")

user_table_insert = ("""
    INSERT  INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(user_id),
            first_name,
            last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT  INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id),
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT  INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id), 
            artist_name, 
            artist_location, 
            artist_latitude, 
            artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
""")

# CHECK RECORDS

tables_check = ("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
""")

staging_events_table_check = ("""
    SELECT COUNT(*) FROM staging_events
""")

staging_songs_table_check = ("""
    SELECT COUNT(*) FROM staging_songs
""")

song_table_check = ("""
    SELECT * FROM songs LIMIT 5
""")

artist_table_check = ("""
    SELECT * FROM artists LIMIT 5
""")

time_table_check = ("""
    SELECT * FROM time LIMIT 5
""")

user_table_check = ("""
    SELECT * FROM users LIMIT 5
""")

songplay_table_check = ("""
    SELECT * FROM songplays LIMIT 5
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
