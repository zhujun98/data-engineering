trip_table_create = ("""
    CREATE TABLE IF NOT EXISTS trip (
        ride_id             varchar,
        rideable_type       varchar,
        started_at          timestamp   NOT NULL,
        ended_at            timestamp   NOT NULL,
        start_station_id    varchar     NOT NULL,
        end_station_id      varchar     NOT NULL,
        member_casual       varchar     NOT NULL,
        tid                 bigint      PRIMARY KEY,
        start_date          date        NOT NULL
    )
""")

station_table_create = ("""
    CREATE TABLE IF NOT EXISTS station (
        station_id     varchar      PRIMARY KEY,
        station_name   varchar      NOT NULL
    )
""")

covid_table_create = ("""
    CREATE TABLE IF NOT EXISTS covid (
        date                    date        PRIMARY KEY, 
        death                   bigint      NOT NULL, 
        deathIncrease           bigint      NOT NULL, 
        hospitalizedCurrently   bigint      NOT NULL, 
        positive                bigint      NOT NULL, 
        positiveIncrease        bigint      NOT NULL, 
        recovered               bigint      NOT NULL
    )
""")

weather_table_create = ("""
    CREATE TABLE IF NOT EXISTS weather (
        DATE    date        PRIMARY KEY,    
        AWND    double precision,
        TAVG    double precision,
        TMAX    double precision,
        TMIN    double precision,
        WT01    bool,
        WT02    bool,
        WT03    bool,
        WT04    bool,
        WT05    bool,
        WT06    bool,
        WT08    bool,
        WT11    bool  
    )
""")


create_table_queries = [
    trip_table_create,
    station_table_create,
    covid_table_create,
    weather_table_create
]
