trip_table_create = ("""
    CREATE TABLE IF NOT EXISTS trip (
        tid                 integer     PRIMARY KEY,
        ride_id             varchar,
        rideable_type       varchar,
        started_at          timestamp   NOT NULL,
        ended_at            timestamp   NOT NULL,
        start_station_id    integer     NOT NULL,
        end_station_id      integer     NOT NULL,
        member_casual       varchar     NOT NULL,
        start_date          date        NOT NULL
    )
""")

station_table_create = ("""
    CREATE TABLE IF NOT EXISTS station (
        station_id     integer      PRIMARY KEY,
        station_name   varchar      NOT NULL
    )
""")

covid_table_create = ("""
    CREATE TABLE IF NOT EXISTS covid (
        date                    date        PRIMARY KEY, 
        death                   integer     NOT NULL, 
        deathIncrease           integer     NOT NULL, 
        hospitalizedCurrently   integer     NOT NULL, 
        positive                integer     NOT NULL, 
        positiveIncrease        integer     NOT NULL, 
        recovered               integer     NOT NULL
    )
""")

weather_table_create = ("""
    CREATE TABLE IF NOT EXISTS weather (
        DATE    date        PRIMARY KEY,    
        AWND    decimal,
        TAVG    decimal,
        TMAX    decimal,
        TMIN    decimal,
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
