class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time) as hour, extract(day from start_time) as day, 
            extract(week from start_time) as week, extract(month from start_time) as month, 
            extract(year from start_time) as year, extract(dayofweek from start_time) as dow
        FROM songplays
    """)

    songplay_table_size = """
        SELECT COUNT(*) AS count FROM songplays;
    """

    user_table_size = """
        SELECT COUNT(*) AS count FROM users;
    """

    song_table_size = """
        SELECT COUNT(*) AS count FROM songs;
    """

    artist_table_size = """
        SELECT COUNT(*) AS count FROM artists;
    """

    artist_table_fields_null = """
        SELECT COUNT(*) AS count FROM artists WHERE artist_name IS NULL OR artist_id IS NULL;
    """

    time_table_size = """
        SELECT COUNT(*) AS count FROM time;
    """

    time_table_fields_null = """
        SELECT COUNT(*) AS count FROM time WHERE start_time IS NULL;
    """