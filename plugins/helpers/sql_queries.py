class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.ts::varchar) songplay_id,
                events.start_time, 
                events."userId", 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events."sessionId", 
                events.location, 
                events."userAgent"
                FROM (select to_timestamp(ts/1000) as start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct "userId", "firstName", "lastName", gender, level
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
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time)
        FROM songplays
    """)