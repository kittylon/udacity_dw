import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(artist VARCHAR(MAX),
                                                                            auth VARCHAR(MAX),
                                                                            firstName VARCHAR(MAX),
                                                                            gender VARCHAR(MAX),
                                                                            itemInSession INT,
                                                                            lastName VARCHAR(MAX),
                                                                            length NUMERIC,
                                                                            level VARCHAR(MAX),
                                                                            location VARCHAR(MAX),
                                                                            method VARCHAR(MAX),
                                                                            page VARCHAR(MAX),
                                                                            registration NUMERIC,
                                                                            sessionId INT,
                                                                            song VARCHAR(MAX),
                                                                            status INT,
                                                                            ts NUMERIC,
                                                                            userAgent VARCHAR(MAX),
                                                                            userId INT)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(num_songs INT,
                                                                        artist_id VARCHAR(MAX),
                                                                        artist_latitude NUMERIC,
                                                                        artist_longitude NUMERIC,
                                                                        artist_location VARCHAR(MAX),
                                                                        artist_name VARCHAR(MAX),
                                                                        song_id VARCHAR(MAX),
                                                                        title VARCHAR(MAX),
                                                                        duration NUMERIC,
                                                                        year INT)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(songplay_id INT PRIMARY KEY IDENTITY(1, 1),
                                                                start_time timestamp NOT NULL,
                                                                user_id INT NOT NULL,
                                                                level VARCHAR,
                                                                song_id VARCHAR,
                                                                artist_id VARCHAR,
                                                                session_id INT,
                                                                location VARCHAR,
                                                                user_agent VARCHAR)
                                                                
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id INT PRIMARY KEY,
                                                        first_name VARCHAR NOT NULL,
                                                        last_name VARCHAR NOT NULL,
                                                        gender VARCHAR,
                                                        level VARCHAR NOT NULL)
                                                        
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song(song_id VARCHAR PRIMARY KEY,
                                                        title VARCHAR,
                                                        artist_id VARCHAR NOT NULL,
                                                        year INT,
                                                        duration NUMERIC)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist(artist_id VARCHAR PRIMARY KEY,
                                                            name  VARCHAR NOT NULL,
                                                            location VARCHAR,
                                                            lattitude NUMERIC,
                                                            longitude NUMERIC)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time timestamp PRIMARY KEY,
                                                        hour INT,
                                                        day INT,
                                                        week INT,
                                                        month INT,
                                                        year INT,
                                                        weekday NUMERIC)
""")

# STAGING TABLES

staging_events_copy = ("""  COPY staging_events
                            FROM 's3://udacity-dend/log_data'
                            iam_role 'arn:aws:iam::575106810476:role/dwhRole'
                            region 'us-west-2'                            
                            format json as 's3://udacity-dend/log_json_path.json'
                            dateformat 'auto';
""")

staging_songs_copy = ("""   COPY staging_songs(num_songs, artist_id, artist_latitude, artist_longitude, artist_location,
                                            artist_name, song_id, title, duration, year)
                            FROM 's3://udacity-dend/song_data'
                            iam_role 'arn:aws:iam::575106810476:role/dwhRole'
                            region 'us-west-2'
                            format as json 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT DISTINCT
                            TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second',
                            event.userId,
                            event.level,
                            songs.song_id, 
                            songs.artist_id,
                            event.sessionId,
                            event.location,
                            event.userAgent
                            FROM staging_events AS event
                            LEFT JOIN staging_songs AS songs ON event.song = songs.title
                            WHERE event.page = 'NextSong'

""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT 
                            userId,
                            firstName,
                            lastName,
                            gender,
                            level
                        FROM staging_events
                        WHERE userId IS NOT NULL
                        AND event.page = 'NextSong'

""")

song_table_insert = ("""INSERT INTO song(song_id, title, artist_id, year, duration)
                        SELECT DISTINCT 
                                song_id,
                                title,
                                artist_id,
                                year,
                                duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL

""")

artist_table_insert =   ("""INSERT INTO artist(artist_id, name, location, lattitude, longitude)
                            SELECT DISTINCT 
                                artist_id,
                                artist_name,
                                artist_location,
                                artist_latitude,
                                artist_longitude
                            FROM staging_songs
                            WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT 
                            TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
                            EXTRACT(hour FROM start_time),
                            EXTRACT(day FROM start_time),
                            EXTRACT(week FROM start_time),
                            EXTRACT(month FROM start_time),
                            EXTRACT(year FROM start_time),
                            date_part(dow, start_time)
                        FROM staging_events
                        WHERE event.page = 'NextSong'

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
