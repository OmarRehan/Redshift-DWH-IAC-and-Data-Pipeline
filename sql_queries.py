import configparser


config = configparser.ConfigParser()
config.read('dwh.cfg')


# Dictionaries contain DDL & DML Constants to be used across the Project
dict_DDL_schemas = {
    'schema_staging_name' : 'STAGING_SCHEMA',
    'schema_sparkify_name' : 'SPARKIFY_SCHEMA'
}



dict_DDL_tables = {
    'stg_log_table':'STG_LOG',    
    'stg_song_table':'STG_SONG',
    'songplay_table':'SONG_PLAY_TBL',
    'user_table':'USER_TBL',
    'song_table':'SONG_TBL',
    'artist_table':'ARTIST_TBL',
    'time_table':'TIME_TBL'
}




# DROP TABLES
drop_staging_log_table = "DROP TABLE IF EXISTS {schema_staging_name}.{stg_log_table};".format(**dict_DDL_schemas,**dict_DDL_tables)
drop_staging_songs_table = "DROP TABLE IF EXISTS {schema_staging_name}.{stg_song_table};".format(**dict_DDL_schemas,**dict_DDL_tables)
drop_songplay_table = "DROP TABLE IF EXISTS {schema_sparkify_name}.{songplay_table};".format(**dict_DDL_schemas,**dict_DDL_tables)
drop_user_table = "DROP TABLE IF EXISTS {schema_sparkify_name}.{user_table};".format(**dict_DDL_schemas,**dict_DDL_tables)
drop_song_table = "DROP TABLE IF EXISTS {schema_sparkify_name}.{song_table};".format(**dict_DDL_schemas,**dict_DDL_tables)
drop_artist_table = "DROP TABLE IF EXISTS {schema_sparkify_name}.{artist_table};".format(**dict_DDL_schemas,**dict_DDL_tables)
drop_time_table = "DROP TABLE IF EXISTS {schema_sparkify_name}.{time_table};".format(**dict_DDL_schemas,**dict_DDL_tables)



# CREATE SCHEMAS
create_schema_staging = 'CREATE SCHEMA IF NOT EXISTS {};'.format(dict_DDL_schemas.get('schema_staging_name'))
create_scehma_sparkify = 'CREATE SCHEMA IF NOT EXISTS {};'.format(dict_DDL_schemas.get('schema_sparkify_name'))



# CREATE TABLES
create_staging_log_table= """
CREATE TABLE IF NOT EXISTS {schema_staging_name}.{stg_log_table}
(
    artist VARCHAR(250),
    auth VARCHAR(50),
    firstname VARCHAR(50),
    gender CHAR(1),
    iteminsession VARCHAR(10),
    lastname VARCHAR(50),
    length VARCHAR(50),
    level VARCHAR(10),
    location VARCHAR(100),
    method VARCHAR(10),
    page VARCHAR(50),
    registration VARCHAR(50),
    sessionid VARCHAR(10),
    song VARCHAR(250),
    status VARCHAR(10),
    ts VARCHAR(50),
    useragent VARCHAR(250),
    userid VARCHAR(10)
);
""".format(**dict_DDL_schemas,**dict_DDL_tables)



create_staging_songs_table = """
CREATE TABLE IF NOT EXISTS {schema_staging_name}.{stg_song_table}
(
    num_songs VARCHAR(10),
    artist_id VARCHAR(50),
    artist_latitude VARCHAR(50),
    artist_longitude VARCHAR(50),
    artist_location VARCHAR(250),
    artist_name VARCHAR(250),
    song_id VARCHAR(50),
    title VARCHAR(250),
    duration VARCHAR(50),
    year VARCHAR(10)
);""".format(**dict_DDL_schemas,**dict_DDL_tables)



create_songplay_table = """
CREATE TABLE IF NOT EXISTS {schema_sparkify_name}.{songplay_table}
(
UNIX_TS BIGINT,
SESSION_ID BIGINT,
ARTIST_ID VARCHAR(50) DEFAULT -9999,
SONG_ID VARCHAR(50) DEFAULT -9999,
USER_ID INTEGER DEFAULT -9999,
USER_LEVEL VARCHAR(10),
USER_LOCATION VARCHAR(100),
USER_AGENT VARCHAR(250),
USER_LOGGED VARCHAR(25),
CREATION_TIMESTAMP TIMESTAMP,
PRIMARY KEY (UNIX_TS,SESSION_ID)
);
""".format(**dict_DDL_schemas,**dict_DDL_tables)



create_user_table = """
CREATE TABLE IF NOT EXISTS {schema_sparkify_name}.{user_table}
(
    USER_ID INTEGER PRIMARY KEY,
    FIRST_NAME VARCHAR(50),
    LAST_NAME VARCHAR(50),
    GENDER CHAR(1),
    LEVEL VARCHAR(10)
);
""".format(**dict_DDL_schemas,**dict_DDL_tables)



create_song_table = """
CREATE TABLE IF NOT EXISTS {schema_sparkify_name}.{song_table}
(
    SONG_ID VARCHAR(50),
    ARTIST_ID VARCHAR(50),
    DURATION DOUBLE PRECISION,
    SONG_TITLE VARCHAR(250) DEFAULT 'Unknwon',
    SONG_YEAR INTEGER DEFAULT -9999,
    PRIMARY KEY (SONG_ID,ARTIST_ID,DURATION)
);
""".format(**dict_DDL_schemas,**dict_DDL_tables)



create_artist_table = """
CREATE TABLE IF NOT EXISTS {schema_sparkify_name}.{artist_table}
(
    ARTIST_ID VARCHAR(50),
    ARTIST_NAME VARCHAR(250),
    ARTIST_LOCATION VARCHAR(250) DEFAULT 'Unknown',
    ARTIST_LATITUDE DOUBLE PRECISION DEFAULT -9999,
    ARTIST_LONGITUTE DOUBLE PRECISION DEFAULT -9999,
    PRIMARY KEY (ARTIST_ID)
);
""".format(**dict_DDL_schemas,**dict_DDL_tables)



create_time_table = """
CREATE TABLE IF NOT EXISTS {schema_sparkify_name}.{time_table}
(
    TIME_ID BIGINT PRIMARY KEY,
    TS_FORMATTED TIMESTAMP,
    YEAR_COL INT,
    MONTH_COL INT,
    DAY_COL INT,
    HOUR_COL INT,
    DAY_NAME VARCHAR(50)
);
""".format(**dict_DDL_schemas,**dict_DDL_tables)



# STAGING TABLES
copy_staging_log = """
TRUNCATE TABLE {schema_staging_name}.{stg_log_table};

COPY {schema_staging_name}.{stg_log_table}
from 's3://udacity-dend/log_data/' 
CREDENTIALS 'aws_iam_role={}'
JSON 's3://udacity-dend/log_json_path.json' REGION 'us-west-2';
""".format(config.get('CLUSTER','dwh_arn'),**dict_DDL_schemas,**dict_DDL_tables)



copy_staging_songs = """
TRUNCATE TABLE {schema_staging_name}.{stg_song_table};

COPY {schema_staging_name}.{stg_song_table}
from 's3://udacity-dend/song_data/' 
CREDENTIALS 'aws_iam_role={}'
JSON 'auto' REGION 'us-west-2';
""".format(config.get('CLUSTER','dwh_arn'),**dict_DDL_schemas,**dict_DDL_tables)



# FINAL TABLES
insert_songplay_table = """
BEGIN TRANSACTION;

DELETE FROM {schema_sparkify_name}.{songplay_table}
USING {schema_staging_name}.{stg_log_table} AS stg_log
WHERE SONG_PLAY_TBL.UNIX_TS = stg_log.ts
AND SONG_PLAY_TBL.SESSION_ID = stg_log.sessionid
AND stg_log.page = 'NextSong';

INSERT INTO {schema_sparkify_name}.{songplay_table}
SELECT DISTINCT
CAST(ts AS BIGINT) AS UNIX_TS,
CAST(sessionid AS BIGINT) AS SESSION_ID,
ARTIST_OUTPUT.ARTIST_ID,
SONG_OUTPUT.SONG_ID,
CAST(userid AS INTEGER) AS USER_ID,
level,
location AS USER_LOCATION,
useragent AS USER_AGENT,
CAST(auth AS VARCHAR(25)) AS USER_LOGGED,
TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS CREATION_TIMESTAMP
FROM {schema_staging_name}.{stg_log_table} AS stg_log
LEFT JOIN 
(
    SELECT ARTIST_ID,ARTIST_NAME,
    ROW_NUMBER() OVER (PARTITION BY ARTIST_NAME ORDER BY ARTIST_LATITUDE DESC, ARTIST_LONGITUTE DESC,ARTIST_LOCATION ASC) AS ROW_NUM
    FROM {schema_sparkify_name}.{artist_table}
) AS ARTIST_OUTPUT
ON STG_LOG.artist = ARTIST_OUTPUT.ARTIST_NAME
AND ARTIST_OUTPUT.ROW_NUM = 1
LEFT JOIN
(
    SELECT SONG_ID,SONG_TITLE,
    ROW_NUMBER() OVER (PARTITION BY SONG_TITLE ORDER BY SONG_YEAR DESC) AS ROW_NUM
    FROM {schema_sparkify_name}.{song_table}
) AS SONG_OUTPUT
ON TRIM(STG_LOG.song) = SONG_OUTPUT.SONG_TITLE
AND SONG_OUTPUT.ROW_NUM = 1
WHERE stg_log.page = 'NextSong';

""".format(**dict_DDL_schemas,**dict_DDL_tables)



insert_user_table = """
BEGIN TRANSACTION;

DELETE FROM {schema_sparkify_name}.{user_table}
USING {schema_staging_name}.{stg_log_table} AS stg_log
WHERE USER_TBL.USER_ID = stg_log.userid;

INSERT INTO {schema_sparkify_name}.{user_table}
SELECT USER_ID,FIRST_NAME,LAST_NAME,gender,level
FROM 
(
    SELECT
    CAST(userid AS INTEGER) AS USER_ID,
    firstname AS FIRST_NAME,
    lastname AS LAST_NAME,
    gender,
    level,
    ROW_NUMBER() OVER (PARTITION BY userid order BY ts DESC) AS ROW_NUM
    FROM {schema_staging_name}.{stg_log_table}
    WHERE auth='Logged In'
) 
WHERE ROW_NUM = 1;

""".format(**dict_DDL_schemas,**dict_DDL_tables)



insert_song_table = """
BEGIN TRANSACTION;

DELETE FROM {schema_sparkify_name}.{song_table}
USING {schema_staging_name}.{stg_song_table} AS STG_SONG
WHERE SONG_TBL.song_id = STG_SONG.SONG_ID
AND SONG_TBL.artist_id = STG_SONG.ARTIST_ID
AND SONG_TBL.duration = CAST(STG_SONG.DURATION AS DOUBLE PRECISION);

INSERT INTO {schema_sparkify_name}.{song_table}
SELECT DISTINCT 
TRIM(song_id),
TRIM(artist_id),
CAST(duration AS DOUBLE PRECISION) AS DURATION,
TRIM(title) AS SONG_TITLE,
CAST(CASE WHEN year = '0' THEN '-9999' ELSE year END AS INTEGER) AS SONG_YEAR
FROM {schema_staging_name}.{stg_song_table};


""".format(**dict_DDL_schemas,**dict_DDL_tables)



insert_artist_table = """
BEGIN TRANSACTION;

DELETE FROM {schema_sparkify_name}.{artist_table}
USING {schema_staging_name}.{stg_song_table} AS STG_SONG
WHERE ARTIST_TBL.ARTIST_ID = STG_SONG.artist_id;

INSERT INTO {schema_sparkify_name}.{artist_table}
SELECT 
ARTIST_ID,
ARTIST_NAME,
ARTIST_LOCATION,
CAST(ARTIST_LATITUDE AS DOUBLE PRECISION),
CAST(ARTIST_LONGITUTE AS DOUBLE PRECISION)
FROM 
(
    SELECT DISTINCT 
    artist_id AS ARTIST_ID,
    artist_name AS ARTIST_NAME,
    ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY LEN(artist_name)) AS ROW_NUM,
    CASE WHEN trim(artist_location) = '' OR artist_location IS NULL THEN 'Unknown' ELSE artist_location END AS ARTIST_LOCATION,
    CASE WHEN trim(artist_latitude) = '' OR artist_latitude IS NULL THEN '-9999' ELSE artist_latitude END AS ARTIST_LATITUDE,
    CASE WHEN trim(artist_longitude) = '' OR artist_longitude IS NULL THEN '-9999' ELSE artist_longitude END AS ARTIST_LONGITUTE
    FROM {schema_staging_name}.{stg_song_table} AS STG_SONG
) 
WHERE ROW_NUM = 1;

""".format(**dict_DDL_schemas,**dict_DDL_tables)



insert_time_table = """
INSERT INTO {schema_sparkify_name}.{time_table}
SELECT DISTINCT 
CAST(ts AS BIGINT) AS TIME_ID
,TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS TS_FORMATTED
,EXTRACT(year FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS YEAR_COL
,EXTRACT(month FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS MONTH_COL
,EXTRACT(day FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS DAY_COL
,EXTRACT(hour FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS HOUR_COL
,CASE 
WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 0 THEN 'Sunday'
WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 1 THEN 'Monday'
WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 2 THEN 'Tuesday'
WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 3 THEN 'Wednesday'
WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 4 THEN 'Thursday'
WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 5 THEN 'Friday'
WHEN EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) = 6 THEN 'Saturday'
END AS DAY_NAME
FROM {schema_staging_name}.{stg_log_table} AS stg_log
LEFT OUTER JOIN {schema_sparkify_name}.{time_table} AS TIME_TBL
ON CAST(stg_log.ts AS BIGINT) = TIME_TBL.TIME_ID
WHERE TIME_TBL.TIME_ID IS NULL;
""".format(**dict_DDL_schemas,**dict_DDL_tables)



# A List of Drop Queries to drop the Tables in case they already exist
drop_table_queries = [drop_staging_log_table,drop_staging_songs_table,drop_songplay_table,drop_user_table,drop_song_table,drop_artist_table,drop_time_table]

# A Dictionary to hold all the DDLs of all Tables
create_table_queries = {
    dict_DDL_tables.get('stg_log_table'):create_staging_log_table,
    dict_DDL_tables.get('stg_song_table'):create_staging_songs_table,
    dict_DDL_tables.get('songplay_table'):create_songplay_table,
    dict_DDL_tables.get('user_table'):create_user_table,
    dict_DDL_tables.get('song_table'):create_song_table,
    dict_DDL_tables.get('artist_table'):create_artist_table,
    dict_DDL_tables.get('time_table'):create_time_table,
}

# A Dictionary to Copy the Data from S3 Buckets to the redshift cluster 
copy_table_queries = {
    dict_DDL_tables.get('stg_log_table') : copy_staging_log,
    dict_DDL_tables.get('stg_song_table') : copy_staging_songs
}


# A Dictionary to contain all the Statements for Data Loading for each Table
insert_table_queries = {
    dict_DDL_tables.get('songplay_table') : insert_songplay_table,
    dict_DDL_tables.get('user_table') : insert_user_table,
    dict_DDL_tables.get('song_table') : insert_song_table,
    dict_DDL_tables.get('artist_table') : insert_artist_table,
    dict_DDL_tables.get('time_table') : insert_time_table
}
