# Build a spakify data Lake
As the business grows, spartify wants to get business insights from current
users and their song plays, to expand market and customer retention rate. For
example, once we can find out which artists are most popular, we can try to add
more songs from the artist. For example, we can check what week days and what
hours the APP is mostly used.
  
Sparkify originally stores songs and events in JSON files. To get business
insights quickly and efficiently, this project builds a data warehouse on
Redshift cluster. Following the STAR schema, there is a FACT table and several
dimenions tables. 

# The data lake is built using SPARK from data stored in AWS S3

# Schema 

## In the FACT table songplay, 
- we include all ids for the interested dimensions such as
users, song, artist, session
- The table also includes the columns level, location, and user_agent which are important
features of users which are often used in business insight queries.
- start_time is important to study user behavior from the time perspective.


### The schema of the FACT table songplay
|    | table_name   | column_name   | is_nullable   | data_type                   |   character_maximum_length |
|---:|:-------------|:--------------|:--------------|:----------------------------|---------------------------:|
|  0 | songplay     | songplay_id   | NO            | integer                     |                        nan |
|  1 | songplay     | start_time    | NO            | timestamp without time zone |                        nan |
|  2 | songplay     | user_id       | NO            | integer                     |                        nan |
|  3 | songplay     | level         | YES           | character varying           |                         50 |
|  4 | songplay     | song_id       | YES           | character varying           |                        255 |
|  5 | songplay     | artist_id     | YES           | character varying           |                        255 |
|  6 | songplay     | session_id    | YES           | integer                     |                        nan |
|  7 | songplay     | location      | YES           | character varying           |                        255 |
|  8 | songplay     | user_agent    | NO            | character varying           |                       1000 |

## We have four dimension tables users, song, artist, time which can be joined to get more details if necessary

### The schema of dimension tables are list below:
- Table users

|    | table_name   | column_name   | is_nullable   | data_type         |   character_maximum_length |
|---:|:-------------|:--------------|:--------------|:------------------|---------------------------:|
|  0 | users        | user_id       | NO            | integer           |                        nan |
|  1 | users        | first_name    | YES           | character varying |                         50 |
|  2 | users        | last_name     | YES           | character varying |                         50 |
|  3 | users        | gender        | YES           | character varying |                          1 |
|  4 | users        | level         | YES           | character varying |                         50 |

- Table song

|    | table_name   | column_name   | is_nullable   | data_type         |   character_maximum_length |
|---:|:-------------|:--------------|:--------------|:------------------|---------------------------:|
|  0 | song         | song_id       | NO            | character varying |                        255 |
|  1 | song         | title         | NO            | character varying |                       1000 |
|  2 | song         | artist_id     | YES           | character varying |                        255 |
|  3 | song         | year          | YES           | integer           |                        nan |
|  4 | song         | duration      | NO            | numeric           |                        nan |

- Table artist

|    | table_name   | column_name   | is_nullable   | data_type         |   character_maximum_length |
|---:|:-------------|:--------------|:--------------|:------------------|---------------------------:|
|  0 | artist       | artist_id     | NO            | character varying |                        255 |
|  1 | artist       | name          | NO            | character varying |                        255 |
|  2 | artist       | location      | YES           | character varying |                       1000 |
|  3 | artist       | latitude      | YES           | double precision  |                        nan |
|  4 | artist       | longitude     | YES           | double precision  |                        nan |

- Table time

|    | table_name   | column_name   | is_nullable   | data_type                   | character_maximum_length   |
|---:|:-------------|:--------------|:--------------|:----------------------------|:---------------------------|
|  0 | time         | start_time    | NO            | timestamp without time zone |                            |
|  1 | time         | hour          | YES           | integer                     |                            |
|  2 | time         | day           | YES           | integer                     |                            |
|  3 | time         | week          | YES           | integer                     |                            |
|  4 | time         | month         | YES           | integer                     |                            |
|  5 | time         | year          | YES           | integer                     |                            |
|  6 | time         | weekday       | YES           | integer                     |                            |



# ETL pipeline
- Read song data from JSON files stored in AWS S3
- Create tables from song data
- Read log data from JSON files stored in AWS S3
- Create tables from log data and from song data



## How to run the Python scripts

### In cmdline
    
    ```bash
        python etl.py
    ```

### In Notebook
    
        ```python
        %run etl.py
        ```

## Files dictioinary
- S3 path/song_data  -- song information stored in .json files 
- S3 path/log_data   -- song play information stored in .json files
- etl.py          -- Copy/load and insert data to tables
- etl.ipynb      -- Check tables and test runs
- runETL.ipynb -- Run etl.py; and check rendering of README.md