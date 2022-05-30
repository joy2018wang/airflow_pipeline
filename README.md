## Build an airflow pipeline for songs and playlist data
There are 8 tasks (except start and end opeartor) and 4 stages
with the below dependence


stage_events_to_redshift >> load_songplays_table  <br />
stage_songs_to_redshift >> load_songplays_table  <br />


load_songplays_table >> load_song_dimension_table  <br />
load_songplays_table >> load_user_dimension_table  <br />
load_songplays_table >> load_artist_dimension_table  <br />
load_songplays_table >> load_time_dimension_table  <br />

load_song_dimension_table >> run_quality_checks  <br />
load_user_dimension_table >> run_quality_checks  <br />
load_artist_dimension_table >> run_quality_checks <br />
load_time_dimension_table >> run_quality_checks  <br />

### Stage 0 (prepare):
Run create_tables.sql on AWS redshift or create a separate airflow dag to create tables
### Stage 1:
--Load event logs JSON files to redshift staging table staging_event
--Load song JSON files to redshfit staging table staging_song
### Stage 2:
Create Fact table songplays
### Stage 3:
Create Dimension tables users, time, songs, and artists
### Stage 4:
Run quality checks

## Operators
### StageToRedshiftOperator in stage_redshift.py
Execute COPY command to copy JSON files to RedShift staging tables
### LoadFactOperator in load_fact.py, stage 1
Insert tables from staging tables; stage 2 and 3
### DataQualityOperator in data_quality.py
Check data quanlity; stage 4 
