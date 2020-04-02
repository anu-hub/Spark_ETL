## Project Summary:

A music start up wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
As the data is growing they want to move from data warehouse to a data lake.
Data storage will be in S3 and the type of file is JSON.

This project involves building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimension & fact tables. Each of the five tables are written to partitioned parquet files in a separate analytics directory on S3.


### Project Implementation Steps:

1. Updated the config file to store the AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY: This is required to read the filed stored in Udacity S3 bucket.


2. Data Extraction: Log & Song json file data read and stored in spark data frames for further processing. 


3. Data Transformation & Load: 

Transformation is done in the data frames created after reading the json files. Filtering only "NextSong" to process log data.
UDF function is created to convert the ts unix epoch time to timestamp columns and extract other columns required to populate Time table.
After the transformation all the 5 tables are written to partitioned parquet files and stored in S3 with separate folders.

Fact Table: ```songplays (partitioned by year and month)```
Fact table is loaded from the spark temp view df_songplay_table and df_song_table joing artist name, song duration and song title.

Dimension Tables: users, ``songs (partitioned by year and artist)``, artists, ```time (partitioned by year and month)```

User & Time Dimenensions are populated from log json files
Songs & Artist dimentions are populated from song json files


3. ETL Flow

We are using one script for the ETL process and a configuration File

Configuration File: Stores the AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY

Output: Each of the five tables are written to parquet files in a separate analytics directory on S3. (Screenshots uploaded in my Project Workspace)

ETL script is used to create spark data frames from the song & log json files located in the AWS S3 storage. 
Analytics tables which are stored in S3 are created as parquet files from spark data frames and temp views after the transformations.


### Steps to execute the scripts:

1.  Execute etl.py script  - This will load, transform and insert the data into the tables

Main function calls the two other functions: process_song_data & process_log_data

process_log_data - Read json files from AWS S3 storage, created data frames, transform and load them back into S3. 
User & Time Dimenensions are populated with in this function.

process_song_data - Read json files from AWS S3 storage, created data frames, transform and load them back into S3. 
Songs & Artist Dimenensions, also the Songplays fact tables are populated with in this function.

### Steps to execute the script from AWS EMR clusters:

1. EMR Cluster creation
m5.xlarge
3 vCore, 16 GiB memory, EBS only storage
EBS Storage:64 GiB
2. Uploaded the etl.py script in S3 bucket, config file is not required. Modified the etl.py script to remove the config parser steps.
3. In EMR cluster created a new step to execute the script from S3 bucket and store the files in S3- bucket-s3-output.
4. Spark execution was very fast, S3 write sometimes takes little longer.



