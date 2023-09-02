
CREATE DATABASE IF NOT EXISTS "OPEN-METEO";
USE DATABASE "OPEN-METEO";
USE ROLE accountadmin;

-- create integration
CREATE OR REPLACE storage integration aws_s3_open_meteo_target_integration
TYPE = external_stage,
storage_provider = 'S3',
enabled = TRUE,
storage_aws_role_arn = '<role-arn>',
storage_allowed_locations = ('s3://<bucket-name>');

-- describe the created integration, from the output copy STORAGE_AWS_IAM_USER_ARN property_value and copy the value to the created aws role "AWS" section.
-- Also copy the STORAGE_AWS_EXTERNAL_ID and insert it to the "sts:ExternalId" 
DESC integration aws_s3_open_meteo_target_integration;

-- granting a role for the integration, it would be wise to create a new snowflake role and not use account admin.
GRANT USAGE ON integration aws_s3_open_meteo_target_integration TO ROLE accountadmin;

-- creating a file format 
CREATE OR REPLACE FILE FORMAT open_meteo_file_format
TYPE = 'json',
COMPRESSION = AUTO,
DATE_FORMAT = AUTO;

-- creating a stage
CREATE OR REPLACE STAGE open_meteo_stage
storage_integration = aws_s3_open_meteo_target_integration,
file_format = open_meteo_file_format
url = 's3://<bucket-name>';

-- testing if we can see the files in the stage. Note: there should be files present in the s3 bucket
LIST @open_meteo_stage;

-- create the table for the json data.
CREATE OR REPLACE TABLE weather_data (
    latitude NUMBER,
    longitude NUMBER,
    generationtime_ms NUMBER,
    utc_offset_seconds NUMBER,
    timezone STRING,
    timezone_abbreviation STRING,
    elevation NUMBER,
    current_weather VARIANT,
    "Timestamp" TIMESTAMP_NTZ,
    customer_id STRING,
    Location_name STRING
);


-- copy data from json to the created table, this is for testing purposes to see if the copy works, if it works we can create the pipe
COPY INTO WEATHER_DATA 
FROM '@open_meteo_stage/modified_1692976060-451911_xxx_Helsinki.json'
file_format = (format_name = open_meteo_file_format)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- review the inserted data
SELECT *
FROM WEATHER_DATA;


-- Create a pipe
CREATE OR REPLACE PIPE open_meteo_s3_snowpipe auto_ingest = TRUE AS
COPY INTO WEATHER_DATA
FROM @open_meteo_stage
file_format = (format_name = open_meteo_file_format)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- review pipes, copy the notification arn, this is needed for creating the SQS for s3
SHOW pipes;

-- review status of the pipe, it should be running. Now upload a file to s3
SELECT SYSTEM$PIPE_STATUS('OPEN_METEO_S3_SNOWPIPE')

-- Lets check the data ingested from the s3
SELECT *
FROM WEATHER_DATA;

-- Finally lets pause the snowpipe to avoid any extra costs
ALTER PIPE open_meteo_s3_snowpipe SET PIPE_EXECUTION_PAUSED = TRUE;

-- Double check that the pipe is paused
SELECT SYSTEM$PIPE_STATUS('OPEN_METEO_S3_SNOWPIPE');

-- The pipe can be started again with the following sql statement
ALTER PIPE open_meteo_s3_snowpipe SET PIPE_EXECUTION_PAUSED = FALSE;

