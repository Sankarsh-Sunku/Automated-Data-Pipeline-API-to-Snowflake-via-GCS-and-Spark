use role accountadmin;
create database news_api;
use news_api;

CREATE FILE FORMAT parquet_format TYPE=parquet;

-- create a storage integration and stage using the storage integration
create or replace storage integration news_api_raw_data
type = external_stage
storage_provider = gcs
enabled = true
storage_allowed_locations = ('gcs://snowflake_projects_sa/news_data_analysis/parquet_files');

-- create a storage integration for some author date and stage using the storage integration
create or replace storage integration author_data
type = external_stage
storage_provider = gcs
enabled = true
storage_allowed_locations = ('gcs://snowflake_projects_sa/news_data_analysis/destination');

-- create stage for both 
create or replace stage gcs_raw_data_stage
url = 'gcs://snowflake_projects_sa/news_data_analysis/parquet_files'
storage_integration = news_api_raw_data
file_format = (type = 'parquet');

create or replace stage gcs_author_date
url = 'gcs://snowflake_projects_sa/news_data_analysis/destination'
storage_integration = author_data
file_format = (type = 'parquet');

show stages;

desc storage integration news_api_raw_data; -- xxxxxxx00@gcpuscentral1-1dfa.iam.gserviceaccount.com
desc storage integration author_data; -- xxxxxxx00@gcpuscentral1-1dfa.iam.gserviceaccount.com