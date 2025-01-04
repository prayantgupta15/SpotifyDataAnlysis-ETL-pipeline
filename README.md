**Introduction**
An ETL (Extract, Transform, Load) pipeline to analyse the Spotify Data and get the key abstracts like top artist, most popular tracks, top genres etc. 
This pipeline has been architectured leveraging AWS Cloud Services like Lambda, Glue crawlers, data catalog, Athena, Glue pyspark jobs, Lake Formation, Glue workflow, Managed Workflow for Apache Airflow (MWAA) etc.

**Architecture:**
![SpotifyProject](https://github.com/user-attachments/assets/1ad8b12a-3cde-4320-ad25-b69fa7421b4d)

**Working:**
Extraction:
  - Data is being fetched from REST Spotify APIs, via lambda function which stores data in S3 bucket.
  - This lambda function is triggered via an Airflow DAG everyday at 07:00 AM UTC or 12:30 PM IST🕧 since playlist is refreshed daily at the same time.
  - Data is stored in hive partitions format. For e.g. ../year=2025/month=1/day=3 and in JSON format
  - As soon as file is uploaded into S3 bucket via Lambda function, an S3 event is triggered (produced) which is consumed by EventBridge and triggers a Glue workflow.
  - Triggered Glue workflow has 2 Glue jobs: spotifyETLAthenaQueriesJob and spotifyETLAthenaQueriesJob

**Transform**
  - convertingJSONToParquetJob: will read current days data and convert JSON format to Parquet format and write to different folder in same manner.
  - This job transforms few columns and add an update_ts column.
  - After succesfully completion of this job, triggers next Glue job in Glue workflow 'spotifyETLAthenaQueriesJob'

Load:
  -  spotifyETLAthenaQueriesJob: Run 2 Athena queries.
  -  One is to add the new partition (year/month/day) in Stage table
  -  Another to MERGE iceberg table with Stage table to insert new added records (current day's records).

**Set Up:**
  - Stage table is created using Crawler for the first time.
  - Iceberg table is created using CREATE TABLE WITH (<options>) AS <SELECT query>(CTAS)  query on Stage table.
  - Enabled S3 EventBridge on S3 bucket.
  - Created a EventBridge rule for particular JSON files path only which will trigger Glue workflow.
  - MSCK Repair Table command is used to load/add new partitions.

**Analysis**

Quicksight:
<img width="727" alt="QuicksightDashboard" src="https://github.com/user-attachments/assets/5504f85c-830d-471b-9d3b-60d8f34bc2d3" />




