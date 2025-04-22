
**Introduction**

An ETL (Extract, Transform, Load) pipeline to analyse the Spotify Data and get the key abstracts like top artist, most popular tracks, top genres etc. 
This pipeline has been architectured leveraging AWS Cloud Services like Lambda, Glue crawlers, data catalog, Athena, Glue pyspark jobs, Lake Formation, Glue workflow, Managed Workflow for Apache Airflow (MWAA) etc.


**Final dashboard:**

Following are the few snippets of the Quicksight dashboard implemented on the Glue catalog table ingested by the ETL pipeline (design discussed below).

1. Check out the trends for artists selectively.
- **Ariana Grande:**
<img width="800" alt="Ariana Grande Performance Metrics" src="https://github.com/user-attachments/assets/24f08291-b8eb-4120-8e7d-04fe1b5d17b0" />

- **Lady Gaga:**
<img width="808" alt="Lady Gaga Performance Metrics" src="https://github.com/user-attachments/assets/d3a8084a-95a5-4d77-9b8c-0b27c9f3b5ff" />

------------------------------------------------------------------------------------------------------------------------

2. Check out the artists trend based on the number of distinct tracks featured in Global 100 playlist on Spotify:
<img width="500" alt="ArtistsTrend" src="https://github.com/user-attachments/assets/e0ebbde7-7000-41a9-a612-a9b28e942319" />

------------------------------------------------------------------------------------------------------------------------

3. Check out the tracks trend based on the number of times each featured in Global 100 playlist on Spotify:
<img width="500" alt="TracksTrends" src="https://github.com/user-attachments/assets/207f5b6d-bad4-4285-85a4-4e644458d5be" />

------------------------------------------------------------------------------------------------------------------------


**Architecture:**
![SpotifyProject](https://github.com/user-attachments/assets/1ad8b12a-3cde-4320-ad25-b69fa7421b4d)

**Working:**

**Extraction:**
  - Data is being fetched from REST Spotify APIs, via lambda function which stores data in S3 bucket.
  - This lambda function is triggered via an Airflow DAG everyday at 07:00 AM UTC or 12:30 PM IST🕧 since playlist is refreshed daily at the same time.
  - Data is stored in hive partitions format. For e.g. ../year=2025/month=1/day=3 and in JSON format
  - As soon as file is uploaded into S3 bucket via Lambda function, an S3 event is triggered (produced) which is consumed by EventBridge and triggers a Glue workflow.
  - Triggered Glue workflow has 2 Glue jobs: spotifyETLAthenaQueriesJob and spotifyETLAthenaQueriesJob

**Transform**
  - convertingJSONToParquetJob: will read current day data and convert JSON format to Parquet format and write to different folder in same manner.
  - This job transforms few columns and add an update_ts column.
  - After succesfully completion of this job, triggers next Glue job in Glue workflow 'spotifyETLAthenaQueriesJob'

**Load:**
  -  spotifyETLAthenaQueriesJob: Run 2 Athena queries.
  -  One is to add the new partition (year/month/day) in Stage table
  -  Another to MERGE iceberg table with Stage table to insert new added records (current day's records).

**Set Up:**
  - Stage table is created using Crawler for the first time.
  - Iceberg table is created using CREATE TABLE WITH (<options>) AS <SELECT query>(CTAS)  query on Stage table.
  - Enabled S3 EventBridge on S3 bucket.
  - Created a EventBridge rule for particular JSON files path only which will trigger Glue workflow.
  - MSCK Repair Table command is used to load/add new partitions.


