import sys
import boto3
import botocore
import time


def execute_query(query):
 try:
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation':'s3://athena-logs-ap-south-1-prayant/',
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3',
                },
                'AclConfiguration': {
                    'S3AclOption': 'BUCKET_OWNER_FULL_CONTROL'
                }
            
        },
        WorkGroup='primary')
        
    queryId = ''    
    queryId = response['QueryExecutionId']
    status = 'RUNNING'
    while status=='RUNNING':
        time.sleep(1)
        response = athena_client.get_query_execution(QueryExecutionId= queryId)
        status = response['QueryExecution']['Status']['State']
    if status=='FAILED':
        reason = response['QueryExecution']['Status']['StateChangeReason']
        raise Exception(f'Query Failed {reason}')
    print(f"Execution completed: {status} {queryId}")
    
    
 except botocore.exceptions.ClientError as error:
    print("error agya")
    raise error
    
    
# Following queries are saved in Athena:
# f73cf9a8-ee72-4dc3-b076-56bb59952c9d finalIcebergTable
# 5151c5af-517f-4054-90b0-c7ea6a6a6d4b StagetableQueries


# TODO: 1. Load partitions in Stage table 
query = 'MSCK REPAIR TABLE sptifydb.stagingsource_raw_data_parquetfiles;'
execute_query(query)


# TODO: 2. MERGE Stage table to Iceberg table
query = """
    merge into sptifydb.spotifyIcebergMORathena  a
using 
(select cast(data_last_refreshed as TIMESTAMP(6)) data_last_refreshed_ts6,* from sptifydb.stageView) AS cte
on a.data_last_refreshed_ts6 = cte.data_last_refreshed_ts6 and a.unique_no = cte.unique_no
when MATCHED
        THEN UPDATE SET 
        -- data_last_refreshed='hogya'
        data_last_refreshed_ts6 = cte.data_last_refreshed_ts6
when not matched then insert values (
cte.data_last_refreshed_ts6,
cte.unique_no,
cte.data_last_refreshed,
cte.followers,
cte.added_at,
cte.artist_id,
cte.artist_name,
cte.disc_number,
cte.duration_ms,
cte.track_id,
cte.track_name,
cte.popularity,
cte.track_number,
cte.album_type,
cte.album_name,
cte.release_date,
cte.total_tracks,
cte.year,cte.month ,cte.day
); """
    
execute_query(query)
 