import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp,current_date

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

from datetime import datetime,timedelta

current_timestamp_python = datetime.now()

year = current_timestamp_python.year
month = current_timestamp_python.month
day = current_timestamp_python.day
source_data_path = f"s3://prayantdatasetbucket/myspotifyAPIData/source_raw_data_json/year={year}/month={month}/day={day}/*.json"
transformed_data_path = f"s3://prayantdatasetbucket/myspotifyAPIData/source_raw_data_parquetFiles/year={year}/month={month}/day={day}/"

df = spark.read.format('json').option('multiline',True).load(source_data_path)
from pyspark.sql.functions import lit,col,explode
df.show(1)
df.printSchema()




#Transformations
df = df.withColumn('tracks',explode('tracks.items'))\
.withColumn('data_last_refreshed',lit(datetime.now()-timedelta(days=0) ))\
.withColumn('artists_exploded',explode('tracks.track.artists'))

df = df.select(
    'data_last_refreshed',
    col('followers.total').alias('followers'),
    'tracks.added_at',
    # 'tracks.added_by',
    # 'tracks.track.album',
   col('artists_exploded.id').alias('artist_id'),
       col('artists_exploded.name').alias('artist_name'),
    # 'tracks.track.available_markets',
    'tracks.track.disc_number',
    'tracks.track.duration_ms',
    # 'tracks.track.episode',
    col('tracks.track.id').alias('track_id'),
    col('tracks.track.name').alias('track_name'),
    'tracks.track.popularity',
    'tracks.track.track_number',
    # 'tracks.track.type',
    # 'tracks.track.uri',
    
#         'tracks.track.album',
    'tracks.track.album.album_type',
        # 'tracks.track.album.id',
        col('tracks.track.album.name').alias('album_name'),
        'tracks.track.album.release_date',
        'tracks.track.album.total_tracks'    
)

df.write.format('parquet').mode('append').save(transformed_data_path)

job.commit()