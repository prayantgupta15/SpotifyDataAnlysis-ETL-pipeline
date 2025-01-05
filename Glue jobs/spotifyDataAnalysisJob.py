import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import count,max,col,date_format,collect_list,dense_rank,min,avg,round
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set('spark.sql.iceberg.handle-timestamp-without-timezone',True)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark.read.format('iceberg').load('glue_catalog.sptifydb.spotifyicebergmorathena')
df.show()

# ====================================================================================
# Top Artist by number of tracks for each day
print("Top Artist by number of tracks for each day:")
cteDF = df.groupBy('data_last_refreshed_ts6','artist_name').agg(count('track_name').alias('NoOfTracksFeatured'))
maxDF = cteDF.groupBy(col('data_last_refreshed_ts6').alias('dt_ts6')).agg(max('NoOfTracksFeatured').alias('maxRecords'))

print("Daily top artist by number of tracks featured:")
ansDF = cteDF.join(maxDF,
    (cteDF['NoOfTracksFeatured'] == maxDF['maxRecords']) &
    (cteDF['data_last_refreshed_ts6'] == maxDF['dt_ts6'])
).select(col('data_last_refreshed_ts6').alias('dt'),'artist_name','NoOfTracksFeatured')\
.withColumn('dt',date_format(col('dt'),'y-MM-d'))\
.orderBy('data_last_refreshed_ts6')

ansDF.show()
# ====================================================================================

# ====================================================================================
# daily top album (for which max no of tracks appeared)
print("Daiy top albums:")
albumDF = df.select('data_last_refreshed_ts6','album_name','track_name').distinct().groupBy(date_format('data_last_refreshed_ts6','y-MM-d').alias('dt'),'album_name').agg(count('track_name').alias('tracksFeatured'))

from pyspark.sql.window import Window
wd = Window.partitionBy('dt').orderBy(col('tracksFeatured').desc())

albumDF.withColumn('rnk',dense_rank().over(wd)).filter(col('rnk')==1)\
.orderBy('dt','album_name',col('tracksFeatured').desc())\
.show()

# ====================================================================================
print("Duration statisticts day wise")

df.select(date_format('data_last_refreshed_ts6','y-MM-d').alias('dt'),'track_id',round(col('duration_ms')/60000,2).alias('duration')).distinct()\
.groupBy('dt').agg(
    max('duration').alias('maxDuration'),
    min('duration').alias('minDuration'),
    avg('duration').alias('avgDuration')
    ).show()

# ====================================================================================


# Top artist by weekly. By most number of tracks featured in playlist
# Most featured albums- max no of times they appeared in playlist


job.commit()