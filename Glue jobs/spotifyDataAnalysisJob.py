import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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


job.commit()