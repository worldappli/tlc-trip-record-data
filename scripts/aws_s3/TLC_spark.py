import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import hour

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = "ruben-tlc-trip-record-data"

df = spark.read.parquet(
    f"s3://{BUCKET}/raw/"
)

df = df.withColumn(
    "hour",
    hour("tpep_pickup_datetime")
)

hourly = (
    df.groupBy("hour")
      .count()
      .orderBy("hour")
)

hourly.show()

df.write.mode("overwrite").parquet(
    f"s3://{BUCKET}/processed/"
)

job.commit()