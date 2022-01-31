from pyspark.sql import functions as F
from pyspark.sql.types import *

aws_access_key_id = '<access_key_id>'
aws_secret_key = '<secret_key>'
kinesis_stream_name = 'flux-realtime-company'
kinesis_region = '<region>'

flogistix_schema = StructType() \
                  .add('@timestamp', TimestampType()) \
                  .add('neptune_id', StringType()) \
                  .add('pk', StringType()) \
                  .add ('register_id', LongType()) \
                  .add ('val', FloatType()) \
                  .add('label', StringType()) \
                  .add ('asset_id', LongType()) \
                  .add('tag_id', StringType()) \
                  .add('tag_name', StringType())

recs = spark.readStream \
            .format('kinesis') \
            .option('streamName', kinesis_stream_name) \
            .option('region', kinesis_region) \
            .option('initialPosition', 'TRIM_HORIZON') \
            .option('awsAccessKey', aws_access_key_id) \
            .option('awsSecretKey', aws_secret_key) \
            .load()

recs.selectExpr('cast (data as STRING) jsonData') \
    .select(F.from_json('jsonData', flogistix_schema).alias('tag')) \
    .select('tag.*') \
    .writeStream \
    .format('parquet') \
    .option('checkpointLocation', 'dbfs:/checkpointPath') \
    .start('dbfs:/outputPath/')

