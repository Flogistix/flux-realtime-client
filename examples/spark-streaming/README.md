## Flux Realtime Pyspark Streaming Client

This is the Flux Realtime example PySpark Streaming client. 
This client captures data straight from the Kinesis stream
and saves it a file store in parquet format.

To run this example, you will need a Spark cluster running
either a Jupyter Notebooks environment. Depending on your Spark Cluster
you may need to import additional libraries and make them available to
Spark. One example would be `spark-streaming-kinesis-asl_2.11`.

https://spark.apache.org/docs/latest/streaming-kinesis-integration.html