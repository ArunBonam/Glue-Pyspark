import os
import signal
import subprocess
import boto3
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
os.environ["PYSPARK_PYTHON"]="/Users/arunbonam/.pyenv/versions/3.7.0/bin/python3.7"
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'


spark = SparkSession.builder.getOrCreate()
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "dummy-value")
hadoop_conf.set("fs.s3a.secret.key", "dummy-value")
hadoop_conf.set("fs.s3a.endpoint", "http://127.0.0.1:5000")
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
process = subprocess.Popen(
    "moto_server s3", stdout=subprocess.PIPE,
    shell=True, preexec_fn=os.setsid
)
s3_conn = boto3.resource(
    "s3", endpoint_url="http://127.0.0.1:5000"
)
s3_conn.create_bucket(Bucket="bucket")


# create a pyspark dataframe.
values = [("k1", 1), ("k2", 2)]
columns = ["key", "value"]
df = spark.createDataFrame(values, columns)
# write the dataframe as csv to s3.
df.write.csv("s3://bucket/source.csv")
# read the dataset from s3
df = spark.read.csv("s3://bucket/source.csv")
# assert df is a DataFrame
assert isinstance(df, DataFrame)
# shut down the moto server.
os.killpg(os.getpgid(process.pid), signal.SIGTERM)
print("yeeey, the test ran without errors.")