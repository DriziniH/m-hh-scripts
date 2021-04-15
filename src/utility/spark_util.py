
from pyspark.sql import SparkSession
import os

# Silence spark logger
import logging

def create_spark_aws_session():
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=com.amazonaws:aws-java-sdk-bundle:1.11.271,org.apache.hadoop:hadoop-aws:3.1.1 pyspark-shell"

    s_logger = logging.getLogger('py4j.java_gateway')
    s_logger.setLevel(logging.ERROR)

    # Create spark session
    spark = SparkSession.builder.appName(
        'Python Spark').config("spark.driver.memory", "2g").getOrCreate()
                                                                                                                                                                                                                                        
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    return spark


def rename_columns(expr, df):
    for column in df.columns:
        name = column[column.find('(')+1:column.find(')')]
        df = df.withColumnRenamed(f'{expr}({name})', name)
    return df