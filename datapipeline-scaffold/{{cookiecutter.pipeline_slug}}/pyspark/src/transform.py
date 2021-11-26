"""
A boilerplate Spark job for {{cookiecutter.pipeline_name}}
"""
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    # ...
    spark.stop()
