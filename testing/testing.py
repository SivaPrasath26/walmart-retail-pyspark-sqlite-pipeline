import os
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = r"C:\hadoop"

spark = SparkSession.builder \
    .appName("TestSpark") \
    .master("local[*]") \
    .getOrCreate()

print("Spark version:", spark.version)
print("Hadoop version:", spark._jsc.hadoopConfiguration().get("hadoop.version"))

spark.stop()
