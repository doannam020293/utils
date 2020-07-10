import findspark
findspark.init()
from pyspark.sql import SparkSession
sparkSession = SparkSession.builder.appName("example-pyspark-hdfs").getOrCreate()
